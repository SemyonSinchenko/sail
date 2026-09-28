use datafusion::datasource::empty::EmptyTable;
use sail_physical_optimizer::{PhysicalOptimizerOptions, get_physical_optimizers};

use super::*;

fn optimize(
    mut plan: Arc<dyn ExecutionPlan>,
    context: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    for optimizer in get_physical_optimizers(PhysicalOptimizerOptions::default()) {
        plan = optimizer.optimize(plan, context.copied_config().options())?;
    }
    Ok(plan)
}

fn routing() -> WorkerInputRouting {
    WorkerInputRouting::IntegerRange {
        column: "owner".into(),
        split_points: vec![1, 2],
    }
}

fn provider(inputs: Vec<Arc<dyn ExecutionPlan>>) -> WorkerTableProvider {
    let schema = inputs[0].schema();
    let mut descriptor = descriptor();
    descriptor.input_names = (0..inputs.len())
        .map(|index| format!("input{index}"))
        .collect();
    descriptor.input_schemas = inputs
        .iter()
        .map(|input| input.schema().as_ref().clone())
        .collect();
    descriptor.input_routing = inputs.iter().map(|_| Some(routing())).collect();
    descriptor.output_schema = schema.as_ref().clone();
    descriptor.partitions = 3;
    WorkerTableProvider {
        descriptor,
        inputs,
        _provider: Arc::new(EmptyTable::new(schema)),
    }
}

fn check_routes(
    plan: &Arc<dyn ExecutionPlan>,
    workers: &mut usize,
    routes: &mut usize,
    leaf_routes: &mut Vec<Arc<dyn ExecutionPlan>>,
) {
    if let Some(worker) = plan.downcast_ref::<WorkerExtensionExec>() {
        *workers += 1;
        for input in worker.children() {
            // Width alone is insufficient: an inner worker also reports P=3
            // after a missing exchange. Require the actual routed operator.
            assert!(
                input.is::<RepartitionExec>(),
                "declared worker routing disappeared: {input:?}"
            );
            assert!(matches!(
                input.properties().partitioning,
                Partitioning::Range(_)
            ));
            assert_eq!(input.properties().partitioning.partition_count(), 3);
        }
    }
    if let Some(exchange) = plan.downcast_ref::<RepartitionExec>() {
        *routes += 1;
        if !sail_common_datafusion::worker_extension::contains_worker_extension(exchange.input()) {
            leaf_routes.push(plan.clone());
        }
    }
    for child in plan.children() {
        check_routes(child, workers, routes, leaf_routes);
    }
}

#[tokio::test]
async fn nested_provider_scans_restore_every_route_before_each_full_optimization() -> Result<()> {
    let context =
        SessionContext::new_with_config(SessionConfig::default().with_target_partitions(8));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "owner",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0, 2, 0, 2]))])?;
    let source = context.read_batch(batch)?.create_physical_plan().await?;
    let mut current = provider(vec![source.clone(), source]);
    for depth in 1..=4 {
        // Repeat scanning the same provider too. Neither reconstruction nor a
        // previously optimized nested child may add duplicate exchanges.
        let mut final_plan = None;
        for _ in 0..2 {
            let plan = current.scan(&context.state(), None, &[], None).await?;
            let plan = optimize(plan, &context)?;
            let (mut workers, mut routes, mut leaf_routes) = (0, 0, vec![]);
            check_routes(&plan, &mut workers, &mut routes, &mut leaf_routes);
            assert_eq!(workers, depth);
            assert_eq!(routes, depth + 1, "exactly one exchange per declared input");
            assert_eq!(leaf_routes.len(), 2);
            for route in leaf_routes {
                let partitions = collect_partitioned(route, context.task_ctx()).await?;
                let counts = partitions
                    .iter()
                    .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>())
                    .collect::<Vec<_>>();
                assert_eq!(counts, [2, 0, 2]);
                for (partition, batches) in partitions.iter().enumerate() {
                    for batch in batches {
                        let owners = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| py_error("expected owner column"))?;
                        assert!(
                            owners
                                .values()
                                .iter()
                                .all(|&owner| owner == partition as i64)
                        );
                    }
                }
            }
            final_plan = Some(plan);
        }
        current = provider(vec![
            final_plan.ok_or_else(|| py_error("missing optimized plan"))?,
        ]);
    }
    Ok(())
}

#[tokio::test]
async fn restoring_a_matching_exchange_replaces_it_without_growth() -> Result<()> {
    let context = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "owner",
        DataType::Int64,
        false,
    )]));
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
    let explicit = route_input(input.clone(), Some(&routing()))?;
    let again = route_input(explicit.clone(), Some(&routing()))?;
    assert!(Arc::ptr_eq(&explicit, &again));
    let lowered = optimize(explicit, &context)?;
    assert!(lowered.is::<RepartitionExec>());
    let restored = route_input(lowered, Some(&routing()))?;
    assert!(restored.is::<ExplicitRepartitionExec>());
    assert_eq!(restored.children().len(), 1);
    assert!(restored.children()[0].is::<EmptyExec>());
    let unchanged = route_input(input.clone(), None)?;
    assert!(Arc::ptr_eq(&input, &unchanged));
    Ok(())
}

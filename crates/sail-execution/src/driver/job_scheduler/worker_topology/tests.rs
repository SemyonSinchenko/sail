use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;

use super::*;
use crate::job_graph::JobGraphOptions;
use crate::shuffle::ShuffleCompression;

fn empty(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(partitions))
}

fn exchange(
    input: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::RoundRobinBatch(partitions),
    )?))
}

fn native(
    input: Arc<dyn ExecutionPlan>,
    partitions: usize,
    package: &str,
    operation: &str,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let schema = input.schema().as_ref().clone();
    Ok(Arc::new(WorkerExtensionExec::new(
        WorkerDescriptor {
            package_identity: package.into(),
            type_url: "fixture.worker".into(),
            operation_id: operation.into(),
            payload: vec![],
            input_names: vec!["input".into()],
            input_schemas: vec![schema.clone()],
            input_routing: vec![None],
            output_schema: schema,
            partitions,
        },
        vec![input],
    )?))
}

fn graph(plan: Arc<dyn ExecutionPlan>, blocking: bool) -> ExecutionResult<JobGraph> {
    JobGraph::try_new(
        plan,
        JobGraphOptions {
            shuffle_backend: if blocking {
                ShuffleBackendKind::Storage {
                    path: None,
                    max_file_size: 1,
                    compression: ShuffleCompression::None,
                }
            } else {
                ShuffleBackendKind::Flight {
                    compression: ShuffleCompression::None,
                }
            },
        },
    )
}

fn validate_graph(graph: &JobGraph) -> ExecutionResult<()> {
    validate(graph, &JobTopology::try_new(graph)?)
}

#[test]
fn equal_width_operations_preserve_each_partition_in_actual_task_sets() -> ExecutionResult<()> {
    for width in [2, 3, 16] {
        let first = native(empty(width), width, "package", "operation")?;
        let last = native(exchange(first, width)?, width, "package", "operation")?;
        let graph = graph(last, false)?;
        assert_eq!(graph.stages().len(), 2);
        let topology = JobTopology::try_new(&graph)?;
        assert_eq!(topology.regions.len(), 1);
        validate(&graph, &topology)?;
        let mut job = JobDescriptor::new(
            graph,
            topology,
            JobState::Draining,
            Arc::new(TaskContext::default()),
        );
        for stage in &mut job.stages {
            for task in &mut stage.tasks {
                task.attempts.push(TaskAttemptDescriptor {
                    state: TaskState::Created,
                    messages: vec![],
                    cause: None,
                    job_output_fetched: false,
                });
            }
        }
        let scheduled =
            JobScheduler::build_task_region(JobId::from(1), &job, &job.topology.regions[0]);
        assert_eq!(scheduled.tasks.len(), width);
        for (placement, set) in scheduled.tasks {
            assert_eq!(placement, TaskPlacement::Worker);
            assert_eq!(set.entries.len(), 2);
            assert_eq!(set.entries[0].key.partition, set.entries[1].key.partition);
        }
    }
    Ok(())
}

#[test]
fn intervening_scalar_or_wider_stage_cannot_move_shared_owners() -> ExecutionResult<()> {
    for intermediate in [1, 3] {
        let first = native(empty(2), 2, "package", "operation")?;
        let middle = exchange(exchange(first, intermediate)?, 2)?;
        let last = native(middle, 2, "package", "operation")?;
        let graph = graph(last, false)?;
        let widths = graph
            .stages()
            .iter()
            .map(|stage| stage.plan.output_partitioning().partition_count())
            .collect::<Vec<_>>();
        assert_eq!(
            widths,
            [2, intermediate, 2],
            "fixture must exercise an intervening stage offset"
        );
        let error = validate_graph(&graph).err().ok_or_else(|| {
            ExecutionError::InternalError("misaligned operation unexpectedly admitted".into())
        })?;
        assert!(
            error
                .to_string()
                .contains("cannot preserve partition ownership")
        );
    }
    Ok(())
}

#[test]
fn independent_package_or_operation_does_not_require_shared_placement() -> ExecutionResult<()> {
    for (package, operation) in [
        ("other-package", "operation"),
        ("package", "other-operation"),
    ] {
        let first = native(empty(2), 2, "package", "operation")?;
        let last = native(exchange(exchange(first, 1)?, 2)?, 2, package, operation)?;
        validate_graph(&graph(last, false)?)?;
    }
    Ok(())
}

#[test]
fn state_cannot_cross_materialized_regions_or_change_width() -> ExecutionResult<()> {
    for (width, blocking) in [(2, true), (3, false)] {
        let first = native(empty(2), 2, "package", "operation")?;
        let last = native(exchange(first, width)?, width, "package", "operation")?;
        assert!(validate_graph(&graph(last, blocking)?).is_err());
    }
    Ok(())
}

#[test]
fn every_nested_occurrence_must_match_the_enclosing_stage_width() -> ExecutionResult<()> {
    let first = native(empty(2), 2, "package", "operation")?;
    let nested = native(first.clone(), 3, "other-package", "independent")?;
    let nested = graph(nested, false)?;
    assert_eq!(nested.stages().len(), 1);
    assert!(validate_graph(&nested).is_err());

    let union = UnionExec::try_new(vec![first, empty(2)])?;
    assert!(validate_graph(&graph(union, false)?).is_err());
    Ok(())
}

#[test]
fn matching_nested_and_forward_sliced_owners_are_allowed() -> ExecutionResult<()> {
    let first = native(empty(3), 3, "package", "operation")?;
    let nested = native(first, 3, "package", "operation")?;
    let graph = graph(nested, false)?;
    let topology = JobTopology::try_new(&graph)?;
    assert_eq!(graph.stages().len(), 1);
    assert_eq!(
        topology.regions.len(),
        3,
        "forward-only topology is sliced by partition"
    );
    validate(&graph, &topology)?;
    Ok(())
}

#[test]
fn ordinary_jobs_keep_mixed_widths_and_blocking_regions() -> ExecutionResult<()> {
    for blocking in [false, true] {
        let plan = exchange(exchange(empty(2), 1)?, 3)?;
        let graph = graph(plan, blocking)?;
        let widths = graph
            .stages()
            .iter()
            .map(|stage| stage.plan.output_partitioning().partition_count())
            .collect::<Vec<_>>();
        // Storage adds a blocking collector after each exchange; pin those
        // real extra stages instead of assuming the Flight fixture's shape.
        let expected = if blocking {
            vec![2, 1, 1, 3, 3]
        } else {
            vec![2, 1, 3]
        };
        assert_eq!(widths, expected);
        assert_eq!(
            graph
                .stages()
                .iter()
                .filter(|stage| matches!(stage.mode, OutputMode::Blocking))
                .count(),
            if blocking { 2 } else { 0 }
        );
        validate_graph(&graph)?;
    }
    Ok(())
}

#[test]
fn two_input_sources_reveal_actual_operation_bucket_offsets() -> ExecutionResult<()> {
    let width = 3;
    let input = exchange(empty(1), width)?;
    let schema = input.schema().as_ref().clone();
    let first: Arc<dyn ExecutionPlan> = Arc::new(WorkerExtensionExec::new(
        WorkerDescriptor {
            package_identity: "package".into(),
            type_url: "fixture.worker".into(),
            operation_id: "operation".into(),
            payload: vec![],
            input_names: vec!["vertices".into(), "edges".into()],
            input_schemas: vec![schema.clone(), schema.clone()],
            input_routing: vec![None, None],
            output_schema: schema,
            partitions: width,
        },
        vec![input, exchange(empty(1), width)?],
    )?);
    let round = native(exchange(first, width)?, width, "package", "operation")?;
    let result = native(exchange(round, width)?, width, "package", "operation")?;
    let graph = graph(result, false)?;
    assert_eq!(
        graph
            .stages()
            .iter()
            .map(|stage| stage.plan.output_partitioning().partition_count())
            .collect::<Vec<_>>(),
        [1, 1, 3, 3, 3]
    );
    let topology = JobTopology::try_new(&graph)?;
    assert_eq!(topology.regions.len(), 1);
    let order = topology.regions[0]
        .tasks
        .iter()
        .map(|task| task.stage)
        .collect::<IndexSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(order, [0, 2, 1, 3, 4]);
    let groups = build_stage_groups(&graph, &topology.regions[0]);
    assert_eq!(groups.len(), 1);
    let group = &groups[&StageGroupKey {
        placement: TaskPlacement::Worker,
        group: String::new(),
    }];
    let buckets = (0..5)
        .map(|stage| group.bucket(stage, 0))
        .collect::<Vec<_>>();
    assert_eq!(buckets, [0, 1, 1, 2, 2]);
    assert!(validate(&graph, &topology).is_err());
    Ok(())
}

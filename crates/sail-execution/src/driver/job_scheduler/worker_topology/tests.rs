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
fn ordinary_scalar_or_wider_stage_cannot_shift_the_operation_group() -> ExecutionResult<()> {
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
        assert!(graph.stages()[1].group.is_empty());
        assert_eq!(graph.stages()[0].group, graph.stages()[2].group);
        let topology = JobTopology::try_new(&graph)?;
        validate(&graph, &topology)?;
        let groups = build_stage_groups(&graph, &topology.regions[0]);
        let group = &groups[&StageGroupKey {
            placement: TaskPlacement::Worker,
            group: graph.stages()[0].group.clone(),
        }];
        for partition in 0..2 {
            assert_eq!(group.bucket(0, partition), group.bucket(2, partition));
        }
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
        let graph = graph(last, false)?;
        assert_ne!(graph.stages()[0].group, graph.stages()[2].group);
        validate_graph(&graph)?;
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
        assert!(graph.stages().iter().all(|stage| stage.group.is_empty()));
        validate_graph(&graph)?;
    }
    Ok(())
}

fn two_input_operation(width: usize) -> ExecutionResult<JobGraph> {
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
    graph(result, false)
}

fn schedule(graph: JobGraph) -> ExecutionResult<TaskRegion> {
    let topology = JobTopology::try_new(&graph)?;
    validate(&graph, &topology)?;
    assert_eq!(topology.regions.len(), 1);
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
    Ok(JobScheduler::build_task_region(
        JobId::from(1),
        &job,
        &job.topology.regions[0],
    ))
}

#[test]
fn two_input_sources_preserve_actual_operation_buckets() -> ExecutionResult<()> {
    for width in [2, 3, 16] {
        let graph = two_input_operation(width)?;
        assert_eq!(
            graph
                .stages()
                .iter()
                .map(|stage| stage.plan.output_partitioning().partition_count())
                .collect::<Vec<_>>(),
            [1, 1, width, width, width],
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
        // Preserve the original counterexample: putting all these actual
        // stages in the default group shifts init relative to round/result.
        let default_group = StageGroup::new(
            order
                .iter()
                .map(|stage| {
                    (
                        *stage,
                        graph.stages()[*stage]
                            .plan
                            .output_partitioning()
                            .partition_count(),
                    )
                })
                .collect(),
        );
        assert_eq!(default_group.bucket(2, 0), 1);
        assert_eq!(default_group.bucket(3, 0), 2 % width);
        assert_ne!(default_group.bucket(2, 0), default_group.bucket(3, 0));

        let groups = build_stage_groups(&graph, &topology.regions[0]);
        assert_eq!(groups.len(), 2);
        assert!(graph.stages()[0].group.is_empty());
        assert!(graph.stages()[1].group.is_empty());
        assert_eq!(graph.stages()[2].group, "worker-extension:2");
        assert_eq!(graph.stages()[2].group, graph.stages()[3].group);
        assert_eq!(graph.stages()[2].group, graph.stages()[4].group);
        validate(&graph, &topology)?;
        let scheduled = schedule(graph)?;
        assert_eq!(scheduled.tasks.len(), width + 1);
        let mut native_partitions = vec![];
        for (placement, set) in scheduled.tasks {
            assert_eq!(placement, TaskPlacement::Worker);
            if set.entries[0].key.stage >= 2 {
                assert_eq!(set.entries.len(), 3);
                let partition = set.entries[0].key.partition;
                assert!(
                    set.entries
                        .iter()
                        .all(|entry| entry.key.partition == partition)
                );
                assert_eq!(
                    set.entries
                        .iter()
                        .map(|entry| entry.key.stage)
                        .collect::<Vec<_>>(),
                    [2, 3, 4]
                );
                native_partitions.push(partition);
            } else {
                assert_eq!(
                    set.entries
                        .iter()
                        .map(|entry| entry.key.stage)
                        .collect::<Vec<_>>(),
                    [0, 1]
                );
            }
        }
        native_partitions.sort_unstable();
        assert_eq!(native_partitions, (0..width).collect::<Vec<_>>());
    }
    Ok(())
}

#[test]
fn co_occurring_operations_share_groups_transitively() -> ExecutionResult<()> {
    let first = native(empty(3), 3, "package", "a")?;
    let ab = native(
        native(exchange(first, 3)?, 3, "package", "a")?,
        3,
        "package",
        "b",
    )?;
    let bc = native(
        native(exchange(ab, 3)?, 3, "package", "b")?,
        3,
        "package",
        "c",
    )?;
    let last = native(exchange(bc, 3)?, 3, "package", "c")?;
    let independent = native(exchange(last, 3)?, 3, "package", "independent")?;
    let graph = graph(independent, false)?;
    assert_eq!(graph.stages().len(), 5);
    assert!(
        graph.stages()[..4]
            .iter()
            .all(|stage| stage.group == "worker-extension:0")
    );
    assert_eq!(graph.stages()[4].group, "worker-extension:4");
    validate_graph(&graph)?;
    let scheduled = schedule(graph)?;
    assert_eq!(scheduled.tasks.len(), 6);
    for (_, set) in scheduled.tasks {
        let partition = set.entries[0].key.partition;
        assert!(
            set.entries
                .iter()
                .all(|entry| entry.key.partition == partition)
        );
        assert_eq!(
            set.entries.len(),
            if set.entries[0].key.stage < 4 { 4 } else { 1 }
        );
    }
    Ok(())
}

#[test]
fn operation_groups_use_existing_slot_capacity_admission() -> ExecutionResult<()> {
    use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
    use crate::id::WorkerId;

    let scheduled = schedule(two_input_operation(3)?)?;
    assert_eq!(scheduled.tasks.len(), 4);
    let mut insufficient = TaskAssigner::new(TaskAssignerOptions::new(1, 3));
    let error = insufficient
        .enqueue_tasks(&scheduled)
        .err()
        .ok_or_else(|| {
            ExecutionError::InternalError("insufficient slots unexpectedly admitted".into())
        })?;
    assert!(error.to_string().contains("requires 4 worker task slots"));
    let mut sufficient = TaskAssigner::new(TaskAssignerOptions::new(2, 2));
    sufficient.activate_worker(WorkerId::from(1));
    sufficient.activate_worker(WorkerId::from(2));
    sufficient.enqueue_tasks(&scheduled)?;
    let assignments = sufficient.assign_tasks();
    assert_eq!(assignments.len(), 4);
    let mut locations = HashMap::new();
    for assignment in assignments {
        let TaskAssignment::Worker { worker_id, slot } = assignment.assignment else {
            return Err(ExecutionError::InternalError(
                "worker region assigned to driver".into(),
            ));
        };
        for entry in assignment.set.entries {
            locations.insert((entry.key.stage, entry.key.partition), (worker_id, slot));
        }
    }
    for partition in 0..3 {
        assert_eq!(locations[&(2, partition)], locations[&(3, partition)]);
        assert_eq!(locations[&(2, partition)], locations[&(4, partition)]);
    }
    Ok(())
}

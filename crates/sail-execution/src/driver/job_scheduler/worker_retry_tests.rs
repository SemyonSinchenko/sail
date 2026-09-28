use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use sail_common_datafusion::worker_extension::{WorkerDescriptor, WorkerExtensionExec};

use super::*;
use crate::job_graph::JobGraphOptions;
use crate::shuffle::ShuffleCompression;

#[test]
fn worker_stateful_job_does_not_retry_even_an_upstream_non_native_region() -> ExecutionResult<()> {
    let schema = Arc::new(Schema::empty());
    let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
    let shuffled: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        empty,
        Partitioning::RoundRobinBatch(2),
    )?);
    let native: Arc<dyn ExecutionPlan> = Arc::new(WorkerExtensionExec::new(
        WorkerDescriptor {
            package_identity: "stateful-fixture@1".into(),
            type_url: "fixture/native".into(),
            operation_id: "operation".into(),
            payload: vec![],
            input_names: vec!["input".into()],
            input_schemas: vec![schema.as_ref().clone()],
            input_routing: vec![None],
            output_schema: schema.as_ref().clone(),
            partitions: 2,
        },
        vec![shuffled.clone()],
    )?);
    // Storage creates a materialization boundary, so the upstream ordinary
    // region really is separate from the native region. A one-region fixture
    // would only re-test region-local retry suppression.
    let backend = ShuffleBackendKind::Storage {
        path: None,
        max_file_size: 1,
        compression: ShuffleCompression::None,
    };
    let options = JobSchedulerOptions::for_retry_test(3, backend.clone());
    for (plan, stateful) in [(shuffled, false), (native, true)] {
        for canceled in [false, true] {
            let graph = JobGraph::try_new(
                plan.clone(),
                JobGraphOptions {
                    shuffle_backend: backend.clone(),
                },
            )?;
            let mut job = JobDescriptor::try_new(
                graph,
                JobState::Draining,
                Arc::new(TaskContext::default()),
            )?;
            let ordinary = job
                .topology
                .regions
                .iter()
                .position(|region| {
                    region.tasks.iter().all(|task| {
                        !contains_worker_extension(&job.graph.stages()[task.stage].plan)
                    })
                })
                .ok_or_else(|| {
                    ExecutionError::InternalError("fixture needs an ordinary retry region".into())
                })?;
            if stateful {
                assert!(job.topology.regions.len() > 1);
                assert!(
                    job.graph
                        .stages()
                        .iter()
                        .any(|stage| contains_worker_extension(&stage.plan))
                );
                assert!(
                    job.graph
                        .stages()
                        .iter()
                        .filter(|stage| contains_worker_extension(&stage.plan))
                        .all(|stage| stage.placement == TaskPlacement::Worker)
                );
            }
            for task in &job.topology.regions[ordinary].tasks {
                job.stages[task.stage].tasks[task.partition]
                    .attempts
                    .push(TaskAttemptDescriptor {
                        state: if canceled {
                            TaskState::Canceled
                        } else {
                            TaskState::Failed
                        },
                        messages: vec![],
                        cause: None,
                        job_output_fetched: false,
                    });
            }
            JobScheduler::update_task_regions(&mut job, &options);
            assert_eq!(
                matches!(job.regions[ordinary].state, TaskRegionState::Failed),
                stateful
            );
        }
    }
    Ok(())
}

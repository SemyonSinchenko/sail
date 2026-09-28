//! Output can fail before a task-status RPC arrives, especially after worker loss.
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use opentelemetry::logs::{AnyValue, LoggerProvider};
use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};

use super::*;
use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
use crate::id::WorkerId;
use crate::job_graph::JobGraphOptions;
use crate::shuffle::ShuffleCompression;

struct Fixture {
    scheduler: JobScheduler,
    assigner: TaskAssigner,
    keys: Vec<TaskKey>,
    events: InMemoryLogExporter,
    _provider: SdkLoggerProvider,
}

fn fixture() -> ExecutionResult<Fixture> {
    let events = InMemoryLogExporter::default();
    let provider = SdkLoggerProvider::builder()
        .with_simple_exporter(events.clone())
        .build();
    let reporter = SystemEventReporter::new(provider.logger("cleanup-test"));
    let backend = ShuffleBackendKind::Flight {
        compression: ShuffleCompression::None,
    };
    let plan = Arc::new(RepartitionExec::try_new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(2)),
        Partitioning::RoundRobinBatch(2),
    )?);
    let graph = JobGraph::try_new(
        plan,
        JobGraphOptions {
            shuffle_backend: backend.clone(),
        },
    )?;
    // Draining and Running take the same cleanup path. Avoid spawning a driver
    // merely to construct its output channel: this tests the actual scheduler.
    let mut job =
        JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
    assert_eq!(
        job.topology.regions.len(),
        1,
        "control needs one failure region"
    );
    let job_id = JobId::from(1);
    let mut keys = vec![];
    for (stage, descriptor) in job.stages.iter_mut().enumerate() {
        for (partition, task) in descriptor.tasks.iter_mut().enumerate() {
            task.attempts.push(TaskAttemptDescriptor {
                state: TaskState::Running,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
            });
            keys.push(TaskKey {
                job_id,
                stage,
                partition,
                attempt: 0,
            });
        }
    }
    assert!(keys.len() > 1);
    let mut scheduler =
        JobScheduler::new(JobSchedulerOptions::for_retry_test(3, backend), reporter);
    scheduler.jobs.insert(job_id, job);
    let mut assigner = TaskAssigner::new(TaskAssignerOptions::new(keys.len(), 1));
    assigner.activate_worker(WorkerId::from(1));
    assigner.enqueue_tasks(&TaskRegion {
        tasks: keys
            .iter()
            .map(|key| {
                (
                    TaskPlacement::Worker,
                    TaskSet {
                        entries: vec![TaskSetEntry {
                            key: key.clone(),
                            output: TaskOutputKind::Local,
                        }],
                    },
                )
            })
            .collect(),
    })?;
    assert_eq!(assigner.assign_tasks().len(), keys.len());
    assert_eq!(
        assigner.find_worker_tasks(WorkerId::from(1)).len(),
        keys.len()
    );
    Ok(Fixture {
        scheduler,
        assigner,
        keys,
        events,
        _provider: provider,
    })
}

fn retire_after_cancellation(fixture: &mut Fixture, actions: Vec<JobAction>) -> usize {
    let mut canceled = 0;
    // This is the actual assigner path used by the actor's CancelTask action.
    for action in actions {
        if let JobAction::CancelTask { key } = action {
            fixture.assigner.exclude_task(&key);
            assert!(fixture.assigner.unassign_task(&key).is_some());
            canceled += 1;
        }
    }
    // A heartbeat retirement cannot repair a missing terminal event after the
    // canceled tasks have already left the worker's active task slots.
    assert!(
        fixture
            .assigner
            .find_worker_tasks(WorkerId::from(1))
            .is_empty()
    );
    fixture.assigner.deactivate_worker(WorkerId::from(1));
    canceled
}

fn task_events(events: &InMemoryLogExporter) -> ExecutionResult<Vec<SystemEvent>> {
    let logs = events
        .get_emitted_logs()
        .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
    logs.into_iter()
        .filter_map(|log| match log.record.body() {
            Some(AnyValue::String(text)) => {
                Some(serde_json::from_str::<SystemEvent>(text.as_str()))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|events| {
            events
                .into_iter()
                .filter(|e| matches!(e, SystemEvent::TaskUpdated { .. }))
                .collect()
        })
        .map_err(|e| ExecutionError::InternalError(e.to_string()))
}

#[test]
fn cleanup_output_failure_terminalizes_attempts_before_worker_retirement() -> ExecutionResult<()> {
    let mut f = fixture()?;
    let actions = f
        .scheduler
        .clean_up_job(JobId::from(1), JobOutputOutcome::Failed);
    assert!(matches!(
        f.scheduler.jobs[&JobId::from(1)].state,
        JobState::Failed
    ));
    assert_eq!(retire_after_cancellation(&mut f, actions), f.keys.len());
    let states = f
        .keys
        .iter()
        .filter_map(|key| f.scheduler.get_task_state(key))
        .collect::<Vec<_>>();
    let events = task_events(&f.events)?;
    assert!(
        states.iter().all(TaskState::is_terminal),
        "output cleanup left {states:?}; task event count={}",
        events.len()
    );
    assert_eq!(events.len(), f.keys.len());
    Ok(())
}

#[test]
fn cleanup_task_failure_cascade_is_a_terminal_control() -> ExecutionResult<()> {
    let mut f = fixture()?;
    f.scheduler
        .update_task(&f.keys[0], TaskState::Failed, None, None);
    let reporter = f.scheduler.event_reporter.clone();
    let session_id = f.scheduler.options.session_id.clone();
    let actions = JobScheduler::cascade_cancel_task_attempts(
        JobId::from(1),
        &mut f.scheduler.jobs[&JobId::from(1)],
        &reporter,
        &session_id,
    );
    assert!(f.assigner.unassign_task(&f.keys[0]).is_some());
    assert_eq!(retire_after_cancellation(&mut f, actions), f.keys.len() - 1);
    assert!(f.keys.iter().all(|key| {
        f.scheduler
            .get_task_state(key)
            .is_some_and(|state| state.is_terminal())
    }));
    assert_eq!(task_events(&f.events)?.len(), f.keys.len());
    Ok(())
}

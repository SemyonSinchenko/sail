//! Host-issued task identity and cleanup for worker-resident extension state.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use log::warn;
use sail_common_datafusion::worker_extension::{
    WorkerExtensionRegistry, WorkerJobIdentity, WorkerTaskScope,
};

use crate::id::JobId;

pub(super) fn scoped_context(context: &TaskContext, scope: WorkerTaskScope) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        context
            .session_config()
            .clone()
            .with_extension(Arc::new(scope)),
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    ))
}

/// Retain only registries used by admitted jobs. Cleanup uses the same CloseJob
/// message as shuffle cleanup; nothing is added to the worker RPC protocol.
#[derive(Default)]
pub(super) struct ExtensionJobs(HashMap<JobId, Arc<WorkerExtensionRegistry>>);

impl ExtensionJobs {
    pub fn admit(&mut self, job: JobId, context: &TaskContext) -> Result<()> {
        let Some(registry) = context
            .session_config()
            .get_extension::<WorkerExtensionRegistry>()
        else {
            return Ok(());
        };
        if let Some(previous) = self.0.get(&job) {
            if !Arc::ptr_eq(previous, &registry) {
                return exec_err!("worker extension registry changed within one job");
            }
        } else {
            self.0.insert(job, registry);
        }
        Ok(())
    }

    pub fn close_job(&mut self, session_id: &str, job_id: JobId) {
        if let Some(registry) = self.0.remove(&job_id) {
            close(&registry, session_id, job_id);
        }
    }

    pub fn close_all(&mut self, session_id: &str) {
        for (job_id, registry) in self.0.drain() {
            close(&registry, session_id, job_id);
        }
    }
}

fn close(registry: &WorkerExtensionRegistry, session_id: &str, job_id: JobId) {
    // close_job tombstones the identity before invoking cleanup callbacks. A
    // blocking preparation can outlive its monitor; its late materialization
    // must not recreate state after this point. In-flight plans own their Arcs.
    if let Err(error) = registry.close_job(&WorkerJobIdentity {
        session_id: session_id.to_owned(),
        job_id: job_id.into(),
    }) {
        warn!("failed to close worker extension state for job {job_id}: {error}");
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionConfig;

    use super::*;

    #[test]
    fn identity_injection_preserves_context_and_does_not_mutate_shared_session()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let retained = Arc::new(String::from("session capability"));
        let source = TaskContext::default()
            .with_task_id("original".into())
            .with_session_config(
                SessionConfig::new()
                    .with_target_partitions(7)
                    .with_extension(retained.clone()),
            );
        let scope = WorkerTaskScope {
            job: WorkerJobIdentity {
                session_id: "session".into(),
                job_id: 12,
            },
            worker_id: 3,
            stage: 4,
            partition: 5,
            attempt: 0,
        };
        let actual = scoped_context(&source, scope);
        assert_eq!(actual.task_id(), source.task_id());
        assert_eq!(actual.session_id(), source.session_id());
        assert!(Arc::ptr_eq(&actual.runtime_env(), &source.runtime_env()));
        assert_eq!(actual.session_config().target_partitions(), 7);
        assert!(
            actual
                .session_config()
                .get_extension::<String>()
                .is_some_and(|v| Arc::ptr_eq(&v, &retained))
        );
        let injected = actual
            .session_config()
            .get_extension::<WorkerTaskScope>()
            .ok_or("missing scope")?;
        assert_eq!(injected.job.job_id, 12);
        assert_eq!(injected.worker_id, 3);
        assert_eq!(injected.stage, 4);
        assert_eq!(injected.partition, 5);
        assert!(
            source
                .session_config()
                .get_extension::<WorkerTaskScope>()
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn closing_a_job_and_shutdown_tombstone_their_scopes_independently() -> Result<()> {
        let registry = Arc::new(WorkerExtensionRegistry::default());
        let context = TaskContext::default()
            .with_session_config(SessionConfig::new().with_extension(registry.clone()));
        let mut jobs = ExtensionJobs::default();
        jobs.admit(JobId::from(1), &context)?;
        jobs.admit(JobId::from(2), &context)?;
        let identity = |job_id| WorkerJobIdentity {
            session_id: "session".into(),
            job_id,
        };
        jobs.close_job("session", JobId::from(1));
        assert!(registry.ensure_open(&identity(1)).is_err());
        assert!(registry.ensure_open(&identity(2)).is_ok());
        jobs.close_all("session");
        assert!(registry.ensure_open(&identity(2)).is_err());
        assert!(jobs.0.is_empty());
        // Repeated cleanup is idempotent; another session's same numeric ID is
        // a distinct scope and must remain usable.
        jobs.close_job("session", JobId::from(1));
        assert!(
            registry
                .ensure_open(&WorkerJobIdentity {
                    session_id: "other".into(),
                    job_id: 1
                })
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn one_job_cannot_swap_registry_ownership_between_batches() -> Result<()> {
        let mut jobs = ExtensionJobs::default();
        let a = Arc::new(WorkerExtensionRegistry::default());
        let b = Arc::new(WorkerExtensionRegistry::default());
        let context = |registry| {
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_extension(registry))
        };
        jobs.admit(JobId::from(1), &context(a.clone()))?;
        assert!(jobs.admit(JobId::from(1), &context(b.clone())).is_err());
        jobs.close_job("s", JobId::from(1));
        let identity = WorkerJobIdentity {
            session_id: "s".into(),
            job_id: 1,
        };
        assert!(a.ensure_open(&identity).is_err());
        assert!(b.ensure_open(&identity).is_ok());
        Ok(())
    }
}

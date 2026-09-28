//! Use existing slot-sharing groups for job-local worker extension owners.
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::worker_extension::{WorkerExtensionExec, contains_worker_extension};

use super::Stage;

/// Ordinary source widths must not shift the task-set buckets of stateful
/// stages. Operations that occur in the same stage necessarily share a task,
/// so join their groups transitively rather than selecting one descriptor.
pub(super) fn assign(stages: &mut [Stage]) {
    if !stages
        .iter()
        .any(|stage| contains_worker_extension(&stage.plan))
    {
        return;
    }
    fn root(parents: &mut [usize], mut stage: usize) -> usize {
        while parents[stage] != stage {
            parents[stage] = parents[parents[stage]];
            stage = parents[stage];
        }
        stage
    }

    fn visit(
        plan: &Arc<dyn ExecutionPlan>,
        stage: usize,
        parents: &mut [usize],
        owners: &mut HashMap<(String, String), usize>,
        stateful: &mut bool,
    ) {
        if let Some(worker) = plan.downcast_ref::<WorkerExtensionExec>() {
            *stateful = true;
            let identity = (
                worker.descriptor.package_identity.clone(),
                worker.descriptor.operation_id.clone(),
            );
            if let Some(previous) = owners.insert(identity, stage) {
                let left = root(parents, previous);
                let right = root(parents, stage);
                // The earliest stage supplies a deterministic job-local name;
                // package/operation strings never become scheduler group names.
                parents[left.max(right)] = left.min(right);
            }
        }
        for child in plan.children() {
            visit(child, stage, parents, owners, stateful);
        }
    }

    let mut parents = (0..stages.len()).collect::<Vec<_>>();
    let mut owners = HashMap::new();
    let mut stateful = vec![false; stages.len()];
    for (stage, definition) in stages.iter().enumerate() {
        visit(
            &definition.plan,
            stage,
            &mut parents,
            &mut owners,
            &mut stateful[stage],
        );
    }
    for (stage, definition) in stages.iter_mut().enumerate() {
        if stateful[stage] {
            definition.group = format!("worker-extension:{}", root(&mut parents, stage));
        }
    }
}

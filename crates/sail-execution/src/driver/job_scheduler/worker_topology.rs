//! Validate job-local worker ownership against the scheduler's actual buckets.
use sail_common_datafusion::worker_extension::{WorkerDescriptor, WorkerExtensionExec};

use super::*;
use crate::driver::job_scheduler::topology::{JobTopology, TaskTopology};

#[derive(Debug, PartialEq, Eq)]
struct OwnerLocation {
    region: usize,
    group: StageGroupKey,
    bucket: usize,
}

pub(super) fn validate(graph: &JobGraph, topology: &JobTopology) -> ExecutionResult<()> {
    fn collect(
        plan: &Arc<dyn ExecutionPlan>,
        stage: usize,
        found: &mut Vec<(usize, Arc<WorkerDescriptor>)>,
    ) {
        if let Some(worker) = plan.downcast_ref::<WorkerExtensionExec>() {
            found.push((stage, worker.descriptor.clone()));
        }
        for child in plan.children() {
            collect(child, stage, found);
        }
    }
    let mut occurrences = vec![];
    for (stage, definition) in graph.stages().iter().enumerate() {
        collect(&definition.plan, stage, &mut occurrences);
    }
    if occurrences.is_empty() {
        return Ok(());
    }

    // Building and validation use the same StageGroup construction and bucket
    // method. Include ordinary intervening stages: their widths affect offsets.
    // Forward-only components have one region per partition but identical stage
    // layouts. Reuse their grouping metadata rather than retaining P copies of
    // P buckets. Total cached bucket storage stays bounded by stage task counts.
    let mut layouts = HashMap::new();
    let mut groups_by_layout = vec![];
    let mut region_layouts = Vec::with_capacity(topology.regions.len());
    for region in &topology.regions {
        let stages = region
            .tasks
            .iter()
            .map(|task| task.stage)
            .collect::<IndexSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let layout = *layouts.entry(stages).or_insert_with(|| {
            let index = groups_by_layout.len();
            groups_by_layout.push(build_stage_groups(graph, region));
            index
        });
        region_layouts.push(layout);
    }
    let mut operations = HashMap::<(String, String), Vec<OwnerLocation>>::new();
    for (stage, descriptor) in occurrences {
        let definition = &graph.stages()[stage];
        let operation = (
            descriptor.package_identity.clone(),
            descriptor.operation_id.clone(),
        );
        if definition.placement != TaskPlacement::Worker
            || definition.plan.output_partitioning().partition_count() != descriptor.partitions
        {
            return Err(ExecutionError::InvalidArgument(format!(
                "worker extension operation {operation:?} requires worker stage {stage} with exactly {} partitions",
                descriptor.partitions
            )));
        }
        let mut owners = Vec::with_capacity(descriptor.partitions);
        for partition in 0..descriptor.partitions {
            let region = topology
                .task_regions
                .get(&TaskTopology { stage, partition })
                .copied()
                .ok_or_else(|| ExecutionError::InternalError("worker task has no region".into()))?;
            let groups = &groups_by_layout[region_layouts[region]];
            let key = StageGroupKey {
                placement: definition.placement,
                group: definition.group.clone(),
            };
            let group = groups.get(&key).ok_or_else(|| {
                ExecutionError::InternalError("worker task has no stage group".into())
            })?;
            owners.push(OwnerLocation {
                region,
                group: key,
                bucket: group.bucket(stage, partition),
            });
        }
        if let Some(previous) = operations.get(&operation) {
            if previous != &owners {
                return Err(ExecutionError::InvalidArgument(format!(
                    "worker extension operation {operation:?} cannot preserve partition ownership at stage {stage}; shared state requires identical partition counts, pipelined task regions, slot groups and task-set buckets"
                )));
            }
        } else {
            operations.insert(operation, owners);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

//! Compiles Sail's actual task assigner without its DataFusion dependency tree.
//! Only the surrounding error/options types are small test shims. No
//! task assignment logic is copied. These counterexamples are not a full Sail
//! cluster gate; they prove what the currently compiled assigner does.
#![allow(dead_code)]

#[path = "../../../../crates/sail-execution/src/id.rs"]
pub mod id;
#[path = "../../../../crates/sail-execution/src/task/scheduling.rs"]
pub mod original_scheduling;
#[path = "../../../../crates/sail-execution/src/driver/task_assigner/mod.rs"]
pub mod original_task_assigner;

pub mod error {
    #[derive(Debug)]
    pub enum ExecutionError {
        InvalidArgument(String),
        InternalError(String),
    }
    impl std::fmt::Display for ExecutionError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::InvalidArgument(value) | Self::InternalError(value) => value.fmt(f),
            }
        }
    }
    pub type ExecutionResult<T> = Result<T, ExecutionError>;
}
pub mod job_graph {
    include!(concat!(env!("OUT_DIR"), "/placement.rs"));
}

pub mod task {
    pub use crate::original_scheduling as scheduling;
}
pub mod driver {
    pub use crate::original_task_assigner as task_assigner;
    pub struct DriverOptions {
        pub worker_task_slots: usize,
        pub worker_max_count: usize,
    }
}

use driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
use id::{JobId, TaskKey, WorkerId};
use job_graph::TaskPlacement;
use task::scheduling::{TaskAssignment, TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry};

fn partition(job: u64) -> TaskRegion {
    TaskRegion {
        tasks: vec![(
            TaskPlacement::Worker,
            TaskSet {
                entries: vec![TaskSetEntry {
                    key: TaskKey {
                        job_id: JobId::from(job),
                        stage: 0,
                        partition: 0,
                        attempt: 0,
                    },
                    output: TaskOutputKind::Local,
                }],
            },
        )],
    }
}

fn worker(assignment: &TaskAssignment) -> WorkerId {
    match assignment {
        TaskAssignment::Worker { worker_id, .. } => *worker_id,
        _ => panic!("expected worker assignment"),
    }
}

#[test]
fn subsequent_query_can_move_the_same_partition_to_another_worker() {
    let options = driver::DriverOptions {
        worker_task_slots: 2,
        worker_max_count: 2,
    };
    let mut assigner = TaskAssigner::new(TaskAssignerOptions::from(&options));
    assigner.activate_worker(WorkerId::from(1));
    assigner.activate_worker(WorkerId::from(2));
    assigner.enqueue_tasks(&partition(1)).unwrap();
    let initialized = assigner.assign_tasks();
    let owner = worker(&initialized[0].assignment);
    assigner.unassign_task(&initialized[0].set.entries[0].key);

    // An unrelated task occupies the former owner after graph initialization.
    assigner.enqueue_tasks(&partition(2)).unwrap();
    let occupied = assigner.assign_tasks();
    assert_eq!(owner, worker(&occupied[0].assignment));
    assigner.enqueue_tasks(&partition(3)).unwrap();
    let next_round = assigner.assign_tasks();
    assert_ne!(owner, worker(&next_round[0].assignment));
    println!(
        "AFFINITY_COUNTEREXAMPLE initial={owner} next={}",
        worker(&next_round[0].assignment)
    );
}

#[test]
fn retained_native_adjacency_has_no_existing_idle_worker_pin() {
    let options = driver::DriverOptions {
        worker_task_slots: 2,
        worker_max_count: 2,
    };
    let mut assigner = TaskAssigner::new(TaskAssignerOptions::from(&options));
    assigner.activate_worker(WorkerId::from(1));
    assigner.enqueue_tasks(&partition(1)).unwrap();
    let initialized = assigner.assign_tasks();
    let owner = worker(&initialized[0].assignment);
    assigner.track_streams(&initialized);
    assigner.unassign_task(&initialized[0].set.entries[0].key);
    assert!(!assigner.is_worker_idle(owner));
    // Existing CloseJob cleanup removes the stream pin, even if extension code
    // retains native adjacency in a process-local registry.
    assigner.untrack_local_streams(JobId::from(1), None);
    assert!(assigner.is_worker_idle(owner));
}

mod stage_group {
    use super::*;
    use indexmap::IndexMap;
    use std::collections::HashMap;
    include!(concat!(env!("OUT_DIR"), "/stage_group.rs"));

    fn placement(counts: &[(usize, usize)]) -> HashMap<(usize, usize), usize> {
        let mut group = StageGroup::new(counts.iter().copied().collect());
        for &(stage, count) in counts {
            for partition in 0..count {
                group.add_task(TaskSetEntry {
                    key: TaskKey {
                        job_id: JobId::from(1),
                        stage,
                        partition,
                        attempt: 0,
                    },
                    output: TaskOutputKind::Local,
                });
            }
        }
        group
            .buckets
            .iter()
            .enumerate()
            .flat_map(|(bucket, entries)| {
                entries
                    .iter()
                    .map(move |entry| ((entry.key.stage, entry.key.partition), bucket))
            })
            .collect()
    }

    #[test]
    fn equal_width_stages_share_partition_owners_within_one_region() {
        for count in [2, 3, 16] {
            let counts: Vec<_> = (0..9).map(|stage| (stage, count)).collect();
            let placement = placement(&counts);
            for partition in 0..count {
                for stage in 1..counts.len() {
                    assert_eq!(placement[&(0, partition)], placement[&(stage, partition)]);
                }
            }
        }
    }

    #[test]
    fn scalar_stage_can_shift_ownership_even_inside_one_region() {
        let placement = placement(&[(0, 2), (1, 1), (2, 2)]);
        assert_ne!(placement[&(0, 0)], placement[&(2, 0)]);
        println!(
            "SCALAR_STAGE_COUNTEREXAMPLE partition=0 before={} after={}",
            placement[&(0, 0)],
            placement[&(2, 0)]
        );
    }
}

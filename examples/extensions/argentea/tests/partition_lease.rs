//! Sole-owner teardown must retain the host lease through admitted storage.
#[path = "delta_support/mod.rs"]
mod support;
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_argentea_core::*;
use sail_native_resource_ffi::MemoryLease;
use std::{
    any::Any,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
const LIMIT: usize = 16 << 20;
struct Probe(ExecutionContext, Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.1
            .store(self.0.usage().unwrap().live_bytes, Ordering::SeqCst);
    }
}
fn check(kind: &str, build: impl FnOnce(Resources) -> Box<dyn Any>) {
    let usage = ExecutionContext::new(ExecutionLimits {
        memory_bytes: LIMIT,
        work_units: usize::MAX,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let at_release = Arc::new(AtomicUsize::new(usize::MAX));
    let lease = MemoryLease::new(
        Arc::new(Probe(usage.clone(), at_release.clone())),
        LIMIT as u64,
    );
    // Only the returned partition owns Resources; the callback's context is an
    // accounting observer, not another host-lease owner.
    let part = build(Resources::new(usage.clone(), lease).unwrap());
    assert_eq!(at_release.load(Ordering::SeqCst), usize::MAX);
    assert!(usage.usage().unwrap().live_bytes > 0);
    drop(part);
    println!(
        "PARTITION_SUCCESSFUL_DROP kind={kind} admitted_at_lease_release={} final_live_bytes={}",
        at_release.load(Ordering::SeqCst),
        usage.usage().unwrap().live_bytes
    );
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(at_release.load(Ordering::SeqCst), 0);
}
#[test]
fn bfs_successful_sole_owner_retains_host_lease_through_storage_drop() {
    check("bfs", |r| {
        Box::new(
            BfsPartition::build(
                support::operation(1, 3),
                0,
                7,
                &[2, 0, 1],
                &[(0, 1), (0, 1), (1, 2)],
                BfsOptions {
                    source: 0,
                    algorithm: BfsAlgorithm::Frontier,
                    max_levels: 8,
                    alpha: 14,
                    beta: 24,
                },
                r,
            )
            .unwrap(),
        )
    });
}
#[test]
fn sssp_successful_sole_owner_retains_host_lease_through_storage_drop() {
    check("sssp", |r| {
        Box::new(
            SsspPartition::build(
                support::operation(1, 3),
                0,
                7,
                &[2, 0, 1],
                &[(0, 1, 1.0), (0, 1, 0.5), (1, 2, 0.0)],
                SsspOptions {
                    source: 0,
                    algorithm: SsspAlgorithm::DeltaStar,
                    max_rounds: 8,
                    delta: 1.0,
                },
                r,
            )
            .unwrap(),
        )
    });
}
#[test]
fn residual_pagerank_successful_sole_owner_retains_host_lease_through_storage_drop() {
    check("residual_pagerank", |r| {
        Box::new(
            DeltaPartition::build(
                support::operation(1, 3),
                0,
                &[2, 0, 1],
                &[(0, 1), (0, 1), (1, 2)],
                support::options(),
                r,
            )
            .unwrap(),
        )
    });
}

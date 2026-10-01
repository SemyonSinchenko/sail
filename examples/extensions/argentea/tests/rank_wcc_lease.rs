//! A successful partition must keep the host lease until native storage drops.
#[path = "delta_support/mod.rs"]
mod support;
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_argentea_core::*;
use sail_native_resource_ffi::MemoryLease;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const LIMIT: usize = 16 << 20;
struct LeaseProbe(ExecutionContext, Arc<AtomicUsize>);
impl Drop for LeaseProbe {
    fn drop(&mut self) {
        self.1
            .store(self.0.usage().unwrap().live_bytes, Ordering::SeqCst);
    }
}
fn resources() -> (Resources, ExecutionContext, Arc<AtomicUsize>) {
    let usage = ExecutionContext::new(ExecutionLimits {
        memory_bytes: LIMIT,
        work_units: usize::MAX,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let at_release = Arc::new(AtomicUsize::new(usize::MAX));
    let lease = MemoryLease::new(
        Arc::new(LeaseProbe(usage.clone(), at_release.clone())),
        LIMIT as u64,
    );
    (
        Resources::new(usage.clone(), lease).unwrap(),
        usage,
        at_release,
    )
}
fn wcc_successful_drop(algorithm: WccAlgorithm) {
    let (resources, usage, at_release) = resources();
    // Move the sole Resources owner: an external clone would mask ordering.
    let part = WccPartition::build(
        support::operation(1, 3),
        0,
        7,
        &[2, 0, 1],
        &[(0, 1), (0, 1), (1, 2)],
        WccOptions {
            algorithm,
            seed: 42,
            max_rounds: 16,
        },
        resources,
    )
    .unwrap();
    assert_eq!(part.state_rows().count(), 3);
    assert_eq!(at_release.load(Ordering::SeqCst), usize::MAX);
    drop(part);
    println!(
        "WCC_SUCCESSFUL_DROP algorithm={algorithm:?} bytes_at_lease_release={} final_live_bytes={}",
        at_release.load(Ordering::SeqCst),
        usage.usage().unwrap().live_bytes
    );
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(at_release.load(Ordering::SeqCst), 0);
}
#[test]
fn successful_wcc_reference_sole_owner_releases_storage_before_host_lease() {
    wcc_successful_drop(WccAlgorithm::Reference);
}
#[test]
fn successful_wcc_star_sole_owner_releases_storage_before_host_lease() {
    wcc_successful_drop(WccAlgorithm::StarContraction);
}
#[test]
fn successful_pagerank_sole_owner_releases_storage_before_host_lease() {
    let (resources, usage, at_release) = resources();
    let part = PageRankPartition::build(
        support::operation(1, 3),
        0,
        &[2, 0, 1],
        &[(0, 1), (0, 1), (1, 2)],
        resources,
    )
    .unwrap();
    assert_eq!(part.ranks().count(), 3);
    assert_eq!(at_release.load(Ordering::SeqCst), usize::MAX);
    drop(part);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(at_release.load(Ordering::SeqCst), 0);
}

#[test]
fn prepared_sole_owner_abandon_cancel_failure_and_success_drop_in_order() {
    use grust_procedures::MemoryReservation;
    for kind in 0..3 {
        for action in 0..4 {
            let (resources, usage, at_release) = resources();
            let op = support::operation(3, 1_024);
            let ids = (0..1_024).map(|i| i * 3).collect::<Vec<_>>();
            let arcs = vec![(0, 3), (3, 6), (0, 3)];
            enum Prepared {
                Pr(PageRankInitialization),
                Wcc(WccInitialization),
            }
            let prepared = if kind == 0 {
                Prepared::Pr(PageRankPartition::prepare(op, 0, &ids, &arcs, resources).unwrap())
            } else {
                Prepared::Wcc(
                    WccPartition::prepare(
                        op,
                        0,
                        7,
                        &ids,
                        &arcs,
                        WccOptions {
                            algorithm: if kind == 1 {
                                WccAlgorithm::Reference
                            } else {
                                WccAlgorithm::StarContraction
                            },
                            seed: 42,
                            max_rounds: 16,
                        },
                        resources,
                    )
                    .unwrap(),
                )
            };
            drop(ids);
            drop(arcs);
            assert_eq!(at_release.load(Ordering::SeqCst), usize::MAX);
            let blocker: Option<MemoryReservation> = if action == 2 {
                let headroom = if kind == 0 {
                    8 * 1_024 + 127
                } else {
                    // Admit origins+roots exactly, then reject StatisticsInbox.
                    3 * (size_of::<Option<WccOrigin>>() + size_of::<Option<(u64, u64)>>())
                        + 256
                        + 8 * 1_024
                        + 128
                };
                Some(
                    usage
                        .reserve(LIMIT - usage.usage().unwrap().live_bytes - headroom)
                        .unwrap(),
                )
            } else {
                None
            };
            let expected_external = blocker.as_ref().map_or(0, MemoryReservation::bytes);
            if action == 0 {
                drop(prepared);
            } else {
                if action == 1 {
                    usage.cancel().unwrap();
                }
                let result = match prepared {
                    Prepared::Pr(p) => p.finish().map(drop),
                    Prepared::Wcc(p) => p.finish().map(drop),
                };
                if action == 3 {
                    result.unwrap();
                } else {
                    assert!(result.is_err());
                }
            }
            assert_eq!(at_release.load(Ordering::SeqCst), expected_external);
            assert_eq!(usage.usage().unwrap().live_bytes, expected_external);
            drop(blocker);
            assert_eq!(usage.usage().unwrap().live_bytes, 0);
        }
    }
}

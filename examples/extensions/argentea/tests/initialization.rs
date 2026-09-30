//! Requested heap and admitted-memory controls across the raw-input lifetime.
#[path = "../src/adjacency_tests/allocations.rs"]
mod allocations;
#[path = "delta_support/mod.rs"]
mod support;
use grust_procedures::MemoryAccount;
use sail_argentea_core::*;
use std::sync::atomic::Ordering;

const LIMIT: usize = 256 << 20;
fn bfs_options() -> BfsOptions {
    BfsOptions {
        source: 0,
        algorithm: BfsAlgorithm::Frontier,
        max_levels: 8,
        alpha: 14,
        beta: 24,
    }
}
fn sssp_options() -> SsspOptions {
    SsspOptions {
        source: 0,
        algorithm: SsspAlgorithm::DeltaStar,
        max_rounds: 8,
        delta: 1.0,
    }
}
struct Raw {
    ids: Vec<i64>,
    bfs: Vec<(i64, i64)>,
    sssp: Vec<(i64, i64, f64)>,
    // Rust drops fields in declaration order, so the charge outlives the vectors.
    _admission: MemoryAccount,
}
impl Raw {
    fn new(n: usize, degree: usize, weighted: bool, r: &Resources) -> Self {
        let mut admission = r.execution.memory_account();
        admission
            .charge(n * 8 + n * degree * if weighted { 24 } else { 16 })
            .unwrap();
        Self {
            ids: (0..n as i64).rev().map(|i| i * 3).collect(),
            bfs: if weighted {
                vec![]
            } else {
                (0..n * degree).map(|i| ((i % n) as i64 * 3, 0)).collect()
            },
            sssp: if weighted {
                (0..n * degree)
                    .map(|i| ((i % n) as i64 * 3, 0, 0.5))
                    .collect()
            } else {
                vec![]
            },
            _admission: admission,
        }
    }
}
enum Prepared {
    Bfs(BfsInitialization),
    Sssp(SsspInitialization),
}
enum Partition {
    Bfs(BfsPartition),
    Sssp(SsspPartition),
}
impl Prepared {
    fn new(op: Operation, raw: &Raw, weighted: bool, r: &Resources) -> Result<Self> {
        if weighted {
            SsspPartition::prepare(op, 0, 7, &raw.ids, &raw.sssp, sssp_options(), r.clone())
                .map(Self::Sssp)
        } else {
            BfsPartition::prepare(op, 0, 7, &raw.ids, &raw.bfs, bfs_options(), r.clone())
                .map(Self::Bfs)
        }
    }
    fn finish(self) -> Result<Partition> {
        match self {
            Self::Bfs(p) => p.finish().map(Partition::Bfs),
            Self::Sssp(p) => p.finish().map(Partition::Sssp),
        }
    }
}
fn build(op: Operation, raw: &Raw, weighted: bool, r: &Resources) -> Result<Partition> {
    if weighted {
        SsspPartition::build(op, 0, 7, &raw.ids, &raw.sssp, sssp_options(), r.clone())
            .map(Partition::Sssp)
    } else {
        BfsPartition::build(op, 0, 7, &raw.ids, &raw.bfs, bfs_options(), r.clone())
            .map(Partition::Bfs)
    }
}
impl Partition {
    fn check(&self, n: usize) {
        match self {
            Self::Bfs(p) => {
                assert_eq!(p.state_rows().count(), n);
                assert_eq!(p.reached_count(), 1);
                assert_eq!(p.frontier_count(), 1);
                assert!(
                    p.state_rows()
                        .all(|r| r.distance == (r.id == 0).then_some(0))
                );
                assert_eq!(p.completed_mode(), BfsMode::Topology);
            }
            Self::Sssp(p) => {
                assert_eq!(p.state_rows().count(), n);
                assert_eq!(p.reached_count(), 1);
                assert_eq!(p.active_count(), 1);
                assert!(
                    p.state_rows()
                        .all(|r| r.label == (r.id == 0).then_some(SsspLabel::source(0)))
                );
                assert_eq!(p.completed_mode(), SsspMode::Topology);
            }
        }
    }
}
fn cost(
    n: usize,
    degree: usize,
    weighted: bool,
    release: bool,
) -> (allocations::Counts, usize, usize) {
    let (r, usage, drops) = support::resources(LIMIT);
    let op = support::operation(3, n as u64);
    // Measure raw-vector allocation too: deallocating an unmeasured input would
    // otherwise subtract bytes that were never added to the heap counter.
    let (part, counts) = allocations::measure(|| {
        let raw = Raw::new(n, degree, weighted, &r);
        if release {
            let ready = Prepared::new(op, &raw, weighted, &r).unwrap();
            drop(raw);
            ready.finish().unwrap()
        } else {
            build(op, &raw, weighted, &r).unwrap()
        }
    });
    part.check(n);
    let used = usage.usage().unwrap();
    println!(
        "INPUT_LIFETIME_COUNTER n={n} degree={degree} weighted={weighted} release={release} allocation_calls={} allocated_bytes={} peak_requested_bytes={} retained_admitted_bytes={} peak_admitted_bytes={} work={}",
        counts.calls,
        counts.bytes,
        counts.peak,
        used.live_bytes,
        used.peak_bytes,
        used.counted_work().unwrap()
    );
    drop(part);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    drop(r);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    (counts, used.peak_bytes, used.counted_work().unwrap())
}
#[test]
fn releasing_raw_input_reduces_heap_overlap_without_changing_work() {
    for n in [1_024, 65_536] {
        for degree in [0, 1, 8] {
            for weighted in [false, true] {
                let old = cost(n, degree, weighted, false);
                let new = cost(n, degree, weighted, true);
                assert_eq!(
                    (new.0.calls, new.0.bytes, new.2),
                    (old.0.calls, old.0.bytes, old.2)
                );
                assert!(old.0.peak >= new.0.peak + n * 8);
                // Conservative build admission still overlaps raw input and CSR;
                // dense graphs need not lower the overall admitted peak.
                assert!(new.1 <= old.1);
            }
        }
    }
}
#[test]
fn abandoned_or_cancelled_preparation_releases_all_storage_and_leases() {
    for weighted in [false, true] {
        for cancel in [false, true] {
            let (r, usage, drops) = support::resources(LIMIT);
            let raw = Raw::new(1_024, 1, weighted, &r);
            let prepared = Prepared::new(support::operation(3, 1_024), &raw, weighted, &r).unwrap();
            drop(raw);
            if cancel {
                usage.cancel().unwrap();
                assert!(prepared.finish().is_err());
            } else {
                drop(prepared);
            }
            assert_eq!(usage.usage().unwrap().live_bytes, 0);
            drop(r);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}
#[test]
fn input_charge_is_released_before_finish_admission() {
    for weighted in [false, true] {
        for release in [false, true] {
            let (r, usage, _) = support::resources(LIMIT);
            let raw = Raw::new(1_024, 1, weighted, &r);
            let prepared = Prepared::new(support::operation(3, 1_024), &raw, weighted, &r).unwrap();
            let raw_bytes = raw._admission.bytes();
            let before = usage.usage().unwrap().live_bytes;
            // 32 KiB cannot hold the 40 KiB labels/frontier, but releasing raw
            // input first creates enough room. The blocker stays until finish.
            let blocker = usage.reserve(LIMIT - before - 32 * 1024).unwrap();
            let raw = if release {
                drop(raw);
                None
            } else {
                Some(raw)
            };
            assert_eq!(
                usage.usage().unwrap().live_bytes,
                LIMIT - 32 * 1024 - if release { raw_bytes } else { 0 }
            );
            let result = prepared.finish();
            if release {
                result.as_ref().unwrap().check(1_024);
            } else {
                assert!(result.is_err_and(|e| e.contains("memory")));
            }
            drop(raw);
            drop(blocker);
        }
    }
}
#[test]
fn prepare_preserves_validation_errors_and_cancellation() {
    for weighted in [false, true] {
        for malformed in 0..6 {
            let (r, usage, _) = support::resources(LIMIT);
            let mut raw = Raw::new(3, 1, weighted, &r);
            let mut op = support::operation(3, 3);
            match malformed {
                0 => raw.ids[1] = raw.ids[0],
                1 => raw.ids[1] = 1,
                2 => {
                    raw.bfs.iter_mut().for_each(|e| e.0 = 30);
                    raw.sssp.iter_mut().for_each(|e| e.0 = 30);
                }
                3 => op.generation = 0,
                4 => op.vertices = 1,
                5 => usage.cancel().unwrap(),
                _ => unreachable!(),
            }
            let old = build(op.clone(), &raw, weighted, &r).err().unwrap();
            let new = Prepared::new(op, &raw, weighted, &r).err().unwrap();
            assert_eq!(new, old);
            drop(raw);
            assert_eq!(usage.usage().unwrap().live_bytes, 0);
        }
    }
    for weight in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let (r, usage, _) = support::resources(LIMIT);
        let mut raw = Raw::new(3, 1, true, &r);
        raw.sssp[0].2 = weight;
        let op = support::operation(3, 3);
        let old = build(op.clone(), &raw, true, &r).err().unwrap();
        assert!(old.contains("weight"));
        assert_eq!(old, Prepared::new(op, &raw, true, &r).err().unwrap());
        drop(raw);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
}

#[test]
fn last_preparation_owner_releases_csr_before_host_lease_on_abort_or_failure() {
    use grust_procedures::{ExecutionContext, ExecutionLimits};
    use sail_native_resource_ffi::MemoryLease;
    use std::sync::{Arc, atomic::AtomicUsize};
    struct LeaseProbe(ExecutionContext, Arc<AtomicUsize>);
    impl Drop for LeaseProbe {
        fn drop(&mut self) {
            self.1
                .store(self.0.usage().unwrap().live_bytes, Ordering::SeqCst);
        }
    }
    for (weighted, cancel) in [false, true]
        .into_iter()
        .flat_map(|weighted| [false, true].map(|cancel| (weighted, cancel)))
    {
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
        let r = Resources::new(usage.clone(), lease).unwrap();
        let raw = Raw::new(1_024, 1, weighted, &r);
        let op = support::operation(3, 1_024);
        // Move the only Resources owner; retaining r here would mask ordering.
        let prepared = if weighted {
            Prepared::Sssp(
                SsspPartition::prepare(op, 0, 7, &raw.ids, &raw.sssp, sssp_options(), r).unwrap(),
            )
        } else {
            Prepared::Bfs(
                BfsPartition::prepare(op, 0, 7, &raw.ids, &raw.bfs, bfs_options(), r).unwrap(),
            )
        };
        drop(raw);
        assert_eq!(at_release.load(Ordering::SeqCst), usize::MAX);
        assert!(usage.usage().unwrap().live_bytes > 0);
        if cancel {
            usage.cancel().unwrap();
            assert!(prepared.finish().is_err());
        } else {
            drop(prepared);
        }
        assert_eq!(at_release.load(Ordering::SeqCst), 0);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
}

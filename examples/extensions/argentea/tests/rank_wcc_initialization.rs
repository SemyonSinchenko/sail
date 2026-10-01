//! Matched requested heap and admission at the raw-input ownership boundary.
#[path = "../src/adjacency_tests/allocations.rs"]
mod allocations;
mod rank_wcc_support;
use rank_wcc_support::*;
use sail_argentea_core::*;
use std::sync::atomic::Ordering;

fn cost(n: usize, degree: usize, kind: Kind, release: bool) -> (allocations::Counts, usize, usize) {
    let (r, usage, drops) = resources(LIMIT);
    let op = operation(3, n as u64);
    let (part, counts) = allocations::measure(|| {
        // Count the raw allocations too: their later deallocation must not
        // subtract bytes that were absent from the measured interval.
        let raw = Raw::new(n, degree, &r);
        if release {
            let prepared = Prepared::new(kind, op, &raw, r.clone()).unwrap();
            drop(raw);
            prepared.finish().unwrap()
        } else {
            Partition::build(kind, op, &raw, r.clone()).unwrap()
        }
    });
    part.check(n);
    let used = usage.usage().unwrap();
    println!(
        "RANK_WCC_INPUT_LIFETIME n={n} degree={degree} kind={kind:?} release={release} allocation_calls={} allocated_bytes={} peak_requested_bytes={} retained_admitted_bytes={} peak_admitted_bytes={} work={}",
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
fn release_reduces_heap_overlap_without_changing_allocation_volume_or_work() {
    for n in [1, 1_024, 65_536] {
        for degree in [0, 1, 8] {
            for kind in KINDS {
                let old = cost(n, degree, kind, false);
                let new = cost(n, degree, kind, true);
                assert_eq!(
                    (new.0.calls, new.0.bytes, new.2),
                    (old.0.calls, old.0.bytes, old.2)
                );
                assert!(new.0.peak <= old.0.peak);
                // At nontrivial n, rank/root initialization overlaps at least
                // one full n*8 buffer on the borrowed path. Tiny fixtures can
                // instead peak on fixed bookkeeping; retain those cells too.
                if n >= 1_024 {
                    assert!(old.0.peak >= new.0.peak + n * 8);
                }
                // Conservative CSR construction may still dominate admission.
                assert!(new.1 <= old.1);
            }
        }
    }
}
#[test]
fn release_creates_finish_headroom_while_the_blocker_remains_live() {
    for kind in KINDS {
        for release in [false, true] {
            let (r, usage, _) = resources(LIMIT);
            let raw = Raw::new(1_024, 1, &r);
            let ready = Prepared::new(kind, operation(3, 1_024), &raw, r.clone()).unwrap();
            let before = usage.usage().unwrap().live_bytes;
            // 4KiB is smaller than the 8KiB rank/root vector; releasing raw
            // vertices+edges adds 24KiB. The blocker stays throughout finish.
            let blocker = usage.reserve(LIMIT - before - 4_096).unwrap();
            let raw = if release {
                drop(raw);
                None
            } else {
                Some(raw)
            };
            let result = ready.finish();
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
fn prepare_preserves_input_validation_and_cancellation_errors() {
    for kind in KINDS {
        for malformed in 0..6 {
            let (r, usage, _) = resources(LIMIT);
            let mut raw = Raw::new(3, 1, &r);
            let mut op = operation(3, 3);
            match malformed {
                0 => raw.ids[1] = raw.ids[0],
                1 => raw.ids[1] = 1,
                2 => raw.edges[0].0 = 30,
                3 => op.generation = 0,
                4 => op.vertices = 1,
                5 => usage.cancel().unwrap(),
                _ => unreachable!(),
            }
            let old = Partition::build(kind, op.clone(), &raw, r.clone())
                .err()
                .unwrap();
            let new = Prepared::new(kind, op, &raw, r.clone()).err().unwrap();
            assert_eq!(new, old);
            drop(raw);
            assert_eq!(usage.usage().unwrap().live_bytes, 0);
        }
    }
    // Options still fail before malformed graph identity; zero rounds remains
    // valid construction and is dealt with by the existing convergence logic.
    for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
        let (r, _, _) = resources(LIMIT);
        let opts = WccOptions {
            algorithm,
            seed: 7,
            max_rounds: u64::MAX,
        };
        let mut op = operation(1, 1);
        op.generation = 0;
        let old = WccPartition::build(op.clone(), 0, 7, &[0], &[], opts, r.clone())
            .err()
            .unwrap();
        let new = WccPartition::prepare(op, 0, 7, &[0], &[], opts, r.clone())
            .err()
            .unwrap();
        assert_eq!(old, "WCC phase budget overflow");
        assert_eq!(new, old);
        let opts = WccOptions {
            max_rounds: 0,
            ..opts
        };
        WccPartition::prepare(operation(1, 1), 0, 7, &[0], &[], opts, r)
            .unwrap()
            .finish()
            .unwrap();
    }
}

//! Baseline-compatible finish controls: the old source fails the reuse bound.
use super::*;
use crate::adjacency_tests::allocations::{measure, watch};
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_native_resource_ffi::MemoryLease;
use std::sync::atomic::{AtomicUsize, Ordering};
mod controls;

const LIMIT: usize = 128 << 20;
#[derive(Default)]
struct Observation {
    callbacks: AtomicUsize,
    admitted: AtomicUsize,
    watched_live: AtomicUsize,
}
struct LeaseProbe(ExecutionContext, Arc<Observation>);
impl Drop for LeaseProbe {
    fn drop(&mut self) {
        self.1.callbacks.fetch_add(1, Ordering::SeqCst);
        self.1
            .admitted
            .store(self.0.usage().unwrap().live_bytes, Ordering::SeqCst);
        self.1
            .watched_live
            .store(watch::snapshot().live, Ordering::SeqCst);
    }
}
struct Fixture {
    part: SsspPartition,
    phase: Round,
    execution: ExecutionContext,
    observed: Arc<Observation>,
}
fn barrier(parts: &mut [SsspPartition]) -> Round {
    let phase = Round {
        operation: parts[0].operation.clone(),
        number: parts[0].next_phase(),
    };
    let reports = parts
        .iter()
        .map(|p| p.statistics().unwrap())
        .collect::<Vec<_>>();
    for part in &mut *parts {
        for report in reports.iter().rev() {
            part.receive_statistics(report).unwrap();
        }
        part.finish_statistics(&phase).unwrap();
    }
    let mut cursors = parts
        .iter_mut()
        .map(|p| p.start_emission(&phase).unwrap())
        .collect::<Vec<_>>();
    for cursor in cursors.iter_mut().rev() {
        while let Some(message) = cursor.next_values().unwrap() {
            parts[message.recipient]
                .receive_values(&phase, message)
                .unwrap();
        }
    }
    for cursor in cursors {
        let completion = cursor.finish().unwrap();
        for part in &mut *parts {
            part.finish_producer(&completion).unwrap();
        }
    }
    phase
}
fn fixture(n: usize, algorithm: SsspAlgorithm, wanted: &str) -> Fixture {
    let execution = ExecutionContext::new(ExecutionLimits {
        memory_bytes: LIMIT,
        work_units: usize::MAX,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let observed = Arc::new(Observation::default());
    let r = Resources::new(
        execution.clone(),
        MemoryLease::new(
            Arc::new(LeaseProbe(execution.clone(), observed.clone())),
            LIMIT as u64,
        ),
    )
    .unwrap();
    let operation = Operation {
        package: "sssp-buffer-test".into(),
        session: "session".into(),
        operation: "operation".into(),
        snapshot: "snapshot".into(),
        generation: 1,
        partitions: 3,
        vertices: n as u64,
    };
    let ids = (0..n).map(|i| i as i64 * 3).collect::<Vec<_>>();
    let edges = if n >= 4 {
        vec![
            (0, 3, 1.0),
            (0, 6, 9.0),
            (3, 6, 1.0),
            (6, 9, 0.0),
            (0, 3, 1.0),
        ]
    } else {
        vec![(0, 0, 0.0)]
    };
    let options = SsspOptions {
        source: 0,
        algorithm,
        max_rounds: 100,
        delta: 4.0,
    };
    let mut parts = (0..3)
        .map(|p| {
            SsspPartition::build(
                operation.clone(),
                p,
                p as u64,
                if p == 0 { &ids } else { &[] },
                if p == 0 { &edges } else { &[] },
                options,
                r.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    drop(r);
    let phase = loop {
        let phase = barrier(&mut parts);
        let State::Receiving(inbox) = &parts[0].state else {
            unreachable!()
        };
        let matched = match wanted {
            "topology" => inbox.mode == SsspMode::Topology,
            "active" => matches!(inbox.mode, SsspMode::Reference | SsspMode::DeltaStar),
            "done" => inbox.mode == SsspMode::Done,
            _ => unreachable!(),
        };
        if matched {
            break phase;
        }
        for part in &mut parts {
            part.finish(&phase).unwrap();
        }
        assert!(parts[0].next_phase() < 20);
    };
    let part = parts.remove(0);
    drop(parts);
    Fixture {
        part,
        phase,
        execution,
        observed,
    }
}
fn pointers(part: &SsspPartition) -> Vec<usize> {
    let State::Receiving(inbox) = &part.state else {
        unreachable!()
    };
    let mut pointers = vec![part.values.labels.as_ptr() as usize];
    if !inbox.candidates.is_empty() {
        pointers.push(inbox.candidates.as_ptr() as usize);
    }
    pointers
}
fn check_release(f: Fixture) {
    let observed = f.observed.clone();
    let execution = f.execution.clone();
    drop(f);
    assert_eq!(observed.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(observed.admitted.load(Ordering::SeqCst), 0);
    assert_eq!(observed.watched_live.load(Ordering::SeqCst), 0);
    assert_eq!(execution.usage().unwrap().live_bytes, 0);
}

#[test]
fn finish_allocation_inventory_and_transferred_pointer() {
    let mut failures = Vec::new();
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for n in [1, 1024, 65536] {
            for mode in ["topology", "active", "done"] {
                let mut f = fixture(n, algorithm, mode);
                let State::Receiving(inbox) = &f.part.state else {
                    unreachable!()
                };
                let candidate_capacity = inbox.candidates.capacity();
                assert_eq!(candidate_capacity, if mode == "done" { 0 } else { n });
                let old = f.part.values.clone();
                let old_labels = old.labels.clone();
                let pointers = pointers(&f.part);
                let label_bytes = n * size_of::<Option<SsspLabel>>();
                let _watch = watch::start(label_bytes, &pointers);
                // Lift the historical admission peak without changing live bytes.
                let before = f.execution.usage().unwrap();
                assert!(before.peak_bytes < LIMIT / 2);
                let lift = f.execution.reserve(LIMIT / 2 - before.live_bytes).unwrap();
                let before = f.execution.usage().unwrap();
                let (result, cost) = measure(|| f.part.finish(&f.phase));
                result.unwrap();
                let after = f.execution.usage().unwrap();
                let labels = watch::snapshot();
                let reused = f.part.values.labels.as_ptr() as usize == *pointers.last().unwrap();
                println!(
                    "SSSP_REUSE algorithm={algorithm:?} local_vertices={n} partitions=3 mode={mode} candidate_capacity={candidate_capacity} calls={} bytes={} requested_peak={} matched_allocations={} matched_peak={} transferred={} admitted_before={} admitted_after={} finish_peak_admitted_delta={} work={}",
                    cost.calls,
                    cost.bytes,
                    cost.peak,
                    labels.allocations,
                    labels.peak,
                    usize::from(reused),
                    before.live_bytes,
                    after.live_bytes,
                    after.peak_bytes - before.peak_bytes,
                    after.counted_work().unwrap() - before.counted_work().unwrap()
                );
                assert_eq!(old.labels, old_labels, "published snapshot mutated");
                assert_eq!(f.part.next_phase(), f.phase.number + 1);
                if !reused || (n >= 1024 && labels.allocations != 0) {
                    failures.push((algorithm, n, mode));
                }
                drop(lift);
                drop(old_labels);
                drop(old);
                check_release(f);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "finish allocated/copied labels: {failures:?}"
    );
}

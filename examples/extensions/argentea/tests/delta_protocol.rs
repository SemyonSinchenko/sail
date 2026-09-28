mod delta_support;
use delta_support::*;
use sail_argentea_core::{DeltaMode, DeltaPartition};
use std::sync::{Arc, Mutex, atomic::Ordering, mpsc::sync_channel};

#[test]
fn statistics_barrier_rejects_replay_drift_missing_and_wrong_cardinality() {
    for defect in 0..9 {
        let op = operation(2, if defect == 8 { 4 } else { 3 });
        let (resources, _, _) = resources(1024 * 1024);
        let mut parts = partitions(&op, &[0, 1, 2], &[(0, 1)], options(), &resources);
        let phase = phase(&op, &parts);
        let mut report = parts[0].statistics().unwrap();
        match defect {
            0 => Arc::make_mut(&mut report.phase).number += 1,
            1 => Arc::make_mut(&mut report.phase).operation.generation += 1,
            2 => report.pushes += 1,
            3 => report.options.tolerance *= 2.0,
            4 => report.mass = f64::NAN,
            5 => report.min_score = -1.0,
            6 => {
                parts[1].receive_statistics(&report).unwrap();
            }
            7 => {
                assert!(parts[1].start_emission(&phase).is_err());
                continue;
            }
            8 => {
                stats(&mut parts).unwrap();
                assert!(parts[0].start_emission(&phase).is_err());
                continue;
            }
            _ => unreachable!(),
        }
        assert!(parts[1].receive_statistics(&report).is_err());
        assert!(parts[1].rank_cursor().is_err());
    }
}

#[test]
fn invalid_or_truncated_updates_do_not_publish_candidate_scores() {
    for defect in 0..11 {
        let op = operation(1, 3);
        let (resources, usage, _) = resources(1024 * 1024);
        let mut parts = partitions(&op, &[0, 1, 2], &[(0, 1), (1, 1)], options(), &resources);
        stats(&mut parts).unwrap();
        let phase = phase(&op, &parts);
        let before = parts[0].state_rows().collect::<Vec<_>>();
        let mut cursor = parts[0].start_emission(&phase).unwrap();
        let mut message = cursor.next_update().unwrap().unwrap();
        match defect {
            0 => message.sequence += 1,
            1 => message.mode = DeltaMode::Push,
            2 => message.value = -0.1,
            3 => message.value = f64::NAN,
            4 => message.target = 99,
            5 => Arc::make_mut(&mut message.phase).operation.snapshot = "foreign".into(),
            6 => {
                parts[0].receive(&message).unwrap();
            }
            7 => {
                assert!(parts[0].finish(&phase).is_err());
                drop(cursor);
                assert_eq!(parts[0].state_rows().collect::<Vec<_>>(), before);
                continue;
            }
            8..=10 => {
                parts[0].receive(&message).unwrap();
                while let Some(m) = cursor.next_update().unwrap() {
                    parts[0].receive(&m).unwrap();
                }
                let mut completion = cursor.finish().unwrap();
                match defect {
                    8 => completion.vertices += 1,
                    9 => completion.sequences[0] += 1,
                    10 => {
                        parts[0].finish_producer(&completion).unwrap();
                    }
                    _ => unreachable!(),
                }
                assert!(parts[0].finish_producer(&completion).is_err());
                assert_eq!(parts[0].state_rows().collect::<Vec<_>>(), before);
                continue;
            }
            _ => unreachable!(),
        }
        assert!(parts[0].receive(&message).is_err());
        assert_eq!(parts[0].state_rows().collect::<Vec<_>>(), before);
        drop(cursor);
        assert!(usage.checkpoint().is_err());
    }
}

#[test]
fn incremental_admission_failure_preserves_old_state_and_releases_every_buffer() {
    let op = operation(1, 3);
    let limit = 64 * 1024;
    let (resources, usage, drops) = resources(limit);
    let mut parts = partitions(&op, &[0, 1, 2], &[(0, 1)], options(), &resources);
    stats(&mut parts).unwrap();
    let phase = phase(&op, &parts);
    let before = parts[0].state_rows().collect::<Vec<_>>();
    let live = usage.usage().unwrap().live_bytes;
    let blocker = usage.reserve(limit - live - 200).unwrap();
    assert!(parts[0].start_emission(&phase).is_err());
    assert_eq!(parts[0].state_rows().collect::<Vec<_>>(), before);
    drop(parts);
    drop(blocker);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    drop(resources);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn partitions_share_one_budget_and_statistics_keep_the_host_lease() {
    let op = operation(2, 2);
    let (resources, usage, drops) = resources(12 * 1024);
    let a = DeltaPartition::build(op.clone(), 0, &[0], &[], options(), resources.clone()).unwrap();
    let live = usage.usage().unwrap().live_bytes;
    let blocker = usage.reserve(12 * 1024 - live - 4096).unwrap();
    assert!(DeltaPartition::build(op, 1, &[1], &[], options(), resources.clone()).is_err());
    drop(blocker);
    let report = a.statistics().unwrap();
    drop(a);
    drop(resources);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(report);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_emission_can_cross_backpressure_without_an_owner_lock() {
    let op = operation(1, 3);
    let (resources, usage, _) = resources(64 * 1024);
    let mut parts = partitions(
        &op,
        &[0, 1, 2],
        &[(0, 1), (0, 2), (1, 2)],
        options(),
        &resources,
    );
    stats(&mut parts).unwrap();
    let phase = phase(&op, &parts);
    let part = Arc::new(Mutex::new(parts.pop().unwrap()));
    let mut cursor = part.lock().unwrap().start_emission(&phase).unwrap();
    let (tx, rx) = sync_channel(1);
    let producer = std::thread::spawn(move || {
        while let Some(update) = cursor.next_update().unwrap() {
            tx.send(update).unwrap();
        }
        cursor.finish().unwrap()
    });
    for update in rx {
        part.lock().unwrap().receive(&update).unwrap();
    }
    let completion = producer.join().unwrap();
    {
        let mut state = part.lock().unwrap();
        state.finish_producer(&completion).unwrap();
        state.finish(&phase).unwrap();
    }
    drop(part);
    drop(completion);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn dropping_output_cancels_pending_work_and_live_updates_retain_admission() {
    let op = operation(1, 3);
    let (resources, usage, drops) = resources(64 * 1024);
    let mut parts = partitions(&op, &[0, 1, 2], &[(0, 1), (0, 2)], options(), &resources);
    stats(&mut parts).unwrap();
    let phase = phase(&op, &parts);
    let mut cursor = parts[0].start_emission(&phase).unwrap();
    let update = cursor.next_update().unwrap().unwrap();
    drop(cursor);
    assert!(parts[0].receive(&update).is_err());
    assert!(usage.checkpoint().is_err());
    drop(parts);
    drop(resources);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(update);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn options_phase_budget_and_shared_work_limit_are_enforced() {
    use grust_procedures::{ExecutionContext, ExecutionLimits};
    use sail_argentea_core::Resources;
    use sail_native_resource_ffi::MemoryLease;
    for damping in [f64::NAN, f64::INFINITY, -0.1, 1.0] {
        assert!(
            sail_argentea_core::DeltaOptions {
                damping,
                ..options()
            }
            .validate()
            .is_err()
        );
    }
    for tolerance in [f64::NAN, f64::INFINITY, 0.0, -1.0] {
        assert!(
            sail_argentea_core::DeltaOptions {
                tolerance,
                ..options()
            }
            .validate()
            .is_err()
        );
    }
    for k in [0, 1, 7, 32] {
        assert_eq!(
            sail_argentea_core::DeltaOptions {
                max_pushes: k,
                ..options()
            }
            .native_phase_bound()
            .unwrap(),
            4 * k + 4
        );
    }
    assert!(
        sail_argentea_core::DeltaOptions {
            max_pushes: u64::MAX,
            ..options()
        }
        .validate()
        .is_err()
    );
    let usage = ExecutionContext::new(ExecutionLimits {
        memory_bytes: 64 * 1024,
        work_units: 10_000,
        batch_rows: 16,
        deadline: None,
    })
    .unwrap();
    let resources =
        Resources::new(usage.clone(), MemoryLease::new(Arc::new(()), 64 * 1024)).unwrap();
    let op = operation(1, 2);
    let mut parts = partitions(&op, &[0, 1], &[(0, 1)], options(), &resources);
    stats(&mut parts).unwrap();
    let phase = phase(&op, &parts);
    let before = parts[0].state_rows().collect::<Vec<_>>();
    // Exhaust the shared operation budget after graph admission. No timer or
    // guessed fixture cost controls whether the next phase has work available.
    usage
        .charge_work(10_000 - usage.usage().unwrap().counted_work().unwrap())
        .unwrap();
    assert!(parts[0].start_emission(&phase).is_err());
    assert_eq!(parts[0].state_rows().collect::<Vec<_>>(), before);
    drop(parts);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn borrowed_statistics_and_phase_readiness_use_the_same_state_machine() {
    let op = operation(1, 2);
    let (resources, _, _) = resources(64 * 1024);
    let mut parts = partitions(&op, &[0, 1], &[(0, 1)], options(), &resources);
    assert_eq!(parts[0].collecting_phase(), Some(0));
    assert_eq!(parts[0].receiving_phase(), None);
    let report = parts[0].statistics().unwrap();
    parts[0]
        .receive_statistics_values(&report.phase, report.producer, report.values())
        .unwrap();
    let round = phase(&op, &parts);
    let cursor = parts[0].start_emission(&round).unwrap();
    assert_eq!(parts[0].collecting_phase(), None);
    assert_eq!(parts[0].receiving_phase(), Some(0));
    drop(cursor);
    assert!(
        parts[0]
            .receive_statistics_values(&report.phase, report.producer, report.values())
            .is_err()
    );
}

#[test]
fn typed_cap_evidence_requires_a_fresh_complete_failed_certificate() {
    use sail_argentea_core::DeltaCapFailure;
    for stationary in [false, true] {
        let op = operation(3, 3);
        let (resources, usage, _) = resources(1024 * 1024);
        let mut opts = options();
        opts.max_pushes = 0;
        let edges = if stationary {
            vec![(0, 1), (1, 2), (2, 0)]
        } else {
            vec![(0, 1)]
        };
        let mut parts = partitions(&op, &[0, 1, 2], &edges, opts, &resources);
        let phase0 = phase(&op, &parts);
        assert!(parts[0].cap_failure(&phase0).is_err());
        stats(&mut parts).unwrap();
        assert_eq!(parts[0].cap_failure(&phase0).unwrap(), None);
        assert_eq!(exchange(&op, &mut parts).unwrap().0, DeltaMode::Certify);
        let phase1 = phase(&op, &parts);
        assert!(parts[0].cap_failure(&phase1).is_err());
        stats(&mut parts).unwrap();
        let failure = parts[0].cap_failure(&phase1).unwrap();
        if stationary {
            assert_eq!(failure, None);
            assert!(parts[0].seal(&phase1).unwrap().is_some());
        } else {
            let failure: DeltaCapFailure = failure.unwrap();
            assert_eq!(failure.pushes, 0);
            assert_eq!(failure.max_pushes, 0);
            assert_eq!(failure.certificate_passes, 1);
            assert!(failure.residual_l1 > failure.tolerance);
            assert_eq!(parts[0].seal(&phase1).unwrap_err(), failure.to_string());
        }
        usage.cancel().unwrap();
        assert!(parts[1].cap_failure(&phase1).is_err());
    }
}

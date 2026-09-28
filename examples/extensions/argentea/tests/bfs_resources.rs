mod bfs_support;
use bfs_support::*;
use sail_argentea_core::*;
use std::sync::{Arc, Mutex, atomic::Ordering, mpsc::sync_channel};

#[test]
fn bounded_one_message_channel_completes_topology_and_pull_without_owner_lock() {
    let op = operation(1, 4);
    let (res, usage, _) = resources(1024 * 1024);
    let mut ps = parts(
        &op,
        &[0, 1, 2, 3],
        &[(0, 1), (0, 2), (1, 3), (2, 3)],
        options(0, BfsAlgorithm::DirectionOptimizing),
        &res,
    )
    .unwrap();
    let owner = Arc::new(Mutex::new(ps.pop().unwrap()));
    for expected in [BfsMode::Topology, BfsMode::Pull, BfsMode::Pull] {
        let phase;
        let mut cursor;
        {
            let mut state = owner.lock().unwrap();
            phase = Round {
                operation: op.clone(),
                number: state.next_phase(),
            };
            let report = state.statistics().unwrap();
            state.receive_statistics(&report).unwrap();
            state.finish_statistics(&phase).unwrap();
            cursor = state.start_emission(&phase).unwrap();
            assert_eq!(cursor.mode(), expected);
        }
        let (tx, rx) = sync_channel(1);
        let sender = std::thread::spawn(move || {
            while let Some(message) = cursor.next_update().unwrap() {
                tx.send(message).unwrap();
            }
            cursor.finish().unwrap()
        });
        for message in rx {
            owner.lock().unwrap().receive(&message).unwrap();
        }
        let complete = sender.join().unwrap();
        let mut state = owner.lock().unwrap();
        state.finish_producer(&complete).unwrap();
        state.finish(&phase).unwrap();
    }
    assert_eq!(
        owner
            .lock()
            .unwrap()
            .state_rows()
            .find(|r| r.id == 3)
            .unwrap()
            .parent,
        Some(1)
    );
    drop(owner);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn quota_failures_preserve_committed_labels_and_all_leases_release() {
    for when in [0, 1] {
        let op = operation(1, 3);
        let limit = 1024 * 1024;
        let (res, usage, drops) = resources(limit);
        let mut ps = parts(
            &op,
            &[0, 1, 2],
            &[(0, 1), (1, 2)],
            options(0, BfsAlgorithm::DirectionOptimizing),
            &res,
        )
        .unwrap();
        setup(&op, &mut ps).unwrap();
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let blocker;
        if when == 0 {
            blocker = usage
                .reserve(limit - usage.usage().unwrap().live_bytes - 128)
                .unwrap();
            assert!(ps[0].start_emission(&phase).is_err());
        } else {
            let mut cursor = ps[0].start_emission(&phase).unwrap();
            while let Some(m) = cursor.next_update().unwrap() {
                ps[0].receive(&m).unwrap();
            }
            let c = cursor.finish().unwrap();
            ps[0].finish_producer(&c).unwrap();
            blocker = usage
                .reserve(limit - usage.usage().unwrap().live_bytes - 128)
                .unwrap();
            assert!(ps[0].finish(&phase).is_err());
        }
        assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
        assert!(ps[0].row_cursor().is_err());
        drop(ps);
        drop(blocker);
        drop(res);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn retained_message_statistics_and_result_snapshot_keep_shared_pool_lease() {
    let op = operation(1, 2);
    let (res, usage, drops) = resources(1024 * 1024);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(0, BfsAlgorithm::Frontier),
        &res,
    )
    .unwrap();
    let report = ps[0].statistics().unwrap();
    run(&op, &mut ps).unwrap();
    let mut cursor = ps[0].row_cursor().unwrap();
    drop(ps);
    drop(res);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(cursor.next_row().unwrap().unwrap().distance, Some(0));
    drop(cursor);
    assert!(usage.usage().unwrap().live_bytes > 0);
    drop(report);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (res, usage, drops) = resources(1024 * 1024);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(0, BfsAlgorithm::Frontier),
        &res,
    )
    .unwrap();
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let mut cursor = ps[0].start_emission(&phase).unwrap();
    let message = cursor.next_update().unwrap().unwrap();
    drop(cursor);
    assert!(usage.checkpoint().is_err());
    assert!(ps[0].receive(&message).is_err());
    drop(ps);
    drop(res);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(message);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_and_work_exhaustion_block_before_publication_and_done_is_idle() {
    let op = operation(3, 1);
    let (res, usage, _) = resources(1024 * 1024);
    let mut ps = parts(&op, &[0], &[], options(0, BfsAlgorithm::Frontier), &res).unwrap();
    setup(&op, &mut ps).unwrap();
    stats(&op, &mut ps).unwrap();
    exchange(&op, &mut ps).unwrap();
    for _ in 0..3 {
        stats(&op, &mut ps).unwrap();
        assert_eq!(exchange(&op, &mut ps).unwrap(), BfsMode::Done);
        assert!(
            ps.iter()
                .all(|p| p.levels() == 1 && p.last_work() == BfsWork::default())
        );
    }
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    for p in &mut ps {
        assert_eq!(
            p.seal(&phase).unwrap(),
            Some(BfsConvergence {
                levels: 1,
                reached: 1
            })
        );
    }
    usage.cancel().unwrap();
    assert!(ps[0].row_cursor().is_err());
    let (res, usage, _) = resources(1024 * 1024);
    let mut ps = parts(&op, &[0], &[], options(0, BfsAlgorithm::Frontier), &res).unwrap();
    stats(&op, &mut ps).unwrap();
    let before = ps[0].state_rows().collect::<Vec<_>>();
    usage
        .charge_work(usize::MAX - usage.usage().unwrap().counted_work().unwrap())
        .unwrap();
    assert!(
        ps[0]
            .start_emission(&Round {
                operation: op,
                number: 0
            })
            .is_err()
    );
    assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
}

#[test]
fn shared_quota_and_incremental_incoming_admission_are_real() {
    let op = operation(2, 2);
    let limit = 32 * 1024;
    let (res, usage, drops) = resources(limit);
    let a = BfsPartition::build(
        op.clone(),
        0,
        100,
        &[0],
        &[(0, 1)],
        options(0, BfsAlgorithm::DirectionOptimizing),
        res.clone(),
    )
    .unwrap();
    let blocker = usage
        .reserve(limit - usage.usage().unwrap().live_bytes - 4000)
        .unwrap();
    assert!(
        BfsPartition::build(
            op,
            1,
            101,
            &[1],
            &[],
            options(0, BfsAlgorithm::DirectionOptimizing),
            res.clone()
        )
        .is_err()
    );
    drop(a);
    drop(blocker);
    drop(res);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let op = operation(1, 2);
    let (res, usage, _) = resources(limit);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(0, BfsAlgorithm::DirectionOptimizing),
        &res,
    )
    .unwrap();
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let mut cursor = ps[0].start_emission(&phase).unwrap();
    let message = cursor.next_update().unwrap().unwrap();
    let blocker = usage
        .reserve(limit - usage.usage().unwrap().live_bytes - 200)
        .unwrap();
    assert!(ps[0].receive(&message).is_err());
    assert_eq!(ps[0].incoming_identity(), None);
    drop(message);
    drop(cursor);
    drop(ps);
    drop(blocker);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn poisoned_emission_cannot_resume_after_message_admission_failure() {
    let op = operation(1, 2);
    let limit = 32 * 1024;
    let (res, usage, _) = resources(limit);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(0, BfsAlgorithm::Frontier),
        &res,
    )
    .unwrap();
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let mut cursor = ps[0].start_emission(&phase).unwrap();
    let blocker = usage
        .reserve(limit - usage.usage().unwrap().live_bytes - 200)
        .unwrap();
    assert!(cursor.next_update().is_err());
    drop(blocker);
    // Retrying after quota is returned cannot silently skip the advanced edge.
    assert!(cursor.next_update().is_err());
    assert!(usage.checkpoint().is_err());
}

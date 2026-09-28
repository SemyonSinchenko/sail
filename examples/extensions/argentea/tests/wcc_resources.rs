mod wcc_support;
use sail_argentea_core::*;
use std::sync::{Arc, Mutex, atomic::Ordering, mpsc::sync_channel};
use wcc_support::*;
#[test]
fn bounded_one_message_channel_all_star_phases_release_owner_lock() {
    let op = operation(1, 3);
    let (res, usage, _) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0, 1, 2],
        &[(0, 1), (1, 2)],
        options(WccAlgorithm::StarContraction, 42),
        &res,
    )
    .unwrap();
    let owner = Arc::new(Mutex::new(ps.pop().unwrap()));
    let mut modes = Vec::new();
    loop {
        let phase;
        let mut cursor;
        {
            let mut s = owner.lock().unwrap();
            phase = Round {
                operation: op.clone(),
                number: s.next_phase(),
            };
            let report = s.statistics().unwrap();
            s.receive_statistics(&report).unwrap();
            s.finish_statistics(&phase).unwrap();
            if s.seal(&phase).unwrap().is_some() {
                break;
            }
            cursor = s.start_emission(&phase).unwrap();
            modes.push(cursor.mode());
        }
        let (tx, rx) = sync_channel(1);
        let sender = std::thread::spawn(move || {
            while let Some(m) = cursor.next_update().unwrap() {
                tx.send(m).unwrap();
            }
            cursor.finish().unwrap()
        });
        for message in rx {
            owner.lock().unwrap().receive(&message).unwrap();
        }
        let complete = sender.join().unwrap();
        let mut s = owner.lock().unwrap();
        s.finish_producer(&complete).unwrap();
        s.finish(&phase).unwrap();
    }
    for mode in [
        WccMode::Topology,
        WccMode::Neighbors,
        WccMode::HookRoute,
        WccMode::HookReturn,
        WccMode::NormalizeRoute,
        WccMode::NormalizeReturn,
    ] {
        assert!(modes.contains(&mode));
    }
    assert!(owner.lock().unwrap().state_rows().all(|r| r.component == 0));
    drop(owner);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}
#[test]
fn quota_failure_during_start_receive_and_publish_preserves_committed_roots() {
    for step in 0..3 {
        let op = operation(1, 2);
        let limit = 1 << 20;
        let (res, usage, drops) = resources(limit);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 42),
            &res,
        )
        .unwrap();
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let blocker;
        if step == 0 {
            blocker = usage
                .reserve(limit - usage.usage().unwrap().live_bytes - 64)
                .unwrap();
            assert!(ps[0].start_emission(&phase).is_err());
        } else {
            let mut c = ps[0].start_emission(&phase).unwrap();
            if step == 1 {
                let message = c.next_update().unwrap().unwrap();
                blocker = usage
                    .reserve(limit - usage.usage().unwrap().live_bytes - 64)
                    .unwrap();
                assert!(ps[0].receive(&message).is_err());
            } else {
                while let Some(message) = c.next_update().unwrap() {
                    ps[0].receive(&message).unwrap();
                }
                let done = c.finish().unwrap();
                ps[0].finish_producer(&done).unwrap();
                blocker = usage
                    .reserve(limit - usage.usage().unwrap().live_bytes - 64)
                    .unwrap();
                assert!(ps[0].finish(&phase).is_err());
            }
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
fn retained_statistics_result_and_message_keep_lease_until_last_owner() {
    let op = operation(1, 2);
    let (res, usage, drops) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(WccAlgorithm::StarContraction, 42),
        &res,
    )
    .unwrap();
    let report = ps[0].statistics().unwrap();
    run(&op, &mut ps).unwrap();
    let mut cursor = ps[0].row_cursor().unwrap();
    drop(ps);
    drop(res);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(cursor.next_row().unwrap().unwrap().component, 0);
    drop(cursor);
    assert!(usage.usage().unwrap().live_bytes > 0);
    drop(report);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (res, usage, drops) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(WccAlgorithm::Reference, 0),
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
    drop(message);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn message_admission_failure_cannot_resume_cursor_and_skip_an_edge() {
    let op = operation(1, 2);
    let limit = 1 << 20;
    let (res, usage, _) = resources(limit);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(WccAlgorithm::Reference, 0),
        &res,
    )
    .unwrap();
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let mut c = ps[0].start_emission(&phase).unwrap();
    let blocker = usage
        .reserve(limit - usage.usage().unwrap().live_bytes - 64)
        .unwrap();
    assert!(c.next_update().is_err());
    drop(blocker);
    assert!(c.next_update().is_err());
    assert!(usage.checkpoint().is_err());
}
#[test]
fn done_relays_have_no_graph_work_and_cancellation_blocks_publication() {
    let op = operation(3, 1);
    let (res, usage, _) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0],
        &[],
        options(WccAlgorithm::StarContraction, 0),
        &res,
    )
    .unwrap();
    for _ in 0..4 {
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
    }
    for _ in 0..3 {
        stats(&op, &mut ps).unwrap();
        assert_eq!(exchange(&op, &mut ps).unwrap(), WccMode::Done);
        assert!(
            ps.iter()
                .all(|p| p.last_work() == WccWork::default() && p.rounds() == 0)
        );
    }
    stats(&op, &mut ps).unwrap();
    usage.cancel().unwrap();
    let phase = phase(&op, &ps);
    assert!(ps[0].seal(&phase).is_err());
    assert!(ps[0].row_cursor().is_err());
}

#[test]
fn skewed_component_member_growth_is_admitted_before_replacement() {
    let ids = (0..129).collect::<Vec<_>>();
    let edges = (1..129).map(|id| (0, id)).collect::<Vec<_>>();
    let op = operation(11, ids.len() as u64);
    let limit = 4 << 20;
    let (res, usage, drops) = resources(limit);
    let mut ps = parts(
        &op,
        &ids,
        &edges,
        options(WccAlgorithm::StarContraction, 42),
        &res,
    )
    .unwrap();
    loop {
        stats(&op, &mut ps).unwrap();
        let zero = ps
            .iter()
            .map(|p| p.statistics().unwrap().values.crossing)
            .sum::<u64>()
            == 0;
        if ps[0].completed_mode() == WccMode::Neighbors && zero {
            break;
        }
        exchange(&op, &mut ps).unwrap();
        assert!(ps[0].rounds() < 128);
    }
    let root = ps
        .iter()
        .flat_map(WccPartition::state_rows)
        .next()
        .unwrap()
        .component;
    assert!(
        ps.iter()
            .flat_map(WccPartition::state_rows)
            .all(|r| r.component == root)
    );
    let owner = op.owner(root);
    assert!(ps[owner].state_rows().count() < 64);
    let phase = phase(&op, &ps);
    let before = ps[owner].state_rows().collect::<Vec<_>>();
    let mut cs = ps
        .iter_mut()
        .map(|p| p.start_emission(&phase).unwrap())
        .collect::<Vec<_>>();
    assert!(cs.iter().all(|c| c.mode() == WccMode::NormalizeRoute));
    let mut received = 0;
    let blocker;
    'outer: loop {
        for c in &mut cs {
            if let Some(message) = c.next_update().unwrap() {
                assert_eq!(message.recipient, owner);
                if received == 64 {
                    blocker = usage
                        .reserve(limit - usage.usage().unwrap().live_bytes - 64)
                        .unwrap();
                    assert!(ps[owner].receive(&message).is_err());
                    break 'outer;
                }
                ps[owner].receive(&message).unwrap();
                received += 1;
            }
        }
    }
    assert_eq!(received, 64);
    assert_eq!(ps[owner].state_rows().collect::<Vec<_>>(), before);
    assert!(ps[owner].finish(&phase).is_err());
    drop(cs);
    drop(ps);
    drop(blocker);
    drop(res);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

//! Adversarial delivery and transactional publication controls.
use super::*;

#[test]
fn incomplete_and_replayed_statistics_poison_the_partition() {
    for fault in 0..4 {
        let (r, _, _) = support::resources(1 << 20);
        let op = support::operation(2, 2);
        let mut ps = parts(&op, &[-5, 0], &[], options(SsspAlgorithm::DeltaStar), &r);
        let reports = ps
            .iter()
            .map(|p| p.statistics().unwrap())
            .collect::<Vec<_>>();
        let phase = phase(&op, &ps);
        ps[0].receive_statistics(&reports[0]).unwrap();
        let result = match fault {
            0 => ps[0].finish_statistics(&phase),
            1 => ps[0].receive_statistics(&reports[0]),
            2 => {
                let mut v = reports[1].values;
                v.bucket = Some(f64::NAN);
                ps[0].receive_statistics_values(&phase, 1, v)
            }
            _ => {
                let mut foreign = phase.clone();
                foreign.operation.generation += 1;
                ps[0].receive_statistics_values(&foreign, 1, reports[1].values)
            }
        };
        assert!(result.is_err());
        assert!(ps[0].statistics().is_err());
        assert!(ps[0].start_emission(&phase).is_err());
    }
}

#[test]
fn contribution_sequence_origin_mode_bucket_and_eof_are_enforced() {
    for fault in 0..6 {
        let (r, _, _) = support::resources(1 << 20);
        let op = support::operation(2, 2);
        let mut ps = parts(
            &op,
            &[-5, 0],
            &[(-5, 0, 1.0)],
            options(SsspAlgorithm::DeltaStar),
            &r,
        );
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let mut cursors = ps
            .iter_mut()
            .map(|p| p.start_emission(&phase).unwrap())
            .collect::<Vec<_>>();
        let message = cursors[1].next_update().unwrap().unwrap();
        let mut v = message.values;
        let result = match fault {
            0 => {
                v.sequence += 1;
                ps[0].receive_values(&phase, v)
            }
            1 => {
                v.origin.adjacency_id += 99;
                ps[0].receive_values(&phase, v)
            }
            2 => {
                v.mode = SsspMode::Reference;
                ps[0].receive_values(&phase, v)
            }
            3 => {
                v.bucket = Some(0.0);
                ps[0].receive_values(&phase, v)
            }
            4 => {
                ps[0].receive(&message).unwrap();
                ps[0].receive(&message)
            }
            _ => ps[0].finish(&phase),
        };
        assert!(result.is_err());
        assert!(ps[0].row_cursor().is_err());
        assert!(ps[0].receive(&message).is_err());
    }
}

#[test]
fn truncated_producer_and_late_message_cannot_publish_state() {
    for late in [false, true] {
        let (r, _, _) = support::resources(1 << 20);
        let op = support::operation(2, 2);
        let mut ps = parts(
            &op,
            &[-5, 0],
            &[(-5, 0, 1.0)],
            options(SsspAlgorithm::DeltaStar),
            &r,
        );
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let mut cursors = ps
            .iter_mut()
            .map(|p| p.start_emission(&phase).unwrap())
            .collect::<Vec<_>>();
        let mut message = None;
        for c in &mut cursors {
            while let Some(m) = c.next_update().unwrap() {
                if late {
                    ps[m.values.recipient].receive(&m).unwrap();
                }
                message = Some(m);
            }
        }
        let complete = cursors
            .into_iter()
            .map(|c| c.finish().unwrap())
            .collect::<Vec<_>>();
        if late {
            for c in &complete {
                for p in &mut ps {
                    p.finish_producer(c).unwrap();
                }
            }
            assert!(ps[0].receive(message.as_ref().unwrap()).is_err());
        } else {
            assert!(ps[0].finish_producer(&complete[1]).is_err());
        }
        assert!(ps[0].finish(&phase).is_err());
        assert_eq!(ps[0].next_phase(), 0);
    }
}

#[test]
fn overflow_and_successor_admission_fail_without_partial_publication() {
    let (r, _, _) = support::resources(1 << 20);
    let op = support::operation(2, 3);
    let edges = [(-5, 0, f64::MAX), (0, 1, f64::MAX), (-5, 1, 1.0)];
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let (r, _, _) = support::resources(1 << 20);
        let mut ps = parts(&op, &[-5, 0, 1], &edges, options(algorithm), &r);
        assert!(run(&op, &mut ps).unwrap_err().contains("distance overflow"));
        assert!(ps.iter().all(|p| p.row_cursor().is_err()));
    }
    // Exhaust the shared budget after complete receipt but before next snapshot.
    let mut ps = parts(
        &op,
        &[-5, 0, 1],
        &[(-5, 0, 1.0)],
        options(SsspAlgorithm::DeltaStar),
        &r,
    );
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let mut cursors = ps
        .iter_mut()
        .map(|p| p.start_emission(&phase).unwrap())
        .collect::<Vec<_>>();
    for c in &mut cursors {
        while let Some(m) = c.next_update().unwrap() {
            ps[m.values.recipient].receive(&m).unwrap();
        }
    }
    let completions = cursors
        .into_iter()
        .map(|c| c.finish().unwrap())
        .collect::<Vec<_>>();
    for c in &completions {
        for p in &mut ps {
            p.finish_producer(c).unwrap();
        }
    }
    let remaining = r.execution.limits().memory_bytes - r.execution.usage().unwrap().live_bytes;
    let block = r.execution.reserve(remaining).unwrap();
    assert!(ps[0].finish(&phase).unwrap_err().contains("memory"));
    assert_eq!(ps[0].next_phase(), 0);
    assert!(ps[0].row_cursor().is_err());
    drop(block);
}

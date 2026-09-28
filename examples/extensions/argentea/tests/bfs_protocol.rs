mod bfs_support;
use bfs_support::*;
use sail_argentea_core::*;
use std::sync::Arc;

#[test]
fn statistics_reject_replay_foreign_scope_missing_eof_options_and_origin_drift() {
    for defect in 0..13 {
        let op = operation(2, 3);
        let (res, _, _) = resources(1024 * 1024);
        let mut ps = parts(
            &op,
            &[0, 1, 2],
            &[(0, 1)],
            options(0, BfsAlgorithm::Frontier),
            &res,
        )
        .unwrap();
        if defect >= 10 {
            setup(&op, &mut ps).unwrap();
        }
        let phase = phase(&op, &ps);
        let mut report = ps[0].statistics().unwrap();
        match defect {
            0 => Arc::make_mut(&mut report.phase).operation.snapshot = "other".into(),
            1 => Arc::make_mut(&mut report.phase).operation.generation += 1,
            2 => Arc::make_mut(&mut report.phase).number += 1,
            3 => report.values.options.source = 1,
            4 => report.values.levels += 1,
            5 => report.values.frontier = 99,
            6 => report.values.completed = BfsMode::Pull,
            7 => {
                ps[1].receive_statistics(&report).unwrap();
            }
            8 => {
                assert!(ps[1].finish_statistics(&phase).is_err());
                continue;
            }
            9 => {
                let reports = ps
                    .iter()
                    .map(BfsPartition::statistics)
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                for r in &reports {
                    ps[1].receive_statistics(r).unwrap();
                }
                // P reports do not establish EOF. No decision may publish yet.
                assert!(ps[1].start_emission(&phase).is_err());
                continue;
            }
            10 => report.values.origin.worker_id += 1,
            11 => report.values.origin.adjacency_id += 1,
            12 => report.values.arcs += 1,
            _ => unreachable!(),
        }
        assert!(ps[1].receive_statistics(&report).is_err());
        assert!(ps[1].row_cursor().is_err());
    }
}
#[test]
fn topology_stream_rejects_sequence_mode_owner_origin_and_unknown_target() {
    for defect in 0..9 {
        let op = operation(1, 3);
        let (res, _, _) = resources(1024 * 1024);
        let mut ps = parts(
            &op,
            &[0, 1, 2],
            &[(0, 1), (0, 2)],
            options(0, BfsAlgorithm::DirectionOptimizing),
            &res,
        )
        .unwrap();
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let mut cursor = ps[0].start_emission(&phase).unwrap();
        let mut message = cursor.next_update().unwrap().unwrap();
        match defect {
            0 => message.sequence += 1,
            1 => message.mode = BfsMode::Push,
            2 => message.recipient = 1,
            3 => message.origin.worker_id += 1,
            4 => message.origin.adjacency_id += 1,
            5 => {
                message.payload = BfsPayload::Topology {
                    source: 0,
                    target: 99,
                }
            }
            6 => Arc::make_mut(&mut message.phase).operation.operation = "mixed".into(),
            7 => {
                ps[0].receive(&message).unwrap();
            }
            8 => message.payload = BfsPayload::Membership { vertex: 0 },
            _ => unreachable!(),
        }
        assert!(ps[0].receive(&message).is_err());
        assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
    }
}
#[test]
fn missing_late_or_false_completion_never_commits_labels_or_incoming_csr() {
    for defect in 0..7 {
        let op = operation(1, 2);
        let (res, _, _) = resources(1024 * 1024);
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
        if defect == 0 {
            assert!(ps[0].finish(&phase).is_err());
            continue;
        }
        if defect == 1 {
            assert!(cursor.finish().is_err());
            continue;
        }
        if defect != 2 {
            ps[0].receive(&message).unwrap();
        }
        assert!(cursor.next_update().unwrap().is_none());
        let mut complete = cursor.finish().unwrap();
        match defect {
            2 => {}
            3 => complete.sequences[0] += 1,
            4 => complete.origin.worker_id += 1,
            5 => {
                ps[0].finish_producer(&complete).unwrap();
            }
            6 => {
                ps[0].finish_producer(&complete).unwrap();
                assert!(ps[0].receive(&message).is_err());
                continue;
            }
            _ => unreachable!(),
        }
        assert!(ps[0].finish_producer(&complete).is_err());
        assert_eq!(ps[0].incoming_identity(), None);
        assert_eq!(
            ps[0].state_rows().find(|r| r.id == 1).unwrap().distance,
            None
        );
    }
}
#[test]
fn membership_requires_complete_ordered_unique_frontier_before_real_pull() {
    for defect in 0..5 {
        let op = operation(1, 4);
        let (res, _, _) = resources(1024 * 1024);
        let mut ps = parts(
            &op,
            &[0, 1, 2, 3],
            &[(0, 1), (0, 2), (1, 3), (2, 3)],
            options(0, BfsAlgorithm::DirectionOptimizing),
            &res,
        )
        .unwrap();
        setup(&op, &mut ps).unwrap();
        stats(&op, &mut ps).unwrap();
        assert_eq!(exchange(&op, &mut ps).unwrap(), BfsMode::Pull);
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let mut cursor = ps[0].start_emission(&phase).unwrap();
        assert_eq!(cursor.mode(), BfsMode::Pull);
        let first = cursor.next_update().unwrap().unwrap();
        ps[0].receive(&first).unwrap();
        let mut second = cursor.next_update().unwrap().unwrap();
        match defect {
            0 => {
                assert!(ps[0].finish(&phase).is_err());
            }
            1 => {
                second.payload = first.payload;
                assert!(ps[0].receive(&second).is_err());
            }
            2 => {
                second.mode = BfsMode::Push;
                assert!(ps[0].receive(&second).is_err());
            }
            3 => {
                second.origin.adjacency_id += 1;
                assert!(ps[0].receive(&second).is_err());
            }
            4 => {
                assert!(cursor.next_update().unwrap().is_none());
                let c = cursor.finish().unwrap();
                assert!(ps[0].finish_producer(&c).is_err());
            }
            _ => unreachable!(),
        }
        assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
        assert!(ps[0].row_cursor().is_err());
    }
}

#[test]
fn bad_options_source_owner_input_and_phase_overflow_are_rejected() {
    let mut opts = options(0, BfsAlgorithm::Frontier);
    for k in [0, 1, 7, 14, 32, 10000] {
        opts.max_levels = k;
        assert_eq!(opts.native_phase_bound().unwrap(), 2 * k + 4);
        assert!(opts.validate().is_ok());
    }
    opts.max_levels = u64::MAX;
    assert!(opts.validate().is_err());
    opts = options(0, BfsAlgorithm::Frontier);
    opts.alpha = 0;
    assert!(opts.validate().is_err());
    opts.alpha = 14;
    opts.beta = 0;
    assert!(opts.validate().is_err());
    let op = operation(2, 3);
    let (res, _, _) = resources(1024 * 1024);
    assert!(
        BfsPartition::build(
            op.clone(),
            0,
            100,
            &[0, 0],
            &[],
            options(0, BfsAlgorithm::Frontier),
            res.clone()
        )
        .is_err()
    );
    assert!(
        BfsPartition::build(
            op.clone(),
            0,
            100,
            &[0, 1],
            &[],
            options(0, BfsAlgorithm::Frontier),
            res.clone()
        )
        .is_err()
    );
    assert!(
        BfsPartition::build(
            op,
            0,
            100,
            &[0, 2],
            &[(1, 0)],
            options(0, BfsAlgorithm::Frontier),
            res
        )
        .is_err()
    );
}

#[test]
fn borrowed_rows_and_scalar_completions_preserve_exact_barriers_without_row_allocations() {
    use sail_argentea_core::BfsCompletionValues;
    let op = operation(3, 3);
    let (res, usage, _) = resources(1024 * 1024);
    let mut ps = parts(
        &op,
        &[0, 1, 2],
        &[(0, 1), (1, 2)],
        options(0, BfsAlgorithm::DirectionOptimizing),
        &res,
    )
    .unwrap();
    for _ in 0..3 {
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        assert!(
            ps.iter().all(
                |p| p.collecting_phase() == Some(phase.number) && p.receiving_phase().is_none()
            )
        );
        let mut cursors = ps
            .iter_mut()
            .map(|p| p.start_emission(&phase))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(
            ps.iter().all(
                |p| p.receiving_phase() == Some(phase.number) && p.collecting_phase().is_none()
            )
        );
        for cursor in &mut cursors {
            loop {
                let before = usage.usage().unwrap().live_bytes;
                let row = cursor.next_values().unwrap();
                assert_eq!(usage.usage().unwrap().live_bytes, before);
                let Some(row) = row else {
                    break;
                };
                ps[row.recipient].receive_values(&phase, row).unwrap();
            }
        }
        for cursor in cursors {
            let c = cursor.finish().unwrap();
            let total_messages = c.sequences.iter().sum();
            for (p, part) in ps.iter_mut().enumerate() {
                part.finish_producer_values(
                    &phase,
                    BfsCompletionValues {
                        origin: c.origin,
                        producer: c.producer,
                        mode: c.mode,
                        sequence: c.sequences[p],
                        total_messages,
                    },
                )
                .unwrap();
            }
        }
        for part in &mut ps {
            part.finish(&phase).unwrap();
        }
    }
    // All levels share retained outgoing and incoming state; no transport
    // vector was required by any receiver above.
    assert_eq!(ps[2].state_rows().next().unwrap().distance, Some(2));
}
#[test]
fn typed_bfs_cap_requires_completed_topology_and_complete_global_frontier() {
    let op = operation(3, 1);
    let (res, _, _) = resources(1024 * 1024);
    let mut opts = options(0, BfsAlgorithm::Frontier);
    opts.max_levels = 0;
    let mut ps = parts(&op, &[0], &[], opts, &res).unwrap();
    let phase0 = phase(&op, &ps);
    assert!(ps[0].cap_failure(&phase0).is_err());
    stats(&op, &mut ps).unwrap();
    assert_eq!(ps[0].cap_failure(&phase0).unwrap(), None);
    exchange(&op, &mut ps).unwrap();
    let phase1 = phase(&op, &ps);
    assert!(ps[0].cap_failure(&phase1).is_err());
    stats(&op, &mut ps).unwrap();
    let cap = ps[0].cap_failure(&phase1).unwrap().unwrap();
    assert_eq!(
        (cap.levels, cap.max_levels, cap.frontier, cap.reached),
        (0, 0, 1, 1)
    );
    assert_eq!(ps[0].seal(&phase1).unwrap_err(), cap.to_string());
}

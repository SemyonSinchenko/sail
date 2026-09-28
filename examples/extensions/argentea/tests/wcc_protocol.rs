mod wcc_support;
use sail_argentea_core::*;
use wcc_support::*;
#[test]
fn unknown_destination_in_disconnected_component_fails_topology() {
    for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
        let op = operation(3, 3);
        let (res, usage, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1, 10],
            &[(0, 1), (10, 999)],
            options(algorithm, 42),
            &res,
        )
        .unwrap();
        stats(&op, &mut ps).unwrap();
        assert!(
            exchange(&op, &mut ps)
                .unwrap_err()
                .contains("unknown WCC topology")
        );
        assert!(ps.iter().all(|p| p.row_cursor().is_err()));
        drop(ps);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
}
#[test]
fn duplicate_or_misowned_vertices_and_global_cardinality_are_rejected() {
    let op = operation(2, 3);
    let (res, _, _) = resources(1 << 20);
    let opts = options(WccAlgorithm::Reference, 0);
    assert!(WccPartition::build(op.clone(), 0, 1, &[0, 0], &[], opts, res.clone()).is_err());
    assert!(WccPartition::build(op.clone(), 0, 1, &[1], &[], opts, res.clone()).is_err());
    assert!(WccPartition::build(op.clone(), 0, 1, &[0], &[(2, 0)], opts, res.clone()).is_err());
    let mut ps = parts(&op, &[0, 1], &[], opts, &res).unwrap();
    assert!(stats(&op, &mut ps).is_err());
}
#[test]
fn duplicate_missing_foreign_option_and_late_statistics_poison_state() {
    for defect in 0..6 {
        let op = operation(2, 2);
        let (res, _, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 42),
            &res,
        )
        .unwrap();
        let reports = ps
            .iter()
            .map(WccPartition::statistics)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let phase = phase(&op, &ps);
        match defect {
            0 => {
                ps[0].receive_statistics(&reports[0]).unwrap();
                assert!(ps[0].receive_statistics(&reports[0]).is_err());
            }
            1 => {
                ps[0].receive_statistics(&reports[0]).unwrap();
                assert!(ps[0].finish_statistics(&phase).is_err());
            }
            2 => {
                let mut bad = reports[1].clone();
                bad.values.options.seed += 1;
                assert!(ps[0].receive_statistics(&bad).is_err());
            }
            3 => {
                let mut bad = reports[0].clone();
                bad.values.origin.worker_id += 1;
                assert!(ps[0].receive_statistics(&bad).is_err());
            }
            4 => {
                let mut bad = reports[1].clone();
                bad.phase = std::sync::Arc::new(Round {
                    operation: op.clone(),
                    number: 1,
                });
                assert!(ps[0].receive_statistics(&bad).is_err());
            }
            _ => {
                for r in &reports {
                    ps[0].receive_statistics(r).unwrap();
                }
                ps[0].finish_statistics(&phase).unwrap();
                assert!(ps[0].receive_statistics(&reports[0]).is_err());
            }
        }
        assert!(ps[0].statistics().is_err());
        assert!(ps[0].row_cursor().is_err());
    }
}
#[test]
fn origin_and_dimensions_must_remain_pinned() {
    for change in 0..3 {
        let op = operation(2, 2);
        let (res, _, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 42),
            &res,
        )
        .unwrap();
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
        let mut r = ps[1].statistics().unwrap();
        match change {
            0 => r.values.origin.adjacency_id += 1,
            1 => r.values.origin.worker_id += 1,
            _ => r.values.arcs += 1,
        }
        assert!(ps[0].receive_statistics(&r).is_err());
    }
}
#[test]
fn replay_misroute_wrong_mode_and_late_messages_are_rejected() {
    for defect in 0..5 {
        let op = operation(1, 2);
        let (res, _, _) = resources(1 << 20);
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
        let m = c.next_update().unwrap().unwrap();
        match defect {
            0 => {
                ps[0].receive(&m).unwrap();
                assert!(ps[0].receive(&m).is_err());
            }
            1 => {
                let mut bad = m.values();
                bad.recipient = 1;
                assert!(ps[0].receive_values(&phase, bad).is_err());
            }
            2 => {
                let mut bad = m.values();
                bad.origin.worker_id += 1;
                assert!(ps[0].receive_values(&phase, bad).is_err());
            }
            3 => {
                let mut bad = m.values();
                bad.mode = WccMode::Done;
                assert!(ps[0].receive_values(&phase, bad).is_err());
            }
            _ => {
                ps[0].receive(&m).unwrap();
                assert!(c.next_update().unwrap().is_none());
                let done = c.finish().unwrap();
                ps[0].finish_producer(&done).unwrap();
                assert!(ps[0].receive(&m).is_err());
                continue;
            }
        }
        assert!(ps[0].finish(&phase).is_err());
    }
}
#[test]
fn early_result_incomplete_or_forged_completion_cannot_certify() {
    for defect in 0..3 {
        let op = operation(1, 2);
        let (res, _, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 0),
            &res,
        )
        .unwrap();
        let phase = phase(&op, &ps);
        assert!(ps[0].row_cursor().is_err());
        assert!(ps[0].start_emission(&phase).is_err());
        let (res, _, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 0),
            &res,
        )
        .unwrap();
        stats(&op, &mut ps).unwrap();
        let mut c = ps[0].start_emission(&phase).unwrap();
        while let Some(m) = c.next_update().unwrap() {
            ps[0].receive(&m).unwrap();
        }
        let done = c.finish().unwrap();
        if defect == 0 {
            assert!(ps[0].finish(&phase).is_err());
        } else {
            let bad = WccCompletionValues {
                origin: done.origin,
                producer: 0,
                mode: done.mode,
                sequence: 1,
                total_messages: if defect == 1 { 0 } else { 2 },
            };
            assert!(ps[0].finish_producer_values(&phase, bad).is_err());
            assert!(ps[0].finish(&phase).is_err());
        }
    }
}
#[test]
fn duplicated_member_assignment_and_missing_member_block_commit() {
    for duplicate in [false, true] {
        let op = operation(1, 2);
        let (res, _, _) = resources(1 << 20);
        let mut ps = parts(
            &op,
            &[0, 1],
            &[(0, 1)],
            options(WccAlgorithm::StarContraction, 42),
            &res,
        )
        .unwrap();
        for _ in 0..3 {
            stats(&op, &mut ps).unwrap();
            exchange(&op, &mut ps).unwrap();
        }
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let mut c = ps[0].start_emission(&phase).unwrap();
        let mut seen = 0;
        while let Some(m) = c.next_update().unwrap() {
            if seen == 0 || duplicate {
                ps[0].receive(&m).unwrap();
            }
            seen += 1;
        }
        let done = c.finish().unwrap();
        if duplicate {
            let mut bad = WccMessageValues {
                origin: done.origin,
                producer: 0,
                recipient: 0,
                sequence: seen,
                mode: done.mode,
                payload: WccPayload::Assignment {
                    vertex: 0,
                    old_root: 0,
                    new_root: 0,
                },
            };
            bad.sequence = seen;
            assert!(ps[0].receive_values(&phase, bad).is_err());
        } else {
            assert!(ps[0].finish_producer(&done).is_err());
        }
        assert!(ps[0].finish(&phase).is_err());
        assert_eq!(ps[0].rounds(), 0);
    }
}

#[test]
fn false_global_zero_change_claim_does_not_override_full_edge_certificate() {
    let op = operation(2, 2);
    let (res, _, _) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(WccAlgorithm::Reference, 0),
        &res,
    )
    .unwrap();
    for _ in 0..2 {
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
    }
    let mut reports = ps
        .iter()
        .map(WccPartition::statistics)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(reports[0].values.changed, 0);
    assert_eq!(reports[1].values.changed, 1);
    reports[1].values.changed = 0;
    // Local producer0 is unmodified; receiver0 independently checks the
    // all-edge certificate instead of trusting producer1's scalar change claim.
    for r in &reports {
        ps[0].receive_statistics(r).unwrap();
    }
    let phase = phase(&op, &ps);
    ps[0].finish_statistics(&phase).unwrap();
    assert!(
        ps[0]
            .seal(&phase)
            .unwrap_err()
            .contains("zero-change certificate")
    );
    assert!(ps[0].row_cursor().is_err());
}

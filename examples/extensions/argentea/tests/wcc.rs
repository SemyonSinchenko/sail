mod wcc_support;
use sail_argentea_core::*;
use std::sync::atomic::Ordering;
use wcc_support::*;
#[test]
fn exact_signed_labels_across_owner_counts_seeds_duplicates_loops_and_isolates() {
    let ids = [i64::MIN, -100, -5, -1, 0, 1, 2, 6, 10, 19, 23, i64::MAX];
    let edges = [
        (i64::MIN, -5),
        (-5, 2),
        (2, 10),
        (10, 6),
        (6, -5),
        (-5, 2),
        (2, -5),
        (19, 23),
        (23, 23),
        (i64::MAX, -1),
    ];
    let expected = oracle(&ids, &edges);
    for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
        for p in [1, 2, 3, 11, 17] {
            for seed in [0, 1, 42, u64::MAX] {
                let op = operation(p, ids.len() as u64);
                let (res, usage, drops) = resources(16 << 20);
                let mut ps = parts(&op, &ids, &edges, options(algorithm, seed), &res).unwrap();
                let trace = run(&op, &mut ps).unwrap();
                assert_eq!(collected(&ps), expected, "{algorithm:?} P{p} seed{seed}");
                assert_eq!(trace[0].0, WccMode::Topology);
                if algorithm == WccAlgorithm::StarContraction {
                    assert!(trace.iter().any(|x| x.0 == WccMode::HookReturn));
                    assert_eq!(trace.last().unwrap().0, WccMode::NormalizeReturn);
                    assert!(
                        trace
                            .iter()
                            .filter(|x| x.0 == WccMode::Neighbors)
                            .all(|x| x.2 == 2 * edges.len() as u64)
                    );
                }
                drop(ps);
                drop(res);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
    }
}
#[test]
fn chains_stars_cycles_and_disconnected_components_match_union_find() {
    let ids = (-24..24).collect::<Vec<_>>();
    let chain = ids.windows(2).map(|w| (w[0], w[1])).collect::<Vec<_>>();
    let star = ids[1..].iter().map(|&v| (v, -24)).collect::<Vec<_>>();
    let mut cycles = chain
        .iter()
        .copied()
        .filter(|(a, _)| *a != -1)
        .collect::<Vec<_>>();
    cycles.extend([(-1, -24), (23, 0)]);
    for edges in [chain, star, cycles] {
        for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
            for p in [1, 3, 7] {
                let op = operation(p, ids.len() as u64);
                let (res, usage, _) = resources(16 << 20);
                let mut ps = parts(&op, &ids, &edges, options(algorithm, 42), &res).unwrap();
                run(&op, &mut ps).unwrap();
                assert_eq!(collected(&ps), oracle(&ids, &edges));
                drop(ps);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}
#[test]
fn phase_bounds_and_zero_edge_certification() {
    assert_eq!(
        options(WccAlgorithm::Reference, 0)
            .native_phase_bound()
            .unwrap(),
        2052
    );
    assert_eq!(
        WccOptions {
            algorithm: WccAlgorithm::StarContraction,
            max_rounds: 3,
            seed: 0
        }
        .native_phase_bound()
        .unwrap(),
        28
    );
    assert_eq!(
        WccOptions {
            algorithm: WccAlgorithm::Reference,
            max_rounds: 14,
            seed: 0
        }
        .native_phase_bound()
        .unwrap(),
        32
    );
    assert!(
        WccOptions {
            algorithm: WccAlgorithm::StarContraction,
            max_rounds: u64::MAX,
            seed: 0
        }
        .native_phase_bound()
        .is_err()
    );
    for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
        let op = operation(7, 3);
        let (res, _, _) = resources(1 << 20);
        let ids = [-5, 0, 20];
        let mut opts = options(algorithm, 0);
        opts.max_rounds = if algorithm == WccAlgorithm::Reference {
            1
        } else {
            0
        };
        let mut ps = parts(&op, &ids, &[], opts, &res).unwrap();
        let trace = run(&op, &mut ps).unwrap();
        assert_eq!(collected(&ps), oracle(&ids, &[]));
        assert_eq!(
            trace.len(),
            if algorithm == WccAlgorithm::Reference {
                2
            } else {
                4
            }
        );
    }
}
#[test]
fn no_hook_round_is_counted_and_does_not_claim_convergence() {
    let seed = (0..100)
        .find(|&s| wcc_head(s, 0, 0) == wcc_head(s, 0, 1))
        .unwrap();
    let op = operation(3, 2);
    let (res, _, _) = resources(1 << 20);
    let mut opts = options(WccAlgorithm::StarContraction, seed);
    opts.max_rounds = 1;
    let mut ps = parts(&op, &[0, 1], &[(0, 1)], opts, &res).unwrap();
    for mode in [
        WccMode::Topology,
        WccMode::Neighbors,
        WccMode::HookRoute,
        WccMode::HookReturn,
        WccMode::Neighbors,
    ] {
        stats(&op, &mut ps).unwrap();
        assert_eq!(exchange(&op, &mut ps).unwrap(), mode);
    }
    assert_eq!(ps[0].rounds(), 1);
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    for p in &mut ps {
        let cap = p.cap_failure(&phase).unwrap().unwrap();
        assert_eq!(cap.rounds, 1);
        assert_eq!(cap.unresolved, 2);
        assert!(p.seal(&phase).is_err());
        assert!(p.row_cursor().is_err());
    }
    let (res, _, _) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[0, 1],
        &[(0, 1)],
        options(WccAlgorithm::StarContraction, seed),
        &res,
    )
    .unwrap();
    run(&op, &mut ps).unwrap();
    assert_eq!(collected(&ps), oracle(&[0, 1], &[(0, 1)]));
}
#[test]
fn representative_is_not_the_final_minimum_label() {
    let seed = (0..1000)
        .find(|&s| !wcc_head(s, 0, -5) && wcc_head(s, 0, 8))
        .unwrap();
    let op = operation(3, 2);
    let (res, _, _) = resources(1 << 20);
    let mut ps = parts(
        &op,
        &[-5, 8],
        &[(-5, 8)],
        options(WccAlgorithm::StarContraction, seed),
        &res,
    )
    .unwrap();
    for _ in 0..4 {
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
    }
    assert!(
        ps.iter()
            .flat_map(WccPartition::state_rows)
            .all(|r| r.component == 8)
    );
    run(&op, &mut ps).unwrap();
    assert!(collected(&ps).values().all(|&v| v == -5));
}
#[test]
fn reference_cap_needs_zero_change_round() {
    let op = operation(2, 2);
    let (res, _, _) = resources(1 << 20);
    let mut opts = options(WccAlgorithm::Reference, 0);
    opts.max_rounds = 1;
    let mut ps = parts(&op, &[0, 1], &[(0, 1)], opts, &res).unwrap();
    stats(&op, &mut ps).unwrap();
    exchange(&op, &mut ps).unwrap();
    stats(&op, &mut ps).unwrap();
    exchange(&op, &mut ps).unwrap();
    assert!(
        ps.iter()
            .flat_map(WccPartition::state_rows)
            .all(|r| r.component == 0)
    );
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    assert!(ps[0].cap_failure(&phase).unwrap().is_some());
    assert!(ps[0].seal(&phase).is_err());
}

#[test]
fn fixed_coin_bits_and_star_snapshots_ignore_partition_and_input_order() {
    let roots = [i64::MIN, -5, -1, 0, 1, 6, i64::MAX];
    let expected = [
        [true, false, true, false, false, true, true],
        [false, true, true, true, true, true, false],
        [false, true, true, false, false, true, false],
    ];
    for (round, expected) in expected.into_iter().enumerate() {
        assert_eq!(roots.map(|root| wcc_head(42, round as u64, root)), expected);
    }
    let ids = (-10..11).collect::<Vec<_>>();
    let edges = ids.windows(2).map(|w| (w[0], w[1])).collect::<Vec<_>>();
    let mut golden = None;
    for p in [1, 7, 13] {
        for reverse in [false, true] {
            let mut vertices = ids.clone();
            let mut input = edges.clone();
            if reverse {
                vertices.reverse();
                input.reverse();
            }
            let op = operation(p, ids.len() as u64);
            let (res, _, _) = resources(4 << 20);
            let mut ps = parts(
                &op,
                &vertices,
                &input,
                options(WccAlgorithm::StarContraction, 42),
                &res,
            )
            .unwrap();
            let mut snapshots = Vec::new();
            loop {
                stats(&op, &mut ps).unwrap();
                let phase = phase(&op, &ps);
                let done = ps
                    .iter_mut()
                    .map(|s| s.seal(&phase).unwrap())
                    .collect::<Vec<_>>();
                if done[0].is_some() {
                    assert!(done.iter().all(|x| *x == done[0]));
                    break;
                }
                let mode = exchange(&op, &mut ps).unwrap();
                if mode == WccMode::HookReturn {
                    snapshots.push(
                        ps.iter()
                            .flat_map(WccPartition::state_rows)
                            .map(|r| (r.id, r.component))
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    );
                }
            }
            assert_eq!(collected(&ps), oracle(&ids, &edges));
            if let Some(expected) = &golden {
                assert_eq!(&snapshots, expected);
            } else {
                golden = Some(snapshots);
            }
        }
    }
}

#[test]
fn zero_reference_cap_reports_unattempted_certificate_without_fabricated_count() {
    let op = operation(3, 1);
    let (res, _, _) = resources(1 << 20);
    let mut opts = options(WccAlgorithm::Reference, 0);
    opts.max_rounds = 0;
    let mut ps = parts(&op, &[0], &[], opts, &res).unwrap();
    stats(&op, &mut ps).unwrap();
    exchange(&op, &mut ps).unwrap();
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    for p in &mut ps {
        let cap = p.cap_failure(&phase).unwrap().unwrap();
        assert_eq!(cap.unresolved, 0);
        assert!(cap.certificate_not_attempted);
        assert_eq!(cap.rounds, 0);
        assert!(p.seal(&phase).is_err());
    }
}

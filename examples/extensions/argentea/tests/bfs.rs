mod bfs_support;
use bfs_support::*;
use sail_argentea_core::*;
#[test]
fn all_variants_match_independent_integer_bfs_directed_and_undirected() {
    let ids = [-17, -4, -1, 0, 2, 11, 40, 70];
    let edges = [
        (-17, -4),
        (-17, 11),
        (-4, 2),
        (11, 2),
        (2, 40),
        (2, 40),
        (40, 40),
        (0, -1),
    ];
    for directed in [true, false] {
        for p in [1, 2, 3, 11] {
            for algorithm in [
                BfsAlgorithm::Reference,
                BfsAlgorithm::Frontier,
                BfsAlgorithm::DirectionOptimizing,
            ] {
                let mut arcs = edges.to_vec();
                if !directed {
                    arcs.extend(edges.iter().map(|&(s, t)| (t, s)));
                }
                let op = operation(p, ids.len() as u64);
                let (res, usage, _) = resources(8 * 1024 * 1024);
                let mut partitions =
                    parts(&op, &ids, &arcs, options(-17, algorithm), &res).unwrap();
                run(&op, &mut partitions).unwrap();
                let result = collected(&partitions);
                assert_eq!(result, oracle(&ids, &arcs, -17));
                assert!(certificate(&result, &arcs, -17));
                assert_eq!(result[&2].parent, Some(-4));
                assert_eq!(result[&70].distance, None);
                assert_eq!(result[&70].parent, None);
                drop(partitions);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}
#[test]
fn reference_scans_every_arc_frontier_avoids_old_arcs_and_pull_really_exits_early() {
    let ids = (0..400).collect::<Vec<_>>();
    let mut arcs = vec![(0, 1), (202, 203), (203, 204)];
    arcs.extend((2..102).map(|v| (1, v)));
    for u in 2..102 {
        arcs.extend((102..202).map(|v| (u, v)));
    }
    arcs.extend((102..202).map(|v| (v, 202)));
    let expected = oracle(&ids, &arcs, 0);
    let mut traces = Vec::new();
    for algorithm in [
        BfsAlgorithm::Reference,
        BfsAlgorithm::Frontier,
        BfsAlgorithm::DirectionOptimizing,
    ] {
        let op = operation(11, ids.len() as u64);
        let (res, _, _) = resources(32 * 1024 * 1024);
        let mut partitions = parts(&op, &ids, &arcs, options(0, algorithm), &res).unwrap();
        let trace = run(&op, &mut partitions).unwrap();
        assert_eq!(collected(&partitions), expected);
        traces.push(trace);
    }
    assert!(
        traces[0]
            .work
            .iter()
            .all(|w| w.examined_edges == arcs.len() as u64)
    );
    assert_eq!(
        traces[1].work.iter().map(|w| w.examined_edges).sum::<u64>(),
        arcs.len() as u64
    );
    assert_eq!(
        &traces[2].modes[..5],
        &[
            BfsMode::Push,
            BfsMode::Push,
            BfsMode::Pull,
            BfsMode::Pull,
            BfsMode::Push
        ]
    );
    // Layer two has 100 parents per unvisited destination. Actual pull stops
    // after its smallest frontier parent, well below all 10,000 incoming arcs.
    assert!(traces[2].work[2].examined_edges < 250);
    assert_eq!(traces[2].work[2].emitted_messages, 11 * 100);
    assert_eq!(traces[2].work[2].received_messages, 11 * 100);
    assert!(
        traces[2].work.iter().map(|w| w.examined_edges).sum::<u64>()
            < traces[1].work.iter().map(|w| w.examined_edges).sum::<u64>()
    );
}
#[test]
fn cap_requires_empty_global_frontier_and_zero_owners_participate() {
    for algorithm in [
        BfsAlgorithm::Reference,
        BfsAlgorithm::Frontier,
        BfsAlgorithm::DirectionOptimizing,
    ] {
        for (ids, arcs, cap, passes) in [
            (vec![0], vec![], 0, false),
            (vec![0], vec![], 1, true),
            (vec![0, 1, 2], vec![(0, 1), (1, 2)], 2, false),
            (vec![0, 1, 2], vec![(0, 1), (1, 2)], 3, true),
        ] {
            let op = operation(11, ids.len() as u64);
            let (res, _, _) = resources(1024 * 1024);
            let mut opts = options(0, algorithm);
            opts.max_levels = cap;
            let mut partitions = parts(&op, &ids, &arcs, opts, &res).unwrap();
            let result = run(&op, &mut partitions);
            assert_eq!(result.is_ok(), passes);
            if passes {
                assert_eq!(collected(&partitions), oracle(&ids, &arcs, 0));
            } else {
                assert!(result.unwrap_err().contains("did not converge"));
                assert!(partitions[0].row_cursor().is_err());
            }
        }
    }
}
#[test]
fn independent_certificate_rejects_plausible_but_wrong_vectors() {
    let ids = [0, 1, 2, 3, 8, 9];
    let arcs = [(0, 1), (1, 2), (0, 2), (2, 3), (8, 9), (9, 8)];
    let good = oracle(&ids, &arcs, 0);
    assert!(certificate(&good, &arcs, 0));
    let mut wrong = good.clone();
    wrong.get_mut(&2).unwrap().distance = Some(2);
    wrong.get_mut(&2).unwrap().parent = Some(1);
    wrong.get_mut(&3).unwrap().distance = Some(3);
    assert!(!certificate(&wrong, &arcs, 0));
    let mut wrong = good.clone();
    wrong.get_mut(&8).unwrap().distance = Some(0);
    wrong.get_mut(&8).unwrap().parent = Some(8);
    assert!(!certificate(&wrong, &arcs, 0));
    let mut wrong = good.clone();
    wrong.get_mut(&3).unwrap().distance = None;
    wrong.get_mut(&3).unwrap().parent = None;
    assert!(!certificate(&wrong, &arcs, 0));
    let mut wrong = good;
    wrong.get_mut(&3).unwrap().parent = Some(0);
    assert!(!certificate(&wrong, &arcs, 0));
}
#[test]
fn setup_rejects_missing_source_wrong_cardinality_and_disconnected_unknown_endpoint() {
    for case in 0..3 {
        let op = operation(3, if case == 1 { 4 } else { 3 });
        let (res, _, _) = resources(1024 * 1024);
        let mut partitions = parts(
            &op,
            &[0, 1, 2],
            &[(2, if case == 2 { 99 } else { 2 })],
            options(if case == 0 { 9 } else { 0 }, BfsAlgorithm::Frontier),
            &res,
        )
        .unwrap();
        assert!(setup(&op, &mut partitions).is_err());
        assert!(partitions[0].row_cursor().is_err());
    }
}

mod bfs_support;
use bfs_support::*;
use sail_argentea_core::*;

#[test]
fn prepared_bfs_matches_owned_input_oracle_and_protocol_trace() {
    let ids = [i64::MIN, -17, -4, -1, 0, 2, 11, 40, 70, i64::MAX];
    let edges = [
        (i64::MIN, -17),
        (-17, -4),
        (-17, 11),
        (-4, 2),
        (11, 2),
        (2, 40),
        (2, 40),
        (40, 40),
        (0, -1),
    ];
    for directed in [false, true] {
        for width in [1, 3, 13] {
            for algorithm in [
                BfsAlgorithm::Reference,
                BfsAlgorithm::Frontier,
                BfsAlgorithm::DirectionOptimizing,
            ] {
                let mut arcs = edges.to_vec();
                if !directed {
                    arcs.extend(edges.iter().map(|&(s, t)| (t, s)));
                }
                let op = operation(width, ids.len() as u64);
                let (res, usage, _) = resources(16 << 20);
                let opts = options(i64::MIN, algorithm);
                let mut baseline = parts(&op, &ids, &arcs, opts, &res).unwrap();
                let mut prepared = (0..width)
                    .map(|p| {
                        let vertices = ids
                            .iter()
                            .copied()
                            .filter(|id| op.owner(*id) == p)
                            .collect::<Vec<_>>();
                        let edges = arcs
                            .iter()
                            .copied()
                            .filter(|(s, _)| op.owner(*s) == p)
                            .collect::<Vec<_>>();
                        let ready = BfsPartition::prepare(
                            op.clone(),
                            p,
                            100 + p as u64,
                            &vertices,
                            &edges,
                            opts,
                            res.clone(),
                        )
                        .unwrap();
                        drop(vertices);
                        drop(edges);
                        ready.finish().unwrap()
                    })
                    .collect::<Vec<_>>();
                let old = run(&op, &mut baseline).unwrap();
                let new = run(&op, &mut prepared).unwrap();
                assert_eq!((new.modes, new.work), (old.modes, old.work));
                assert_eq!(collected(&prepared), collected(&baseline));
                assert_eq!(collected(&prepared), oracle(&ids, &arcs, i64::MIN));
                drop((prepared, baseline));
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}

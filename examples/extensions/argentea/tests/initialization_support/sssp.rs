use super::*;

#[test]
fn prepared_sssp_preserves_exact_labels_and_protocol_trace() {
    // Include ties, duplicates, both signed zeros, subnormal and exact binary
    // fractions, disconnected components and extreme signed vertex IDs.
    let mut edges = EDGES.to_vec();
    edges.extend([(1, 2, -0.0), (-5, 1, 1.0), (9, 10, f64::from_bits(1))]);
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for width in [1, 3, 13] {
            for delta in [0.25, 4.0] {
                let op = support::operation(width, IDS.len() as u64);
                let (r, usage, _) = support::resources(16 << 20);
                let mut opts = options(algorithm);
                opts.delta = delta;
                let mut baseline = parts(&op, &IDS, &edges, opts, &r);
                let mut candidate = (0..width)
                    .map(|p| {
                        let ids = IDS
                            .iter()
                            .copied()
                            .filter(|id| op.owner(*id) == p)
                            .collect::<Vec<_>>();
                        let arcs = edges
                            .iter()
                            .copied()
                            .filter(|(s, _, _)| op.owner(*s) == p)
                            .collect::<Vec<_>>();
                        let prepared = SsspPartition::prepare(
                            op.clone(),
                            p,
                            100 + p as u64,
                            &ids,
                            &arcs,
                            opts,
                            r.clone(),
                        )
                        .unwrap();
                        drop(ids);
                        drop(arcs);
                        prepared.finish().unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    run(&op, &mut candidate).unwrap(),
                    run(&op, &mut baseline).unwrap()
                );
                let bits = |parts: &[SsspPartition]| {
                    collected(parts)
                        .into_iter()
                        .map(|(id, label)| (id, label.map(|(d, h, p)| (d.to_bits(), h, p))))
                        .collect::<BTreeMap<_, _>>()
                };
                assert_eq!(bits(&candidate), bits(&baseline));
                assert_eq!(collected(&candidate), oracle(&IDS, &edges, -5));
                drop((candidate, baseline));
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}

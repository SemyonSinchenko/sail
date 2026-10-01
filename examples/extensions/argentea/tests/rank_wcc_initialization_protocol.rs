//! Borrowed and prepared construction retain exact round ordering and results.
mod wcc_support;
use wcc_support::{operation, resources};
const LIMIT: usize = 256 << 20;
use sail_argentea_core::*;

const IDS: [i64; 8] = [i64::MIN, -9, -2, 0, 3, 11, 9_007_199_254_740_993, i64::MAX];
const EDGES: [(i64, i64); 8] = [
    (-9, -2),
    (-9, -2),
    (-2, 0),
    (0, 0),
    (3, 11),
    (11, 3),
    (i64::MIN, i64::MAX),
    (9_007_199_254_740_993, 0),
];
fn inputs(op: &Operation, p: usize) -> (Vec<i64>, Vec<(i64, i64)>) {
    (
        IDS.iter()
            .rev()
            .copied()
            .filter(|&v| op.owner(v) == p)
            .collect(),
        EDGES
            .iter()
            .copied()
            .filter(|&(s, _)| op.owner(s) == p)
            .collect(),
    )
}
#[derive(Debug, PartialEq)]
struct PrTrace {
    updates: Vec<(usize, u64, i64, u64)>,
    completions: Vec<(Vec<u64>, u64)>,
    ranks: Vec<Vec<(i64, u64)>>,
    results: Vec<(u64, u64)>,
}
fn pagerank(p: usize, split: bool) -> PrTrace {
    let (r, usage, _) = resources(LIMIT);
    let op = operation(p, IDS.len() as u64);
    let mut parts = (0..p)
        .map(|p| {
            let (ids, edges) = inputs(&op, p);
            if split {
                let prepared =
                    PageRankPartition::prepare(op.clone(), p, &ids, &edges, r.clone()).unwrap();
                drop(ids);
                drop(edges);
                prepared.finish().unwrap()
            } else {
                PageRankPartition::build(op.clone(), p, &ids, &edges, r.clone()).unwrap()
            }
        })
        .collect::<Vec<_>>();
    let identities = parts
        .iter()
        .map(PageRankPartition::adjacency_identity)
        .collect::<Vec<_>>();
    let mut trace = PrTrace {
        updates: vec![],
        completions: vec![],
        ranks: vec![],
        results: vec![],
    };
    for number in 0..8 {
        let round = Round {
            operation: op.clone(),
            number,
        };
        for part in &mut parts {
            part.begin(&round).unwrap();
        }
        let mut outputs = vec![];
        let mut completions = vec![];
        for part in &mut parts {
            completions.push(
                part.emit(&round, |update| {
                    trace.updates.push((
                        update.producer,
                        update.sequence,
                        update.target,
                        update.value.to_bits(),
                    ));
                    outputs.push(update);
                    Ok(())
                })
                .unwrap(),
            );
        }
        for update in outputs {
            parts[op.owner(update.target)].receive(update).unwrap();
        }
        for (producer, emission) in completions.iter().enumerate() {
            trace
                .completions
                .push((emission.sequences.clone(), emission.dangling_mass.to_bits()));
            for part in &mut parts {
                part.finish_producer(
                    &round,
                    producer,
                    emission.sequences[part.partition()],
                    emission.dangling_mass,
                )
                .unwrap();
            }
        }
        for part in &mut parts {
            let result = part.finish(&round, 0.85).unwrap();
            trace
                .results
                .push((result.l1_delta.to_bits(), result.rank_mass.to_bits()));
        }
        trace.ranks.push(
            parts
                .iter()
                .flat_map(|p| p.ranks().map(|(id, value)| (id, value.to_bits())))
                .collect(),
        );
        assert_eq!(
            parts
                .iter()
                .map(PageRankPartition::adjacency_identity)
                .collect::<Vec<_>>(),
            identities
        );
    }
    drop(parts);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    trace
}
#[test]
fn prepared_pagerank_preserves_float_bits_message_order_and_empty_owners() {
    for p in [1, 3, 11] {
        assert_eq!(pagerank(p, false), pagerank(p, true));
    }
}
#[test]
fn prepared_wcc_preserves_both_protocol_traces_and_independent_components() {
    for p in [1, 3, 11] {
        for algorithm in [WccAlgorithm::Reference, WccAlgorithm::StarContraction] {
            let mut old_trace = None;
            for split in [false, true] {
                let (r, usage, _) = resources(LIMIT);
                let op = operation(p, IDS.len() as u64);
                let opts = wcc_support::options(algorithm, 42);
                let mut parts = (0..p)
                    .map(|p| {
                        let (ids, arcs) = inputs(&op, p);
                        if split {
                            let prepared = WccPartition::prepare(
                                op.clone(),
                                p,
                                7 + p as u64,
                                &ids,
                                &arcs,
                                opts,
                                r.clone(),
                            )
                            .unwrap();
                            drop(ids);
                            drop(arcs);
                            prepared.finish().unwrap()
                        } else {
                            WccPartition::build(
                                op.clone(),
                                p,
                                7 + p as u64,
                                &ids,
                                &arcs,
                                opts,
                                r.clone(),
                            )
                            .unwrap()
                        }
                    })
                    .collect::<Vec<_>>();
                let trace = wcc_support::run(&op, &mut parts).unwrap();
                assert_eq!(
                    wcc_support::collected(&parts),
                    wcc_support::oracle(&IDS, &EDGES)
                );
                if let Some(ref old) = old_trace {
                    assert_eq!(&trace, old);
                } else {
                    old_trace = Some(trace);
                }
                drop(parts);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}

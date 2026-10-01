//! Exact local protocol and float-bit parity across the ownership boundary.
#[path = "delta_support/mod.rs"]
mod support;
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
#[derive(Debug, PartialEq)]
struct Trace {
    updates: Vec<(DeltaMode, usize, u64, i64, u64)>,
    completions: Vec<(DeltaMode, usize, Vec<u64>, u64)>,
    states: Vec<Vec<(i64, u64, u64)>>,
}
fn run(p: usize, split: bool) -> Trace {
    let (r, usage, _) = support::resources(16 << 20);
    let op = support::operation(p, IDS.len() as u64);
    let mut parts = (0..p)
        .map(|p| {
            let ids = IDS
                .iter()
                .rev()
                .copied()
                .filter(|&id| op.owner(id) == p)
                .collect::<Vec<_>>();
            let arcs = EDGES
                .iter()
                .copied()
                .filter(|&(src, _)| op.owner(src) == p)
                .collect::<Vec<_>>();
            if split {
                let ready = DeltaPartition::prepare(
                    op.clone(),
                    p,
                    &ids,
                    &arcs,
                    support::options(),
                    r.clone(),
                )
                .unwrap();
                drop(ids);
                drop(arcs);
                ready.finish().unwrap()
            } else {
                DeltaPartition::build(op.clone(), p, &ids, &arcs, support::options(), r.clone())
                    .unwrap()
            }
        })
        .collect::<Vec<_>>();
    let mut trace = Trace {
        updates: vec![],
        completions: vec![],
        states: vec![],
    };
    for _ in 0..16 {
        support::stats(&mut parts).unwrap();
        let phase = support::phase(&op, &parts);
        let mut cursors = parts
            .iter_mut()
            .map(|part| part.start_emission(&phase).unwrap())
            .collect::<Vec<_>>();
        loop {
            let mut any = false;
            for cursor in &mut cursors {
                if let Some(update) = cursor.next_update().unwrap() {
                    any = true;
                    trace.updates.push((
                        update.mode,
                        update.producer,
                        update.sequence,
                        update.target,
                        update.value.to_bits(),
                    ));
                    parts[op.owner(update.target)].receive(&update).unwrap();
                }
            }
            if !any {
                break;
            }
        }
        for cursor in cursors {
            let complete = cursor.finish().unwrap();
            trace.completions.push((
                complete.mode,
                complete.producer,
                complete.sequences.clone(),
                complete.dangling.to_bits(),
            ));
            for part in &mut parts {
                part.finish_producer(&complete).unwrap();
            }
        }
        for part in &mut parts {
            part.finish(&phase).unwrap();
        }
        trace.states.push(
            parts
                .iter()
                .flat_map(|part| {
                    part.state_rows()
                        .map(|(id, score, residual)| (id, score.to_bits(), residual.to_bits()))
                })
                .collect(),
        );
    }
    drop(parts);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    trace
}
#[test]
fn prepared_residual_pagerank_preserves_exact_message_and_state_bits() {
    for p in [1, 3, 11] {
        assert_eq!(run(p, false), run(p, true));
    }
}

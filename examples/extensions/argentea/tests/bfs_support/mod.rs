#![allow(dead_code)]
#[path = "../delta_support/mod.rs"]
mod shared;
use sail_argentea_core::*;
pub use shared::{operation, resources};
use std::collections::{BTreeMap, VecDeque};
pub fn options(source: i64, algorithm: BfsAlgorithm) -> BfsOptions {
    BfsOptions {
        source,
        algorithm,
        max_levels: 1024,
        alpha: 14,
        beta: 24,
    }
}
pub fn parts(
    op: &Operation,
    ids: &[i64],
    arcs: &[(i64, i64)],
    opts: BfsOptions,
    res: &Resources,
) -> Result<Vec<BfsPartition>> {
    (0..op.partitions)
        .map(|p| {
            BfsPartition::build(
                op.clone(),
                p,
                100 + p as u64,
                &ids.iter()
                    .copied()
                    .filter(|id| op.owner(*id) == p)
                    .collect::<Vec<_>>(),
                &arcs
                    .iter()
                    .copied()
                    .filter(|(s, _)| op.owner(*s) == p)
                    .collect::<Vec<_>>(),
                opts,
                res.clone(),
            )
        })
        .collect()
}
pub fn phase(op: &Operation, parts: &[BfsPartition]) -> Round {
    Round {
        operation: op.clone(),
        number: parts[0].next_phase(),
    }
}
pub fn stats(op: &Operation, parts: &mut [BfsPartition]) -> Result<()> {
    let reports = parts
        .iter()
        .map(BfsPartition::statistics)
        .collect::<Result<Vec<_>>>()?;
    for (p, part) in parts.iter_mut().enumerate() {
        for i in 0..reports.len() {
            part.receive_statistics(&reports[(p + i) % reports.len()])?;
        }
        part.finish_statistics(&Round {
            operation: op.clone(),
            number: part.next_phase(),
        })?;
    }
    Ok(())
}
pub fn exchange(op: &Operation, parts: &mut [BfsPartition]) -> Result<BfsMode> {
    let phase = phase(op, parts);
    let mut cursors = parts
        .iter_mut()
        .map(|p| p.start_emission(&phase))
        .collect::<Result<Vec<_>>>()?;
    let mode = cursors[0].mode();
    assert!(cursors.iter().all(|c| c.mode() == mode));
    // Interleaved producer order, maintaining per-producer sequences.
    loop {
        let mut any = false;
        for cursor in &mut cursors {
            if let Some(message) = cursor.next_update()? {
                any = true;
                parts[message.recipient].receive(&message)?;
            }
        }
        if !any {
            break;
        }
    }
    for cursor in cursors {
        let c = cursor.finish()?;
        for part in &mut *parts {
            part.finish_producer(&c)?;
        }
    }
    for part in parts {
        part.finish(&phase)?;
    }
    Ok(mode)
}
pub fn setup(op: &Operation, parts: &mut [BfsPartition]) -> Result<()> {
    stats(op, parts)?;
    assert_eq!(exchange(op, parts)?, BfsMode::Topology);
    Ok(())
}
#[derive(Default, Debug)]
pub struct Trace {
    pub modes: Vec<BfsMode>,
    pub work: Vec<BfsWork>,
}
pub fn run(op: &Operation, parts: &mut [BfsPartition]) -> Result<Trace> {
    setup(op, parts)?;
    let identities = parts
        .iter()
        .map(|p| (p.origin(), p.incoming_identity()))
        .collect::<Vec<_>>();
    let mut trace = Trace::default();
    loop {
        stats(op, parts)?;
        let phase = phase(op, parts);
        let results = parts
            .iter_mut()
            .map(|p| p.seal(&phase))
            .collect::<Result<Vec<_>>>()?;
        if results[0].is_some() {
            assert!(results.iter().all(|r| *r == results[0]));
            break;
        }
        assert!(results.iter().all(Option::is_none));
        trace.modes.push(exchange(op, parts)?);
        let mut work = BfsWork::default();
        for p in &*parts {
            let w = p.last_work();
            work.examined_edges += w.examined_edges;
            work.examined_vertices += w.examined_vertices;
            work.emitted_messages += w.emitted_messages;
            work.received_messages += w.received_messages;
        }
        trace.work.push(work);
        assert_eq!(
            parts
                .iter()
                .map(|p| (p.origin(), p.incoming_identity()))
                .collect::<Vec<_>>(),
            identities
        );
    }
    Ok(trace)
}
pub fn collected(parts: &[BfsPartition]) -> BTreeMap<i64, BfsRow> {
    let mut result = BTreeMap::new();
    for part in parts {
        let mut cursor = part.row_cursor().unwrap();
        while let Some(row) = cursor.next_row().unwrap() {
            assert!(result.insert(row.id, row).is_none());
        }
    }
    result
}
pub fn oracle(ids: &[i64], arcs: &[(i64, i64)], source: i64) -> BTreeMap<i64, BfsRow> {
    // Queue BFS and an independent full-edge parent witness pass, no CSR,
    // partitioning, frontier barrier or direction-selection code is reused.
    let mut rows = ids
        .iter()
        .map(|&id| {
            (
                id,
                BfsRow {
                    id,
                    distance: None,
                    parent: None,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    rows.get_mut(&source).unwrap().distance = Some(0);
    let mut queue = VecDeque::from([source]);
    while let Some(source) = queue.pop_front() {
        let distance = rows[&source].distance.unwrap() + 1;
        for &(_, target) in arcs.iter().filter(|(s, _)| *s == source) {
            let row = rows.get_mut(&target).unwrap();
            if row.distance.is_none() {
                row.distance = Some(distance);
                queue.push_back(target);
            }
        }
    }
    rows.get_mut(&source).unwrap().parent = Some(source);
    for &(s, t) in arcs {
        if rows[&s]
            .distance
            .is_some_and(|d| rows[&t].distance == Some(d + 1))
        {
            let row = rows.get_mut(&t).unwrap();
            row.parent = Some(row.parent.map_or(s, |old| old.min(s)));
        }
    }
    rows
}
pub fn certificate(rows: &BTreeMap<i64, BfsRow>, arcs: &[(i64, i64)], source: i64) -> bool {
    if rows
        .get(&source)
        .is_none_or(|r| r.distance != Some(0) || r.parent != Some(source))
    {
        return false;
    }
    for row in rows.values() {
        match row.distance {
            None if row.parent.is_some() => return false,
            Some(0) if row.id != source => return false,
            Some(d) if d > 0 => {
                let Some(parent) = row.parent.and_then(|p| rows.get(&p)) else {
                    return false;
                };
                if parent.distance != Some(d - 1) || !arcs.contains(&(parent.id, row.id)) {
                    return false;
                }
            }
            _ => {}
        }
    }
    // Both the rooted tight parent path and every edge inequality are needed:
    // witnesses alone admit unnecessarily long paths; inequalities alone admit
    // disconnected zero labels or unreachable reachable vertices.
    arcs.iter().all(|(s, t)| match (rows.get(s), rows.get(t)) {
        (Some(a), Some(b)) => a
            .distance
            .is_none_or(|d| b.distance.is_some_and(|x| x <= d + 1)),
        _ => false,
    })
}

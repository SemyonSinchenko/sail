#![allow(dead_code)]
#[path = "../delta_support/mod.rs"]
mod shared;
use sail_argentea_core::*;
pub use shared::{operation, resources};
use std::collections::BTreeMap;
pub fn options(algorithm: WccAlgorithm, seed: u64) -> WccOptions {
    WccOptions {
        algorithm,
        seed,
        max_rounds: 1024,
    }
}
pub fn parts(
    op: &Operation,
    ids: &[i64],
    edges: &[(i64, i64)],
    opts: WccOptions,
    res: &Resources,
) -> Result<Vec<WccPartition>> {
    (0..op.partitions)
        .map(|p| {
            WccPartition::build(
                op.clone(),
                p,
                100 + p as u64,
                &ids.iter()
                    .copied()
                    .filter(|&id| op.owner(id) == p)
                    .collect::<Vec<_>>(),
                &edges
                    .iter()
                    .copied()
                    .filter(|&(id, _)| op.owner(id) == p)
                    .collect::<Vec<_>>(),
                opts,
                res.clone(),
            )
        })
        .collect()
}
pub fn phase(op: &Operation, ps: &[WccPartition]) -> Round {
    Round {
        operation: op.clone(),
        number: ps[0].next_phase(),
    }
}
pub fn stats(op: &Operation, ps: &mut [WccPartition]) -> Result<()> {
    let reports = ps
        .iter()
        .map(WccPartition::statistics)
        .collect::<Result<Vec<_>>>()?;
    for (p, state) in ps.iter_mut().enumerate() {
        for i in 0..reports.len() {
            state.receive_statistics(&reports[(p + i) % reports.len()])?;
        }
        state.finish_statistics(&Round {
            operation: op.clone(),
            number: state.next_phase(),
        })?;
    }
    Ok(())
}
pub fn exchange(op: &Operation, ps: &mut [WccPartition]) -> Result<WccMode> {
    let phase = phase(op, ps);
    let mut cs = ps
        .iter_mut()
        .map(|p| p.start_emission(&phase))
        .collect::<Result<Vec<_>>>()?;
    let mode = cs[0].mode();
    assert!(cs.iter().all(|c| c.mode() == mode));
    loop {
        let mut any = false;
        for c in &mut cs {
            if let Some(m) = c.next_update()? {
                any = true;
                ps[m.recipient].receive(&m)?;
            }
        }
        if !any {
            break;
        }
    }
    for c in cs {
        let complete = c.finish()?;
        for p in &mut *ps {
            p.finish_producer(&complete)?;
        }
    }
    for p in ps {
        p.finish(&phase)?;
    }
    Ok(mode)
}
pub fn run(op: &Operation, ps: &mut [WccPartition]) -> Result<Vec<(WccMode, u64, u64)>> {
    let origins = ps.iter().map(WccPartition::origin).collect::<Vec<_>>();
    let mut trace = Vec::new();
    loop {
        stats(op, ps)?;
        let phase = phase(op, ps);
        let seals = ps
            .iter_mut()
            .map(|p| p.seal(&phase))
            .collect::<Result<Vec<_>>>()?;
        if seals[0].is_some() {
            assert!(seals.iter().all(|s| *s == seals[0]));
            break;
        }
        assert!(seals.iter().all(Option::is_none));
        let mode = exchange(op, ps)?;
        assert_eq!(
            ps.iter().map(WccPartition::origin).collect::<Vec<_>>(),
            origins
        );
        trace.push((
            mode,
            ps[0].rounds(),
            ps.iter().map(|p| p.last_work().examined_edges).sum(),
        ));
        assert!(trace.len() < 4000, "bounded test fixture did not converge");
    }
    Ok(trace)
}
pub fn collected(ps: &[WccPartition]) -> BTreeMap<i64, i64> {
    let mut result = BTreeMap::new();
    for p in ps {
        let mut c = p.row_cursor().unwrap();
        while let Some(r) = c.next_row().unwrap() {
            assert!(result.insert(r.id, r.component).is_none());
        }
    }
    result
}
pub fn oracle(ids: &[i64], edges: &[(i64, i64)]) -> BTreeMap<i64, i64> {
    // Independent union-find over arbitrary signed IDs, no owner routing/coins.
    let mut parents = (0..ids.len()).collect::<Vec<_>>();
    fn root(p: &[usize], mut i: usize) -> usize {
        while p[i] != i {
            i = p[i];
        }
        i
    }
    for &(a, b) in edges {
        let x = root(&parents, ids.iter().position(|&v| v == a).unwrap());
        let y = root(&parents, ids.iter().position(|&v| v == b).unwrap());
        parents[x] = y;
    }
    let mut minima = BTreeMap::new();
    for (i, &id) in ids.iter().enumerate() {
        let r = root(&parents, i);
        minima
            .entry(r)
            .and_modify(|v: &mut i64| *v = (*v).min(id))
            .or_insert(id);
    }
    ids.iter()
        .enumerate()
        .map(|(i, &id)| (id, minima[&root(&parents, i)]))
        .collect()
}

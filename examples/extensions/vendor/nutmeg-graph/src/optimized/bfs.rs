//! Direction-optimizing BFS: sparse push, dense pull with first-parent exit.
use super::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const UNSEEN: usize = usize::MAX;

pub(super) fn run(
    graph: &GraphProjection,
    args: &ValidatedArguments,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let context = graph.execution();
    let n = graph.node_count();
    let source = match args.options().get("source") {
        Some(Value::String(source)) => graph.node_ids().iter().position(|id| id.as_str() == source),
        _ => None,
    }
    .ok_or_else(|| err("bfsDirection requires a selected source"))?;
    let limit = integer(args, "maxIterations")? as usize;
    let alpha = number(args, "alpha")?;
    let beta = number(args, "beta")?;
    let undirected = graph.orientation() == grust_algorithms::Orientation::Undirected;
    let m = graph
        .edge_count()
        .checked_mul(if undirected { 2 } else { 1 })
        .ok_or_else(|| err("BFS arc count overflow"))?;
    // Two CSR directions, atomic labels, bit membership, sparse frontiers and
    // parallel collection capacity. Admission precedes every data allocation.
    let _scratch = admitted(context, n, m, 256, 32)?;
    let _trace = context.reserve(limit.saturating_mul(1024)).map_err(err)?;
    let (workers, threads) = pool(context, n)?;
    let mut out = allocated(n + 1, 0usize)?;
    let mut incoming = allocated(n + 1, 0usize)?;
    let mut meter = context.work_meter();
    for edge in graph.edges() {
        meter.charge(1).map_err(err)?;
        out[edge.source + 1] += 1;
        incoming[edge.target + 1] += 1;
        if undirected {
            out[edge.target + 1] += 1;
            incoming[edge.source + 1] += 1;
        }
    }
    for i in 0..n {
        meter.charge(1).map_err(err)?;
        out[i + 1] += out[i];
        incoming[i + 1] += incoming[i];
    }
    let mut targets = allocated(m, 0usize)?;
    let mut sources = allocated(m, 0usize)?;
    let mut oc = out[..n].to_vec();
    let mut ic = incoming[..n].to_vec();
    for edge in graph.edges() {
        meter.charge(1).map_err(err)?;
        targets[oc[edge.source]] = edge.target;
        oc[edge.source] += 1;
        sources[ic[edge.target]] = edge.source;
        ic[edge.target] += 1;
        if undirected {
            targets[oc[edge.target]] = edge.source;
            oc[edge.target] += 1;
            sources[ic[edge.source]] = edge.target;
            ic[edge.source] += 1;
        }
    }
    drop(oc);
    drop(ic);
    let depths: Vec<_> = (0..n).map(|_| AtomicUsize::new(UNSEEN)).collect();
    let parents: Vec<_> = (0..n).map(|_| AtomicUsize::new(UNSEEN)).collect();
    depths[source].store(0, Ordering::Relaxed);
    parents[source].store(source, Ordering::Relaxed);
    let mut frontier = vec![source];
    let mut membership = allocated(n, false)?;
    let mut remaining = m.saturating_sub(out[source + 1] - out[source]);
    let mut pull = false;
    let mut just_left_pull = false;
    let mut level = 0usize;
    let mut rounds = Vec::new();
    rounds.try_reserve_exact(limit).map_err(err)?;
    while !frontier.is_empty() {
        context.checkpoint().map_err(err)?;
        if level == limit {
            return exec_err!("nutmeg: bfsDirection did not converge within maxIterations={limit}");
        }
        let frontier_edges: usize = frontier.iter().map(|&u| out[u + 1] - out[u]).sum();
        if !pull && !just_left_pull && frontier_edges as f64 > remaining as f64 / alpha {
            pull = true;
        }
        just_left_pull = false;
        let examined = AtomicUsize::new(0);
        let next_level = level + 1;
        let next = if pull {
            membership.fill(false);
            for &u in &frontier {
                membership[u] = true;
            }
            let visit = |(block, chunk): (usize, &[AtomicUsize])| -> Result<Vec<usize>> {
                let mut found = Vec::new();
                let mut work = context.work_meter();
                let mut count = 0;
                for (offset, depth) in chunk.iter().enumerate() {
                    // An all-isolate or previously visited block still does
                    // work and must observe cancellation. Reuse one meter per
                    // fixed block instead of acquiring credit per vertex.
                    work.charge(1).map_err(err)?;
                    if depth.load(Ordering::Relaxed) != UNSEEN {
                        continue;
                    }
                    let v = block * BLOCK + offset;
                    for &u in &sources[incoming[v]..incoming[v + 1]] {
                        work.charge(1).map_err(err)?;
                        count += 1;
                        if membership[u] {
                            parents[v].store(u, Ordering::Relaxed);
                            depth.store(next_level, Ordering::Relaxed);
                            found.push(v);
                            break;
                        }
                    }
                }
                examined.fetch_add(count, Ordering::Relaxed);
                Ok(found)
            };
            let pieces: Vec<Vec<usize>> = match &workers {
                Some(pool) => pool.install(|| {
                    depths
                        .par_chunks(BLOCK)
                        .enumerate()
                        .map(visit)
                        .collect::<Result<Vec<_>>>()
                })?,
                None => depths
                    .chunks(BLOCK)
                    .enumerate()
                    .map(visit)
                    .collect::<Result<Vec<_>>>()?,
            };
            pieces.into_iter().flatten().collect::<Vec<_>>()
        } else {
            let visit = |chunk: &[usize]| -> Result<Vec<usize>> {
                let mut found = Vec::new();
                let mut work = context.work_meter();
                let mut count = 0;
                for &u in chunk {
                    for &v in &targets[out[u]..out[u + 1]] {
                        work.charge(1).map_err(err)?;
                        count += 1;
                        if depths[v]
                            .compare_exchange(
                                UNSEEN,
                                next_level,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            found.push(v);
                        }
                        if depths[v].load(Ordering::Relaxed) == next_level {
                            parents[v].fetch_min(u, Ordering::Relaxed);
                        }
                    }
                }
                examined.fetch_add(count, Ordering::Relaxed);
                Ok(found)
            };
            let pieces: Vec<Vec<usize>> = match &workers {
                Some(pool) => pool.install(|| {
                    frontier
                        .par_chunks(64)
                        .map(visit)
                        .collect::<Result<Vec<_>>>()
                })?,
                None => frontier.chunks(64).map(visit).collect::<Result<Vec<_>>>()?,
            };
            pieces.into_iter().flatten().collect::<Vec<_>>()
        };
        rounds.push(
            serde_json::json!({"direction": if pull {"pull"} else {"push"},
            "frontier_vertices": frontier.len(), "frontier_edges": frontier_edges,
            "examined_edges": examined.load(Ordering::Relaxed), "discovered": next.len()}),
        );
        remaining = remaining.saturating_sub(next.iter().map(|&u| out[u + 1] - out[u]).sum());
        if pull && (next.len() as f64) < n as f64 / beta {
            pull = false;
            just_left_pull = true;
        }
        frontier = next;
        level += 1;
    }
    save_diagnostics(
        query,
        serde_json::json!({"variant":"direction-optimizing-bfs-v1",
        "kernel_threads":threads,"alpha":alpha,"beta":beta,"rounds":rounds,"converged":true}),
    )?;
    let schema = schema("bfsDirection").ok_or_else(|| err("BFS schema missing"))?;
    for start in (0..n).step_by(context.limits().batch_rows) {
        context.checkpoint().map_err(err)?;
        let end = n.min(start.saturating_add(context.limits().batch_rows));
        let ids = &graph.node_ids()[start..end];
        let strings: usize = ids.iter().map(|id| id.as_str().len()).sum();
        let parent_strings: usize = (start..end)
            .map(|i| parents[i].load(Ordering::Relaxed))
            .filter(|&p| p != UNSEEN)
            .map(|p| graph.node_ids()[p].as_str().len())
            .sum();
        let owner = Arc::new(
            context
                .reserve(
                    strings
                        .saturating_add(parent_strings)
                        .saturating_add(ids.len().saturating_mul(128))
                        .saturating_add(4096),
                )
                .map_err(err)?,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| id.as_str()),
                )),
                Arc::new(Float64Array::from_iter((start..end).map(|i| {
                    let d = depths[i].load(Ordering::Relaxed);
                    (d != UNSEEN).then_some(d as f64)
                }))),
                Arc::new(StringArray::from_iter((start..end).map(|i| {
                    let p = parents[i].load(Ordering::Relaxed);
                    (p != UNSEEN).then(|| graph.node_ids()[p].as_str())
                }))),
            ],
        )?;
        if !emit(graph_tables::retain_owner(batch, owner)?)? {
            return Ok(false);
        }
    }
    Ok(true)
}

//! Parallel all-edge bucket stepping (delta-star), with an indexed O(V) queue.
use super::stepping_queue::Queue;
use super::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

pub(crate) fn run(
    graph: &GraphProjection,
    weights: &[f64],
    args: &ValidatedArguments,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let context = graph.execution();
    let n = graph.node_count();
    let source = match args.options().get("source") {
        Some(Value::String(s)) => graph.node_ids().iter().position(|id| id.as_str() == s),
        _ => None,
    }
    .ok_or_else(|| err("ssspDeltaStar requires a selected source"))?;
    if weights.len() != graph.edge_count() || weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
        return exec_err!("ssspDeltaStar requires one finite nonnegative weight per edge");
    }
    let delta = number(args, "delta")?;
    let limit = integer(args, "maxIterations")? as usize;
    let undirected = graph.orientation() == grust_algorithms::Orientation::Undirected;
    let m = graph
        .edge_count()
        .checked_mul(if undirected { 2 } else { 1 })
        .ok_or_else(|| err("arc count overflow"))?;
    let _scratch = admitted(context, n, m, 256, 24)?;
    let _trace = context.reserve(limit.saturating_mul(1024)).map_err(err)?;
    let (workers, threads) = pool(context, n)?;
    let mut offsets = allocated(n + 1, 0usize)?;
    let mut meter = context.work_meter();
    for e in graph.edges() {
        meter.charge(1).map_err(err)?;
        offsets[e.source + 1] += 1;
        if undirected {
            offsets[e.target + 1] += 1;
        }
    }
    for i in 0..n {
        meter.charge(1).map_err(err)?;
        offsets[i + 1] += offsets[i];
    }
    let mut cursor = offsets[..n].to_vec();
    let mut arcs = allocated(m, (0usize, 0.0f64))?;
    for (e, &w) in graph.edges().iter().zip(weights) {
        meter.charge(1).map_err(err)?;
        arcs[cursor[e.source]] = (e.target, w);
        cursor[e.source] += 1;
        if undirected {
            arcs[cursor[e.target]] = (e.source, w);
            cursor[e.target] += 1;
        }
    }
    drop(cursor);
    let mut distance = Vec::new();
    distance.try_reserve_exact(n).map_err(err)?;
    let mut changed = Vec::new();
    changed.try_reserve_exact(n).map_err(err)?;
    for _ in 0..n {
        distance.push(AtomicU64::new(f64::INFINITY.to_bits()));
        changed.push(AtomicBool::new(false));
    }
    distance[source].store(0.0f64.to_bits(), Ordering::Relaxed);
    let mut queue = Queue::new(n)?;
    queue.improve(source, &distance);
    let mut frontier = Vec::new();
    frontier.try_reserve_exact(n).map_err(err)?;
    let mut rounds = Vec::new();
    rounds.try_reserve_exact(limit).map_err(err)?;
    while let Some(first) = queue.peek() {
        context.checkpoint().map_err(err)?;
        if rounds.len() == limit {
            return exec_err!("ssspDeltaStar did not converge within maxIterations={limit}");
        }
        let bucket = (f64::from_bits(distance[first].load(Ordering::Relaxed)) / delta).floor();
        if !bucket.is_finite() {
            return exec_err!("ssspDeltaStar bucket overflow; choose larger delta");
        }
        frontier.clear();
        while let Some(u) = queue.peek() {
            let cost = f64::from_bits(distance[u].load(Ordering::Relaxed));
            if (cost / delta).floor() != bucket {
                break;
            }
            queue.pop();
            frontier.push((u, cost));
        }
        let examined = AtomicUsize::new(0);
        let relax = |chunk: &[(usize, f64)]| -> Result<Vec<usize>> {
            let mut updates = Vec::new();
            let mut work = context.work_meter();
            let mut count = 0;
            for &(u, cost) in chunk {
                for &(v, weight) in &arcs[offsets[u]..offsets[u + 1]] {
                    work.charge(1).map_err(err)?;
                    count += 1;
                    let candidate = cost + weight;
                    if !candidate.is_finite() {
                        return exec_err!("ssspDeltaStar distance overflow");
                    }
                    let bits = candidate.to_bits();
                    if bits < distance[v].fetch_min(bits, Ordering::Relaxed)
                        && !changed[v].swap(true, Ordering::Relaxed)
                    {
                        updates.push(v);
                    }
                }
            }
            examined.fetch_add(count, Ordering::Relaxed);
            Ok(updates)
        };
        let updates: Vec<Vec<usize>> = match &workers {
            Some(pool) => pool.install(|| {
                frontier
                    .par_chunks(64)
                    .map(relax)
                    .collect::<Result<Vec<_>>>()
            })?,
            None => frontier.chunks(64).map(relax).collect::<Result<Vec<_>>>()?,
        };
        let mut count = 0;
        for v in updates.into_iter().flatten() {
            meter.charge(1).map_err(err)?;
            queue.improve(v, &distance);
            changed[v].store(false, Ordering::Relaxed);
            count += 1;
        }
        rounds.push(serde_json::json!({"bucket":bucket,"active_vertices":frontier.len(),"updated_vertices":count,
            "examined_edges":examined.load(Ordering::Relaxed)}));
    }
    save_diagnostics(
        query,
        serde_json::json!({"variant":"delta-star-indexed-queue-v1","delta":delta,
        "kernel_threads":threads,"rounds":rounds,"converged":true}),
    )?;
    let schema = schema("ssspDeltaStar").ok_or_else(|| err("SSSP schema missing"))?;
    for start in (0..n).step_by(context.limits().batch_rows) {
        context.checkpoint().map_err(err)?;
        let end = n.min(start.saturating_add(context.limits().batch_rows));
        let ids = &graph.node_ids()[start..end];
        let bytes = ids.iter().map(|id| id.as_str().len()).sum::<usize>();
        let owner = Arc::new(
            context
                .reserve(
                    bytes
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
                    let d = f64::from_bits(distance[i].load(Ordering::Relaxed));
                    d.is_finite().then_some(d)
                }))),
            ],
        )?;
        if !emit(graph_tables::retain_owner(batch, owner)?)? {
            return Ok(false);
        }
    }
    Ok(true)
}

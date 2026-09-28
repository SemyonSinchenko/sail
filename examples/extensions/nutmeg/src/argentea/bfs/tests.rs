use super::super::{
    state::lock,
    tests::{Dispatch, ints, memory, route, worker},
};
use super::*;
use arrow::{
    array::{Array, Int64Array},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::TableProvider,
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionContext,
};
use request::{Request, Verb};
use state::BfsState;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
mod controls;
fn request(verb: Verb, phase: u64, p: usize, n: u64, algorithm: &str) -> Request {
    Request {
        version: 3,
        algorithm: algorithm.into(),
        verb,
        operation_id: "op".into(),
        snapshot_id: "snapshot".into(),
        generation: 1,
        partitions: p,
        vertices: n,
        source: -5,
        max_levels: 14,
        alpha: 14,
        beta: 24,
        max_phase_budget: 32,
        phase,
        batch_rows: 2,
    }
}
async fn stage(
    ctx: &SessionContext,
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<BfsState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plans = vec![];
    for state in states {
        state.configure(&request).unwrap();
        plans.push(
            plan::BfsTable {
                request: request.clone(),
                inputs: inputs.clone(),
                state: Some(state.clone()),
            }
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap(),
        );
    }
    Arc::new(Dispatch(plans))
}
async fn graph(
    ctx: &SessionContext,
    ids: &[i64],
    edges: &[(i64, i64)],
    p: usize,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let nodes = memory(
        ctx,
        (0..p)
            .map(|owner| {
                vec![ints(
                    &["id", "owner"],
                    &ids.iter()
                        .filter(|id| id.rem_euclid(p as i64) == owner as i64)
                        .map(|id| vec![*id, owner as i64])
                        .collect::<Vec<_>>(),
                )]
            })
            .collect(),
    )
    .await;
    let edges = memory(
        ctx,
        (0..p)
            .map(|owner| {
                vec![ints(
                    &["src", "dst", "owner"],
                    &edges
                        .iter()
                        .filter(|(id, _)| id.rem_euclid(p as i64) == owner as i64)
                        .map(|(s, t)| vec![*s, *t, owner as i64])
                        .collect::<Vec<_>>(),
                )]
            })
            .collect(),
    )
    .await;
    vec![nodes, edges]
}
async fn build(
    ctx: &SessionContext,
    base: &Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<BfsState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plan = stage(ctx, base.clone(), inputs, states).await;
    for phase in 0..base.work_slots() {
        for verb in [Verb::Decide, Verb::Apply] {
            let mut request = base.clone();
            request.verb = verb;
            request.phase = phase;
            plan = stage(ctx, request, vec![route(plan, base.partitions)], states).await;
        }
    }
    let mut request = base.clone();
    request.verb = Verb::Result;
    request.phase = base.work_slots();
    stage(ctx, request, vec![route(plan, base.partitions)], states).await
}
fn integer<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}
fn value(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let col = integer(batch, name);
    (!col.is_null(row)).then(|| col.value(row))
}
const IDS: [i64; 8] = [-5, 0, 1, 6, 10, 11, 20, 21];
const EDGES: [(i64, i64); 8] = [
    (-5, 0),
    (-5, 1),
    (-5, 1),
    (0, 6),
    (1, 6),
    (6, 10),
    (10, 10),
    (20, 21),
];
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_bfs_variants_run_real_range_exchanges_with_nullable_results_and_empty_owners() {
    // In-process ownership shim, not Sail workers. Range exchanges and native
    // Arrow barriers are real. Topology/P=11 includes several empty partitions.
    for algorithm in ["bfs_reference", "bfs_frontier", "bfs_direction"] {
        for p in [2, 3, 11] {
            for undirected in [false, true] {
                let ctx = SessionContext::new();
                let drops = Arc::new(AtomicUsize::new(0));
                let states = [
                    BfsState::new(worker(10, 64 << 20, &drops)).unwrap(),
                    BfsState::new(worker(11, 64 << 20, &drops)).unwrap(),
                ];
                let mut edges = EDGES.to_vec();
                if undirected {
                    edges.extend(EDGES.iter().map(|&(s, t)| (t, s)));
                }
                let base = request(Verb::Init, 0, p, 8, algorithm);
                let plan = build(&ctx, &base, graph(&ctx, &IDS, &edges, p).await, &states).await;
                let batches = tokio::time::timeout(
                    Duration::from_secs(30),
                    collect(plan.clone(), ctx.task_ctx()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
                let mut found = std::collections::BTreeMap::new();
                let mut retained = None;
                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        let id = value(batch, "id", row).unwrap();
                        let owner = value(batch, "owner", row).unwrap() as usize;
                        assert!(
                            found
                                .insert(
                                    id,
                                    (value(batch, "distance", row), value(batch, "parent", row))
                                )
                                .is_none()
                        );
                        assert_eq!(value(batch, "distance", row), value(batch, "hops", row));
                        assert_eq!(value(batch, "levels", row), Some(4));
                        assert_eq!(value(batch, "reached", row), Some(5));
                        assert_eq!(value(batch, "converged", row), Some(1));
                        assert_eq!(
                            value(batch, "worker_id", row),
                            Some(states[owner % 2].base.incarnation.worker_id as i64)
                        );
                        let part = states[owner % 2].partition(owner).unwrap().unwrap();
                        let native = lock(&part).unwrap();
                        assert_eq!(
                            value(batch, "adjacency_id", row),
                            Some(native.origin().adjacency_id as i64)
                        );
                        assert_eq!(
                            value(batch, "incoming_adjacency_id", row),
                            Some(native.incoming_identity().unwrap_or(0) as i64)
                        );
                        if id == 11 {
                            retained = Some(batch.column_by_name("parent").unwrap().slice(row, 1));
                        }
                    }
                }
                assert_eq!(
                    found,
                    std::collections::BTreeMap::from([
                        (-5, (Some(0), Some(-5))),
                        (0, (Some(1), Some(-5))),
                        (1, (Some(1), Some(-5))),
                        (6, (Some(2), Some(0))),
                        (10, (Some(3), Some(6))),
                        (11, (None, None)),
                        (20, (None, None)),
                        (21, (None, None))
                    ])
                );
                for owner in 0..p {
                    let part = states[owner % 2].partition(owner).unwrap().unwrap();
                    let part = lock(&part).unwrap();
                    assert_eq!(part.next_phase(), base.work_slots());
                    assert_eq!(
                        part.incoming_identity().is_some(),
                        algorithm == "bfs_direction"
                    );
                }
                for state in &states {
                    state.base.close().unwrap();
                    state.close().unwrap();
                    state.close().unwrap();
                }
                assert!(collect(plan.clone(), ctx.task_ctx()).await.is_err());
                drop(plan);
                drop(states);
                drop(batches);
                let retained = retained.unwrap();
                assert!(retained.is_null(0));
                assert!(drops.load(Ordering::SeqCst) < 2);
                drop(retained);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while drops.load(Ordering::SeqCst) != 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
        }
    }
}
#[test]
fn bfs_request_and_wire_schema_are_strict() {
    let base = request(Verb::Init, 0, 2, 8, "bfs_direction");
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&base).unwrap()).is_ok());
    let mut expanded = base.clone();
    expanded.max_levels = 62;
    expanded.max_phase_budget = 128;
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&expanded).unwrap()).is_ok());
    expanded.max_levels = 63;
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&expanded).unwrap()).is_err());
    expanded.max_levels = 62;
    expanded.max_phase_budget = 129;
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&expanded).unwrap()).is_err());
    for defect in 0..7 {
        let mut v = serde_json::to_value(&base).unwrap();
        match defect {
            0 => v["version"] = 2.into(),
            1 => v["phase"] = 1.into(),
            2 => v["max_levels"] = 15.into(),
            3 => v["alpha"] = 0.into(),
            4 => v["beta"] = 0.into(),
            5 => v["algorithm"] = "bfs".into(),
            6 => v["extra"] = true.into(),
            _ => unreachable!(),
        };
        assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&v).unwrap()).is_err());
    }
    let mut result = base.clone();
    result.verb = Verb::Result;
    result.phase = base.work_slots();
    for key in [
        "argentea.operation",
        "argentea.algorithm",
        "argentea.phase",
        "argentea.channel",
    ] {
        let mut schema = result.message_schema(result.phase, true).as_ref().clone();
        schema.metadata.insert(key.into(), "changed".into());
        let plan = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            schema,
        )));
        assert!(result.validate_inputs(&[plan]).is_err());
    }
    for name in ["distance", "hops", "parent"] {
        assert!(
            Request::result_schema()
                .field_with_name(name)
                .unwrap()
                .is_nullable()
        );
    }
}

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
use state::WccState;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn request(verb: Verb, phase: u64, p: usize, n: u64, algorithm: &str) -> Request {
    Request {
        version: 4,
        algorithm: algorithm.into(),
        verb,
        operation_id: "op".into(),
        snapshot_id: "snapshot".into(),
        generation: 1,
        partitions: p,
        vertices: n,
        max_rounds: 14,
        seed: 42,
        max_phase_budget: 128,
        phase,
        batch_rows: 2,
    }
}
async fn stage(
    ctx: &SessionContext,
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<WccState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plans = vec![];
    for state in states {
        state.configure(&request).unwrap();
        plans.push(
            plan::WccTable {
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
    states: &[Arc<WccState>],
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
async fn wcc_exact_components_through_range_exchanges() {
    // Real Arrow/range exchange, in-process owner dispatch; not a Sail runtime gate.
    for algorithm in ["wcc_reference", "wcc_star"] {
        for p in [1, 2, 3, 11] {
            let ctx = SessionContext::new();
            let drops = Arc::new(AtomicUsize::new(0));
            let states = [
                WccState::new(worker(10, 64 << 20, &drops)).unwrap(),
                WccState::new(worker(11, 64 << 20, &drops)).unwrap(),
            ];
            let mut base = request(Verb::Init, 0, p, IDS.len() as u64, algorithm);
            if p == 1 {
                base.max_rounds = if algorithm == "wcc_reference" { 62 } else { 19 };
            }
            let plan = build(&ctx, &base, graph(&ctx, &IDS, &EDGES, p).await, &states).await;
            let batches = tokio::time::timeout(
                Duration::from_secs(30),
                collect(plan.clone(), ctx.task_ctx()),
            )
            .await
            .unwrap()
            .unwrap();
            let mut found = std::collections::BTreeMap::new();
            for b in &batches {
                for row in 0..b.num_rows() {
                    let id = value(b, "id", row).unwrap();
                    assert!(
                        found
                            .insert(id, value(b, "component", row).unwrap())
                            .is_none()
                    );
                    let owner = id.rem_euclid(p as i64) as usize;
                    assert_eq!(value(b, "owner", row), Some(owner as i64));
                    assert_eq!(
                        value(b, "worker_id", row),
                        Some(states[owner % 2].base.incarnation.worker_id as i64)
                    );
                    assert_eq!(value(b, "converged", row), Some(1));
                    let part = states[owner % 2].partition(owner).unwrap().unwrap();
                    assert_eq!(
                        value(b, "adjacency_id", row),
                        Some(lock(&part).unwrap().origin().adjacency_id as i64)
                    );
                }
            }
            assert_eq!(
                found,
                std::collections::BTreeMap::from([
                    (-5, -5),
                    (0, -5),
                    (1, -5),
                    (6, -5),
                    (10, -5),
                    (11, 11),
                    (20, 20),
                    (21, 20)
                ])
            );
            for owner in 0..p {
                assert_eq!(
                    lock(&states[owner % 2].partition(owner).unwrap().unwrap())
                        .unwrap()
                        .next_phase(),
                    base.work_slots()
                );
            }
            drop(batches);
            drop(plan);
            for state in &states {
                state.base.close().unwrap();
                state.close().unwrap();
            }
            drop(states);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
    }
}
#[test]
fn wcc_request_rejects_unknown_fields_and_overflow_before_phase_arithmetic() {
    let mut base = request(Verb::Init, 0, 3, 8, "wcc_star");
    base.max_rounds = 3;
    // Three contraction rounds need 6K+10=28 stages: init/result plus13 pairs.
    assert_eq!(base.work_slots(), 13);
    base.validate().unwrap();
    for rounds in [20, u64::MAX] {
        let mut r = base.clone();
        r.max_rounds = rounds;
        assert!(r.validate().is_err());
    }
    let mut value = serde_json::to_value(&base).unwrap();
    value["source"] = 0.into();
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_err());
    let mut changed = base.clone();
    changed.seed = 0;
    assert!(!base.compatible(&changed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn star_three_round_cap_never_returns_partial_components() {
    // Seed42 on this fixture was observed to need more than three rounds.
    // Retain that case as a cap control instead of selecting an easier seed.
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let states = [
        WccState::new(worker(10, 64 << 20, &drops)).unwrap(),
        WccState::new(worker(11, 64 << 20, &drops)).unwrap(),
    ];
    let mut base = request(Verb::Init, 0, 3, 8, "wcc_star");
    base.max_rounds = 3;
    base.max_phase_budget = 32;
    let plan = build(&ctx, &base, graph(&ctx, &IDS, &EDGES, 3).await, &states).await;
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        collect(plan.clone(), ctx.task_ctx()),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "a capped operation must not publish components"
    );
    assert!((0..3).any(|owner| {
        states[owner % 2]
            .retained_partition_for_test(owner)
            .is_some_and(|part| lock(&part).unwrap().rounds() == 3)
    }));
    drop(result);
    drop(plan);
    for state in &states {
        state.base.close().unwrap();
        state.close().unwrap();
    }
    drop(states);
    // Range-exchange tasks are aborted asynchronously after the failed collect.
    // Require eventual release of both leases, without assuming same-tick teardown.
    tokio::time::timeout(Duration::from_secs(5), async {
        while drops.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed WCC exchange retained host leases");
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn statistics_require_all_fields_completion_and_stable_mode() {
    use wire::{COMPLETE, Message, STATISTIC, Statistics};
    let base = request(Verb::Init, 0, 2, 8, "wcc_reference");
    let message = Message {
        owner: 0,
        kind: STATISTIC,
        producer: 0,
        sequence: 0,
        target: 0,
        source: 0,
        value: 0,
        mode: 0,
        aux: 4,
        worker: 10,
        adjacency: 1,
    };
    let mut stats = Statistics::default();
    assert!(stats.values(&base, (10, 1)).is_err());
    stats.receive(message).unwrap();
    assert!(stats.clone().receive(message).is_err());
    assert!(
        stats
            .clone()
            .receive(Message {
                sequence: 1,
                target: 1,
                mode: 1,
                ..message
            })
            .is_err()
    );
    assert!(
        stats
            .clone()
            .receive(Message {
                sequence: 1,
                kind: COMPLETE,
                target: 0,
                ..message
            })
            .is_err()
    );
    for field in 1..7 {
        stats
            .receive(Message {
                sequence: field,
                target: field,
                aux: 0,
                ..message
            })
            .unwrap();
    }
    assert!(stats.values(&base, (10, 1)).is_err());
    assert!(
        stats
            .clone()
            .receive(Message {
                sequence: 7,
                kind: COMPLETE,
                target: 0,
                aux: 3,
                ..message
            })
            .is_err()
    );
    let end = Message {
        sequence: 7,
        kind: COMPLETE,
        target: 0,
        ..message
    };
    stats.receive(end).unwrap();
    assert_eq!(stats.values(&base, (10, 1)).unwrap().vertices, 4);
    assert!(stats.receive(end).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_extremes_survive_wcc_wire_and_retained_result_slice() {
    for algorithm in ["wcc_reference", "wcc_star"] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let states = [
            WccState::new(worker(10, 64 << 20, &drops)).unwrap(),
            WccState::new(worker(11, 64 << 20, &drops)).unwrap(),
        ];
        let ids = [i64::MIN, 0, i64::MAX];
        let edges = [(i64::MAX, i64::MIN)];
        let base = request(Verb::Init, 0, 3, 3, algorithm);
        let plan = build(&ctx, &base, graph(&ctx, &ids, &edges, 3).await, &states).await;
        let batches = tokio::time::timeout(
            Duration::from_secs(30),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await
        .unwrap()
        .unwrap();
        let mut found = std::collections::BTreeMap::new();
        let mut retained = None;
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let id = value(batch, "id", row).unwrap();
                found.insert(id, value(batch, "component", row).unwrap());
                if id == i64::MAX {
                    retained = Some(batch.column_by_name("component").unwrap().slice(row, 1));
                }
            }
        }
        assert_eq!(
            found,
            std::collections::BTreeMap::from([(i64::MIN, i64::MIN), (0, 0), (i64::MAX, i64::MIN)])
        );
        drop(batches);
        drop(plan);
        for state in &states {
            state.base.close().unwrap();
            state.close().unwrap();
        }
        drop(states);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "one result slice must still retain its host lease"
        );
        assert_eq!(
            retained
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MIN
        );
        drop(retained);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}

#[path = "tests/resource_failure.rs"]
mod resource_failure;

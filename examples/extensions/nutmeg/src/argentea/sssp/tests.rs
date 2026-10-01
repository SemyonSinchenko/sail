use super::super::{
    state::lock,
    tests::{Dispatch, ints, memory, route, worker},
};
use super::*;
use arrow::{
    array::{Array, Float64Array, Int64Array},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::TableProvider,
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionContext,
};
use request::{Request, Verb};
use state::SsspState;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn request(verb: Verb, phase: u64, p: usize, n: u64, algorithm: &str) -> Request {
    Request {
        version: 5,
        algorithm: algorithm.into(),
        verb,
        operation_id: "op".into(),
        snapshot_id: "snapshot".into(),
        generation: 1,
        partitions: p,
        vertices: n,
        max_rounds: 14,
        source: -5,
        delta: 4.0,
        max_phase_budget: 128,
        phase,
        batch_rows: 2,
    }
}
async fn stage(
    ctx: &SessionContext,
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<SsspState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plans = vec![];
    for state in states {
        state.configure(&request).unwrap();
        plans.push(
            plan::SsspTable {
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
    edges: &[(i64, i64, f64)],
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
                let selected = edges
                    .iter()
                    .filter(|(id, _, _)| id.rem_euclid(p as i64) == owner as i64)
                    .collect::<Vec<_>>();
                let integers = ints(
                    &["src", "dst", "owner"],
                    &selected
                        .iter()
                        .map(|(s, t, _)| vec![*s, *t, owner as i64])
                        .collect::<Vec<_>>(),
                );
                let mut fields = integers
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (**f).clone())
                    .collect::<Vec<_>>();
                fields.push(arrow::datatypes::Field::new(
                    "weight",
                    arrow::datatypes::DataType::Float64,
                    false,
                ));
                let mut columns = integers.columns().to_vec();
                columns.push(Arc::new(Float64Array::from(
                    selected.iter().map(|e| e.2).collect::<Vec<_>>(),
                )));
                vec![
                    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
                        .unwrap(),
                ]
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
    states: &[Arc<SsspState>],
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
const EDGES: [(i64, i64, f64); 10] = [
    (-5, 0, 12.0),
    (-5, 1, 1.0),
    (-5, 1, 1.0),
    (1, 0, 1.0),
    (0, 6, 0.0),
    (1, 6, 1.0),
    (6, 10, 0.25),
    (10, 10, 0.0),
    (20, 21, 1.0),
    (0, 1, 0.0),
];
fn distance(batch: &RecordBatch, row: usize) -> Option<f64> {
    let a = batch
        .column_by_name("distance")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (!a.is_null(row)).then(|| a.value(row))
}
fn label(batch: &RecordBatch, row: usize) -> Option<(f64, i64, i64)> {
    let result = distance(batch, row).map(|d| {
        (
            d,
            value(batch, "hops", row).unwrap(),
            value(batch, "parent", row).unwrap(),
        )
    });
    if result.is_none() {
        assert_eq!(value(batch, "hops", row), None);
        assert_eq!(value(batch, "parent", row), None);
    }
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sssp_exact_weighted_labels_through_range_exchanges() {
    // Real Arrow/range exchange, in-process owner dispatch; not a Sail runtime gate.
    for algorithm in ["sssp_reference", "sssp_delta_star"] {
        for p in [1, 2, 3, 11] {
            let ctx = SessionContext::new();
            let drops = Arc::new(AtomicUsize::new(0));
            let states = [
                SsspState::new(worker(10, 64 << 20, &drops)).unwrap(),
                SsspState::new(worker(11, 64 << 20, &drops)).unwrap(),
            ];
            let mut base = request(Verb::Init, 0, p, IDS.len() as u64, algorithm);
            if p == 1 {
                base.max_rounds = 62;
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
                    assert!(found.insert(id, label(b, row)).is_none());
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
                    (-5, Some((0.0, 0, -5))),
                    (0, Some((2.0, 2, 1))),
                    (1, Some((1.0, 1, -5))),
                    (6, Some((2.0, 2, 1))),
                    (10, Some((2.25, 3, 6))),
                    (11, None),
                    (20, None),
                    (21, None)
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
fn sssp_request_rejects_unknown_fields_and_overflow_before_phase_arithmetic() {
    let mut base = request(Verb::Init, 0, 3, 8, "sssp_delta_star");
    base.max_rounds = 3;
    // Three relaxation rounds plus topology: init/result plus four pairs.
    assert_eq!(base.work_slots(), 4);
    base.validate().unwrap();
    for rounds in [63, u64::MAX] {
        let mut r = base.clone();
        r.max_rounds = rounds;
        assert!(r.validate().is_err());
    }
    let mut value = serde_json::to_value(&base).unwrap();
    value["seed"] = 0.into();
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_err());
    let mut changed = base.clone();
    changed.source = 0;
    assert!(!base.compatible(&changed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_round_cap_never_returns_partial_distances() {
    // The target at three hops cannot be reached in one synchronous round.
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let states = [
        SsspState::new(worker(10, 64 << 20, &drops)).unwrap(),
        SsspState::new(worker(11, 64 << 20, &drops)).unwrap(),
    ];
    let mut base = request(Verb::Init, 0, 3, 8, "sssp_delta_star");
    base.max_rounds = 1;
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
        "a capped operation must not publish distances"
    );
    assert!((0..3).any(|owner| {
        states[owner % 2]
            .retained_partition_for_test(owner)
            .is_some_and(|part| lock(&part).unwrap().rounds() == 1)
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
    .expect("failed SSSP exchange retained host leases");
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn statistics_require_all_fields_completion_and_stable_mode() {
    use wire::{COMPLETE, Message, STATISTIC, Statistics};
    let base = request(Verb::Init, 0, 2, 8, "sssp_reference");
    let message = Message {
        owner: 0,
        kind: STATISTIC,
        producer: 0,
        sequence: 0,
        target: 0,
        source: 0,
        hops: 0,
        distance: 0.0,
        bucket: -1.0,
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
    for field in 1..8 {
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
                sequence: 8,
                kind: COMPLETE,
                target: 0,
                aux: 3,
                ..message
            })
            .is_err()
    );
    let end = Message {
        sequence: 8,
        kind: COMPLETE,
        target: 0,
        ..message
    };
    stats.receive(end).unwrap();
    assert_eq!(stats.values(&base, (10, 1)).unwrap().vertices, 4);
    assert!(stats.receive(end).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_extremes_survive_sssp_wire_and_retained_result_slice() {
    for algorithm in ["sssp_reference", "sssp_delta_star"] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let states = [
            SsspState::new(worker(10, 64 << 20, &drops)).unwrap(),
            SsspState::new(worker(11, 64 << 20, &drops)).unwrap(),
        ];
        let ids = [i64::MIN, 0, i64::MAX];
        let edges = [(i64::MAX, i64::MIN, 2.5)];
        let mut base = request(Verb::Init, 0, 3, 3, algorithm);
        base.source = i64::MAX;
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
                found.insert(id, label(batch, row));
                if id == i64::MIN {
                    retained = Some(batch.column_by_name("distance").unwrap().slice(row, 1));
                }
            }
        }
        assert_eq!(
            found,
            std::collections::BTreeMap::from([
                (i64::MIN, Some((2.5, 1, i64::MAX))),
                (0, None),
                (i64::MAX, Some((0.0, 0, i64::MAX)))
            ])
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
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.5
        );
        drop(retained);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}

#[path = "tests/adversarial.rs"]
mod adversarial;

#[path = "tests/resource_failure.rs"]
mod resource_failure;

#[path = "tests/initialization.rs"]
mod initialization;

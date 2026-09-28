use super::super::{
    batches,
    state::lock,
    tests::{Dispatch, ints, memory, route, worker},
};
use super::*;
use arrow::{
    array::{Array, Float64Array},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::TableProvider,
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionContext,
};
mod controls;
use request::{Request, Verb};
use state::DeltaState;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn request(verb: Verb, phase: u64, p: usize, n: u64) -> Request {
    Request {
        version: 2,
        algorithm: "pagerank_delta".into(),
        verb,
        operation_id: "op".into(),
        snapshot_id: "snapshot".into(),
        generation: 1,
        partitions: p,
        vertices: n,
        damping: 0.85,
        tolerance: 1e-3,
        max_pushes: 7,
        max_phase_budget: 32,
        phase,
        batch_rows: 2,
    }
}
async fn stage(
    ctx: &SessionContext,
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<DeltaState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plans = vec![];
    for state in states {
        state.configure(&request).unwrap();
        plans.push(
            plan::DeltaTable {
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
    states: &[Arc<DeltaState>],
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
fn floats<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_datafusion_ranges_run_signed_frontiers_certificates_and_done_with_empty_owners() {
    // The third vertex is dangling: uniform initialization creates negative
    // edge and dangling residual pushes. P=11 forces eight empty owners.
    for p in [2, 3, 11] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let states = [
            DeltaState::new(worker(10, 64 << 20, &drops)).unwrap(),
            DeltaState::new(worker(11, 64 << 20, &drops)).unwrap(),
        ];
        let base = request(Verb::Init, 0, p, 3);
        let plan = build(
            &ctx,
            &base,
            graph(&ctx, &[0, 1, 2], &[(0, 1), (1, 1)], p).await,
            &states,
        )
        .await;
        let batches = tokio::time::timeout(
            Duration::from_secs(30),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let mut ranks = [0.0; 3];
        let mut certificate = None;
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let id = batches::integers(batch, "id").unwrap().value(row) as usize;
                let owner = batches::integers(batch, "owner").unwrap().value(row) as usize;
                ranks[id] = floats(batch, "pagerank").value(row);
                let residual = floats(batch, "residual_l1").value(row);
                assert!(residual <= base.tolerance);
                let bound = floats(batch, "stationary_error_bound").value(row);
                assert_eq!(bound, residual / (1.0 - base.damping));
                if let Some(previous) = certificate {
                    assert_eq!(residual, previous);
                }
                certificate = Some(residual);
                assert_eq!(batches::integers(batch, "converged").unwrap().value(row), 1);
                assert!(batches::integers(batch, "pushes").unwrap().value(row) > 0);
                assert_eq!(
                    batches::integers(batch, "worker_id").unwrap().value(row),
                    states[owner % 2].base.incarnation.worker_id as i64
                );
                let native = states[owner % 2].partition(owner).unwrap().unwrap();
                assert_eq!(
                    batches::integers(batch, "adjacency_id").unwrap().value(row) as u64,
                    lock(&native).unwrap().adjacency_identity()
                );
            }
        }
        assert!((ranks.iter().sum::<f64>() - 1.0).abs() < 1e-14);
        let transformed = [
            (0.15 + 0.85 * ranks[2]) / 3.0,
            0.15 / 3.0 + 0.85 * (ranks[0] + ranks[1] + ranks[2] / 3.0),
            (0.15 + 0.85 * ranks[2]) / 3.0,
        ];
        let residual: f64 = transformed
            .iter()
            .zip(ranks)
            .map(|(a, b)| (a - b).abs())
            .sum();
        assert!((residual - certificate.unwrap()).abs() < 1e-14);
        for owner in 0..p {
            let part = states[owner % 2].partition(owner).unwrap().unwrap();
            assert_eq!(lock(&part).unwrap().next_phase(), base.work_slots());
        }
        let retained = batches[0]
            .column_by_name("residual_l1")
            .unwrap()
            .slice(0, 1);
        for state in &states {
            state.base.close().unwrap();
            state.close().unwrap();
            state.close().unwrap();
        }
        assert!(collect(plan.clone(), ctx.task_ctx()).await.is_err());
        drop(plan);
        drop(states);
        drop(batches);
        assert!(
            retained
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .is_finite()
        );
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

#[test]
fn v2_request_budget_and_statistics_channel_are_strict() {
    let base = request(Verb::Init, 0, 2, 3);
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&base).unwrap()).is_ok());
    for defect in 0..5 {
        let mut value = serde_json::to_value(&base).unwrap();
        match defect {
            0 => value["version"] = 1.into(),
            1 => value["phase"] = 1.into(),
            2 => value["max_pushes"] = 8.into(),
            3 => value["extra"] = true.into(),
            4 => value["max_phase_budget"] = 31.into(),
            _ => unreachable!(),
        };
        assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut result = base.clone();
    result.verb = Verb::Result;
    result.phase = base.work_slots();
    for defect in ["argentea.channel", "argentea.phase", "argentea.operation"] {
        let mut schema = result.message_schema(result.phase, true).as_ref().clone();
        schema.metadata.insert(defect.into(), "changed".into());
        let plan = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            schema,
        )));
        assert!(result.validate_inputs(&[plan]).is_err());
    }
}

#[tokio::test]
async fn capped_result_and_quota_failure_never_return_rows() {
    for cap in [true, false] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let bytes = if cap { 4 << 20 } else { 24 << 10 };
        let state = DeltaState::new(worker(1, bytes, &drops)).unwrap();
        let mut base = request(Verb::Init, 0, 1, 3);
        base.max_pushes = 0;
        let plan = build(
            &ctx,
            &base,
            graph(&ctx, &[0, 1, 2], &[(0, 1)], 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let result = collect(plan.clone(), ctx.task_ctx()).await;
        assert!(result.is_err());
        if cap {
            assert!(result.unwrap_err().to_string().contains("push cap"));
        }
        assert!(state.base.resources.execution.usage().unwrap().peak_bytes <= bytes);
        state.base.close().unwrap();
        state.close().unwrap();
        drop(plan);
        drop(state);
    }
}

#[test]
fn typed_statistics_reject_duplicate_missing_and_infinite_nonempty_fields() {
    let request = request(Verb::Decide, 0, 2, 3);
    fn row(kind: i64, sequence: i64, target: i64, value: f64, aux: i64) -> wire::Message {
        wire::Message {
            owner: 0,
            kind,
            producer: 0,
            sequence,
            target,
            value,
            mode: 0,
            aux,
            worker: 1,
            adjacency: 1,
        }
    }
    let mut stats = wire::Statistics::default();
    stats.receive(row(wire::FLOAT_STAT, 0, 0, 0.0, 0)).unwrap();
    assert!(stats.receive(row(wire::FLOAT_STAT, 1, 0, 0.0, 0)).is_err());
    let mut stats = wire::Statistics::default();
    assert!(stats.receive(row(wire::COMPLETE, 0, 0, 0.0, 0)).is_err());
    for vertices in [0, 1] {
        let mut stats = wire::Statistics::default();
        for target in 0..3 {
            stats
                .receive(row(
                    wire::FLOAT_STAT,
                    target,
                    target,
                    if target == 2 { f64::INFINITY } else { 0.0 },
                    0,
                ))
                .unwrap();
        }
        for target in 3..9 {
            stats
                .receive(row(
                    wire::INT_STAT,
                    target,
                    target,
                    0.0,
                    if target == 3 { vertices } else { 0 },
                ))
                .unwrap();
        }
        stats
            .receive(row(wire::COMPLETE, 9, 0, 0.0, vertices))
            .unwrap();
        assert_eq!(stats.values(&request).is_ok(), vertices == 0);
    }
}

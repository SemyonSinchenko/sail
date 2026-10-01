use super::*;
mod controls;
pub(super) mod initialization;
pub(super) mod lifetime_allocations;
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    physical_expr::{
        PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint, expressions::Column,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
        collect, repartition::RepartitionExec,
    },
    prelude::SessionContext,
};
use datafusion_common::{Result, ScalarValue, tree_node::TreeNodeRecursion};
use futures::TryStreamExt;
use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct LeaseOwner(Arc<AtomicUsize>);
impl Drop for LeaseOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn worker(id: u64, bytes: usize, drops: &Arc<AtomicUsize>) -> Arc<WorkerState> {
    WorkerState::new(
        Incarnation {
            session_id: "test-session".into(),
            job_id: 42,
            worker_id: id,
            package_identity: "test-package".into(),
            operation_id: "op".into(),
        },
        bytes,
        MemoryLease::new(Arc::new(LeaseOwner(drops.clone())), bytes as u64),
    )
    .unwrap()
}

pub(super) fn ints(names: &[&str], rows: &[Vec<i64>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        (0..names.len())
            .map(|i| {
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row[i]).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect(),
    )
    .unwrap()
}

pub(super) async fn memory(
    ctx: &SessionContext,
    batches: Vec<Vec<RecordBatch>>,
) -> Arc<dyn ExecutionPlan> {
    let schema = batches.iter().flatten().next().unwrap().schema();
    MemTable::try_new(schema, batches)
        .unwrap()
        .scan(&ctx.state(), None, &[], None)
        .await
        .unwrap()
}

pub(super) fn route(input: Arc<dyn ExecutionPlan>, partitions: usize) -> Arc<dyn ExecutionPlan> {
    let expr = Arc::new(Column::new(
        "owner",
        input.schema().index_of("owner").unwrap(),
    )) as Arc<dyn PhysicalExpr>;
    let partitioning = RangePartitioning::try_new(
        [PhysicalSortExpr::new_default(expr)].into(),
        (1..partitions)
            .map(|p| SplitPoint::new(vec![ScalarValue::Int64(Some(p as i64))]))
            .collect(),
    )
    .unwrap();
    Arc::new(RepartitionExec::try_new(input, Partitioning::Range(partitioning)).unwrap())
}

/// In-process placement shim only. Real DataFusion range exchanges and native
/// execution run unchanged; this is not a Sail scheduler or two-process test.
#[derive(Debug)]
pub(super) struct Dispatch(pub(super) Vec<Arc<dyn ExecutionPlan>>);
impl DisplayAs for Dispatch {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestNativeOwners")
    }
}
impl ExecutionPlan for Dispatch {
    fn name(&self) -> &str {
        "TestNativeOwners"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.0[0].properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.iter().collect()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(children)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0[partition % self.0.len()].execute(partition, context)
    }
}

async fn stage(
    ctx: &SessionContext,
    request: Request,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    states: &[Arc<WorkerState>],
) -> Arc<dyn ExecutionPlan> {
    let mut plans = Vec::new();
    for state in states {
        state.configure(&request).unwrap();
        plans.push(
            ArgenteaTable {
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

fn request(verb: request::Verb, round: u64, partitions: usize, vertices: u64) -> Request {
    Request {
        version: 1,
        verb,
        operation_id: "op".into(),
        snapshot_id: "snapshot".into(),
        generation: 1,
        partitions,
        vertices,
        damping: 0.85,
        round,
        batch_rows: 2,
    }
}

#[test]
fn protocol_rejects_changed_round_and_unknown_fields() {
    let mut value = serde_json::to_value(request(request::Verb::Init, 0, 3, 5)).unwrap();
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_ok());
    value["round"] = 1.into();
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_err());
    value["round"] = 0.into();
    value["unrecognized"] = true.into();
    assert!(Request::parse(request::TYPE_URL, &serde_json::to_vec(&value).unwrap()).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_range_exchange_reuses_two_native_owners_across_four_rounds() {
    let ids: [i64; 7] = [-9, -2, 0, 3, 11, 28, 100];
    let edges: [(i64, i64); 7] = [
        (-9, -2),
        (-9, -2),
        (-2, 0),
        (0, 0),
        (3, 11),
        (11, 3),
        (28, 0),
    ];
    for count in [2, 3, 11] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let states = [worker(10, 4 << 20, &drops), worker(11, 4 << 20, &drops)];
        let nodes = memory(
            &ctx,
            (0..count)
                .map(|p| {
                    vec![ints(
                        &["id", "owner"],
                        &ids.iter()
                            .filter(|id| id.rem_euclid(count as i64) == p as i64)
                            .map(|id| vec![*id, p as i64])
                            .collect::<Vec<_>>(),
                    )]
                })
                .collect(),
        )
        .await;
        let input_edges = memory(
            &ctx,
            (0..count)
                .map(|p| {
                    vec![ints(
                        &["src", "dst", "owner"],
                        &edges
                            .iter()
                            .filter(|(id, _)| id.rem_euclid(count as i64) == p as i64)
                            .map(|(src, dst)| vec![*src, *dst, p as i64])
                            .collect::<Vec<_>>(),
                    )]
                })
                .collect(),
        )
        .await;
        let mut plan = stage(
            &ctx,
            request(request::Verb::Init, 0, count, ids.len() as u64),
            vec![nodes, input_edges],
            &states,
        )
        .await;
        for round in 1..4 {
            plan = stage(
                &ctx,
                request(request::Verb::Round, round, count, ids.len() as u64),
                vec![route(plan, count)],
                &states,
            )
            .await;
        }
        let plan = stage(
            &ctx,
            request(request::Verb::Result, 3, count, ids.len() as u64),
            vec![route(plan, count)],
            &states,
        )
        .await;
        let batches = tokio::time::timeout(
            Duration::from_secs(20),
            collect(plan.clone(), ctx.task_ctx()),
        )
        .await
        .unwrap()
        .unwrap();
        let mut expected = vec![1.0 / ids.len() as f64; ids.len()];
        for _ in 0..4 {
            let previous = expected.clone();
            expected.fill(0.0);
            let mut dangling = 0.0;
            for (i, id) in ids.iter().enumerate() {
                let outgoing: Vec<_> = edges.iter().filter(|(src, _)| src == id).collect();
                if outgoing.is_empty() {
                    dangling += previous[i];
                }
                for (_, dst) in &outgoing {
                    expected[ids.iter().position(|id| id == dst).unwrap()] +=
                        previous[i] / outgoing.len() as f64;
                }
            }
            for rank in &mut expected {
                *rank = 0.15 / ids.len() as f64 + 0.85 * (*rank + dangling / ids.len() as f64);
            }
        }
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            ids.len()
        );
        for batch in &batches {
            let row_ids = batches::integers(batch, "id").unwrap();
            let owners = batches::integers(batch, "owner").unwrap();
            let workers = batches::integers(batch, "worker_id").unwrap();
            let adjacency = batches::integers(batch, "adjacency_id").unwrap();
            let ranks = batch
                .column_by_name("pagerank")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let p = owners.value(row) as usize;
                assert!(
                    (ranks.value(row)
                        - expected[ids.iter().position(|id| *id == row_ids.value(row)).unwrap()])
                    .abs()
                        < 1e-14
                );
                assert_eq!(
                    workers.value(row),
                    states[p % 2].incarnation.worker_id as i64
                );
                let part = states[p % 2].partition(p).unwrap().unwrap();
                assert_eq!(
                    adjacency.value(row) as u64,
                    state::lock(&part).unwrap().adjacency_identity()
                );
            }
        }
        // Even owners with no vertices finish every round and retain one CSR.
        for p in 0..count {
            assert_eq!(
                state::lock(&states[p % 2].partition(p).unwrap().unwrap())
                    .unwrap()
                    .next_round(),
                4
            );
        }
        for state in &states {
            state.close().unwrap();
            state.close().unwrap();
        }
        assert!(collect(plan.clone(), ctx.task_ctx()).await.is_err());
        drop(plan);
        drop(states);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "Arrow buffers retain both worker leases after close"
        );
        drop(batches);
        // RepartitionExec owns background tasks whose final plan drops occur
        // after receiver EOF. Wait for actual lease-owner destruction rather
        // than assuming collect() joins those tasks synchronously.
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
fn output_array_slice_retains_admission_after_worker_close() {
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 1 << 20, &drops);
    let usage = state.resources.execution.clone();
    let mut output =
        batches::BatchBuilder::new(Request::result_schema(), 1, &state.resources).unwrap();
    output.push(&[1, 0, 1, 99, 3, 0], 0.5);
    let batch = output.finish().unwrap();
    let slice = batch.column(0).slice(0, 1);
    state.close().unwrap();
    drop(state);
    drop(batch);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    drop(slice);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[test]
fn scoped_configuration_replay_and_quota_are_fail_closed() {
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 16 << 10, &drops);
    let request = request(request::Verb::Init, 0, 2, 2);
    state.claim(&request, 0).unwrap();
    assert!(state.claim(&request, 0).is_err());
    let mut wrong = request.clone();
    wrong.snapshot_id = "other".into();
    assert!(state.configure(&wrong).is_err());
    let mut wrong = request.clone();
    wrong.operation_id = "other".into();
    assert!(state.configure(&wrong).is_err());
    state.close().unwrap();
    assert!(state.configure(&request).is_err());
    let state = worker(2, 16 << 10, &drops);
    assert!(
        batches::BatchBuilder::new(Request::result_schema(), 100_000, &state.resources).is_err()
    );
}

#[tokio::test]
async fn missing_producer_marker_fails_without_publishing_ranks() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 1 << 20, &drops);
    let init = request(request::Verb::Init, 0, 1, 2);
    let nodes = memory(
        &ctx,
        vec![vec![ints(&["id", "owner"], &[vec![0, 0], vec![1, 0]])]],
    )
    .await;
    let edges = memory(
        &ctx,
        vec![vec![ints(&["src", "dst", "owner"], &[vec![0, 1, 0]])]],
    )
    .await;
    let plan = stage(
        &ctx,
        init.clone(),
        vec![nodes, edges],
        std::slice::from_ref(&state),
    )
    .await;
    let messages: Vec<RecordBatch> = plan
        .execute(0, ctx.task_ctx())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let first = &messages[0];
    let truncated = first.slice(0, 1); // update only, missing producer completion
    let incoming = memory(&ctx, vec![vec![truncated]]).await;
    let result = stage(
        &ctx,
        request(request::Verb::Result, 0, 1, 2),
        vec![incoming],
        std::slice::from_ref(&state),
    )
    .await;
    assert!(
        collect(result, ctx.task_ctx())
            .await
            .unwrap_err()
            .to_string()
            .contains("incomplete")
    );
    // Failure cancels the operation. The retained state never committed round0.
    assert!(state.check().is_err());
}

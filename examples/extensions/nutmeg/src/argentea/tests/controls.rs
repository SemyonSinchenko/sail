use super::*;
use datafusion::physical_plan::{empty::EmptyExec, stream::RecordBatchStreamAdapter};
use std::task::Poll;
use tokio::sync::Notify;

#[test]
fn schema_round_identity_and_nullability_are_checked_before_execution() {
    let result = request(request::Verb::Result, 4, 2, 3);
    for key in [
        "argentea.operation",
        "argentea.snapshot",
        "argentea.generation",
        "argentea.round",
    ] {
        let mut schema = result.message_schema(4).as_ref().clone();
        schema.metadata.insert(key.into(), "changed".into());
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(schema)).with_partitions(2));
        assert!(result.validate_inputs(&[plan]).is_err(), "{key}");
    }
    let schema = result.message_schema(4);
    let mut fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[0] = fields[0].clone().with_nullable(true);
    let wrong = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    assert!(
        result
            .validate_inputs(&[Arc::new(EmptyExec::new(wrong))])
            .is_err()
    );
    let good: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema).with_partitions(2));
    result.validate_inputs(&[good]).unwrap();
}

#[derive(Debug)]
struct PendingInput {
    inner: Arc<dyn ExecutionPlan>,
    polled: Arc<Notify>,
}
impl DisplayAs for PendingInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PendingInput")
    }
}
impl ExecutionPlan for PendingInput {
    fn name(&self) -> &str {
        "PendingInput"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let polled = self.polled.clone();
        let stream = futures::stream::poll_fn(move |_| {
            polled.notify_one();
            Poll::Pending
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            stream,
        )))
    }
}

#[tokio::test]
async fn close_interrupts_pending_input_without_waiting_for_a_batch() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 1 << 20, &drops);
    let nodes = memory(&ctx, vec![vec![ints(&["id", "owner"], &[vec![0, 0]])]]).await;
    let edges = memory(&ctx, vec![vec![ints(&["src", "dst", "owner"], &[])]]).await;
    let polled = Arc::new(Notify::new());
    let pending: Arc<dyn ExecutionPlan> = Arc::new(PendingInput {
        inner: nodes,
        polled: polled.clone(),
    });
    let plan = stage(
        &ctx,
        request(request::Verb::Init, 0, 1, 1),
        vec![pending, edges],
        std::slice::from_ref(&state),
    )
    .await;
    let task = tokio::spawn(collect(plan, ctx.task_ctx()));
    tokio::time::timeout(Duration::from_secs(5), polled.notified())
        .await
        .unwrap();
    state.close().unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[tokio::test]
async fn dropping_unfinished_output_cancels_but_keeps_emitted_arrays_valid() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 1 << 20, &drops);
    let nodes = memory(
        &ctx,
        vec![vec![ints(&["id", "owner"], &[vec![0, 0], vec![1, 0]])]],
    )
    .await;
    let edges = memory(
        &ctx,
        vec![vec![ints(
            &["src", "dst", "owner"],
            &vec![vec![0, 1, 0]; 4],
        )]],
    )
    .await;
    let plan = stage(
        &ctx,
        request(request::Verb::Init, 0, 1, 2),
        vec![nodes, edges],
        std::slice::from_ref(&state),
    )
    .await;
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let emitted = stream.try_next().await.unwrap().unwrap();
    drop(stream);
    assert!(state.check().is_err());
    state.close().unwrap();
    drop(plan);
    drop(state);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(batches::integers(&emitted, "target").unwrap().value(0), 1);
    drop(emitted);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn incorrect_global_vertex_count_is_rejected_before_rank_publication() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = worker(1, 1 << 20, &drops);
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
    let init = stage(
        &ctx,
        request(request::Verb::Init, 0, 1, 3),
        vec![nodes, edges],
        std::slice::from_ref(&state),
    )
    .await;
    let messages = collect(init, ctx.task_ctx()).await.unwrap();
    let incoming = memory(&ctx, vec![messages]).await;
    let result = stage(
        &ctx,
        request(request::Verb::Result, 0, 1, 3),
        vec![incoming],
        std::slice::from_ref(&state),
    )
    .await;
    assert!(
        collect(result, ctx.task_ctx())
            .await
            .unwrap_err()
            .to_string()
            .contains("global vertex count")
    );
}

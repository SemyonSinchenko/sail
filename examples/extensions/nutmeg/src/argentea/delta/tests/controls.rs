use super::*;
use datafusion::{
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_common::{Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use futures::TryStreamExt;
use std::{fmt, task::Poll};
use tokio::sync::Notify;

#[derive(Debug)]
struct PendingInput {
    inner: Arc<dyn ExecutionPlan>,
    polled: Arc<Notify>,
}
impl DisplayAs for PendingInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PendingV2Input")
    }
}
impl ExecutionPlan for PendingInput {
    fn name(&self) -> &str {
        "PendingV2Input"
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
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            futures::stream::poll_fn(move |_| {
                polled.notify_one();
                Poll::Pending
            }),
        )))
    }
}

#[tokio::test]
async fn close_interrupts_both_residual_barriers_with_initialized_state() {
    for verb in [Verb::Decide, Verb::Apply] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = DeltaState::new(worker(1, 4 << 20, &drops)).unwrap();
        let init = stage(
            &ctx,
            request(Verb::Init, 0, 1, 3),
            graph(&ctx, &[0, 1, 2], &[(0, 1)], 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let batches = collect(init, ctx.task_ctx()).await.unwrap();
        let mut input = memory(&ctx, vec![batches]).await;
        if verb == Verb::Apply {
            let plan = stage(
                &ctx,
                request(Verb::Decide, 0, 1, 3),
                vec![input],
                std::slice::from_ref(&state),
            )
            .await;
            let batches = collect(plan, ctx.task_ctx()).await.unwrap();
            input = memory(&ctx, vec![batches]).await;
        }
        let polled = Arc::new(Notify::new());
        let pending = Arc::new(PendingInput {
            inner: input,
            polled: polled.clone(),
        });
        let plan = stage(
            &ctx,
            request(verb, 0, 1, 3),
            vec![pending],
            std::slice::from_ref(&state),
        )
        .await;
        let task = tokio::spawn(collect(plan, ctx.task_ctx()));
        tokio::time::timeout(Duration::from_secs(5), polled.notified())
            .await
            .unwrap();
        let native = state.partition(0).unwrap().unwrap();
        assert!(lock(&native).unwrap().vertex_count() > 0);
        drop(native);
        state.base.close().unwrap();
        state.close().unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }
}

#[tokio::test]
async fn abandoned_statistics_output_cancels_but_its_float_slice_stays_valid() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = DeltaState::new(worker(1, 4 << 20, &drops)).unwrap();
    let usage = state.base.resources.execution.clone();
    let plan = stage(
        &ctx,
        request(Verb::Init, 0, 1, 3),
        graph(&ctx, &[0, 1, 2], &[(0, 1)], 1).await,
        std::slice::from_ref(&state),
    )
    .await;
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    let retained = batch.column_by_name("value").unwrap().slice(0, 1);
    drop(stream);
    assert!(state.base.check().is_err());
    state.base.close().unwrap();
    state.close().unwrap();
    drop(plan);
    drop(state);
    drop(batch);
    assert_eq!(
        retained
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(usage.usage().unwrap().live_bytes > 0);
    drop(retained);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}

#[tokio::test]
async fn late_or_missing_statistics_rows_fail_before_a_decision() {
    for duplicate in [false, true] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = DeltaState::new(worker(1, 4 << 20, &drops)).unwrap();
        let init = stage(
            &ctx,
            request(Verb::Init, 0, 1, 3),
            graph(&ctx, &[0, 1, 2], &[(0, 1)], 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let mut batches = collect(init, ctx.task_ctx()).await.unwrap();
        if duplicate {
            batches.push(batches[0].clone());
        } else {
            batches.pop();
        }
        let input = memory(&ctx, vec![batches]).await;
        let plan = stage(
            &ctx,
            request(Verb::Decide, 0, 1, 3),
            vec![input],
            std::slice::from_ref(&state),
        )
        .await;
        assert!(collect(plan, ctx.task_ctx()).await.is_err());
        // The adapter guard cancels the shared domain on failure, including any
        // concurrently waiting phase; no certified result can be obtained.
        assert!(state.base.check().is_err());
        state.base.close().unwrap();
        state.close().unwrap();
    }
}

#[test]
fn producer_origin_and_late_publication_remain_pinned_to_the_job() {
    let drops = Arc::new(AtomicUsize::new(0));
    let state = DeltaState::new(worker(10, 4 << 20, &drops)).unwrap();
    let request = request(Verb::Init, 0, 2, 2);
    state.claim(&request, 0).unwrap();
    assert!(state.claim(&request, 0).is_err());
    state.origin(0, (10, 1)).unwrap();
    state.origin(0, (10, 1)).unwrap();
    assert!(state.origin(0, (11, 1)).is_err());
    assert!(state.origin(0, (10, 2)).is_err());
    state.base.close().unwrap();
    state.close().unwrap();
    assert!(state.configure(&request).is_err());
    assert!(state.claim(&request, 1).is_err());
}

//! The production adapter must release temporary input admission before state.
use super::*;

#[tokio::test]
async fn initialization_releases_raw_input_before_state() {
    for n in [1_024usize, 65_536] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = SsspState::new(worker(7, 32 << 20, &drops)).unwrap();
        let mut request = request(Verb::Init, 0, 1, n as u64, "sssp_delta_star");
        request.source = 0;
        let op = state.configure(&request).unwrap();
        let usage = state.base.resources.execution.clone();
        let before = usage.usage().unwrap().live_bytes;
        // For these 64-bit test targets: admitted raw vertices (8n) plus the
        // conservative CSR build (56n + 4104). No arcs, all declared isolates.
        // Retaining raw input through labels/statistics exceeds this envelope.
        let build_bytes = n * 64 + 4096 + size_of::<usize>();
        let blocker = usage.reserve((32 << 20) - before - build_bytes).unwrap();
        let ids = (0..n as i64).collect::<Vec<_>>();
        let inputs = graph(&ctx, &ids, &[], 1).await;
        let part = input::initialize(&state, &request, op, 0, &inputs, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(part.state_rows().count(), n);
        assert_eq!(part.reached_count(), 1);
        assert_eq!(usage.usage().unwrap().peak_bytes, 32 << 20);
        println!(
            "NATIVE_INPUT_LIFETIME kind=sssp n={n} build_headroom={build_bytes} peak_admitted_bytes={}",
            usage.usage().unwrap().peak_bytes
        );
        drop(part);
        drop(blocker);
        assert_eq!(usage.usage().unwrap().live_bytes, before);
        drop(state);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

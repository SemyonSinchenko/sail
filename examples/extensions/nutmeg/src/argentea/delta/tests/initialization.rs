//! Production residual initialization releases raw vertices before dense state.
use super::super::super::tests::{initialization::inputs, lifetime_allocations};
use super::*;
#[tokio::test]
async fn residual_initialization_releases_raw_vertex_storage_before_dense_state() {
    let mut observed = vec![];
    for n in [1_024, 65_536] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = DeltaState::new(worker(7, 32 << 20, &drops)).unwrap();
        let request = request(Verb::Init, 0, 3, n as u64);
        let op = state.configure(&request).unwrap();
        let inputs = inputs(&ctx, n).await;
        let usage = state.base.resources.execution.clone();
        let before = usage.usage().unwrap().live_bytes;
        let (result, counts) = lifetime_allocations::measure(
            n * 8,
            input::initialize(&state, &request, op, 0, &inputs, ctx.task_ctx()),
        )
        .await;
        let part = result.unwrap();
        assert_eq!(part.state_rows().count(), n);
        assert!(part.state_rows().all(|(_, score, residual)| score.to_bits()
            == (1.0 / n as f64).to_bits()
            && residual.to_bits() == 0.0f64.to_bits()));
        assert_eq!(part.collecting_phase(), Some(0));
        // Raw vertices, CSR IDs, scores and residuals each allocate n*8.
        assert_eq!(counts.calls, 4);
        observed.push((n, counts.peak));
        println!(
            "NATIVE_RESIDUAL_INPUT_LIFETIME n={n} matched_allocations={} peak_simultaneous_vertex_buffers={}",
            counts.calls, counts.peak
        );
        drop(part);
        assert_eq!(usage.usage().unwrap().live_bytes, before);
        drop(state);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    assert_eq!(observed, vec![(1_024, 3), (65_536, 3)]);
}

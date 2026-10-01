//! Production WCC initialization releases raw vertex buffers before roots.
use super::super::super::tests::{initialization::inputs, lifetime_allocations};
use super::*;
async fn initialize(algorithm: &str) {
    let mut observed = vec![];
    for n in [1_024, 65_536] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = WccState::new(worker(7, 32 << 20, &drops)).unwrap();
        let request = request(Verb::Init, 0, 3, n as u64, algorithm);
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
        assert!(part.state_rows().all(|row| row.id == row.component));
        assert_eq!(part.collecting_phase(), Some(0));
        assert_eq!(counts.calls, 3);
        observed.push((n, counts.peak));
        println!(
            "NATIVE_WCC_INPUT_LIFETIME algorithm={algorithm} n={n} matched_allocations={} peak_simultaneous_vertex_buffers={}",
            counts.calls, counts.peak
        );
        drop(part);
        assert_eq!(usage.usage().unwrap().live_bytes, before);
        drop(state);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    assert_eq!(observed, vec![(1_024, 2), (65_536, 2)]);
}

#[tokio::test]
async fn wcc_reference_initialization_releases_raw_vertex_storage_before_roots() {
    initialize("wcc_reference").await;
}
#[tokio::test]
async fn wcc_star_initialization_releases_raw_vertex_storage_before_roots() {
    initialize("wcc_star").await;
}

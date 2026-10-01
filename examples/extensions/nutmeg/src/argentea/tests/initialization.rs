//! Production initialization: split batches, retained CSR arcs and raw release.
use super::*;

pub(in crate::argentea) async fn inputs(
    ctx: &SessionContext,
    n: usize,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let nodes = memory(
        ctx,
        vec![
            (0..n)
                .collect::<Vec<_>>()
                .chunks(n / 2)
                .map(|chunk| {
                    ints(
                        &["id", "owner"],
                        &chunk
                            .iter()
                            .map(|&i| vec![i as i64 * 3, 0])
                            .collect::<Vec<_>>(),
                    )
                })
                .collect(),
        ],
    )
    .await;
    let edges = memory(
        ctx,
        vec![vec![
            ints(&["src", "dst", "owner"], &[vec![0, 3, 0], vec![0, 3, 0]]),
            ints(&["src", "dst", "owner"], &[vec![3, 6, 0], vec![6, 6, 0]]),
        ]],
    )
    .await;
    vec![nodes, edges]
}
#[tokio::test]
async fn pagerank_initialization_releases_raw_vertex_storage_before_ranks() {
    let mut observed = vec![];
    for n in [1_024, 65_536] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = worker(7, 32 << 20, &drops);
        let request = request(request::Verb::Init, 0, 3, n as u64);
        let op = state.configure(&request).unwrap();
        let inputs = inputs(&ctx, n).await;
        let usage = state.resources.execution.clone();
        let before = usage.usage().unwrap().live_bytes;
        let (result, counts) = lifetime_allocations::measure(
            n * 8,
            input::initialize(&state, &request, op, 0, &inputs, ctx.task_ctx()),
        )
        .await;
        let part = result.unwrap();
        assert_eq!(part.ranks().count(), n);
        assert!(
            part.ranks()
                .all(|(_, value)| value.to_bits() == (1.0 / n as f64).to_bits())
        );
        // The raw final-capacity vertices, CSR IDs, and ranks all allocate n*8.
        // The old path keeps all three live; the split path holds at most two.
        assert_eq!(counts.calls, 3);
        observed.push((n, counts.peak));
        println!(
            "NATIVE_RANK_INPUT_LIFETIME n={n} matched_allocations={} peak_simultaneous_vertex_buffers={}",
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

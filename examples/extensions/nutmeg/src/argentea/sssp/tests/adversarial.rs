use super::*;

#[test]
fn weighted_request_and_wire_reject_nonfinite_or_ambiguous_values() {
    for delta in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut base = request(Verb::Init, 0, 2, 8, "sssp_delta_star");
        base.delta = delta;
        assert!(base.validate().is_err());
    }
    for bucket in [-2.0, 0.5, f64::NAN, f64::INFINITY] {
        assert!(wire::bucket(bucket).is_err());
    }
    assert_eq!(wire::bucket(-1.0).unwrap(), None);
    assert_eq!(wire::bucket(0.0).unwrap(), Some(0.0));
    let base = request(Verb::Init, 0, 2, 8, "sssp_delta_star");
    let message = wire::Message {
        owner: 0,
        kind: wire::STATISTIC,
        producer: 0,
        sequence: 0,
        target: 0,
        source: 0,
        hops: 0,
        mode: 0,
        aux: 4,
        worker: 10,
        adjacency: 1,
        distance: 0.0,
        bucket: 0.0,
    };
    let mut stats = wire::Statistics::default();
    stats.receive(message).unwrap();
    assert!(stats.values(&base, (10, 1)).is_err());
    assert!(
        stats
            .receive(wire::Message {
                sequence: 1,
                target: 1,
                bucket: 1.0,
                ..message
            })
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_weights_fail_before_partition_publication() {
    for weight in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = SsspState::new(worker(10, 64 << 20, &drops)).unwrap();
        let base = request(Verb::Init, 0, 1, 2, "sssp_delta_star");
        let plan = stage(
            &ctx,
            base,
            graph(&ctx, &[-5, 0], &[(-5, 0, weight)], 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let result = collect(plan.clone(), ctx.task_ctx()).await;
        assert!(result.unwrap_err().to_string().contains("weight"));
        assert!(state.retained_partition_for_test(0).is_none());
        drop(plan);
        state.base.close().unwrap();
        state.close().unwrap();
        drop(state);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

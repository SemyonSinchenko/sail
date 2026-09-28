use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_init_claim_refusal_keeps_a_causal_record_after_unwind() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = SsspState::new(worker(10, 1 << 20, &drops)).unwrap();
    let path = std::env::temp_dir().join(format!(
        "argentea-sssp-memory-failure-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    state.base.test_audit_file(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap(),
    );
    let base = request(Verb::Init, 0, 1, 2, "sssp_reference");
    let init = stage(
        &ctx,
        base.clone(),
        graph(&ctx, &[-5, 0], &[], 1).await,
        std::slice::from_ref(&state),
    )
    .await;
    let batches = collect(init.clone(), ctx.task_ctx()).await.unwrap();
    assert!(state.partition(0).unwrap().is_some());
    let input = memory(&ctx, vec![batches]).await;
    let mut next = base;
    next.verb = Verb::Decide;
    let plan = stage(&ctx, next, vec![input], std::slice::from_ref(&state)).await;
    let used = state.base.resources.execution.usage().unwrap().live_bytes;
    let held = state
        .base
        .resources
        .execution
        .reserve((1 << 20) - used)
        .unwrap();
    let failure = collect(plan.clone(), ctx.task_ctx()).await.unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("procedure memory budget exceeded")
    );
    let records = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let causes = records
        .iter()
        .filter(|r| r["code"] == "native_memory_budget")
        .collect::<Vec<_>>();
    assert_eq!(causes.len(), 1);
    assert_eq!(causes[0]["initialized"], true);
    assert_eq!(causes[0]["native_phase"], 0);
    assert_eq!(causes[0]["memory_limit"], 1 << 20);
    assert!(causes[0]["adjacency_id"].as_u64().unwrap() > 0);
    assert!(!records.iter().any(|r| r["event"] == "result"));
    drop(held);
    drop(plan);
    drop(init);
    state.base.close().unwrap();
    state.close().unwrap();
    drop(state);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    std::fs::remove_file(path).unwrap();
}

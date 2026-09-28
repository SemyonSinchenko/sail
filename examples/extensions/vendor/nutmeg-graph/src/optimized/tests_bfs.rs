use super::tests::{execute, graph};
use super::*;

#[test]
fn direction_bfs_matches_reference_and_exercises_parallel_push_pull() {
    let n = BLOCK * 8;
    let ids: Vec<_> = (0..n as i64).collect();
    let mut edges = Vec::new();
    for u in 1..65 {
        edges.push((0, u));
        for v in 65..4161 {
            edges.push((u, v));
        }
    }
    for u in 65..4161 {
        edges.push((u, 4161));
    }
    edges.extend([(4161, 4162), (4162, 4163), (4163, 4163), (0, 1)]);
    let mut baseline = None;
    for width in [1, 2, 8] {
        let g = graph(&ids, &edges, width);
        let reference = grust_algorithms::bfs(&g, "0").unwrap();
        let (batches, diagnostics) = execute(&g, "bfsDirection", serde_json::json!({"source":"0"}));
        let mut row = 0;
        for batch in &batches {
            let distance = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let parent = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let want = reference.values()[row];
                if want.is_infinite() {
                    assert!(distance.is_null(i));
                    assert!(parent.is_null(i));
                } else {
                    assert_eq!(distance.value(i), want);
                    let p: usize = parent.value(i).parse().unwrap();
                    if row == 0 {
                        assert_eq!(p, 0);
                    } else {
                        assert!(edges.contains(&(p, row)));
                        assert_eq!(reference.values()[p] + 1.0, want);
                    }
                }
                row += 1;
            }
        }
        assert_eq!(row, n);
        let rounds = diagnostics["rounds"].as_array().unwrap();
        assert_eq!(rounds[0]["direction"], "push");
        assert!(rounds.iter().any(|r| r["direction"] == "pull"));
        assert!(
            rounds
                .windows(2)
                .any(|r| r[0]["direction"] == "pull" && r[1]["direction"] == "push")
        );
        assert_eq!(
            diagnostics["kernel_threads"].as_u64().unwrap(),
            width as u64
        );
        if let Some(expected) = &baseline {
            assert_eq!(
                &batches, expected,
                "parent trees must also be deterministic"
            );
        } else {
            baseline = Some(batches);
        }
    }
}

#[test]
fn direction_bfs_pull_charges_and_checks_blocks_without_edges() {
    let n = BLOCK * 8;
    let ids: Vec<_> = (0..n as i64).collect();
    // Removing the source's only edge from remaining volume selects pull.
    // Everything after vertex 1 has no adjacency; an edge-only work meter
    // would let this whole scan bypass the budget and cancellation checks.
    let graph = graph(&ids, &[(0, 1)], 8);
    let context = graph
        .execution()
        .child(ChildLimits {
            work_units: 2 + n + 100,
            concurrency: Some(8),
            ..ChildLimits::default()
        })
        .unwrap();
    let view = graph.with_execution(&context).unwrap();
    let query = Query {
        context,
        diagnostics: Default::default(),
    };
    let options = serde_json::json!({"source":"0"});
    let args = validate("bfsDirection", options.as_object().unwrap()).unwrap();
    let error = run("bfsDirection", &view, &args, &query, &mut |_| {
        panic!("partial output")
    })
    .unwrap_err();
    assert!(error.to_string().contains("work budget"), "{error}");
    assert_eq!(query.usage().unwrap().live_bytes, 0);
}

#[test]
fn direction_bfs_rejects_missing_source_and_iteration_exhaustion() {
    let g = graph(&[0, 1, 2], &[(0, 1), (1, 2)], 1);
    let query = Query {
        context: g.execution().clone(),
        diagnostics: Default::default(),
    };
    for options in [
        serde_json::json!({"source":"missing"}),
        serde_json::json!({"source":"0","maxIterations":1}),
    ] {
        let args = validate("bfsDirection", options.as_object().unwrap()).unwrap();
        assert!(run("bfsDirection", &g, &args, &query, &mut |_| Ok(true)).is_err());
    }
}

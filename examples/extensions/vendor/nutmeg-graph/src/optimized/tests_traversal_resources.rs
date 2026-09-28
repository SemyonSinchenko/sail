use super::tests::graph;
use super::*;

fn execute(
    name: &str,
    graph: &GraphProjection,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let options = if name == "bfsDirection" {
        serde_json::json!({"source":"0"})
    } else {
        serde_json::json!({"source":"0", "weightProperty":"w"})
    };
    let args = validate(name, options.as_object().unwrap())?;
    let view = graph.with_execution(&query.context).map_err(err)?;
    if name == "bfsDirection" {
        run(name, &view, &args, query, emit)
    } else {
        stepping::run(&view, &vec![1.0; view.edge_count()], &args, query, emit)
    }
}

#[test]
fn traversal_refusals_and_stream_stop_release_scratch_but_retain_exported_arrays() {
    let n = BLOCK * 2;
    let ids: Vec<_> = (0..n as i64).collect();
    let edges: Vec<_> = (1..n).map(|v| (0, v)).collect();
    let graph = graph(&ids, &edges, 2);
    for name in ["bfsDirection", "ssspDeltaStar"] {
        let query = |limits| Query {
            context: graph.execution().child(limits).unwrap(),
            diagnostics: Default::default(),
        };
        let refused = query(ChildLimits {
            memory_bytes: Some(1024),
            ..ChildLimits::default()
        });
        let error = execute(name, &graph, &refused, &mut |_| panic!("refused output")).unwrap_err();
        assert!(error.to_string().contains("memory"), "{name}: {error}");
        assert_eq!(refused.usage().unwrap().live_bytes, 0);

        // CSR preparation charges 2E+V. The first traversal must inspect E
        // edges in this star, so 3E+V-1 fails during traversal, not preparation.
        let limited = query(ChildLimits {
            work_units: 3 * edges.len() + n - 1,
            concurrency: Some(2),
            ..ChildLimits::default()
        });
        let error = execute(name, &graph, &limited, &mut |_| panic!("partial output")).unwrap_err();
        assert!(error.to_string().contains("work budget"), "{name}: {error}");
        let usage = limited.usage().unwrap();
        assert!(usage.counted_work().unwrap() >= 2 * edges.len() + n);
        assert_eq!(usage.live_bytes, 0);

        let cancelled = query(ChildLimits::default());
        let mut count = 0;
        let error = execute(name, &graph, &cancelled, &mut |_| {
            count += 1;
            cancelled.cancel()?;
            Ok(true)
        })
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{name}: {error}");
        assert_eq!(count, 1);
        assert_eq!(cancelled.usage().unwrap().live_bytes, 0);

        let stopped = query(ChildLimits::default());
        let mut retained = None;
        assert!(
            !execute(name, &graph, &stopped, &mut |batch| {
                // Consumers may retain a single exported Arrow array after the
                // batch/producer disappears. Its owner must still hold admission.
                retained = Some(batch.column(1).clone());
                Ok(false)
            })
            .unwrap()
        );
        assert!(stopped.usage().unwrap().live_bytes > 0);
        assert!(stopped.usage().unwrap().live_bytes < 100_000);
        drop(retained);
        assert_eq!(stopped.usage().unwrap().live_bytes, 0);

        let complete = query(ChildLimits::default());
        let mut rows = 0;
        assert!(
            execute(name, &graph, &complete, &mut |batch| {
                rows += batch.num_rows();
                Ok(true)
            })
            .unwrap()
        );
        assert_eq!(rows, n, "refusal/cancellation must not poison the graph");
        assert_eq!(complete.usage().unwrap().live_bytes, 0);
    }
}

#[test]
fn traversal_invalid_options_are_rejected_before_execution() {
    for (name, options) in [
        ("bfsDirection", serde_json::json!({"source":"0", "alpha":0})),
        ("bfsDirection", serde_json::json!({"source":"0", "beta":-1})),
        (
            "bfsDirection",
            serde_json::json!({"source":"0", "defaultWeight":1}),
        ),
        (
            "bfsDirection",
            serde_json::json!({"source":"0", "orientation":"incoming"}),
        ),
        (
            "ssspDeltaStar",
            serde_json::json!({"source":"0", "weightProperty":"w", "delta":0}),
        ),
        (
            "ssspDeltaStar",
            serde_json::json!({"source":"0", "weightProperty":"w", "delta":-1}),
        ),
        (
            "ssspDeltaStar",
            serde_json::json!({"source":"0", "weightProperty":"w", "defaultWeight":1}),
        ),
        (
            "ssspDeltaStar",
            serde_json::json!({"source":"0", "weightProperty":"w", "maxIterations":0}),
        ),
    ] {
        assert!(
            validate(name, options.as_object().unwrap()).is_err(),
            "{name}: {options}"
        );
    }
}

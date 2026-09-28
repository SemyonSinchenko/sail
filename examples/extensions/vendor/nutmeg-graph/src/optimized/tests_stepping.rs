use super::*;
use grust_algorithms::{Orientation, ProjectionEdge};

#[test]
fn parallel_stepping_matches_dijkstra_across_buckets_widths_and_zero_cycles() {
    let n = BLOCK * 8;
    let mut edges = vec![(0, 1, 8.), (0, 2, 1.), (2, 1, 1.), (1, 3, 0.), (3, 1, 0.)];
    let mut seed = 42u64;
    for u in 3..n - 1 {
        for _ in 0..4 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            edges.push((u, (seed as usize) % (n - 1), ((seed >> 32) % 16) as f64));
        }
    }
    edges.push((2, 4, 2.));
    for width in [1, 2, 8] {
        for delta in [0.5, 4., 100.] {
            let context = ExecutionContext::new(ExecutionLimits {
                memory_bytes: 256 << 20,
                work_units: usize::MAX,
                batch_rows: 257,
                deadline: None,
            })
            .unwrap()
            .with_concurrency(width)
            .unwrap();
            let weights: Vec<_> = edges.iter().map(|e| e.2).collect();
            let graph = GraphProjection::from_topology(
                SnapshotIdentity::new("stepping".into(), "1".into(), "test".into()).unwrap(),
                (0..n).map(|i| i.to_string().into()).collect(),
                edges
                    .iter()
                    .enumerate()
                    .map(|(ordinal, &(source, target, _))| ProjectionEdge {
                        source,
                        target,
                        ordinal,
                        id: None,
                    })
                    .collect(),
                Some(weights.clone()),
                Orientation::Outgoing,
                &context,
            )
            .unwrap();
            let expected = grust_algorithms::dijkstra(&graph, "0").unwrap();
            let options = serde_json::json!({"source":"0","weightProperty":"w","delta":delta,"maxIterations":1000});
            let args = validate("ssspDeltaStar", options.as_object().unwrap()).unwrap();
            let query = Query {
                context: context.clone(),
                diagnostics: Default::default(),
            };
            let mut offset = 0;
            stepping::run(&graph, &weights, &args, &query, &mut |batch| {
                let actual = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let want = expected.values()[offset + row];
                    if want.is_infinite() {
                        assert!(actual.is_null(row));
                    } else {
                        assert_eq!(actual.value(row), want);
                    }
                }
                offset += batch.num_rows();
                Ok(true)
            })
            .unwrap();
            assert_eq!(offset, n);
            let trace = query.diagnostics.lock().unwrap();
            assert_eq!(trace["kernel_threads"].as_u64().unwrap(), width as u64);
            assert!(trace["rounds"].as_array().unwrap().len() > 1);
        }
    }
}

#[test]
fn stepping_reports_overflow_and_caps_without_emitting_partial_results() {
    let graph = super::tests::graph(&[0, 1, 2], &[(0, 1), (0, 2), (1, 2)], 1);
    for (weights, options, expected) in [
        // The overflowing candidate to 2 is dominated by the direct cost 1.
        // Overflow remains an error even when it cannot improve a label.
        (
            vec![f64::MAX * 0.75, 1.0, f64::MAX * 0.75],
            serde_json::json!({"source":"0", "weightProperty":"w", "delta":1}),
            "distance overflow",
        ),
        (
            vec![10.0, 1.0, 1.0],
            serde_json::json!({"source":"0", "weightProperty":"w", "delta":f64::MIN_POSITIVE}),
            "bucket overflow",
        ),
        (
            vec![1.0, 1.0, 1.0],
            serde_json::json!({"source":"0", "weightProperty":"w", "maxIterations":1}),
            "did not converge",
        ),
        (
            vec![1.0, 1.0, 1.0],
            serde_json::json!({"source":"absent", "weightProperty":"w"}),
            "requires a selected source",
        ),
    ] {
        let context = graph.execution().child(ChildLimits::default()).unwrap();
        let view = graph.with_execution(&context).unwrap();
        let query = Query {
            context,
            diagnostics: Default::default(),
        };
        let args = validate("ssspDeltaStar", options.as_object().unwrap()).unwrap();
        let error = stepping::run(&view, &weights, &args, &query, &mut |_| {
            panic!("partial output")
        })
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(query.usage().unwrap().live_bytes, 0);
    }
}

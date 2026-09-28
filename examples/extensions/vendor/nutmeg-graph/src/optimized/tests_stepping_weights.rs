use super::*;

fn edge_batch(rows: &[(&str, &str, &str, Option<f64>)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "source",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "target",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
        (
            "w",
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.3))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn replace(registry: &SessionRegistry, batches: &[RecordBatch]) {
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::AsStaged);
    for batch in batches {
        tx.push_edges(batch).unwrap();
    }
    tx.finish().unwrap();
}

fn distances(table: &AlgorithmTable) -> BTreeMap<String, Option<f64>> {
    table
        .batches()
        .unwrap()
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| {
                    (
                        ids.value(i).to_string(),
                        (!values.is_null(i)).then(|| values.value(i)),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn stepping_weights_follow_filtered_ordinals_across_batches_and_undirected_arcs() {
    prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(32 << 20);
    // Selected ordinals skip both an entire batch and rows inside batches.
    // Reversed selected endpoints require undirected adjacency to reach c.
    replace(
        &registry,
        &[
            edge_batch(&[("a", "b", "skip", None)]),
            edge_batch(&[
                ("b", "a", "keep", Some(2.0)),
                ("a", "c", "skip", Some(-1.0)),
            ]),
            edge_batch(&[
                ("b", "c", "keep", Some(3.0)),
                ("c", "d", "keep", Some(-0.0)),
            ]),
        ],
    );
    let options = serde_json::json!({"source":"a", "weightProperty":"w", "relationshipTypes":["keep"], "orientation":"undirected"});
    let args = validate("ssspDeltaStar", options.as_object().unwrap()).unwrap();
    let query = registry.store.query(QueryLimits::default()).unwrap();
    let (graph, weights, owner) = registry
        .store
        .stepping_projection("g", &args, &query.context)
        .unwrap();
    assert_eq!(
        graph.edges().iter().map(|e| e.ordinal).collect::<Vec<_>>(),
        [1, 3, 4]
    );
    assert_eq!(weights, [2.0, 3.0, 0.0]);
    assert_eq!(weights[2].to_bits(), 0.0f64.to_bits());
    assert!(query.usage().unwrap().live_bytes >= weights.len() * 8);
    drop((weights, owner));
    assert_eq!(query.usage().unwrap().live_bytes, 0);
    let table = registry
        .algorithm(
            "ssspDeltaStar",
            "g",
            options.as_object().unwrap(),
            ColumnNames::Grust,
        )
        .unwrap();
    assert_eq!(
        distances(&table),
        BTreeMap::from([
            ("a".into(), Some(0.0)),
            ("b".into(), Some(2.0)),
            ("c".into(), Some(5.0)),
            ("d".into(), Some(5.0)),
        ])
    );
}

#[test]
fn lazy_stepping_read_keeps_original_weights_after_drop_and_recreate() {
    prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(32 << 20);
    let options = serde_json::json!({"source":"a", "weightProperty":"w"});
    replace(&registry, &[edge_batch(&[("a", "b", "e", Some(2.0))])]);
    let original = registry
        .algorithm(
            "ssspDeltaStar",
            "g",
            options.as_object().unwrap(),
            ColumnNames::Grust,
        )
        .unwrap();
    assert!(registry.drop_graph("g").unwrap());
    replace(&registry, &[edge_batch(&[("a", "b", "e", Some(20.0))])]);
    let recreated = registry
        .algorithm(
            "ssspDeltaStar",
            "g",
            options.as_object().unwrap(),
            ColumnNames::Grust,
        )
        .unwrap();
    assert_eq!(distances(&original)["b"], Some(2.0));
    assert_eq!(distances(&recreated)["b"], Some(20.0));
    assert_eq!(distances(&original)["b"], Some(2.0));
}

#[test]
fn stepping_rejects_invalid_weights_and_releases_capture_reservations() {
    prepare_output_schemas().unwrap();
    for weight in [
        None,
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ] {
        let registry = SessionRegistry::new(32 << 20);
        replace(&registry, &[edge_batch(&[("a", "b", "e", weight)])]);
        let options = serde_json::json!({"source":"a", "weightProperty":"w"});
        let table = registry
            .algorithm(
                "ssspDeltaStar",
                "g",
                options.as_object().unwrap(),
                ColumnNames::Grust,
            )
            .unwrap();
        let query = table.query().unwrap();
        assert!(table.batches_for(&query).is_err(), "{weight:?}");
        assert_eq!(query.usage().unwrap().live_bytes, 0);
    }
}

#[test]
fn stepping_requires_double_weight_even_when_integer_projection_is_valid() {
    prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(32 << 20);
    let batch = RecordBatch::try_from_iter([
        ("source", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
        ("target", Arc::new(StringArray::from(vec!["b"])) as ArrayRef),
        ("w", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap();
    replace(&registry, &[batch]);
    let options = serde_json::json!({"source":"a", "weightProperty":"w"});
    let table = registry
        .algorithm(
            "ssspDeltaStar",
            "g",
            options.as_object().unwrap(),
            ColumnNames::Grust,
        )
        .unwrap();
    let query = table.query().unwrap();
    let error = table.batches_for(&query).unwrap_err();
    assert!(error.to_string().contains("must be DOUBLE"), "{error}");
    assert_eq!(query.usage().unwrap().live_bytes, 0);
}

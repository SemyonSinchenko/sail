use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::min_max;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_function::aggregate::struct_min::{StructMin, struct_min_udaf};

use super::{RemoteExecutionCodec, decode_remote_physical_plan, encode_remote_physical_plan};

#[tokio::test]
async fn worker_plan_round_trip_keeps_compact_min_accumulator() -> Result<()> {
    let fields = Fields::from(vec![
        Field::new("distance", DataType::Float64, true),
        Field::new("hops", DataType::Int64, true),
        Field::new("parent", DataType::Int64, true),
    ]);
    let groups = 4096;
    let values: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![2.0; groups])),
            Arc::new(Int64Array::from(vec![1; groups])),
            Arc::new(Int64Array::from(vec![-7; groups])),
        ],
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Struct(fields), true),
    ]));
    let keys = Int64Array::from_iter_values((0..groups).map(|i| i as i64));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(keys), Arc::clone(&values)],
    )?;
    let input = DataSourceExec::from_data_source(MemorySourceConfig::try_new(
        &[vec![batch]],
        Arc::clone(&schema),
        None,
    )?);
    let expression = Arc::new(
        AggregateExprBuilder::new(struct_min_udaf(), vec![Arc::new(Column::new("value", 1))])
            .schema(Arc::clone(&schema))
            .alias("minimum")
            .build()?,
    );
    let plan: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![(Arc::new(Column::new("key", 0)), "key".into())]),
        vec![expression],
        vec![None],
        input,
        schema,
    )?);
    let codec = RemoteExecutionCodec;
    // A worker context already knows DataFusion's built-in `min`. The explicit
    // function payload must bypass that registry during remote plan decoding.
    let worker = SessionContext::new().task_ctx();
    assert!(worker.udaf("min")?.inner().is::<min_max::Min>());
    let encoded = encode_remote_physical_plan(&codec, plan)?;
    let decoded = decode_remote_physical_plan(worker.as_ref(), &codec, &encoded)?;
    let aggregate = decoded
        .downcast_ref::<AggregateExec>()
        .ok_or_else(|| plan_datafusion_err!("expected aggregate after worker decode"))?;
    let function = &aggregate.aggr_expr()[0];
    assert!(function.fun().inner().is::<StructMin>());
    assert!(function.groups_accumulator_supported());

    // Naming alone is insufficient: exercise the decoded expression's actual
    // factory. The original singleton-StructArray implementation needs hundreds
    // of bytes per group; a compact accumulator must stay below this envelope.
    let mut accumulator = function.create_groups_accumulator()?;
    let indexes: Vec<_> = (0..groups).collect();
    accumulator.update_batch(&[values], &indexes, None, groups)?;
    assert!(accumulator.size() < groups * 128);

    let output = collect(decoded, worker).await?;
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
        groups
    );
    for batch in output {
        let minimum = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| plan_datafusion_err!("expected struct result"))?;
        assert_eq!(minimum.null_count(), 0);
        let distance = minimum
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| plan_datafusion_err!("expected float distance"))?;
        let parent = minimum
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| plan_datafusion_err!("expected signed parent"))?;
        assert!(distance.values().iter().all(|value| *value == 2.0));
        assert!(parent.values().iter().all(|value| *value == -7));
    }
    Ok(())
}

#[test]
fn only_compact_min_gets_an_explicit_worker_codec_marker() -> Result<()> {
    let codec = RemoteExecutionCodec;
    let mut encoded = Vec::new();
    codec.try_encode_udaf(&struct_min_udaf(), &mut encoded)?;
    assert!(!encoded.is_empty());
    assert!(
        codec
            .try_decode_udaf("min", &encoded)?
            .inner()
            .is::<StructMin>()
    );
    let mut original = Vec::new();
    codec.try_encode_udaf(&min_max::min_udaf(), &mut original)?;
    assert!(original.is_empty());
    Ok(())
}

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Result, internal_err};
use datafusion::functions_aggregate::min_max::min_udaf;
use datafusion::logical_expr::{AggregateUDF, EmitTo};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::Column;

use super::{struct_min_udaf, supports_struct_min};

fn expression(function: Arc<AggregateUDF>, data_type: DataType) -> Result<AggregateFunctionExpr> {
    AggregateExprBuilder::new(function, vec![Arc::new(Column::new("value", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "value", data_type, true,
        )])))
        .alias("minimum")
        .build()
}

fn arrays() -> Vec<ArrayRef> {
    let distances: ArrayRef = Arc::new(Float64Array::from(vec![Some(3.0), None, Some(1.0)]));
    let integers: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(-1)]));
    vec![
        Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("distance", DataType::Float64, true)),
                Arc::clone(&distances),
            ),
            (
                Arc::new(Field::new("hops", DataType::Int64, true)),
                Arc::clone(&integers),
            ),
            (
                Arc::new(Field::new("parent", DataType::Int64, true)),
                Arc::clone(&integers),
            ),
        ])),
        Arc::clone(&integers),
        Arc::new(StringArray::from(vec![Some("z"), None, Some("a")])),
        Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("distance", DataType::Int64, true)),
                Arc::clone(&integers),
            ),
            (
                Arc::new(Field::new("hops", DataType::Int64, true)),
                Arc::clone(&integers),
            ),
            (
                Arc::new(Field::new("parent", DataType::Int64, true)),
                integers,
            ),
        ])),
    ]
}

#[test]
fn scalar_and_sliding_accumulators_keep_datafusion_behavior() -> Result<()> {
    for array in arrays() {
        let wrapped = expression(struct_min_udaf(), array.data_type().clone())?;
        let original = expression(min_udaf(), array.data_type().clone())?;
        assert_eq!(wrapped.field(), original.field());
        assert_eq!(wrapped.state_fields()?, original.state_fields()?);
        let mut left = wrapped.create_accumulator()?;
        let mut right = original.create_accumulator()?;
        left.update_batch(&[Arc::clone(&array)])?;
        right.update_batch(&[Arc::clone(&array)])?;
        assert_eq!(left.evaluate()?, right.evaluate()?);
        assert_eq!(left.state()?, right.state()?);

        match (
            wrapped.create_sliding_accumulator(),
            original.create_sliding_accumulator(),
        ) {
            (Ok(mut left), Ok(mut right)) => {
                left.update_batch(&[Arc::clone(&array)])?;
                right.update_batch(&[Arc::clone(&array)])?;
                assert_eq!(left.evaluate()?, right.evaluate()?);
                let first = array.slice(0, 1);
                left.retract_batch(&[Arc::clone(&first)])?;
                right.retract_batch(&[first])?;
                assert_eq!(left.evaluate()?, right.evaluate()?);
            }
            (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
            _ => return internal_err!("wrapper changed sliding support"),
        }
    }
    Ok(())
}

#[test]
fn other_grouped_types_and_planner_properties_keep_datafusion_behavior() -> Result<()> {
    let wrapped = struct_min_udaf();
    let original = min_udaf();
    assert_eq!(wrapped.signature(), original.signature());
    assert_eq!(wrapped.order_sensitivity(), original.order_sensitivity());
    assert_eq!(wrapped.is_descending(), original.is_descending());
    for array in arrays() {
        assert_eq!(
            wrapped.coerce_types(&[array.data_type().clone()])?,
            original.coerce_types(&[array.data_type().clone()])?,
        );
        if supports_struct_min(array.data_type()) {
            continue;
        }
        let left = expression(Arc::clone(&wrapped), array.data_type().clone())?;
        let right = expression(Arc::clone(&original), array.data_type().clone())?;
        assert_eq!(
            left.groups_accumulator_supported(),
            right.groups_accumulator_supported()
        );
        let mut left = left.create_groups_accumulator()?;
        let mut right = right.create_groups_accumulator()?;
        left.update_batch(&[Arc::clone(&array)], &[0, 1, 0], None, 2)?;
        right.update_batch(&[array], &[0, 1, 0], None, 2)?;
        assert_eq!(
            left.evaluate(EmitTo::All)?.to_data(),
            right.evaluate(EmitTo::All)?.to_data()
        );
    }
    Ok(())
}

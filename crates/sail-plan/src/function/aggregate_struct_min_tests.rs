use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;
use datafusion_expr::col;

use super::*;
use crate::config::PlanConfig;
use crate::function::common::FunctionContextInput;

#[test]
fn min_specializes_only_the_supported_struct_and_preserves_clauses() -> PlanResult<()> {
    use sail_function::aggregate::struct_min::StructMin;

    let target = DataType::Struct(
        vec![
            Field::new("distance", DataType::Float64, true),
            Field::new("hops", DataType::Int64, true),
            Field::new("parent", DataType::Int64, true),
        ]
        .into(),
    );
    let other_struct = DataType::Struct(
        vec![
            Field::new("distance", DataType::Int64, true),
            Field::new("hops", DataType::Int64, true),
            Field::new("parent", DataType::Int64, true),
        ]
        .into(),
    );
    for (data_type, compact) in [
        (target, true),
        (other_struct, false),
        (DataType::Int64, false),
    ] {
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("value", data_type.clone(), true),
            Field::new("included", DataType::Boolean, true),
        ]))?);
        let argument_display_names = ["value".to_string()];
        let plan_config = Arc::new(PlanConfig::default());
        let session_context = SessionContext::new();
        let filter = col("included");
        let expression = min_value(AggFunctionInput {
            arguments: vec![col("value")],
            distinct: true,
            ignore_nulls: None,
            filter: Some(Box::new(filter.clone())),
            order_by: vec![],
            preserve_count_argument_columns: false,
            function_context: FunctionContextInput {
                argument_display_names: &argument_display_names,
                plan_config: &plan_config,
                session_context: &session_context,
                schema: &schema,
            },
        })?;
        assert_eq!(expression.get_type(schema.as_ref())?, data_type);
        let expr::Expr::AggregateFunction(minimum) = expression else {
            return Err(PlanError::internal("expected aggregate MIN expression"));
        };
        assert_eq!(minimum.func.inner().is::<StructMin>(), compact);
        assert_eq!(minimum.func.inner().is::<min_max::Min>(), !compact);
        assert_eq!(minimum.params.filter, Some(Box::new(filter)));
        assert!(!minimum.params.distinct);
        assert!(crate::resolver::PlanResolver::is_order_irrelevant_udaf(
            &minimum.func,
            &minimum.params.args,
            &schema,
        )?);
    }
    Ok(())
}

//! Native scalar return fields carry extension metadata that DataFusion's
//! standard scalar-expression protobuf currently omits.
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalExtensionExprNode, physical_expr_node};
use sail_common_datafusion::native_scalar::OwnedScalar;
use serde::{Deserialize, Serialize};

use super::decode::try_decode_field_ref;
use super::encode::try_encode_field_ref;

const PREFIX: &[u8] = b"SAIL_NATIVE_SCALAR_EXPR_V1\0";
const LITERAL_PREFIX: &[u8] = b"SAIL_METADATA_LITERAL_V1\0";
const MAX_DESCRIPTOR: usize = 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    name: String,
    udf: Vec<u8>,
    field: Vec<u8>,
    nullable: bool,
}

pub(super) fn encode(
    expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
    converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<PhysicalExprNode>> {
    if expr.downcast_ref::<Literal>().is_some() {
        let field = expr.return_field(&Schema::empty())?;
        if !field.metadata().is_empty() {
            let bytes = try_encode_field_ref(&field)?;
            if bytes.len() > MAX_DESCRIPTOR {
                return Err(plan_datafusion_err!("literal field descriptor too large"));
            }
            return Ok(Some(PhysicalExprNode {
                expr_type: Some(physical_expr_node::ExprType::Extension(
                    PhysicalExtensionExprNode {
                        expr: [LITERAL_PREFIX, bytes.as_slice()].concat(),
                        inputs: vec![serialize_physical_expr_with_converter(
                            expr, codec, converter,
                        )?],
                    },
                )),
                expr_id: expr.expression_id(),
            }));
        }
    }
    let Some(scalar) = expr.downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(None);
    };
    if !scalar.fun().inner().is::<OwnedScalar>() {
        return Ok(None);
    }
    let mut udf = vec![];
    codec.try_encode_udf(scalar.fun(), &mut udf)?;
    let descriptor = Descriptor {
        name: scalar.fun().name().to_owned(),
        udf,
        field: try_encode_field_ref(&expr.return_field(&Schema::empty())?)?,
        nullable: scalar.nullable(),
    };
    let bytes = serde_json::to_vec(&descriptor)
        .map_err(|e| plan_datafusion_err!("native scalar descriptor: {e}"))?;
    if bytes.len() > MAX_DESCRIPTOR {
        return Err(plan_datafusion_err!("native scalar descriptor too large"));
    }
    let inputs = scalar
        .args()
        .iter()
        .map(|arg| converter.physical_expr_to_proto(arg, codec))
        .collect::<Result<_>>()?;
    Ok(Some(PhysicalExprNode {
        expr_type: Some(physical_expr_node::ExprType::Extension(
            PhysicalExtensionExprNode {
                expr: [PREFIX, bytes.as_slice()].concat(),
                inputs,
            },
        )),
        expr_id: expr.expression_id(),
    }))
}

pub(super) fn decode(
    proto: &PhysicalExprNode,
    schema: &Schema,
    ctx: &PhysicalPlanDecodeContext<'_>,
    converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let Some(physical_expr_node::ExprType::Extension(node)) = &proto.expr_type else {
        return Ok(None);
    };
    if let Some(bytes) = node.expr.strip_prefix(LITERAL_PREFIX) {
        if bytes.len() > MAX_DESCRIPTOR {
            return Err(plan_datafusion_err!("literal field descriptor too large"));
        }
        let [input] = node.inputs.as_slice() else {
            return Err(plan_datafusion_err!(
                "metadata literal requires one literal input"
            ));
        };
        if !matches!(
            input.expr_type,
            Some(physical_expr_node::ExprType::Literal(_))
        ) {
            return Err(plan_datafusion_err!(
                "metadata literal input is not a literal"
            ));
        }
        let value = converter.proto_to_physical_expr(input, schema, ctx)?;
        let literal = value
            .downcast_ref::<Literal>()
            .ok_or_else(|| plan_datafusion_err!("metadata literal input is not a literal"))?;
        let field = try_decode_field_ref(bytes)?;
        if field.data_type() != &literal.value().data_type() {
            return Err(plan_datafusion_err!("metadata literal type mismatch"));
        }
        let restored = Literal::new_with_metadata(
            literal.value().clone(),
            Some(FieldMetadata::new_from_field(&field)),
        );
        return Ok(Some(Arc::new(restored)));
    }
    let Some(bytes) = node.expr.strip_prefix(PREFIX) else {
        return Ok(None);
    };
    if bytes.len() > MAX_DESCRIPTOR {
        return Err(plan_datafusion_err!("native scalar descriptor too large"));
    }
    let descriptor: Descriptor = serde_json::from_slice(bytes)
        .map_err(|e| plan_datafusion_err!("native scalar descriptor: {e}"))?;
    let udf = ctx
        .codec()
        .try_decode_udf(&descriptor.name, &descriptor.udf)?;
    if !udf.inner().is::<OwnedScalar>() {
        return Err(plan_datafusion_err!(
            "native scalar descriptor resolved a non-native function"
        ));
    }
    let field = try_decode_field_ref(&descriptor.field)?;
    let args = node
        .inputs
        .iter()
        .map(|arg| converter.proto_to_physical_expr(arg, schema, ctx))
        .collect::<Result<_>>()?;
    Ok(Some(Arc::new(
        ScalarFunctionExpr::new(
            &descriptor.name,
            udf,
            args,
            field,
            Arc::clone(ctx.task_ctx().session_config().options()),
        )
        .with_nullable(descriptor.nullable),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        RemoteExecutionCodec, decode_remote_physical_expr, encode_remote_physical_expr,
    };
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::Column;
    use sail_common_datafusion::native_scalar::retain_scalar;

    #[test]
    fn native_scalar_expression_round_trip_preserves_full_return_field() -> Result<()> {
        let task = TaskContext::default();
        let udf = ScalarUDF::new_from_impl(OwnedScalar {
            name: "metadata_fixture".into(),
            identity: "fixture@1:metadata".into(),
            udf: (*datafusion::functions::math::abs()).clone(),
            owner: Arc::new(()),
        });
        retain_scalar(udf.clone())?;
        let field = Arc::new(
            Field::new("geometry", DataType::Float64, false).with_metadata(
                [
                    ("ARROW:extension:name".into(), "fixture.geometry".into()),
                    (
                        "ARROW:extension:metadata".into(),
                        "{\"crs\":\"EPSG:4326\"}".into(),
                    ),
                ]
                .into(),
            ),
        );
        let schema = Schema::new(vec![Field::new("x", DataType::Float64, false)]);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            ScalarFunctionExpr::new(
                "metadata_fixture",
                Arc::new(udf),
                vec![Arc::new(Column::new("x", 0))],
                field.clone(),
                Arc::clone(task.session_config().options()),
            )
            .with_nullable(false),
        );
        let bytes = encode_remote_physical_expr(&RemoteExecutionCodec, &expr)?;
        let decoded = decode_remote_physical_expr(&task, &RemoteExecutionCodec, &bytes, &schema)?;
        assert_eq!(decoded.return_field(&schema)?, field);
        assert!(!decoded.nullable(&schema)?);
        assert_eq!(decoded.children().len(), 1);
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new_with_metadata(
            datafusion::common::ScalarValue::Float64(Some(2.0)),
            Some(FieldMetadata::new_from_field(&field)),
        ));
        let bytes = encode_remote_physical_expr(&RemoteExecutionCodec, &literal)?;
        let decoded = decode_remote_physical_expr(&task, &RemoteExecutionCodec, &bytes, &schema)?;
        assert_eq!(
            decoded.return_field(&schema)?,
            literal.return_field(&schema)?
        );
        assert_eq!(
            decoded
                .downcast_ref::<Literal>()
                .ok_or_else(|| plan_datafusion_err!("expected restored literal"))?
                .value(),
            &datafusion::common::ScalarValue::Float64(Some(2.0))
        );
        Ok(())
    }
}

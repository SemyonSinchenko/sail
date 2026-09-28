//! Bounded, schema-only protocol validation shared by driver and worker.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Result, plan_err};
use serde::{Deserialize, Serialize};

pub const TYPE_URL: &str = "type.googleapis.com/nutmeg.v1.ArgenteaApi";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    Init,
    Round,
    Result,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub verb: Verb,
    pub operation_id: String,
    pub snapshot_id: String,
    pub generation: u64,
    pub partitions: usize,
    pub vertices: u64,
    pub damping: f64,
    pub round: u64,
    pub batch_rows: usize,
}

impl Request {
    pub fn parse(type_url: &str, payload: &[u8]) -> Result<Self> {
        if type_url != TYPE_URL || payload.len() > 16_384 {
            return plan_err!("argentea: unsupported type URL or oversized payload");
        }
        let value: Self = serde_json::from_slice(payload)
            .map_err(|e| super::error(format!("invalid request: {e}")))?;
        if value.version != 1
            || value.generation == 0
            || value.vertices == 0
            || value.vertices > i64::MAX as u64
            || !(1..=65_536).contains(&value.partitions)
            || !(1..=65_536).contains(&value.batch_rows)
            || value.round > 1023
            || !value.damping.is_finite()
            || !(0.0..1.0).contains(&value.damping)
            || [&value.operation_id, &value.snapshot_id]
                .iter()
                .any(|s| s.is_empty() || s.len() > 128)
            || (value.verb == Verb::Init && value.round != 0)
            || (value.verb == Verb::Round && value.round == 0)
        {
            return plan_err!(
                "argentea: invalid identity, graph dimensions, round or PageRank options"
            );
        }
        Ok(value)
    }

    pub fn compatible(&self, other: &Self) -> bool {
        self.operation_id == other.operation_id
            && self.snapshot_id == other.snapshot_id
            && self.generation == other.generation
            && self.partitions == other.partitions
            && self.vertices == other.vertices
            && self.damping == other.damping
            && self.batch_rows == other.batch_rows
    }

    pub fn validate_inputs(&self, inputs: &[Arc<dyn ExecutionPlan>]) -> Result<()> {
        if inputs.len() != if self.verb == Verb::Init { 2 } else { 1 } {
            return plan_err!("argentea: input arity does not match verb");
        }
        if self.verb == Verb::Init {
            for (input, names) in inputs
                .iter()
                .zip([&["id", "owner"][..], &["src", "dst", "owner"][..]])
            {
                let schema = input.schema();
                for name in names {
                    let field = schema.field_with_name(name)?;
                    if field.data_type() != &DataType::Int64 {
                        return plan_err!("argentea: {name} must be Int64");
                    }
                }
            }
        } else {
            let number = if self.verb == Verb::Result {
                self.round
            } else {
                self.round - 1
            };
            if inputs[0].schema() != self.message_schema(number) {
                return plan_err!("argentea: message schema or operation/round metadata mismatch");
            }
        }
        Ok(())
    }

    pub fn output_schema(&self) -> SchemaRef {
        if self.verb == Verb::Result {
            Self::result_schema()
        } else {
            self.message_schema(self.round)
        }
    }

    pub fn message_schema(&self, round: u64) -> SchemaRef {
        let mut metadata = HashMap::new();
        metadata.insert("argentea.protocol".into(), "1".into());
        metadata.insert("argentea.operation".into(), self.operation_id.clone());
        metadata.insert("argentea.snapshot".into(), self.snapshot_id.clone());
        metadata.insert("argentea.generation".into(), self.generation.to_string());
        metadata.insert("argentea.round".into(), round.to_string());
        metadata.insert("argentea.partitions".into(), self.partitions.to_string());
        metadata.insert("argentea.vertices".into(), self.vertices.to_string());
        Arc::new(Schema::new_with_metadata(
            vec![
                int("owner"),
                int("kind"),
                int("producer"),
                int("sequence"),
                int("target"),
                Field::new("value", DataType::Float64, false),
                int("producer_worker"),
                int("adjacency_id"),
            ],
            metadata,
        ))
    }

    pub fn result_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            int("id"),
            Field::new("pagerank", DataType::Float64, false),
            int("owner"),
            int("worker_id"),
            int("pid"),
            int("adjacency_id"),
            int("round"),
        ]))
    }
}

fn int(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

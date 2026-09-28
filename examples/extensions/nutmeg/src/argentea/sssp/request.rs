//! Strict weighted SSSP schema and bounded static phase budget (runtime qualification is separate).
use super::super::error;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_argentea_core::{SsspAlgorithm, SsspOptions};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
pub const TYPE_URL: &str = "type.googleapis.com/nutmeg.v5.ArgenteaSsspApi";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    Init,
    Decide,
    Apply,
    Result,
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub algorithm: String,
    pub verb: Verb,
    pub operation_id: String,
    pub snapshot_id: String,
    pub generation: u64,
    pub partitions: usize,
    pub vertices: u64,
    pub max_rounds: u64,
    pub source: i64,
    pub delta: f64,
    pub max_phase_budget: u64,
    pub phase: u64,
    pub batch_rows: usize,
}
impl Request {
    pub fn parse(type_url: &str, payload: &[u8]) -> Result<Self> {
        if type_url != TYPE_URL || payload.len() > 16_384 {
            return Err(error("unsupported v5 type URL or oversized payload"));
        }
        let value: Self = serde_json::from_slice(payload).map_err(error)?;
        value.validate()?;
        Ok(value)
    }
    pub fn options(&self) -> SsspOptions {
        SsspOptions {
            max_rounds: self.max_rounds,
            source: self.source,
            delta: self.delta,
            algorithm: if self.algorithm == "sssp_reference" {
                SsspAlgorithm::Reference
            } else {
                SsspAlgorithm::DeltaStar
            },
        }
    }
    pub fn work_slots(&self) -> u64 {
        // Validated requests have a checked phase bound before this is called.
        (self
            .options()
            .native_phase_bound()
            .expect("validated SSSP phase bound")
            - 2)
            / 2
    }
    pub fn validate(&self) -> Result<()> {
        self.options().validate().map_err(error)?;
        if self.version != 5
            || !matches!(
                self.algorithm.as_str(),
                "sssp_reference" | "sssp_delta_star"
            )
            || self.generation == 0
            || self.vertices == 0
            || self.vertices > i64::MAX as u64
            || !(1..=64).contains(&self.partitions)
            || !(1..=65_536).contains(&self.batch_rows)
            || !(4..=128).contains(&self.max_phase_budget)
            || self.options().native_phase_bound().map_err(error)? > self.max_phase_budget
            || [&self.operation_id, &self.snapshot_id]
                .iter()
                .any(|s| s.is_empty() || s.len() > 128)
            || match self.verb {
                Verb::Init => self.phase != 0,
                Verb::Decide | Verb::Apply => self.phase >= self.work_slots(),
                Verb::Result => self.phase != self.work_slots(),
            }
        {
            return Err(error(
                "invalid v5 identity, options, phase or bounded phase budget",
            ));
        }
        Ok(())
    }
    pub fn compatible(&self, other: &Self) -> bool {
        let mut a = self.clone();
        let mut b = other.clone();
        a.verb = Verb::Init;
        b.verb = Verb::Init;
        a.phase = 0;
        b.phase = 0;
        a == b
    }
    pub fn validate_inputs(&self, inputs: &[Arc<dyn ExecutionPlan>]) -> Result<()> {
        self.validate()?;
        if inputs.len() != if self.verb == Verb::Init { 2 } else { 1 } {
            return Err(error("v5 input arity mismatch"));
        }
        if self.verb == Verb::Init {
            for (input, names) in inputs
                .iter()
                .zip([&["id", "owner"][..], &["src", "dst", "owner"][..]])
            {
                for name in names {
                    if input.schema().field_with_name(name)?.data_type() != &DataType::Int64 {
                        return Err(error("v5 owned graph columns must be Int64"));
                    }
                }
            }
            if inputs[1].schema().field_with_name("weight")?.data_type() != &DataType::Float64 {
                return Err(error("v5 weight must be Float64"));
            }
        } else {
            let stats = self.verb != Verb::Apply;
            if inputs[0].schema() != self.message_schema(self.phase, stats) {
                return Err(error("v5 input schema/channel/phase metadata mismatch"));
            }
        }
        Ok(())
    }
    pub fn output_schema(&self) -> SchemaRef {
        match self.verb {
            Verb::Init => self.message_schema(0, true),
            Verb::Decide => self.message_schema(self.phase, false),
            Verb::Apply => self.message_schema(self.phase + 1, true),
            Verb::Result => Self::result_schema(),
        }
    }
    pub fn message_schema(&self, phase: u64, stats: bool) -> SchemaRef {
        let mut metadata = HashMap::new();
        for (key, value) in [
            ("protocol", "5".into()),
            ("algorithm", self.algorithm.clone()),
            ("operation", self.operation_id.clone()),
            ("snapshot", self.snapshot_id.clone()),
            ("generation", self.generation.to_string()),
            ("phase", phase.to_string()),
            ("partitions", self.partitions.to_string()),
            ("vertices", self.vertices.to_string()),
            (
                "channel",
                if stats { "statistics" } else { "contributions" }.into(),
            ),
        ] {
            metadata.insert(format!("argentea.{key}"), value);
        }
        Arc::new(Schema::new_with_metadata(
            vec![
                int("owner"),
                int("kind"),
                int("producer"),
                int("sequence"),
                int("target"),
                int("source"),
                int("hops"),
                int("mode"),
                int("aux"),
                int("producer_worker"),
                int("adjacency_id"),
                Field::new("distance", DataType::Float64, false),
                Field::new("bucket", DataType::Float64, false),
            ],
            metadata,
        ))
    }
    pub fn result_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            int("id"),
            Field::new("distance", DataType::Float64, true),
            Field::new("hops", DataType::Int64, true),
            Field::new("parent", DataType::Int64, true),
            int("owner"),
            int("worker_id"),
            int("pid"),
            int("adjacency_id"),
            int("phase"),
            int("rounds"),
            int("reached"),
            int("converged"),
        ]))
    }
}
fn int(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

//! Version 2 is separate: reference messages retain their positive-value checks.
use super::super::error;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_argentea_core::DeltaOptions;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
pub const TYPE_URL: &str = "type.googleapis.com/nutmeg.v2.ArgenteaDeltaApi";

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
    pub damping: f64,
    pub tolerance: f64,
    pub max_pushes: u64,
    pub max_phase_budget: u64,
    pub phase: u64,
    pub batch_rows: usize,
}
impl Request {
    pub fn parse(type_url: &str, payload: &[u8]) -> Result<Self> {
        if type_url != TYPE_URL || payload.len() > 16_384 {
            return Err(error("unsupported v2 type URL or oversized payload"));
        }
        let value: Self = serde_json::from_slice(payload).map_err(error)?;
        value.validate()?;
        Ok(value)
    }
    pub fn options(&self) -> DeltaOptions {
        DeltaOptions {
            damping: self.damping,
            tolerance: self.tolerance,
            max_pushes: self.max_pushes,
        }
    }
    pub fn work_slots(&self) -> u64 {
        self.max_pushes * 2 + 1
    }
    pub fn validate(&self) -> Result<()> {
        self.options().validate().map_err(error)?;
        if self.version != 2
            || self.algorithm != "pagerank_delta"
            || self.generation == 0
            || self.vertices == 0
            || self.vertices > i64::MAX as u64
            || !(1..=64).contains(&self.partitions)
            || !(1..=65_536).contains(&self.batch_rows)
            || !(4..=32).contains(&self.max_phase_budget)
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
                "invalid v2 identity, options, phase or qualified phase budget",
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
            return Err(error("v2 input arity mismatch"));
        }
        if self.verb == Verb::Init {
            for (input, names) in inputs
                .iter()
                .zip([&["id", "owner"][..], &["src", "dst", "owner"][..]])
            {
                for name in names {
                    if input.schema().field_with_name(name)?.data_type() != &DataType::Int64 {
                        return Err(error("v2 owned graph columns must be Int64"));
                    }
                }
            }
        } else {
            let stats = self.verb != Verb::Apply;
            if inputs[0].schema() != self.message_schema(self.phase, stats) {
                return Err(error("v2 input schema/channel/phase metadata mismatch"));
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
            ("protocol", "2".into()),
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
                float("value"),
                int("mode"),
                int("aux"),
                int("producer_worker"),
                int("adjacency_id"),
            ],
            metadata,
        ))
    }
    pub fn result_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            int("id"),
            float("pagerank"),
            int("owner"),
            int("worker_id"),
            int("pid"),
            int("adjacency_id"),
            int("phase"),
            int("pushes"),
            int("certificate_passes"),
            float("residual_l1"),
            float("stationary_error_bound"),
            int("converged"),
        ]))
    }
}
fn int(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}
fn float(name: &str) -> Field {
    Field::new(name, DataType::Float64, false)
}

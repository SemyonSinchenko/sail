use super::{
    super::{batches::integers, error},
    request::Request,
};
use arrow::{
    array::{Array, Float64Array, Int64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use sail_argentea_core::{DeltaMode, DeltaStatisticsValues};
use std::sync::Arc;

pub const UPDATE: i64 = 0;
pub const COMPLETE: i64 = 1;
pub const FLOAT_STAT: i64 = 2;
pub const INT_STAT: i64 = 3;
pub fn mode_number(mode: DeltaMode) -> i64 {
    match mode {
        DeltaMode::Initial => 0,
        DeltaMode::Push => 1,
        DeltaMode::Certify => 2,
        DeltaMode::Done => 3,
    }
}
pub fn mode(value: i64) -> Result<DeltaMode> {
    match value {
        0 => Ok(DeltaMode::Initial),
        1 => Ok(DeltaMode::Push),
        2 => Ok(DeltaMode::Certify),
        3 => Ok(DeltaMode::Done),
        _ => Err(error("invalid v2 work mode")),
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Message {
    pub owner: i64,
    pub kind: i64,
    pub producer: i64,
    pub sequence: i64,
    pub target: i64,
    pub value: f64,
    pub mode: i64,
    pub aux: i64,
    pub worker: i64,
    pub adjacency: i64,
}
pub struct Messages<'a> {
    ints: [&'a Int64Array; 9],
    values: &'a Float64Array,
}
impl<'a> Messages<'a> {
    pub fn new(batch: &'a RecordBatch, expected: &SchemaRef) -> Result<Self> {
        if batch.schema() != *expected {
            return Err(error("v2 wire schema/channel/phase changed"));
        }
        let values = batch
            .column_by_name("value")
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| error("v2 value column missing"))?;
        if values.null_count() != 0 {
            return Err(error("v2 message value is null"));
        }
        Ok(Self {
            ints: [
                integers(batch, "owner")?,
                integers(batch, "kind")?,
                integers(batch, "producer")?,
                integers(batch, "sequence")?,
                integers(batch, "target")?,
                integers(batch, "mode")?,
                integers(batch, "aux")?,
                integers(batch, "producer_worker")?,
                integers(batch, "adjacency_id")?,
            ],
            values,
        })
    }
    pub fn get(&self, row: usize) -> Message {
        let [
            owner,
            kind,
            producer,
            sequence,
            target,
            mode,
            aux,
            worker,
            adjacency,
        ] = self.ints.map(|a| a.value(row));
        Message {
            owner,
            kind,
            producer,
            sequence,
            target,
            value: self.values.value(row),
            mode,
            aux,
            worker,
            adjacency,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    floats: [Option<f64>; 3],
    integers: [Option<u64>; 6],
    mode: Option<DeltaMode>,
    sequence: u64,
    complete: bool,
}
impl Statistics {
    pub fn receive(&mut self, message: Message) -> Result<()> {
        let work = mode(message.mode)?;
        if self.complete
            || message.sequence as u64 != self.sequence
            || self.mode.is_some_and(|m| m != work)
        {
            return Err(error("replayed or inconsistent v2 statistics"));
        }
        self.mode = Some(work);
        match message.kind {
            FLOAT_STAT => {
                if !(0..3).contains(&message.target)
                    || message.aux != 0
                    || message.value.is_nan()
                    || (message.target != 2 && !message.value.is_finite())
                    || message.value < 0.0
                {
                    return Err(error("invalid v2 floating statistic"));
                }
                let slot = &mut self.floats[message.target as usize];
                if slot.replace(message.value).is_some() {
                    return Err(error("duplicate v2 floating statistic"));
                }
                self.sequence += 1;
            }
            INT_STAT => {
                if !(3..9).contains(&message.target) || message.aux < 0 || message.value != 0.0 {
                    return Err(error("invalid v2 integer statistic"));
                }
                let slot = &mut self.integers[(message.target - 3) as usize];
                if slot.replace(message.aux as u64).is_some() {
                    return Err(error("duplicate v2 integer statistic"));
                }
                self.sequence += 1;
            }
            COMPLETE => {
                if self.sequence != 9
                    || message.target != 0
                    || message.value != 0.0
                    || message.aux < 0
                    || self.integers[0] != Some(message.aux as u64)
                    || self.floats.iter().any(Option::is_none)
                    || self.integers.iter().any(Option::is_none)
                {
                    return Err(error("incomplete v2 statistics producer"));
                }
                self.complete = true;
            }
            _ => return Err(error("contribution received in v2 statistics channel")),
        }
        Ok(())
    }
    pub fn values(&self, request: &Request) -> Result<DeltaStatisticsValues> {
        if !self.complete {
            return Err(error("missing v2 statistics producer completion"));
        }
        let [mass, residual_l1, min_score] = self.floats.map(Option::unwrap);
        let [
            vertices,
            pushes,
            certificate_passes,
            active_vertices,
            active_edges,
            reactivated_vertices,
        ] = self.integers.map(Option::unwrap);
        // Infinity is meaningful only for an empty minimum, never JSON options,
        // rank values, mass, residuals or dangling/contribution data.
        if (vertices == 0 && min_score != f64::INFINITY) || (vertices > 0 && !min_score.is_finite())
        {
            return Err(error("invalid empty/nonempty v2 minimum score"));
        }
        Ok(DeltaStatisticsValues {
            options: request.options(),
            completed: self.mode.expect("completed producer"),
            vertices,
            pushes,
            certificate_passes,
            mass,
            residual_l1,
            min_score,
            active_vertices,
            active_edges,
            reactivated_vertices,
        })
    }
}

pub fn validate(
    message: &Message,
    request: &Request,
    partition: usize,
    origins: &mut [Option<(i64, i64)>],
    state: &Arc<super::state::DeltaState>,
) -> Result<()> {
    if message.owner != partition as i64
        || message.producer < 0
        || message.producer >= request.partitions as i64
        || message.sequence < 0
        || message.worker < 0
        || message.adjacency <= 0
    {
        return Err(error("invalid v2 message coordinates"));
    }
    mode(message.mode)?;
    let origin = (message.worker, message.adjacency);
    let slot = &mut origins[message.producer as usize];
    if slot.is_some_and(|o| o != origin) {
        return Err(error("v2 producer identity changed midstream"));
    }
    if slot.is_none() {
        state.origin(message.producer as usize, origin)?;
        *slot = Some(origin);
    }
    Ok(())
}

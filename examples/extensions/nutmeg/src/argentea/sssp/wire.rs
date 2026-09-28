//! Integer identities/counts and finite FLOAT64 distances/buckets, with strict barriers.
use super::{
    super::{batches::integers, error},
    request::Request,
    state::SsspState,
};
use arrow::{
    array::{Array, Float64Array, Int64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use sail_argentea_core::{SsspMode, SsspOrigin, SsspStatisticsValues};
use std::sync::Arc;
pub const TOPOLOGY: i64 = 0;
pub const CANDIDATE: i64 = 1;
pub const STATISTIC: i64 = 4;
pub const COMPLETE: i64 = 5;
pub fn mode_number(m: SsspMode) -> i64 {
    match m {
        SsspMode::Topology => 0,
        SsspMode::Reference => 1,
        SsspMode::DeltaStar => 2,
        SsspMode::Done => 3,
    }
}
pub fn mode_name(m: SsspMode) -> &'static str {
    match m {
        SsspMode::Topology => "topology",
        SsspMode::Reference => "reference",
        SsspMode::DeltaStar => "delta_star",
        SsspMode::Done => "done",
    }
}
pub fn mode(v: i64) -> Result<SsspMode> {
    match v {
        0 => Ok(SsspMode::Topology),
        1 => Ok(SsspMode::Reference),
        2 => Ok(SsspMode::DeltaStar),
        3 => Ok(SsspMode::Done),
        _ => Err(error("invalid v5 SSSP mode")),
    }
}
/// -1 represents absence; valid buckets are finite nonnegative integer FLOAT64s.
pub fn bucket(v: f64) -> Result<Option<f64>> {
    if v == -1.0 {
        Ok(None)
    } else if v.is_finite() && v >= 0.0 && v.fract() == 0.0 {
        Ok(Some(v))
    } else {
        Err(error("invalid v5 bucket"))
    }
}
pub fn floats<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    let a = batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| error(format!("missing Float64 column {name}")))?;
    if a.null_count() != 0 {
        return Err(error(format!("null value in {name}")));
    }
    Ok(a)
}
#[derive(Clone, Copy, Debug)]
pub struct Message {
    pub owner: i64,
    pub kind: i64,
    pub producer: i64,
    pub sequence: i64,
    pub target: i64,
    pub source: i64,
    pub hops: i64,
    pub mode: i64,
    pub aux: i64,
    pub worker: i64,
    pub adjacency: i64,
    pub distance: f64,
    pub bucket: f64,
}
pub struct Messages<'a> {
    columns: [&'a Int64Array; 11],
    distance: &'a Float64Array,
    bucket: &'a Float64Array,
}
impl<'a> Messages<'a> {
    pub fn new(batch: &'a RecordBatch, expected: &SchemaRef) -> Result<Self> {
        if batch.schema() != *expected {
            return Err(error("v5 wire schema/channel/phase changed"));
        }
        Ok(Self {
            columns: [
                integers(batch, "owner")?,
                integers(batch, "kind")?,
                integers(batch, "producer")?,
                integers(batch, "sequence")?,
                integers(batch, "target")?,
                integers(batch, "source")?,
                integers(batch, "hops")?,
                integers(batch, "mode")?,
                integers(batch, "aux")?,
                integers(batch, "producer_worker")?,
                integers(batch, "adjacency_id")?,
            ],
            distance: floats(batch, "distance")?,
            bucket: floats(batch, "bucket")?,
        })
    }
    pub fn get(&self, row: usize) -> Message {
        let [
            owner,
            kind,
            producer,
            sequence,
            target,
            source,
            hops,
            mode,
            aux,
            worker,
            adjacency,
        ] = self.columns.map(|c| c.value(row));
        Message {
            owner,
            kind,
            producer,
            sequence,
            target,
            source,
            hops,
            mode,
            aux,
            worker,
            adjacency,
            distance: self.distance.value(row),
            bucket: self.bucket.value(row),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    values: [Option<u64>; 8],
    mode: Option<SsspMode>,
    bucket: Option<f64>,
    sequence: u64,
    complete: bool,
}
impl Statistics {
    pub fn receive(&mut self, m: Message) -> Result<()> {
        let mode = mode(m.mode)?;
        bucket(m.bucket)?;
        if self.complete
            || m.sequence < 0
            || m.sequence as u64 != self.sequence
            || self.mode.is_some_and(|old| old != mode)
            || self.bucket.is_some_and(|old| old != m.bucket)
            || m.source != 0
            || m.hops != 0
            || m.distance != 0.0
            || m.aux < 0
        {
            return Err(error("replayed or inconsistent v5 statistics"));
        }
        self.mode = Some(mode);
        self.bucket = Some(m.bucket);
        match m.kind {
            STATISTIC if (0..8).contains(&m.target) => {
                if self.values[m.target as usize]
                    .replace(m.aux as u64)
                    .is_some()
                {
                    return Err(error("duplicate v5 statistic"));
                }
                self.sequence += 1;
            }
            COMPLETE
                if self.sequence == 8
                    && m.target == 0
                    && self.values.iter().all(Option::is_some)
                    && self.values[0] == Some(m.aux as u64) =>
            {
                self.complete = true
            }
            _ => return Err(error("incomplete or wrong-channel v5 statistics")),
        }
        Ok(())
    }
    pub fn values(&self, request: &Request, origin: (i64, i64)) -> Result<SsspStatisticsValues> {
        if !self.complete {
            return Err(error("missing v5 statistics producer completion"));
        }
        let [
            vertices,
            arcs,
            source_count,
            reached,
            reachable_edges,
            active,
            bucket_edges,
            rounds,
        ] = self.values.map(Option::unwrap);
        Ok(SsspStatisticsValues {
            options: request.options(),
            origin: SsspOrigin {
                worker_id: origin.0 as u64,
                adjacency_id: origin.1 as u64,
            },
            completed: self.mode.expect("complete"),
            rounds,
            vertices,
            arcs,
            source_count,
            reached,
            reachable_edges,
            active,
            bucket: bucket(self.bucket.expect("complete"))?,
            bucket_edges,
        })
    }
}
pub fn validate(
    m: &Message,
    request: &Request,
    p: usize,
    origins: &mut [Option<(i64, i64)>],
    state: &Arc<SsspState>,
) -> Result<()> {
    if m.owner != p as i64
        || m.producer < 0
        || m.producer >= request.partitions as i64
        || m.sequence < 0
        || m.worker < 0
        || m.adjacency <= 0
        || !m.distance.is_finite()
        || m.distance < 0.0
    {
        return Err(error("invalid v5 wire coordinates or distance"));
    }
    mode(m.mode)?;
    bucket(m.bucket)?;
    let origin = (m.worker, m.adjacency);
    let slot = &mut origins[m.producer as usize];
    if slot.is_some_and(|old| old != origin) {
        return Err(error("v5 producer origin changed midstream"));
    }
    if slot.is_none() {
        state.origin(m.producer as usize, origin)?;
        *slot = Some(origin);
    }
    Ok(())
}

//! Fixed-width integer wire fields; producer barriers are decoded after EOF.
use super::{
    super::{batches::integers, error},
    request::Request,
    state::BfsState,
};
use arrow::{array::Int64Array, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;
use sail_argentea_core::{BfsMode, BfsOrigin, BfsStatisticsValues};
use std::sync::Arc;
pub const TOPOLOGY: i64 = 0;
pub const CANDIDATE: i64 = 1;
pub const MEMBERSHIP: i64 = 2;
pub const STATISTIC: i64 = 3;
pub const COMPLETE: i64 = 4;
pub fn mode_number(m: BfsMode) -> i64 {
    match m {
        BfsMode::Topology => 0,
        BfsMode::Reference => 1,
        BfsMode::Push => 2,
        BfsMode::Pull => 3,
        BfsMode::Done => 4,
    }
}
pub fn mode_name(m: BfsMode) -> &'static str {
    match m {
        BfsMode::Topology => "topology",
        BfsMode::Reference => "reference",
        BfsMode::Push => "push",
        BfsMode::Pull => "pull",
        BfsMode::Done => "done",
    }
}
pub fn mode(v: i64) -> Result<BfsMode> {
    match v {
        0 => Ok(BfsMode::Topology),
        1 => Ok(BfsMode::Reference),
        2 => Ok(BfsMode::Push),
        3 => Ok(BfsMode::Pull),
        4 => Ok(BfsMode::Done),
        _ => Err(error("invalid v3 BFS mode")),
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Message {
    pub owner: i64,
    pub kind: i64,
    pub producer: i64,
    pub sequence: i64,
    pub target: i64,
    pub parent: i64,
    pub value: i64,
    pub mode: i64,
    pub aux: i64,
    pub worker: i64,
    pub adjacency: i64,
}
pub struct Messages<'a> {
    columns: [&'a Int64Array; 11],
}
impl<'a> Messages<'a> {
    pub fn new(batch: &'a RecordBatch, expected: &SchemaRef) -> Result<Self> {
        if batch.schema() != *expected {
            return Err(error("v3 wire schema/channel/phase changed"));
        }
        Ok(Self {
            columns: [
                integers(batch, "owner")?,
                integers(batch, "kind")?,
                integers(batch, "producer")?,
                integers(batch, "sequence")?,
                integers(batch, "target")?,
                integers(batch, "parent")?,
                integers(batch, "value")?,
                integers(batch, "mode")?,
                integers(batch, "aux")?,
                integers(batch, "producer_worker")?,
                integers(batch, "adjacency_id")?,
            ],
        })
    }
    pub fn get(&self, row: usize) -> Message {
        let [
            owner,
            kind,
            producer,
            sequence,
            target,
            parent,
            value,
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
            parent,
            value,
            mode,
            aux,
            worker,
            adjacency,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    values: [Option<u64>; 8],
    mode: Option<BfsMode>,
    sequence: u64,
    complete: bool,
}
impl Statistics {
    pub fn receive(&mut self, m: Message) -> Result<()> {
        let mode = mode(m.mode)?;
        if self.complete
            || m.sequence as u64 != self.sequence
            || self.mode.is_some_and(|old| old != mode)
            || m.parent != 0
            || m.value != 0
            || m.aux < 0
        {
            return Err(error("replayed or inconsistent v3 statistics"));
        }
        self.mode = Some(mode);
        match m.kind {
            STATISTIC if (0..8).contains(&m.target) => {
                if self.values[m.target as usize]
                    .replace(m.aux as u64)
                    .is_some()
                {
                    return Err(error("duplicate v3 statistic"));
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
            _ => return Err(error("incomplete or wrong-channel v3 statistics")),
        }
        Ok(())
    }
    pub fn values(&self, request: &Request, origin: (i64, i64)) -> Result<BfsStatisticsValues> {
        if !self.complete {
            return Err(error("missing v3 statistics producer completion"));
        }
        let [
            vertices,
            arcs,
            source_count,
            reached,
            frontier,
            frontier_edges,
            remaining_edges,
            levels,
        ] = self.values.map(Option::unwrap);
        Ok(BfsStatisticsValues {
            options: request.options(),
            origin: BfsOrigin {
                worker_id: origin.0 as u64,
                adjacency_id: origin.1 as u64,
            },
            completed: self.mode.expect("complete"),
            levels,
            vertices,
            arcs,
            source_count,
            reached,
            frontier,
            frontier_edges,
            remaining_edges,
        })
    }
}
pub fn validate(
    m: &Message,
    request: &Request,
    p: usize,
    origins: &mut [Option<(i64, i64)>],
    state: &Arc<BfsState>,
) -> Result<()> {
    if m.owner != p as i64
        || m.producer < 0
        || m.producer >= request.partitions as i64
        || m.sequence < 0
        || m.worker < 0
        || m.adjacency <= 0
    {
        return Err(error("invalid v3 wire coordinates"));
    }
    mode(m.mode)?;
    let origin = (m.worker, m.adjacency);
    let slot = &mut origins[m.producer as usize];
    if slot.is_some_and(|old| old != origin) {
        return Err(error("v3 producer origin changed midstream"));
    }
    if slot.is_none() {
        state.origin(m.producer as usize, origin)?;
        *slot = Some(origin);
    }
    Ok(())
}

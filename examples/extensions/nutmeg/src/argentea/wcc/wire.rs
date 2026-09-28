//! Fixed-width integer wire fields; producer barriers are decoded after EOF.
use super::{
    super::{batches::integers, error},
    request::Request,
    state::WccState,
};
use arrow::{array::Int64Array, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;
use sail_argentea_core::{WccMode, WccOrigin, WccStatisticsValues};
use std::sync::Arc;
pub const TOPOLOGY: i64 = 0;
pub const LABEL: i64 = 1;
pub const MEMBER: i64 = 2;
pub const ASSIGNMENT: i64 = 3;
pub const STATISTIC: i64 = 4;
pub const COMPLETE: i64 = 5;
pub fn mode_number(m: WccMode) -> i64 {
    match m {
        WccMode::Topology => 0,
        WccMode::Reference => 1,
        WccMode::Neighbors => 2,
        WccMode::HookRoute => 3,
        WccMode::HookReturn => 4,
        WccMode::NormalizeRoute => 5,
        WccMode::NormalizeReturn => 6,
        WccMode::Done => 7,
    }
}
pub fn mode_name(m: WccMode) -> &'static str {
    match m {
        WccMode::Topology => "topology",
        WccMode::Reference => "reference",
        WccMode::Neighbors => "neighbors",
        WccMode::HookRoute => "hook_route",
        WccMode::HookReturn => "hook_return",
        WccMode::NormalizeRoute => "normalize_route",
        WccMode::NormalizeReturn => "normalize_return",
        WccMode::Done => "done",
    }
}
pub fn mode(v: i64) -> Result<WccMode> {
    match v {
        0 => Ok(WccMode::Topology),
        1 => Ok(WccMode::Reference),
        2 => Ok(WccMode::Neighbors),
        3 => Ok(WccMode::HookRoute),
        4 => Ok(WccMode::HookReturn),
        5 => Ok(WccMode::NormalizeRoute),
        6 => Ok(WccMode::NormalizeReturn),
        7 => Ok(WccMode::Done),
        _ => Err(error("invalid v4 WCC mode")),
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Message {
    pub owner: i64,
    pub kind: i64,
    pub producer: i64,
    pub sequence: i64,
    pub target: i64,
    pub source: i64,
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
            return Err(error("v4 wire schema/channel/phase changed"));
        }
        Ok(Self {
            columns: [
                integers(batch, "owner")?,
                integers(batch, "kind")?,
                integers(batch, "producer")?,
                integers(batch, "sequence")?,
                integers(batch, "target")?,
                integers(batch, "source")?,
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
            source,
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
            source,
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
    values: [Option<u64>; 7],
    mode: Option<WccMode>,
    sequence: u64,
    complete: bool,
}
impl Statistics {
    pub fn receive(&mut self, m: Message) -> Result<()> {
        let mode = mode(m.mode)?;
        if self.complete
            || m.sequence as u64 != self.sequence
            || self.mode.is_some_and(|old| old != mode)
            || m.source != 0
            || m.value != 0
            || m.aux < 0
        {
            return Err(error("replayed or inconsistent v4 statistics"));
        }
        self.mode = Some(mode);
        match m.kind {
            STATISTIC if (0..7).contains(&m.target) => {
                if self.values[m.target as usize]
                    .replace(m.aux as u64)
                    .is_some()
                {
                    return Err(error("duplicate v4 statistic"));
                }
                self.sequence += 1;
            }
            COMPLETE
                if self.sequence == 7
                    && m.target == 0
                    && self.values.iter().all(Option::is_some)
                    && self.values[0] == Some(m.aux as u64) =>
            {
                self.complete = true
            }
            _ => return Err(error("incomplete or wrong-channel v4 statistics")),
        }
        Ok(())
    }
    pub fn values(&self, request: &Request, origin: (i64, i64)) -> Result<WccStatisticsValues> {
        if !self.complete {
            return Err(error("missing v4 statistics producer completion"));
        }
        let [
            vertices,
            arcs,
            incoming_arcs,
            changed,
            crossing,
            members,
            rounds,
        ] = self.values.map(Option::unwrap);
        Ok(WccStatisticsValues {
            options: request.options(),
            origin: WccOrigin {
                worker_id: origin.0 as u64,
                adjacency_id: origin.1 as u64,
            },
            completed: self.mode.expect("complete"),
            rounds,
            vertices,
            arcs,
            incoming_arcs,
            changed,
            crossing,
            members,
        })
    }
}
pub fn validate(
    m: &Message,
    request: &Request,
    p: usize,
    origins: &mut [Option<(i64, i64)>],
    state: &Arc<WccState>,
) -> Result<()> {
    if m.owner != p as i64
        || m.producer < 0
        || m.producer >= request.partitions as i64
        || m.sequence < 0
        || m.worker < 0
        || m.adjacency <= 0
    {
        return Err(error("invalid v4 wire coordinates"));
    }
    mode(m.mode)?;
    let origin = (m.worker, m.adjacency);
    let slot = &mut origins[m.producer as usize];
    if slot.is_some_and(|old| old != origin) {
        return Err(error("v4 producer origin changed midstream"));
    }
    if slot.is_none() {
        state.origin(m.producer as usize, origin)?;
        *slot = Some(origin);
    }
    Ok(())
}

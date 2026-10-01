//! Producer-complete weighted SSSP state, independent of Sail scheduling.
mod emission;
mod initialization;
pub use initialization::SsspInitialization;
mod protocol;
mod statistics;
use super::{SsspLabel, WeightedAdjacency};
use crate::{Operation, Resources, Result, Round, reserve_vec};
pub use emission::{
    SsspCompletion, SsspCompletionValues, SsspEmissionCursor, SsspMessage, SsspMessageValues,
    SsspPayload,
};
use grust_procedures::MemoryReservation;
use protocol::Inbox;
use statistics::StatisticsInbox;
pub use statistics::{SsspStatistics, SsspStatisticsValues};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SsspAlgorithm {
    Reference,
    DeltaStar,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SsspMode {
    Topology,
    Reference,
    DeltaStar,
    Done,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspOptions {
    pub source: i64,
    pub algorithm: SsspAlgorithm,
    pub max_rounds: u64,
    pub delta: f64,
}
impl SsspOptions {
    pub fn validate(self) -> Result<()> {
        if !self.delta.is_finite() || self.delta <= 0.0 {
            return Err("SSSP delta must be finite and positive".into());
        }
        self.native_phase_bound().map(|_| ())
    }
    pub fn native_phase_bound(self) -> Result<u64> {
        self.max_rounds
            .checked_mul(2)
            .and_then(|n| n.checked_add(4))
            .ok_or("SSSP phase budget overflow".into())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SsspOrigin {
    pub worker_id: u64,
    pub adjacency_id: u64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SsspWork {
    pub examined_edges: u64,
    pub examined_vertices: u64,
    pub emitted_messages: u64,
    pub received_messages: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SsspCapFailure {
    pub rounds: u64,
    pub max_rounds: u64,
    pub active: u64,
    pub reached: u64,
}
impl std::fmt::Display for SsspCapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSSP did not converge within max_rounds")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SsspConvergence {
    pub rounds: u64,
    pub reached: u64,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspRow {
    pub id: i64,
    pub label: Option<SsspLabel>,
}
#[derive(Debug)]
struct Ownership {
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}
impl Ownership {
    fn new(r: &Resources, bytes: usize) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            _admission: r.execution.reserve(bytes).map_err(|e| e.to_string())?,
            _lease: r.lease.clone(),
        }))
    }
}
#[derive(Debug)]
struct Values {
    labels: Vec<Option<SsspLabel>>,
    active: Vec<usize>,
    reached: u64,
    reachable_edges: u64,
    _admission: MemoryReservation,
}
impl Values {
    fn new(n: usize, r: &Resources) -> Result<Self> {
        let bytes = n
            .checked_mul(size_of::<Option<SsspLabel>>() + size_of::<usize>())
            .and_then(|x| x.checked_add(256))
            .ok_or("SSSP state admission overflow")?;
        let admission = r.execution.reserve(bytes).map_err(|e| e.to_string())?;
        Ok(Self {
            labels: filled(n, None)?,
            active: reserve_vec(n)?,
            reached: 0,
            reachable_edges: 0,
            _admission: admission,
        })
    }
    fn copy(&self, r: &Resources) -> Result<Self> {
        let mut v = Self::new(self.labels.len(), r)?;
        v.labels.copy_from_slice(&self.labels);
        v.reached = self.reached;
        v.reachable_edges = self.reachable_edges;
        Ok(v)
    }
}
#[derive(Debug)]
enum State {
    Statistics(StatisticsInbox),
    Receiving(Box<Inbox>),
    Sealed,
    Failed,
}
#[derive(Debug)]
pub struct SsspPartition {
    operation: Operation,
    partition: usize,
    origin: SsspOrigin,
    options: SsspOptions,
    adjacency: Arc<WeightedAdjacency>,
    values: Arc<Values>,
    origins: Vec<Option<SsspOrigin>>,
    shapes: Vec<Option<(u64, u64, u64)>>,
    _origin_admission: MemoryReservation,
    state: State,
    next_phase: u64,
    rounds: u64,
    completed: SsspMode,
    work: SsspWork,
    terminal: Option<SsspConvergence>,
    // Keep the host lease until every owned admitted buffer has dropped.
    resources: Resources,
}
impl SsspPartition {
    pub fn build(
        operation: Operation,
        partition: usize,
        worker_id: u64,
        vertices: &[i64],
        arcs: &[(i64, i64, f64)],
        options: SsspOptions,
        resources: Resources,
    ) -> Result<Self> {
        Self::prepare(
            operation, partition, worker_id, vertices, arcs, options, resources,
        )?
        .finish()
    }

    /// Build and validate the CSR without allocating labels or the frontier.
    /// The caller may release raw inputs and their admission before `finish`.
    pub fn prepare(
        operation: Operation,
        partition: usize,
        worker_id: u64,
        vertices: &[i64],
        arcs: &[(i64, i64, f64)],
        options: SsspOptions,
        resources: Resources,
    ) -> Result<SsspInitialization> {
        options.validate()?;
        let adjacency =
            WeightedAdjacency::build(&operation, partition, vertices, arcs, &resources)?;
        Ok(SsspInitialization {
            operation,
            partition,
            worker_id,
            options,
            resources,
            adjacency,
        })
    }
    pub fn partition(&self) -> usize {
        self.partition
    }
    pub fn origin(&self) -> SsspOrigin {
        self.origin
    }
    pub fn next_phase(&self) -> u64 {
        self.next_phase
    }
    pub fn rounds(&self) -> u64 {
        self.rounds
    }
    pub fn reached_count(&self) -> u64 {
        self.values.reached
    }
    pub fn active_count(&self) -> usize {
        self.values.active.len()
    }
    pub fn completed_mode(&self) -> SsspMode {
        self.completed
    }
    pub fn last_work(&self) -> SsspWork {
        self.work
    }
    pub fn collecting_phase(&self) -> Option<u64> {
        matches!(self.state, State::Statistics(_)).then_some(self.next_phase)
    }
    pub fn receiving_phase(&self) -> Option<u64> {
        matches!(self.state, State::Receiving(_)).then_some(self.next_phase)
    }
    pub fn state_rows(&self) -> impl Iterator<Item = SsspRow> + '_ {
        self.adjacency
            .vertices()
            .iter()
            .enumerate()
            .map(|(i, &id)| SsspRow {
                id,
                label: self.values.labels[i],
            })
    }
    pub fn abort(&mut self) -> Result<()> {
        self.state = State::Failed;
        self.resources.execution.cancel().map_err(|e| e.to_string())
    }
    fn fail<T>(&mut self, e: impl Into<String>) -> Result<T> {
        self.state = State::Failed;
        Err(e.into())
    }
    fn check_phase(&mut self, phase: &Round) -> Result<()> {
        if phase.operation != self.operation || phase.number != self.next_phase {
            return self.fail("foreign, stale or skipped SSSP phase");
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())
    }
    fn decision(&self, g: &statistics::Totals) -> Result<(SsspMode, Option<f64>)> {
        if self.next_phase == 0 {
            return Ok((SsspMode::Topology, None));
        }
        if g.active == 0 {
            return Ok((SsspMode::Done, None));
        }
        if self.rounds >= self.options.max_rounds {
            return Err("SSSP did not converge within max_rounds".into());
        }
        Ok(match self.options.algorithm {
            SsspAlgorithm::Reference => (SsspMode::Reference, None),
            SsspAlgorithm::DeltaStar => (SsspMode::DeltaStar, g.bucket),
        })
    }
    pub fn cap_failure(&mut self, phase: &Round) -> Result<Option<SsspCapFailure>> {
        self.check_phase(phase)?;
        let g = self.global_statistics()?;
        Ok(
            (self.next_phase > 0 && g.active > 0 && self.rounds >= self.options.max_rounds)
                .then_some(SsspCapFailure {
                    rounds: self.rounds,
                    max_rounds: self.options.max_rounds,
                    active: g.active,
                    reached: g.reached,
                }),
        )
    }
    pub fn seal(&mut self, phase: &Round) -> Result<Option<SsspConvergence>> {
        self.check_phase(phase)?;
        let g = self.global_statistics()?;
        match self.decision(&g) {
            Ok((SsspMode::Done, _)) => {
                let c = SsspConvergence {
                    rounds: self.rounds,
                    reached: g.reached,
                };
                self.terminal = Some(c);
                self.state = State::Sealed;
                Ok(Some(c))
            }
            Ok(_) => Ok(None),
            Err(e) => self.fail(e),
        }
    }
    pub fn row_cursor(&self) -> Result<SsspRowCursor> {
        if !matches!(self.state, State::Sealed) || self.terminal.is_none() {
            return Err("SSSP result is not certified".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(SsspRowCursor {
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            position: 0,
        })
    }
}
pub struct SsspRowCursor {
    adjacency: Arc<WeightedAdjacency>,
    values: Arc<Values>,
    resources: Resources,
    position: usize,
}
impl SsspRowCursor {
    pub fn next_row(&mut self) -> Result<Option<SsspRow>> {
        self.resources
            .execution
            .charge_work(1)
            .map_err(|e| e.to_string())?;
        let Some(&id) = self.adjacency.vertices().get(self.position) else {
            return Ok(None);
        };
        let label = self.values.labels[self.position];
        self.position += 1;
        Ok(Some(SsspRow { id, label }))
    }
}
fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut v = reserve_vec(n)?;
    v.resize(n, value);
    Ok(v)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or("SSSP counter overflow".into())
}

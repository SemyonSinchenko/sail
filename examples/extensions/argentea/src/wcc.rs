//! Extension-owned WCC state. Transport and scheduling remain outside this crate.
mod emission;
mod protocol;
mod statistics;
use crate::{Operation, Resources, Result, Round, adjacency::Adjacency, reserve_vec};
pub use emission::{
    WccCompletion, WccCompletionValues, WccEmissionCursor, WccMessage, WccMessageValues, WccPayload,
};
use grust_procedures::MemoryReservation;
use protocol::Inbox;
use statistics::StatisticsInbox;
pub use statistics::{WccStatistics, WccStatisticsValues};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WccAlgorithm {
    Reference,
    StarContraction,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccOptions {
    pub algorithm: WccAlgorithm,
    pub max_rounds: u64,
    pub seed: u64,
}
impl WccOptions {
    pub fn native_phase_bound(self) -> Result<u64> {
        let (factor, overhead) = match self.algorithm {
            WccAlgorithm::Reference => (2, 4),
            WccAlgorithm::StarContraction => (6, 10),
        };
        self.max_rounds
            .checked_mul(factor)
            .and_then(|x| x.checked_add(overhead))
            .ok_or_else(|| "WCC phase budget overflow".into())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WccMode {
    Topology,
    Reference,
    Neighbors,
    HookRoute,
    HookReturn,
    NormalizeRoute,
    NormalizeReturn,
    Done,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccOrigin {
    pub worker_id: u64,
    pub adjacency_id: u64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WccWork {
    pub examined_edges: u64,
    pub examined_vertices: u64,
    pub emitted_messages: u64,
    pub received_messages: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccCapFailure {
    pub rounds: u64,
    pub max_rounds: u64,
    /// Changed vertices (reference) or crossing arcs (star contraction).
    pub unresolved: u64,
    /// Reference K=0 has validated topology but has not attempted propagation.
    pub certificate_not_attempted: bool,
}
impl std::fmt::Display for WccCapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WCC did not converge within max_rounds")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccConvergence {
    pub rounds: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccRow {
    pub id: i64,
    pub component: i64,
}
/// Stable head/tail assignment; signed IDs are interpreted as full-width bits.
pub fn wcc_head(seed: u64, round: u64, root: i64) -> bool {
    let mut z = seed ^ round.wrapping_mul(0x9e3779b97f4a7c15) ^ root as u64;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((z ^ (z >> 31)) & 1) != 0
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
    roots: Vec<i64>,
    _admission: MemoryReservation,
}
impl Values {
    fn new(roots: &[i64], r: &Resources) -> Result<Self> {
        let admission = r
            .execution
            .reserve(
                roots
                    .len()
                    .checked_mul(8)
                    .and_then(|x| x.checked_add(128))
                    .ok_or("WCC values overflow")?,
            )
            .map_err(|e| e.to_string())?;
        let mut values = reserve_vec(roots.len())?;
        values.extend_from_slice(roots);
        Ok(Self {
            roots: values,
            _admission: admission,
        })
    }
}
/// A buffer grows only after the complete replacement and old allocation fit.
#[derive(Debug)]
struct Buffer<T> {
    values: Vec<T>,
    admission: MemoryReservation,
}
impl<T: Copy> Buffer<T> {
    fn new(r: &Resources) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            admission: r.execution.reserve(128).map_err(|e| e.to_string())?,
        })
    }
    fn push(&mut self, value: T, r: &Resources) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .and_then(|n| n.checked_add(64))
                .ok_or("WCC buffer capacity overflow")?;
            let bytes = capacity
                .checked_mul(size_of::<T>())
                .and_then(|n| n.checked_add(128))
                .ok_or("WCC buffer admission overflow")?;
            let admission = r.execution.reserve(bytes).map_err(|e| e.to_string())?;
            let mut next = reserve_vec(capacity)?;
            next.extend_from_slice(&self.values);
            self.values = next;
            self.admission = admission;
        }
        self.values.push(value);
        Ok(())
    }
}
#[derive(Debug)]
struct Routed {
    members: Buffer<(i64, i64)>,
    choices: Vec<i64>,
    _admission: MemoryReservation,
}
#[derive(Debug)]
enum State {
    Statistics(StatisticsInbox),
    Receiving(Box<Inbox>),
    Sealed,
    Failed,
}
#[derive(Debug)]
pub struct WccPartition {
    operation: Operation,
    partition: usize,
    origin: WccOrigin,
    options: WccOptions,
    adjacency: Arc<Adjacency>,
    incoming: Option<Arc<Adjacency>>,
    resources: Resources,
    values: Arc<Values>,
    candidates: Option<Arc<protocol::Candidates>>,
    routed: Option<Arc<Routed>>,
    origins: Vec<Option<WccOrigin>>,
    shapes: Vec<Option<(u64, u64)>>,
    _origin_admission: MemoryReservation,
    state: State,
    next_phase: u64,
    rounds: u64,
    completed: WccMode,
    changed: u64,
    crossing: u64,
    work: WccWork,
    terminal: Option<WccConvergence>,
}
impl WccPartition {
    /// Source-owned original arcs are interpreted as undirected. All endpoints
    /// are validated by the topology exchange, including disconnected vertices.
    pub fn build(
        operation: Operation,
        partition: usize,
        worker_id: u64,
        vertices: &[i64],
        arcs: &[(i64, i64)],
        options: WccOptions,
        resources: Resources,
    ) -> Result<Self> {
        options.native_phase_bound()?;
        let adjacency = Adjacency::build(&operation, partition, vertices, arcs, &resources)?;
        let origin = WccOrigin {
            worker_id,
            adjacency_id: adjacency.identity,
        };
        let admission = resources
            .execution
            .reserve(
                operation.partitions
                    * (size_of::<Option<WccOrigin>>() + size_of::<Option<(u64, u64)>>())
                    + 256,
            )
            .map_err(|e| e.to_string())?;
        let mut origins = filled(operation.partitions, None)?;
        origins[partition] = Some(origin);
        let shapes = filled(operation.partitions, None)?;
        let values = Arc::new(Values::new(&adjacency.vertices, &resources)?);
        let state = State::Statistics(StatisticsInbox::new(operation.partitions, &resources)?);
        Ok(Self {
            operation,
            partition,
            origin,
            options,
            adjacency,
            incoming: None,
            resources,
            values,
            candidates: None,
            routed: None,
            origins,
            shapes,
            _origin_admission: admission,
            state,
            next_phase: 0,
            rounds: 0,
            completed: WccMode::Topology,
            changed: 0,
            crossing: 0,
            work: WccWork::default(),
            terminal: None,
        })
    }
    pub fn partition(&self) -> usize {
        self.partition
    }
    pub fn origin(&self) -> WccOrigin {
        self.origin
    }
    pub fn incoming_identity(&self) -> Option<u64> {
        self.incoming.as_ref().map(|x| x.identity)
    }
    pub fn collecting_phase(&self) -> Option<u64> {
        matches!(self.state, State::Statistics(_)).then_some(self.next_phase)
    }
    pub fn receiving_phase(&self) -> Option<u64> {
        matches!(self.state, State::Receiving(_)).then_some(self.next_phase)
    }
    pub fn next_phase(&self) -> u64 {
        self.next_phase
    }
    pub fn rounds(&self) -> u64 {
        self.rounds
    }
    pub fn completed_mode(&self) -> WccMode {
        self.completed
    }
    pub fn last_work(&self) -> WccWork {
        self.work
    }
    pub fn state_rows(&self) -> impl Iterator<Item = WccRow> + '_ {
        self.adjacency
            .vertices
            .iter()
            .zip(&self.values.roots)
            .map(|(&id, &component)| WccRow { id, component })
    }
    pub fn abort(&mut self) -> Result<()> {
        self.state = State::Failed;
        self.resources.execution.cancel().map_err(|e| e.to_string())
    }
    fn fail<T>(&mut self, error: impl Into<String>) -> Result<T> {
        self.state = State::Failed;
        Err(error.into())
    }
    fn check_phase(&mut self, phase: &Round) -> Result<()> {
        if phase.operation != self.operation || phase.number != self.next_phase {
            return self.fail("foreign, stale or skipped WCC phase");
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())
    }
    fn decision(&self, g: &statistics::Totals) -> Result<WccMode> {
        use WccMode::*;
        if self.next_phase == 0 {
            return Ok(Topology);
        }
        match self.options.algorithm {
            WccAlgorithm::Reference => {
                if self.completed == Done {
                    return Ok(Done);
                }
                if self.completed == Reference && g.changed == 0 {
                    if g.crossing != 0 {
                        return Err("inconsistent WCC zero-change certificate".into());
                    }
                    return Ok(Done);
                }
                if self.rounds >= self.options.max_rounds {
                    return Err("WCC did not converge within max_rounds".into());
                }
                Ok(Reference)
            }
            WccAlgorithm::StarContraction => match self.completed {
                Topology | HookReturn => Ok(Neighbors),
                Neighbors if g.crossing == 0 => Ok(NormalizeRoute),
                Neighbors if self.rounds >= self.options.max_rounds => {
                    Err("WCC did not converge within max_rounds".into())
                }
                Neighbors => Ok(HookRoute),
                HookRoute => Ok(HookReturn),
                NormalizeRoute => Ok(NormalizeReturn),
                NormalizeReturn | Done => Ok(Done),
                Reference => Err("invalid WCC algorithm phase".into()),
            },
        }
    }
    pub fn cap_failure(&mut self, phase: &Round) -> Result<Option<WccCapFailure>> {
        self.check_phase(phase)?;
        let g = self.global_statistics()?;
        let unresolved = match self.options.algorithm {
            WccAlgorithm::Reference
                if self.next_phase > 0
                    && !(self.completed == WccMode::Reference && g.changed == 0)
                    && self.completed != WccMode::Done =>
            {
                Some(g.changed)
            }
            WccAlgorithm::StarContraction
                if self.completed == WccMode::Neighbors && g.crossing > 0 =>
            {
                Some(g.crossing)
            }
            _ => None,
        };
        Ok(unresolved
            .filter(|_| self.rounds >= self.options.max_rounds)
            .map(|unresolved| WccCapFailure {
                rounds: self.rounds,
                max_rounds: self.options.max_rounds,
                unresolved,
                certificate_not_attempted: self.options.algorithm == WccAlgorithm::Reference
                    && self.completed == WccMode::Topology,
            }))
    }
    pub fn seal(&mut self, phase: &Round) -> Result<Option<WccConvergence>> {
        self.check_phase(phase)?;
        let g = self.global_statistics()?;
        match self.decision(&g) {
            Ok(WccMode::Done) => {
                let result = WccConvergence {
                    rounds: self.rounds,
                };
                self.terminal = Some(result);
                self.state = State::Sealed;
                Ok(Some(result))
            }
            Ok(_) => Ok(None),
            Err(e) => self.fail(e),
        }
    }
    pub fn row_cursor(&self) -> Result<WccRowCursor> {
        if self.terminal.is_none() || !matches!(self.state, State::Sealed) {
            return Err("WCC result is not certified".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(WccRowCursor {
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            position: 0,
        })
    }
}
pub struct WccRowCursor {
    adjacency: Arc<Adjacency>,
    values: Arc<Values>,
    resources: Resources,
    position: usize,
}
impl WccRowCursor {
    pub fn next_row(&mut self) -> Result<Option<WccRow>> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let Some(&id) = self.adjacency.vertices.get(self.position) else {
            return Ok(None);
        };
        let component = self.values.roots[self.position];
        self.position += 1;
        Ok(Some(WccRow { id, component }))
    }
}
fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut v = reserve_vec(n)?;
    v.resize(n, value);
    Ok(v)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| "WCC integer overflow".into())
}

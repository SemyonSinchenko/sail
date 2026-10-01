//! Integer BFS over retained source-owned partitions. No scheduler or transport.
//! Input arcs are directed; undirected callers normalize each edge to both arcs.
mod emission;
mod initialization;
pub use initialization::BfsInitialization;
mod protocol;
mod statistics;
use crate::{Operation, Resources, Result, Round, adjacency::Adjacency, reserve_vec};
pub use emission::{
    BfsCompletion, BfsCompletionValues, BfsEmissionCursor, BfsMessage, BfsMessageValues, BfsPayload,
};
use grust_procedures::MemoryReservation;
use protocol::Inbox;
use statistics::StatisticsInbox;
pub use statistics::{BfsStatistics, BfsStatisticsValues};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BfsAlgorithm {
    Reference,
    Frontier,
    DirectionOptimizing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BfsMode {
    Topology,
    Reference,
    Push,
    Pull,
    Done,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsOptions {
    pub source: i64,
    pub algorithm: BfsAlgorithm,
    /// Includes the expansion that proves the next frontier is empty.
    pub max_levels: u64,
    /// Integer hysteresis divisors, matching Banda defaults 14 and 24.
    pub alpha: u64,
    pub beta: u64,
}
impl BfsOptions {
    pub fn validate(self) -> Result<()> {
        if self.alpha == 0 || self.beta == 0 || self.native_phase_bound().is_err() {
            return Err("invalid BFS cap or hysteresis divisors".into());
        }
        Ok(())
    }
    /// Schema-only init, setup decide/apply, K level decide/apply pairs, result.
    /// A future client must impose a separately qualified deployment bound.
    pub fn native_phase_bound(self) -> Result<u64> {
        self.max_levels
            .checked_mul(2)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| "BFS phase budget overflow".into())
    }
}
/// Supplied from host scope and the partition's immutable outgoing CSR.
/// These checks detect replay/substitution; they do not authenticate a network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsOrigin {
    pub worker_id: u64,
    pub adjacency_id: u64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BfsWork {
    pub examined_edges: u64,
    pub examined_vertices: u64,
    pub emitted_messages: u64,
    pub received_messages: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsCapFailure {
    pub levels: u64,
    pub max_levels: u64,
    pub frontier: u64,
    pub reached: u64,
}
impl std::fmt::Display for BfsCapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BFS did not converge within max_levels")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsConvergence {
    pub levels: u64,
    pub reached: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsRow {
    pub id: i64,
    pub distance: Option<u64>,
    pub parent: Option<i64>,
}

#[derive(Debug)]
struct Ownership {
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}
impl Ownership {
    fn new(resources: &Resources, bytes: usize) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            _admission: resources
                .execution
                .reserve(bytes)
                .map_err(|e| e.to_string())?,
            _lease: resources.lease.clone(),
        }))
    }
}
#[derive(Debug)]
struct Values {
    depths: Vec<Option<u64>>,
    parents: Vec<Option<i64>>,
    frontier: Vec<usize>,
    remaining: u64,
    reached: u64,
    _admission: MemoryReservation,
}
impl Values {
    fn new(n: usize, resources: &Resources) -> Result<Self> {
        let bytes = n
            .checked_mul(40)
            .and_then(|x| x.checked_add(256))
            .ok_or("BFS state admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            depths: filled(n, None)?,
            parents: filled(n, None)?,
            frontier: reserve_vec(n)?,
            remaining: 0,
            reached: 0,
            _admission: admission,
        })
    }
    fn copy(&self, resources: &Resources) -> Result<Self> {
        let mut next = Self::new(self.depths.len(), resources)?;
        next.depths.copy_from_slice(&self.depths);
        next.parents.copy_from_slice(&self.parents);
        next.remaining = self.remaining;
        next.reached = self.reached;
        Ok(next)
    }
}
#[derive(Debug)]
struct Incoming {
    adjacency: Arc<Adjacency>,
    ghosts: Vec<i64>,
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
pub struct BfsPartition {
    operation: Operation,
    partition: usize,
    origin: BfsOrigin,
    options: BfsOptions,
    adjacency: Arc<Adjacency>,
    incoming: Option<Arc<Incoming>>,
    values: Arc<Values>,
    origins: Vec<Option<BfsOrigin>>,
    shapes: Vec<Option<(u64, u64, u64)>>,
    _origin_admission: MemoryReservation,
    state: State,
    next_phase: u64,
    levels: u64,
    completed: BfsMode,
    work: BfsWork,
    terminal: Option<BfsConvergence>,
    // Keep the host lease until every owned admitted buffer has dropped.
    resources: Resources,
}
impl BfsPartition {
    /// Each vertex occurs once, on owner(id). Arcs are owned by their source.
    /// Normalize undirected edges to two arcs before partitioning (including
    /// self loops). The topology exchange validates every destination, including
    /// disconnected components. Parallel arcs are retained.
    pub fn build(
        operation: Operation,
        partition: usize,
        worker_id: u64,
        vertices: &[i64],
        arcs: &[(i64, i64)],
        options: BfsOptions,
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
        arcs: &[(i64, i64)],
        options: BfsOptions,
        resources: Resources,
    ) -> Result<BfsInitialization> {
        options.validate()?;
        let adjacency = Adjacency::build(&operation, partition, vertices, arcs, &resources)?;
        Ok(BfsInitialization {
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
    pub fn origin(&self) -> BfsOrigin {
        self.origin
    }
    pub fn incoming_identity(&self) -> Option<u64> {
        self.incoming.as_ref().map(|x| x.adjacency.identity)
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
    pub fn levels(&self) -> u64 {
        self.levels
    }
    pub fn frontier_count(&self) -> usize {
        self.values.frontier.len()
    }
    pub fn reached_count(&self) -> u64 {
        self.values.reached
    }
    pub fn last_work(&self) -> BfsWork {
        self.work
    }
    pub fn completed_mode(&self) -> BfsMode {
        self.completed
    }
    pub fn state_rows(&self) -> impl Iterator<Item = BfsRow> + '_ {
        self.adjacency
            .vertices
            .iter()
            .enumerate()
            .map(|(i, id)| BfsRow {
                id: *id,
                distance: self.values.depths[i],
                parent: self.values.parents[i],
            })
    }
    pub fn abort(&mut self) -> Result<()> {
        self.state = State::Failed;
        self.resources.execution.cancel().map_err(|e| e.to_string())
    }
    fn fail<T>(&mut self, message: impl Into<String>) -> Result<T> {
        self.state = State::Failed;
        Err(message.into())
    }
    fn check_phase(&mut self, phase: &Round) -> Result<()> {
        if phase.operation != self.operation || phase.number != self.next_phase {
            return self.fail("foreign, stale or skipped BFS phase");
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())
    }
    pub fn cap_failure(&mut self, phase: &Round) -> Result<Option<BfsCapFailure>> {
        self.check_phase(phase)?;
        let global = self.global_statistics()?;
        Ok(self.cap_failure_for(&global))
    }
    fn cap_failure_for(&self, g: &statistics::Totals) -> Option<BfsCapFailure> {
        (self.next_phase > 0 && g.frontier > 0 && self.levels >= self.options.max_levels).then_some(
            BfsCapFailure {
                levels: self.levels,
                max_levels: self.options.max_levels,
                frontier: g.frontier,
                reached: g.reached,
            },
        )
    }
    fn decision(&self, g: &statistics::Totals) -> Result<BfsMode> {
        if self.next_phase == 0 {
            return Ok(BfsMode::Topology);
        }
        if g.frontier == 0 {
            return Ok(BfsMode::Done);
        }
        if let Some(failure) = self.cap_failure_for(g) {
            return Err(failure.to_string());
        }
        Ok(match self.options.algorithm {
            BfsAlgorithm::Reference => BfsMode::Reference,
            BfsAlgorithm::Frontier => BfsMode::Push,
            BfsAlgorithm::DirectionOptimizing => {
                // Exact integer comparisons avoid floating reductions/rounding.
                let pull = if self.completed == BfsMode::Pull {
                    (g.frontier as u128) * self.options.beta as u128
                        >= self.operation.vertices as u128
                } else {
                    (g.frontier_edges as u128) * self.options.alpha as u128 > g.remaining as u128
                };
                if pull { BfsMode::Pull } else { BfsMode::Push }
            }
        })
    }
    /// Consume an EOF-complete statistics barrier. No partial result on cap.
    pub fn seal(&mut self, phase: &Round) -> Result<Option<BfsConvergence>> {
        self.check_phase(phase)?;
        let global = self.global_statistics()?;
        match self.decision(&global) {
            Ok(BfsMode::Done) => {
                let result = BfsConvergence {
                    levels: self.levels,
                    reached: global.reached,
                };
                self.terminal = Some(result);
                self.state = State::Sealed;
                Ok(Some(result))
            }
            Ok(_) => Ok(None),
            Err(e) => self.fail(e),
        }
    }
    pub fn row_cursor(&self) -> Result<BfsRowCursor> {
        if self.terminal.is_none() || !matches!(self.state, State::Sealed) {
            return Err("BFS result is not certified".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(BfsRowCursor {
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            position: 0,
        })
    }
}
pub struct BfsRowCursor {
    adjacency: Arc<Adjacency>,
    values: Arc<Values>,
    resources: Resources,
    position: usize,
}
impl BfsRowCursor {
    pub fn next_row(&mut self) -> Result<Option<BfsRow>> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let Some(&id) = self.adjacency.vertices.get(self.position) else {
            return Ok(None);
        };
        let i = self.position;
        self.position += 1;
        Ok(Some(BfsRow {
            id,
            distance: self.values.depths[i],
            parent: self.values.parents[i],
        }))
    }
}
fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut v = reserve_vec(n)?;
    v.resize(n, value);
    Ok(v)
}

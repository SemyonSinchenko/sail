//! Signed residual PageRank. These are partition protocol primitives, not a
//! scheduler: callers supply complete, ordered producer streams and barriers.
mod emission;
mod initialization;
pub use initialization::DeltaInitialization;
mod failure;
pub use failure::DeltaCapFailure;
mod protocol;
mod statistics;

use crate::{Operation, Resources, Result, Round, adjacency::Adjacency, reserve_vec};
pub use emission::{DeltaCompletion, DeltaContribution, DeltaEmissionCursor};
use grust_procedures::MemoryReservation;
use protocol::UpdateInbox;
use statistics::StatisticsInbox;
pub use statistics::{DeltaStatistics, DeltaStatisticsValues};
use std::sync::{Arc, atomic::AtomicBool};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeltaOptions {
    pub damping: f64,
    pub tolerance: f64,
    pub max_pushes: u64,
}
impl DeltaOptions {
    pub fn validate(self) -> Result<()> {
        if !self.damping.is_finite()
            || !(0.0..1.0).contains(&self.damping)
            || !self.tolerance.is_finite()
            || self.tolerance <= 0.0
        {
            return Err("invalid residual PageRank options".into());
        }
        self.native_phase_bound()?;
        Ok(())
    }
    /// Static worst case: initialization + two phases per work operation +
    /// result. One initial certificate, K pushes and at most K later certificates.
    /// The future client imposes its smaller deployment-specific stage budget.
    pub fn native_phase_bound(self) -> Result<u64> {
        self.max_pushes
            .checked_mul(4)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| "residual phase budget overflow".into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaMode {
    Initial,
    Push,
    Certify,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Convergence {
    pub residual_l1: f64,
    pub stationary_error_bound: f64,
    pub mass: f64,
    pub pushes: u64,
    pub certificate_passes: u64,
}

#[derive(Debug)]
struct Values {
    scores: Vec<f64>,
    residual: Vec<f64>,
    ever_active: Vec<bool>,
    was_active: Vec<bool>,
    _admission: MemoryReservation,
}
impl Values {
    fn allocate(n: usize, resources: &Resources) -> Result<Self> {
        let bytes = n
            .checked_mul(18)
            .and_then(|v| v.checked_add(256))
            .ok_or("dense admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            scores: filled(n, 0.0)?,
            residual: filled(n, 0.0)?,
            ever_active: filled(n, false)?,
            was_active: filled(n, false)?,
            _admission: admission,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Metrics {
    active_vertices: u64,
    active_edges: u64,
    reactivated_vertices: u64,
}

#[derive(Debug)]
enum State {
    Statistics(StatisticsInbox),
    Receiving(Box<UpdateInbox>),
    Sealed,
    Failed,
}

#[derive(Debug)]
pub struct DeltaPartition {
    operation: Operation,
    partition: usize,
    adjacency: Arc<Adjacency>,
    options: DeltaOptions,
    values: Arc<Values>,
    next_phase: u64,
    completed: DeltaMode,
    pushes: u64,
    certificates: u64,
    metrics: Metrics,
    state: State,
    terminal: Option<Convergence>,
    // Keep the host lease until every owned admitted buffer has dropped.
    resources: Resources,
}

impl DeltaPartition {
    pub fn build(
        operation: Operation,
        partition: usize,
        vertices: &[i64],
        edges: &[(i64, i64)],
        options: DeltaOptions,
        resources: Resources,
    ) -> Result<Self> {
        Self::prepare(operation, partition, vertices, edges, options, resources)?.finish()
    }
    /// Own the validated CSR before allocating dense score and residual state.
    pub fn prepare(
        operation: Operation,
        partition: usize,
        vertices: &[i64],
        edges: &[(i64, i64)],
        options: DeltaOptions,
        resources: Resources,
    ) -> Result<DeltaInitialization> {
        options.validate()?;
        let adjacency = Adjacency::build(&operation, partition, vertices, edges, &resources)?;
        Ok(DeltaInitialization {
            operation,
            partition,
            options,
            adjacency,
            resources,
        })
    }
    pub fn partition(&self) -> usize {
        self.partition
    }
    pub fn adjacency_identity(&self) -> u64 {
        self.adjacency.identity
    }
    pub fn collecting_phase(&self) -> Option<u64> {
        matches!(self.state, State::Statistics(_)).then_some(self.next_phase)
    }
    pub fn receiving_phase(&self) -> Option<u64> {
        matches!(self.state, State::Receiving(_)).then_some(self.next_phase)
    }
    pub fn vertex_count(&self) -> usize {
        self.adjacency.vertices.len()
    }
    pub fn next_phase(&self) -> u64 {
        self.next_phase
    }
    pub fn pushes(&self) -> u64 {
        self.pushes
    }
    pub fn certificate_passes(&self) -> u64 {
        self.certificates
    }
    pub fn completed_mode(&self) -> DeltaMode {
        self.completed
    }
    /// Committed state for diagnostics only; not a certified public result.
    pub fn state_rows(&self) -> impl Iterator<Item = (i64, f64, f64)> + '_ {
        self.adjacency
            .vertices
            .iter()
            .copied()
            .zip(self.values.scores.iter().copied())
            .zip(self.values.residual.iter().copied())
            .map(|((id, x), r)| (id, x, r))
    }
    pub fn abort(&mut self) -> Result<()> {
        self.state = State::Failed;
        self.resources.execution.cancel().map_err(|e| e.to_string())
    }
    fn check_phase(&mut self, phase: &Round) -> Result<()> {
        if phase.operation != self.operation || phase.number != self.next_phase {
            self.state = State::Failed;
            return Err("foreign, stale or skipped residual phase".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())
    }
    fn fail<T>(&mut self, error: impl Into<String>) -> Result<T> {
        self.state = State::Failed;
        Err(error.into())
    }
    /// A result phase can consume the global certificate and seal immediately,
    /// without creating another contribution exchange. `None` is not success.
    pub fn seal(&mut self, phase: &Round) -> Result<Option<Convergence>> {
        self.check_phase(phase)?;
        let global = match self.global_statistics() {
            Ok(g) => g,
            Err(e) => return self.fail(e),
        };
        match self.decision(&global) {
            Ok(DeltaMode::Done) => {
                let result = match self.convergence(&global) {
                    Ok(result) => result,
                    Err(e) => return self.fail(e),
                };
                self.terminal = Some(result);
                self.state = State::Sealed;
                Ok(Some(result))
            }
            Ok(_) => Ok(None),
            Err(e) => self.fail(e),
        }
    }
    pub fn rank_cursor(&self) -> Result<DeltaRankCursor> {
        if !matches!(self.state, State::Sealed) || self.terminal.is_none() {
            return Err("residual ranks require a globally certified sealed result".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(DeltaRankCursor {
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            position: 0,
        })
    }
}

pub struct DeltaRankCursor {
    adjacency: Arc<Adjacency>,
    values: Arc<Values>,
    resources: Resources,
    position: usize,
}
impl DeltaRankCursor {
    pub fn next_rank(&mut self) -> Result<Option<(i64, f64)>> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let Some(&id) = self.adjacency.vertices.get(self.position) else {
            return Ok(None);
        };
        let rank = self.values.scores[self.position];
        self.position += 1;
        Ok(Some((id, rank)))
    }
}

fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut result = reserve_vec(n)?;
    result.resize(n, value);
    Ok(result)
}

#[cfg(test)]
mod controls;

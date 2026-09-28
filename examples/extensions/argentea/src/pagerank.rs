mod emission;
pub use emission::EmissionCursor;

use crate::{Operation, Resources, Result, Round, reserve_vec};
use grust_procedures::MemoryReservation;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

static NEXT_ADJACENCY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Adjacency {
    identity: u64,
    vertices: Vec<i64>,
    offsets: Vec<usize>,
    targets: Vec<i64>,
    _admission: MemoryReservation,
}

/// A message in one ordered producer-to-owner Arrow stream. The future Sail
/// adapter supplies authenticated stream identities; these fields are checked
/// again so replay, wrong destinations and truncated producers fail explicitly.
#[derive(Clone, Debug)]
pub struct Contribution {
    pub round: Arc<Round>,
    _ownership: Arc<EmissionOwnership>,
    pub producer: usize,
    pub sequence: u64,
    pub target: i64,
    pub value: f64,
}

#[derive(Debug)]
struct EmissionOwnership {
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}

#[derive(Debug)]
pub struct Emission {
    pub dangling_mass: f64,
    pub messages: u64,
    /// Final sequence for every destination, including empty destinations.
    pub sequences: Vec<u64>,
    _ownership: Arc<EmissionOwnership>,
}

#[derive(Debug)]
struct Ranks {
    values: Vec<f64>,
    _admission: MemoryReservation,
}

#[derive(Debug)]
struct Inbox {
    values: Vec<f64>,
    sequences: Vec<u64>,
    finished: Vec<bool>,
    dangling: Vec<f64>,
    emitted: bool,
    _admission: MemoryReservation,
}

#[derive(Debug)]
enum State {
    Ready,
    Receiving(Inbox),
    Failed,
}

/// Owns one native CSR and ranks across many BSP rounds. Graph structure stays
/// immutable; an interrupted round poisons this partition instead of publishing
/// partially accumulated ranks. Whole-operation cancellation remains the host's
/// responsibility.
#[derive(Debug)]
pub struct PageRankPartition {
    operation: Operation,
    partition: usize,
    adjacency: Arc<Adjacency>,
    ranks: Arc<Ranks>,
    next_round: u64,
    state: State,
    resources: Resources,
}

#[derive(Debug)]
pub struct RoundResult {
    pub partition: usize,
    pub round: u64,
    pub l1_delta: f64,
    pub rank_mass: f64,
}

/// Immutable, admitted snapshot for streaming final rows after releasing the
/// partition lock. Both graph and ranks stay alive through this cursor.
pub struct RankCursor {
    adjacency: Arc<Adjacency>,
    ranks: Arc<Ranks>,
    resources: Resources,
    position: usize,
}

impl RankCursor {
    pub fn next_rank(&mut self) -> Result<Option<(i64, f64)>> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let Some(id) = self.adjacency.vertices.get(self.position) else {
            return Ok(None);
        };
        let rank = self.ranks.values[self.position];
        self.position += 1;
        Ok(Some((*id, rank)))
    }
}

impl PageRankPartition {
    /// `vertices` must contain exactly this partition's unique graph vertices.
    /// A Sail anti-join must validate global edge endpoints before construction;
    /// a receiver also rejects an update to an absent destination.
    pub fn build(
        operation: Operation,
        partition: usize,
        vertices: &[i64],
        edges: &[(i64, i64)],
        resources: Resources,
    ) -> Result<Self> {
        operation.validate()?;
        if partition >= operation.partitions || vertices.len() as u64 > operation.vertices {
            return Err("invalid graph partition dimensions".into());
        }
        let n = vertices.len();
        let bytes = n
            .checked_mul(64)
            .and_then(|v| edges.len().checked_mul(16).and_then(|e| v.checked_add(e)))
            .and_then(|v| v.checked_add(4096 + size_of::<usize>()))
            .ok_or("partition admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        let mut ids = reserve_vec(n)?;
        ids.extend_from_slice(vertices);
        ids.sort_unstable();
        let mut meter = resources.execution.work_meter();
        for (i, id) in ids.iter().enumerate() {
            meter.charge(1).map_err(|e| e.to_string())?;
            if operation.owner(*id) != partition || (i > 0 && ids[i - 1] == *id) {
                return Err("vertices are duplicated or belong to another partition".into());
            }
        }
        let mut offsets = reserve_vec(n + 1)?;
        offsets.resize(n + 1, 0usize);
        for &(source, _) in edges {
            meter.charge(1).map_err(|e| e.to_string())?;
            let local = ids
                .binary_search(&source)
                .map_err(|_| "source is not owned")?;
            offsets[local + 1] += 1;
        }
        for i in 0..n {
            meter.charge(1).map_err(|e| e.to_string())?;
            offsets[i + 1] += offsets[i];
        }
        let mut cursor = reserve_vec(n)?;
        cursor.extend_from_slice(&offsets[..n]);
        let mut targets = reserve_vec(edges.len())?;
        targets.resize(edges.len(), 0i64);
        for &(source, target) in edges {
            meter.charge(1).map_err(|e| e.to_string())?;
            let local = ids
                .binary_search(&source)
                .map_err(|_| "source is not owned")?;
            targets[cursor[local]] = target;
            cursor[local] += 1;
        }
        drop(cursor);
        // Retain conservative metadata and all CSR bytes, release build scratch.
        admission
            .shrink(n * 8 + (n + 1) * size_of::<usize>() + edges.len() * 8 + 4096)
            .map_err(|e| e.to_string())?;
        let ranks_admission = resources
            .execution
            .reserve(n * 8 + 128)
            .map_err(|e| e.to_string())?;
        let mut ranks = reserve_vec(n)?;
        ranks.resize(n, 1.0 / operation.vertices as f64);
        Ok(Self {
            operation,
            partition,
            adjacency: Arc::new(Adjacency {
                identity: NEXT_ADJACENCY_ID
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .map_err(|_| "adjacency identity overflow")?,
                vertices: ids,
                offsets,
                targets,
                _admission: admission,
            }),
            ranks: Arc::new(Ranks {
                values: ranks,
                _admission: ranks_admission,
            }),
            next_round: 0,
            state: State::Ready,
            resources,
        })
    }

    pub fn partition(&self) -> usize {
        self.partition
    }
    pub fn vertex_count(&self) -> usize {
        self.adjacency.vertices.len()
    }
    pub fn next_round(&self) -> u64 {
        self.next_round
    }
    pub fn receiving_round(&self) -> Option<u64> {
        matches!(self.state, State::Receiving(_)).then_some(self.next_round)
    }
    pub fn ranks(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.adjacency
            .vertices
            .iter()
            .copied()
            .zip(self.ranks.values.iter().copied())
    }
    pub fn adjacency_identity(&self) -> u64 {
        self.adjacency.identity
    }
    pub fn rank_cursor(&self) -> Result<RankCursor> {
        if !matches!(self.state, State::Ready) {
            return Err("rank snapshot requires a completed round".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(RankCursor {
            adjacency: self.adjacency.clone(),
            ranks: self.ranks.clone(),
            resources: self.resources.clone(),
            position: 0,
        })
    }

    pub fn begin(&mut self, round: &Round) -> Result<()> {
        self.check_round(round)?;
        if !matches!(self.state, State::Ready) {
            return self.fail("round already begun or partition failed");
        }
        let bytes = self
            .ranks
            .values
            .len()
            .checked_mul(8)
            .and_then(|n| {
                self.operation
                    .partitions
                    .checked_mul(24)
                    .and_then(|p| n.checked_add(p))
            })
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or("round admission overflow")?;
        let admission = self
            .resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        let mut values = reserve_vec(self.ranks.values.len())?;
        values.resize(self.ranks.values.len(), 0.0);
        let mut sequences = reserve_vec(self.operation.partitions)?;
        sequences.resize(self.operation.partitions, 0);
        let mut finished = reserve_vec(self.operation.partitions)?;
        finished.resize(self.operation.partitions, false);
        let mut dangling = reserve_vec(self.operation.partitions)?;
        dangling.resize(self.operation.partitions, 0.0);
        self.state = State::Receiving(Inbox {
            values,
            dangling,
            sequences,
            finished,
            emitted: false,
            _admission: admission,
        });
        Ok(())
    }

    /// Convenience synchronous driver for unit tests and local adapters. A
    /// streaming adapter should call start_emission under its owner lock and
    /// drain the returned cursor after releasing that lock.
    pub fn emit(
        &mut self,
        round: &Round,
        mut send: impl FnMut(Contribution) -> Result<()>,
    ) -> Result<Emission> {
        let mut cursor = self.start_emission(round)?;
        while let Some(update) = cursor.next_update()? {
            send(update)?;
        }
        cursor.finish()
    }

    pub fn receive(&mut self, update: Contribution) -> Result<()> {
        self.receive_values(
            &update.round,
            update.producer,
            update.sequence,
            update.target,
            update.value,
        )
    }

    /// Arrow adapters borrow the authenticated stream header and pass scalar
    /// row values without manufacturing native ownership tokens or copying a
    /// descriptor for every update. The input Arrow batch owns its own buffers.
    pub fn receive_values(
        &mut self,
        round: &Round,
        producer: usize,
        sequence: u64,
        target: i64,
        value: f64,
    ) -> Result<()> {
        self.check_round(round)?;
        let result = self.receive_inner(producer, sequence, target, value);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }

    fn receive_inner(
        &mut self,
        producer: usize,
        sequence: u64,
        target: i64,
        value: f64,
    ) -> Result<()> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let State::Receiving(inbox) = &mut self.state else {
            return Err("round is not receiving".into());
        };
        if producer >= self.operation.partitions
            || inbox.finished[producer]
            || sequence != inbox.sequences[producer]
            || self.operation.owner(target) != self.partition
            || !value.is_finite()
            || value < 0.0
        {
            return Err("invalid, duplicated, out-of-order or misrouted contribution".into());
        }
        let local = self
            .adjacency
            .vertices
            .binary_search(&target)
            .map_err(|_| "unknown destination")?;
        let total = inbox.values[local] + value;
        if !total.is_finite() {
            return Err("contribution sum overflow".into());
        }
        inbox.values[local] = total;
        inbox.sequences[producer] += 1;
        Ok(())
    }

    pub fn finish_producer(
        &mut self,
        round: &Round,
        producer: usize,
        messages: u64,
        dangling_mass: f64,
    ) -> Result<()> {
        self.check_round(round)?;
        let State::Receiving(inbox) = &mut self.state else {
            return self.fail("round is not receiving");
        };
        if producer >= self.operation.partitions
            || inbox.finished[producer]
            || inbox.sequences[producer] != messages
            || !dangling_mass.is_finite()
            || dangling_mass < 0.0
        {
            return self.fail("duplicated or incomplete producer stream");
        }
        inbox.finished[producer] = true;
        inbox.dangling[producer] = dangling_mass;
        Ok(())
    }

    pub fn finish(&mut self, round: &Round, damping: f64) -> Result<RoundResult> {
        self.check_round(round)?;
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let State::Receiving(inbox) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("round is not receiving".into());
        };
        let global_dangling: f64 = inbox.dangling.iter().sum();
        if !inbox.emitted
            || !inbox.finished.iter().all(|x| *x)
            || !damping.is_finite()
            || !(0.0..1.0).contains(&damping)
            || !global_dangling.is_finite()
            || global_dangling < 0.0
        {
            return Err("round is incomplete or PageRank parameters are invalid".into());
        }
        let mut next = inbox.values;
        let base = (1.0 - damping + damping * global_dangling) / self.operation.vertices as f64;
        let mut l1_delta = 0.0;
        let mut rank_mass = 0.0;
        let mut meter = self.resources.execution.work_meter();
        for (value, old) in next.iter_mut().zip(&self.ranks.values) {
            meter.charge(1).map_err(|e| e.to_string())?;
            *value = base + damping * *value;
            if !value.is_finite() {
                return Err("rank overflow".into());
            }
            l1_delta += (*value - old).abs();
            rank_mass += *value;
        }
        // The old snapshot stays admitted until every emitting cursor drops.
        // Transfer this inbox's vector and admission to the new rank snapshot.
        drop(inbox.sequences);
        drop(inbox.finished);
        drop(inbox.dangling);
        inbox
            ._admission
            .shrink(next.len() * 8 + 128)
            .map_err(|e| e.to_string())?;
        let next_round = self.next_round.checked_add(1).ok_or("round overflow")?;
        self.ranks = Arc::new(Ranks {
            values: next,
            _admission: inbox._admission,
        });
        self.next_round = next_round;
        self.state = State::Ready;
        Ok(RoundResult {
            partition: self.partition,
            round: round.number,
            l1_delta,
            rank_mass,
        })
    }

    pub fn abort(&mut self) -> Result<()> {
        self.state = State::Failed;
        self.resources.execution.cancel().map_err(|e| e.to_string())
    }

    fn check_round(&mut self, round: &Round) -> Result<()> {
        if round.operation != self.operation || round.number != self.next_round {
            return self.fail("foreign, stale or skipped operation/round identity");
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())
    }

    fn fail<T>(&mut self, message: &str) -> Result<T> {
        self.state = State::Failed;
        Err(message.into())
    }
}

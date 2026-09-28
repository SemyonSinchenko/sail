//! Owned cursors traverse only active sources; they never borrow an owner lock.
use super::*;
use grust_procedures::WorkMeter;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct Ownership {
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}
#[derive(Debug)]
struct EmittedValues {
    values: Vec<f64>,
    _admission: MemoryReservation,
}
#[derive(Clone, Debug)]
pub struct DeltaContribution {
    pub phase: Arc<Round>,
    pub mode: DeltaMode,
    pub producer: usize,
    pub sequence: u64,
    pub target: i64,
    pub value: f64,
    _ownership: Arc<Ownership>,
}
#[derive(Debug)]
pub struct DeltaCompletion {
    pub phase: Arc<Round>,
    pub mode: DeltaMode,
    pub producer: usize,
    pub sequences: Vec<u64>,
    pub dangling: f64,
    pub vertices: u64,
    _ownership: Arc<Ownership>,
}
pub struct DeltaEmissionCursor {
    phase: Arc<Round>,
    mode: DeltaMode,
    producer: usize,
    damping: f64,
    adjacency: Arc<Adjacency>,
    values: Arc<EmittedValues>,
    resources: Resources,
    sequences: Vec<u64>,
    ownership: Arc<Ownership>,
    meter: WorkMeter,
    emitted: Arc<AtomicBool>,
    vertex: usize,
    edge: usize,
    entered: bool,
    dangling: f64,
    exhausted: bool,
    completed: bool,
}
impl DeltaPartition {
    pub fn start_emission(&mut self, phase: &Round) -> Result<DeltaEmissionCursor> {
        self.check_phase(phase)?;
        let result = self.emission_inner(phase);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn emission_inner(&mut self, phase: &Round) -> Result<DeltaEmissionCursor> {
        let global = self.global_statistics()?;
        let mode = self.decision(&global)?;
        let n = self.adjacency.vertices.len();
        let emitted_len = if mode == DeltaMode::Done { 0 } else { n };
        let admission = self
            .resources
            .execution
            .reserve(
                emitted_len
                    .checked_mul(8)
                    .and_then(|v| v.checked_add(128))
                    .ok_or("residual emission admission overflow")?,
            )
            .map_err(|e| e.to_string())?;
        let mut emitted = EmittedValues {
            values: filled(emitted_len, 0.0)?,
            _admission: admission,
        };
        let mut candidate = if mode == DeltaMode::Done {
            None
        } else {
            Some(Values::allocate(n, &self.resources)?)
        };
        let threshold = (global.residual_l1 / (2.0 * self.operation.vertices as f64))
            .min(self.options.tolerance * global.mass / (4.0 * self.operation.vertices as f64));
        let mut metrics = Metrics::default();
        let mut meter = self.resources.execution.work_meter();
        if let Some(next) = &mut candidate {
            for i in 0..n {
                meter.charge(1).map_err(|e| e.to_string())?;
                next.ever_active[i] = self.values.ever_active[i];
                next.was_active[i] = self.values.was_active[i];
                match mode {
                    DeltaMode::Certify => {
                        // Normalize the candidate and emit a fresh affine
                        // transition. Retain y, not T(y), on certification.
                        next.scores[i] = self.values.scores[i] / global.mass;
                        emitted.values[i] = next.scores[i];
                    }
                    DeltaMode::Push => {
                        let r = self.values.residual[i];
                        let push = if r.abs() > threshold { r } else { 0.0 };
                        next.scores[i] = self.values.scores[i] + push;
                        next.residual[i] = r - push;
                        emitted.values[i] = push;
                        if push != 0.0 {
                            metrics.active_vertices += 1;
                            metrics.active_edges = metrics
                                .active_edges
                                .checked_add(
                                    (self.adjacency.offsets[i + 1] - self.adjacency.offsets[i])
                                        as u64,
                                )
                                .ok_or("active edge overflow")?;
                            metrics.reactivated_vertices +=
                                u64::from(self.values.ever_active[i] && !self.values.was_active[i]);
                            next.ever_active[i] = true;
                        }
                        next.was_active[i] = push != 0.0;
                    }
                    _ => unreachable!(),
                }
                if !next.scores[i].is_finite() || next.scores[i] < 0.0 {
                    return Err("invalid residual score after push/normalization".into());
                }
            }
        }
        let p = self.operation.partitions;
        // Admit the temporary copy before allocating; UpdateInbox takes over.
        let count_admission = self
            .resources
            .execution
            .reserve(p * 8)
            .map_err(|e| e.to_string())?;
        let mut counts = reserve_vec(p)?;
        let State::Statistics(stats) = &self.state else {
            unreachable!();
        };
        counts.extend(
            stats
                .slots
                .iter()
                .map(|s| s.expect("validated statistics").vertices),
        );
        let inbox = UpdateInbox::new(
            mode,
            candidate,
            counts,
            metrics,
            if mode == DeltaMode::Done { 0 } else { n },
            &self.resources,
        )?;
        drop(count_admission);
        let cursor_admission = self
            .resources
            .execution
            .reserve(p * 8 + 2048)
            .map_err(|e| e.to_string())?;
        let sequences = filled(p, 0)?;
        let cursor = DeltaEmissionCursor {
            phase: Arc::new(phase.clone()),
            mode,
            producer: self.partition,
            damping: self.options.damping,
            adjacency: self.adjacency.clone(),
            values: Arc::new(emitted),
            resources: self.resources.clone(),
            sequences,
            ownership: Arc::new(Ownership {
                _admission: cursor_admission,
                _lease: self.resources.lease.clone(),
            }),
            meter: self.resources.execution.work_meter(),
            emitted: inbox.emitted.clone(),
            vertex: 0,
            edge: 0,
            entered: false,
            dangling: 0.0,
            exhausted: false,
            completed: false,
        };
        self.state = State::Receiving(Box::new(inbox));
        Ok(cursor)
    }
}
impl DeltaEmissionCursor {
    pub fn mode(&self) -> DeltaMode {
        self.mode
    }
    pub fn next_update(&mut self) -> Result<Option<DeltaContribution>> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        while self.vertex < self.values.values.len() {
            let value = self.values.values[self.vertex];
            let end = self.adjacency.offsets[self.vertex + 1];
            if !self.entered {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                self.edge = self.adjacency.offsets[self.vertex];
                if self.edge == end {
                    self.dangling += value;
                }
                // Inactive sources never traverse their outgoing adjacency.
                if value == 0.0 {
                    self.edge = end;
                }
                self.entered = true;
            }
            if self.edge < end {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                let target = self.adjacency.targets[self.edge];
                let owner = self.phase.operation.owner(target);
                let sequence = self.sequences[owner];
                self.sequences[owner] = sequence
                    .checked_add(1)
                    .ok_or("residual emission sequence overflow")?;
                self.edge += 1;
                return Ok(Some(DeltaContribution {
                    phase: self.phase.clone(),
                    mode: self.mode,
                    producer: self.producer,
                    sequence,
                    target,
                    value: self.damping * value
                        / (end - self.adjacency.offsets[self.vertex]) as f64,
                    _ownership: self.ownership.clone(),
                }));
            }
            self.vertex += 1;
            self.entered = false;
        }
        if !self.dangling.is_finite() {
            return Err("residual dangling emission overflow".into());
        }
        self.exhausted = true;
        Ok(None)
    }
    pub fn finish(mut self) -> Result<DeltaCompletion> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        if !self.exhausted {
            return Err("residual producer stream has not been exhausted".into());
        }
        self.emitted.store(true, Ordering::Release);
        self.completed = true;
        Ok(DeltaCompletion {
            phase: self.phase.clone(),
            mode: self.mode,
            producer: self.producer,
            sequences: std::mem::take(&mut self.sequences),
            dangling: self.dangling,
            vertices: self.adjacency.vertices.len() as u64,
            _ownership: self.ownership.clone(),
        })
    }
}
impl Drop for DeltaEmissionCursor {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.resources.execution.cancel();
        }
    }
}

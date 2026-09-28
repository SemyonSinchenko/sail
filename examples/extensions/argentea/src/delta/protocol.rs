use super::*;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(super) struct UpdateInbox {
    pub mode: DeltaMode,
    pub candidate: Option<Values>,
    pub incoming: Vec<f64>,
    pub sequences: Vec<u64>,
    pub finished: Vec<bool>,
    pub dangling: Vec<f64>,
    pub vertices: Vec<u64>,
    pub emitted: Arc<AtomicBool>,
    pub metrics: Metrics,
    _admission: MemoryReservation,
}
impl UpdateInbox {
    pub(super) fn new(
        mode: DeltaMode,
        candidate: Option<Values>,
        vertices: Vec<u64>,
        metrics: Metrics,
        n: usize,
        resources: &Resources,
    ) -> Result<Self> {
        // The input vertex-count vector was admitted by the caller while copied
        // out of StatisticsInbox. This reservation takes over that accounting.
        let p = vertices.len();
        let bytes = n
            .checked_mul(8)
            .and_then(|v| p.checked_mul(32).and_then(|p| v.checked_add(p)))
            .and_then(|v| v.checked_add(512))
            .ok_or("residual inbox admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            mode,
            candidate,
            incoming: filled(n, 0.0)?,
            sequences: filled(p, 0)?,
            finished: filled(p, false)?,
            dangling: filled(p, 0.0)?,
            vertices,
            emitted: Arc::new(AtomicBool::new(false)),
            metrics,
            _admission: admission,
        })
    }
}
impl DeltaPartition {
    pub fn receive(&mut self, update: &DeltaContribution) -> Result<()> {
        self.receive_values(
            &update.phase,
            update.mode,
            update.producer,
            update.sequence,
            update.target,
            update.value,
        )
    }
    pub fn receive_values(
        &mut self,
        phase: &Round,
        mode: DeltaMode,
        producer: usize,
        sequence: u64,
        target: i64,
        value: f64,
    ) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.receive_inner(mode, producer, sequence, target, value);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn receive_inner(
        &mut self,
        mode: DeltaMode,
        producer: usize,
        sequence: u64,
        target: i64,
        value: f64,
    ) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return Err("residual phase is not receiving".into());
        };
        if mode != inbox.mode
            || mode == DeltaMode::Done
            || producer >= self.operation.partitions
            || inbox.finished[producer]
            || sequence != inbox.sequences[producer]
            || self.operation.owner(target) != self.partition
            || !value.is_finite()
            || (mode == DeltaMode::Certify && value < 0.0)
        {
            return Err("invalid, replayed or misrouted residual contribution".into());
        }
        self.resources
            .execution
            .charge_work(1 + self.adjacency.vertices.len().max(1).ilog2() as usize)
            .map_err(|e| e.to_string())?;
        let local = self
            .adjacency
            .vertices
            .binary_search(&target)
            .map_err(|_| "unknown residual destination")?;
        let total = inbox.incoming[local] + value;
        if !total.is_finite() {
            return Err("residual contribution sum overflow".into());
        }
        inbox.incoming[local] = total;
        inbox.sequences[producer] = sequence
            .checked_add(1)
            .ok_or("residual sequence overflow")?;
        Ok(())
    }
    pub fn finish_producer(&mut self, completion: &DeltaCompletion) -> Result<()> {
        if completion.sequences.len() != self.operation.partitions {
            return self.fail("invalid completion partition count");
        }
        self.finish_producer_values(
            &completion.phase,
            completion.mode,
            completion.producer,
            completion.sequences[self.partition],
            completion.dangling,
            completion.vertices,
        )
    }
    pub fn finish_producer_values(
        &mut self,
        phase: &Round,
        mode: DeltaMode,
        producer: usize,
        messages: u64,
        dangling: f64,
        vertices: u64,
    ) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = &mut self.state else {
            return self.fail("residual phase is not receiving");
        };
        if mode != inbox.mode
            || producer >= self.operation.partitions
            || inbox.finished[producer]
            || inbox.sequences[producer] != messages
            || !dangling.is_finite()
            || (mode == DeltaMode::Certify && dangling < 0.0)
            || (mode == DeltaMode::Done && (dangling != 0.0 || messages != 0))
            || inbox.vertices[producer] != vertices
        {
            return self.fail("incomplete, inconsistent or duplicated residual producer");
        }
        inbox.finished[producer] = true;
        inbox.dangling[producer] = dangling;
        Ok(())
    }
    /// Called after input EOF, not merely after observing P completion markers.
    /// A subsequent row would be rejected, but the adapter must drain the stream
    /// before calling this method to avoid publishing a truncated/extra stream.
    pub fn finish(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("residual phase is not receiving".into());
        };
        let inbox = *inbox;
        if !inbox.emitted.load(Ordering::Acquire) || !inbox.finished.iter().all(|v| *v) {
            return Err("incomplete residual contribution barrier".into());
        }
        let dangling: f64 = inbox.dangling.iter().sum();
        if !dangling.is_finite() {
            return Err("residual dangling sum overflow".into());
        }
        let base = match inbox.mode {
            DeltaMode::Push => self.options.damping * dangling / self.operation.vertices as f64,
            DeltaMode::Certify => {
                (1.0 - self.options.damping + self.options.damping * dangling)
                    / self.operation.vertices as f64
            }
            DeltaMode::Done => 0.0,
            DeltaMode::Initial => return Err("initial mode cannot emit contributions".into()),
        };
        let mut candidate = inbox.candidate;
        if let Some(next) = &mut candidate {
            let mut meter = self.resources.execution.work_meter();
            let mut mass = 0.0;
            let mut norm = 0.0;
            for (i, incoming) in inbox.incoming.iter().enumerate() {
                meter.charge(1).map_err(|e| e.to_string())?;
                next.residual[i] = match inbox.mode {
                    DeltaMode::Push => next.residual[i] + incoming + base,
                    DeltaMode::Certify => incoming + base - next.scores[i],
                    _ => unreachable!(),
                };
                if !next.scores[i].is_finite()
                    || next.scores[i] < 0.0
                    || !next.residual[i].is_finite()
                {
                    return Err("nonfinite or negative residual PageRank state".into());
                }
                mass += next.scores[i];
                norm += next.residual[i].abs();
            }
            if !mass.is_finite() || !norm.is_finite() {
                return Err("residual statistic overflow before publication".into());
            }
        }
        let next_phase = self
            .next_phase
            .checked_add(1)
            .ok_or("residual phase overflow")?;
        let pushes = self
            .pushes
            .checked_add(u64::from(inbox.mode == DeltaMode::Push))
            .ok_or("push count overflow")?;
        let certificates = self
            .certificates
            .checked_add(u64::from(inbox.mode == DeltaMode::Certify))
            .ok_or("certificate count overflow")?;
        // Allocate the next barrier before publishing. Failure leaves old scores
        // intact, and old cursors retain their own admitted snapshots.
        let statistics = StatisticsInbox::new(self.operation.partitions, &self.resources)?;
        if let Some(next) = candidate {
            self.values = Arc::new(next);
        }
        self.next_phase = next_phase;
        self.pushes = pushes;
        self.certificates = certificates;
        self.completed = inbox.mode;
        self.metrics = inbox.metrics;
        self.state = State::Statistics(statistics);
        Ok(())
    }
}

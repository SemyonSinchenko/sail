//! Immutable admitted cursors release partition locks before transport backpressure.
use super::*;
use grust_procedures::WorkMeter;
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SsspPayload {
    Topology { source: i64, target: i64 },
    Candidate { target: i64, label: SsspLabel },
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspMessageValues {
    pub origin: SsspOrigin,
    pub producer: usize,
    pub recipient: usize,
    pub sequence: u64,
    pub mode: SsspMode,
    pub bucket: Option<f64>,
    pub payload: SsspPayload,
}
#[derive(Clone, Debug)]
pub struct SsspMessage {
    pub phase: Arc<Round>,
    pub values: SsspMessageValues,
    _ownership: Arc<Ownership>,
    _cursor: Arc<Ownership>,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspCompletionValues {
    pub origin: SsspOrigin,
    pub producer: usize,
    pub mode: SsspMode,
    pub bucket: Option<f64>,
    pub sequence: u64,
    pub total_messages: u64,
}
#[derive(Debug)]
pub struct SsspCompletion {
    pub phase: Arc<Round>,
    pub origin: SsspOrigin,
    pub producer: usize,
    pub mode: SsspMode,
    pub bucket: Option<f64>,
    pub sequences: Vec<u64>,
    _ownership: Arc<Ownership>,
}
pub struct SsspEmissionCursor {
    phase: Arc<Round>,
    origin: SsspOrigin,
    producer: usize,
    mode: SsspMode,
    bucket: Option<f64>,
    delta: f64,
    adjacency: Arc<WeightedAdjacency>,
    values: Arc<Values>,
    resources: Resources,
    sequences: Vec<u64>,
    ownership: Arc<Ownership>,
    meter: WorkMeter,
    emitted: Arc<OnceLock<SsspWork>>,
    work: SsspWork,
    position: usize,
    edge: usize,
    entered: bool,
    exhausted: bool,
    complete: bool,
}
impl SsspPartition {
    pub fn start_emission(&mut self, phase: &Round) -> Result<SsspEmissionCursor> {
        self.check_phase(phase)?;
        match self.emission_inner(phase) {
            Ok(c) => Ok(c),
            Err(e) => self.fail(e),
        }
    }
    fn emission_inner(&mut self, phase: &Round) -> Result<SsspEmissionCursor> {
        let global = self.global_statistics()?;
        let (mode, bucket) = self.decision(&global)?;
        let State::Statistics(stats) = &self.state else {
            unreachable!()
        };
        let inbox = Inbox::new(
            mode,
            bucket,
            &stats.slots,
            self.values.labels.len(),
            &self.resources,
        )?;
        let ownership = Ownership::new(&self.resources, self.operation.partitions * 16 + 2048)?;
        let result = SsspEmissionCursor {
            phase: Arc::new(phase.clone()),
            origin: self.origin,
            producer: self.partition,
            mode,
            bucket,
            delta: self.options.delta,
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            sequences: filled(self.operation.partitions, 0)?,
            ownership,
            meter: self.resources.execution.work_meter(),
            emitted: inbox.emitted.clone(),
            work: SsspWork::default(),
            position: 0,
            edge: 0,
            entered: false,
            exhausted: false,
            complete: false,
        };
        self.state = State::Receiving(Box::new(inbox));
        Ok(result)
    }
}
impl SsspEmissionCursor {
    pub fn mode(&self) -> SsspMode {
        self.mode
    }
    pub fn bucket(&self) -> Option<f64> {
        self.bucket
    }
    pub fn next_update(&mut self) -> Result<Option<SsspMessage>> {
        let value = self.next_values()?;
        match value {
            Some(values) => match Ownership::new(&self.resources, 512) {
                Ok(owner) => Ok(Some(SsspMessage {
                    phase: self.phase.clone(),
                    values,
                    _ownership: owner,
                    _cursor: self.ownership.clone(),
                })),
                Err(e) => {
                    let _ = self.resources.execution.cancel();
                    Err(e)
                }
            },
            None => Ok(None),
        }
    }
    pub fn next_values(&mut self) -> Result<Option<SsspMessageValues>> {
        let result = self.next_inner();
        if result.is_err() {
            let _ = self.resources.execution.cancel();
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<SsspMessageValues>> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        if self.exhausted {
            return Ok(None);
        }
        if self.mode == SsspMode::Done {
            self.exhausted = true;
            return Ok(None);
        }
        loop {
            let local = if self.mode == SsspMode::DeltaStar {
                self.values.active.get(self.position).copied()
            } else {
                (self.position < self.adjacency.vertices().len()).then_some(self.position)
            };
            let Some(local) = local else {
                self.exhausted = true;
                return Ok(None);
            };
            let source = self.adjacency.vertices()[local];
            if !self.entered {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                self.work.examined_vertices = add(self.work.examined_vertices, 1)?;
                self.entered = true;
                let skip = match self.mode {
                    SsspMode::Reference => self.values.labels[local].is_none(),
                    SsspMode::DeltaStar => {
                        Some(
                            self.values.labels[local]
                                .ok_or("active vertex lacks label")?
                                .bucket(self.delta)?,
                        ) != self.bucket
                    }
                    _ => false,
                };
                if skip {
                    self.position += 1;
                    self.edge = 0;
                    self.entered = false;
                    continue;
                }
            }
            let arcs = self.adjacency.outgoing_at(local);
            if let Some(&(target, weight)) = arcs.get(self.edge) {
                self.edge += 1;
                self.meter.charge(1).map_err(|e| e.to_string())?;
                self.work.examined_edges = add(self.work.examined_edges, 1)?;
                let payload = if self.mode == SsspMode::Topology {
                    SsspPayload::Topology { source, target }
                } else {
                    SsspPayload::Candidate {
                        target,
                        label: self.values.labels[local]
                            .ok_or("unreached emission source")?
                            .extend(source, weight)?,
                    }
                };
                let recipient = self.phase.operation.owner(target);
                let sequence = self.sequences[recipient];
                self.sequences[recipient] = add(sequence, 1)?;
                self.work.emitted_messages = add(self.work.emitted_messages, 1)?;
                return Ok(Some(SsspMessageValues {
                    origin: self.origin,
                    producer: self.producer,
                    recipient,
                    sequence,
                    mode: self.mode,
                    bucket: self.bucket,
                    payload,
                }));
            }
            self.position += 1;
            self.edge = 0;
            self.entered = false;
        }
    }
    pub fn finish(mut self) -> Result<SsspCompletion> {
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        if !self.exhausted {
            return Err("SSSP emission not exhausted".into());
        }
        self.emitted
            .set(self.work)
            .map_err(|_| "SSSP emission completed twice")?;
        let result = SsspCompletion {
            phase: self.phase.clone(),
            origin: self.origin,
            producer: self.producer,
            mode: self.mode,
            bucket: self.bucket,
            sequences: std::mem::take(&mut self.sequences),
            _ownership: self.ownership.clone(),
        };
        self.complete = true;
        Ok(result)
    }
}
impl Drop for SsspEmissionCursor {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.resources.execution.cancel();
        }
    }
}

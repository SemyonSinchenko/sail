//! Bounded owned emissions: no partition lock is held while a sink blocks.
use super::*;
use grust_procedures::WorkMeter;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BfsPayload {
    Topology {
        source: i64,
        target: i64,
    },
    Candidate {
        target: i64,
        parent: i64,
        distance: u64,
    },
    Membership {
        vertex: i64,
    },
}
/// Borrowed Arrow decoder fields: this stack value owns no output allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsMessageValues {
    pub origin: BfsOrigin,
    pub producer: usize,
    pub recipient: usize,
    pub sequence: u64,
    pub mode: BfsMode,
    pub payload: BfsPayload,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsCompletionValues {
    pub origin: BfsOrigin,
    pub producer: usize,
    pub mode: BfsMode,
    pub sequence: u64,
    pub total_messages: u64,
}
#[derive(Clone, Debug)]
pub struct BfsMessage {
    pub phase: Arc<Round>,
    pub origin: BfsOrigin,
    pub producer: usize,
    pub recipient: usize,
    pub sequence: u64,
    pub mode: BfsMode,
    pub payload: BfsPayload,
    _ownership: Arc<Ownership>,
    _cursor: Arc<Ownership>,
}
impl BfsMessage {
    pub fn values(&self) -> BfsMessageValues {
        BfsMessageValues {
            origin: self.origin,
            producer: self.producer,
            recipient: self.recipient,
            sequence: self.sequence,
            mode: self.mode,
            payload: self.payload,
        }
    }
}
#[derive(Debug)]
pub struct BfsCompletion {
    pub phase: Arc<Round>,
    pub origin: BfsOrigin,
    pub producer: usize,
    pub mode: BfsMode,
    pub sequences: Vec<u64>,
    _ownership: Arc<Ownership>,
}
pub struct BfsEmissionCursor {
    phase: Arc<Round>,
    origin: BfsOrigin,
    producer: usize,
    mode: BfsMode,
    adjacency: Arc<Adjacency>,
    values: Arc<Values>,
    resources: Resources,
    sequences: Vec<u64>,
    ownership: Arc<Ownership>,
    meter: WorkMeter,
    emitted: Arc<OnceLock<BfsWork>>,
    work: BfsWork,
    position: usize,
    edge: usize,
    recipient: usize,
    entered: bool,
    level: u64,
    exhausted: bool,
    completed: bool,
}
impl BfsPartition {
    pub fn start_emission(&mut self, phase: &Round) -> Result<BfsEmissionCursor> {
        self.check_phase(phase)?;
        let result = self.emission_inner(phase);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn emission_inner(&mut self, phase: &Round) -> Result<BfsEmissionCursor> {
        let g = self.global_statistics()?;
        let mode = self.decision(&g)?;
        let State::Statistics(stats) = &self.state else {
            unreachable!()
        };
        let inbox = Inbox::new(
            mode,
            &stats.slots,
            self.values.depths.len(),
            self.incoming.as_ref().map_or(0, |x| x.ghosts.len()),
            &self.resources,
        )?;
        let ownership = Ownership::new(&self.resources, self.operation.partitions * 8 + 2048)?;
        let cursor = BfsEmissionCursor {
            phase: Arc::new(phase.clone()),
            origin: self.origin,
            producer: self.partition,
            mode,
            adjacency: self.adjacency.clone(),
            values: self.values.clone(),
            resources: self.resources.clone(),
            sequences: filled(self.operation.partitions, 0)?,
            ownership,
            meter: self.resources.execution.work_meter(),
            emitted: inbox.emitted.clone(),
            work: BfsWork::default(),
            position: 0,
            edge: 0,
            recipient: 0,
            entered: false,
            level: self.levels,
            exhausted: false,
            completed: false,
        };
        self.state = State::Receiving(Box::new(inbox));
        Ok(cursor)
    }
}
impl BfsEmissionCursor {
    pub fn mode(&self) -> BfsMode {
        self.mode
    }
    pub fn next_update(&mut self) -> Result<Option<BfsMessage>> {
        let result = self.next_values().and_then(|value| {
            value
                .map(|m| {
                    let ownership = Ownership::new(&self.resources, 512)?;
                    Ok(BfsMessage {
                        phase: self.phase.clone(),
                        origin: m.origin,
                        producer: m.producer,
                        recipient: m.recipient,
                        sequence: m.sequence,
                        mode: m.mode,
                        payload: m.payload,
                        _ownership: ownership,
                        _cursor: self.ownership.clone(),
                    })
                })
                .transpose()
        });
        if result.is_err() {
            let _ = self.resources.execution.cancel();
        }
        result
    }
    /// Caller retains an admitted Arrow batch while serializing these fields.
    /// No per-row Vec, Arc or native memory reservation is allocated here.
    pub fn next_values(&mut self) -> Result<Option<BfsMessageValues>> {
        let result = self.next_inner();
        if result.is_err() {
            let _ = self.resources.execution.cancel();
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<BfsMessageValues>> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        if self.mode == BfsMode::Pull {
            if let Some(&i) = self.values.frontier.get(self.position) {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                let recipient = self.recipient;
                if recipient == 0 {
                    self.work.examined_vertices += 1;
                }
                self.recipient += 1;
                if self.recipient == self.sequences.len() {
                    self.recipient = 0;
                    self.position += 1;
                }
                return self
                    .message(
                        recipient,
                        BfsPayload::Membership {
                            vertex: self.adjacency.vertices[i],
                        },
                    )
                    .map(Some);
            }
        } else if self.mode != BfsMode::Done {
            let full = matches!(self.mode, BfsMode::Topology | BfsMode::Reference);
            let length = if full {
                self.adjacency.vertices.len()
            } else {
                self.values.frontier.len()
            };
            while self.position < length {
                let i = if full {
                    self.position
                } else {
                    self.values.frontier[self.position]
                };
                if !self.entered {
                    self.meter.charge(1).map_err(|e| e.to_string())?;
                    self.work.examined_vertices += 1;
                    self.edge = self.adjacency.offsets[i];
                    self.entered = true;
                }
                while self.edge < self.adjacency.offsets[i + 1] {
                    self.meter.charge(1).map_err(|e| e.to_string())?;
                    self.work.examined_edges += 1;
                    let target = self.adjacency.targets[self.edge];
                    self.edge += 1;
                    if self.mode == BfsMode::Reference && self.values.depths[i] != Some(self.level)
                    {
                        continue;
                    }
                    let source = self.adjacency.vertices[i];
                    let payload = if self.mode == BfsMode::Topology {
                        BfsPayload::Topology { source, target }
                    } else {
                        BfsPayload::Candidate {
                            target,
                            parent: source,
                            distance: self.level + 1,
                        }
                    };
                    return self
                        .message(self.phase.operation.owner(target), payload)
                        .map(Some);
                }
                self.position += 1;
                self.entered = false;
            }
        }
        self.exhausted = true;
        Ok(None)
    }
    fn message(&mut self, recipient: usize, payload: BfsPayload) -> Result<BfsMessageValues> {
        let sequence = self.sequences[recipient];
        self.sequences[recipient] = sequence.checked_add(1).ok_or("BFS sequence overflow")?;
        self.work.emitted_messages = self
            .work
            .emitted_messages
            .checked_add(1)
            .ok_or("BFS message count overflow")?;
        Ok(BfsMessageValues {
            origin: self.origin,
            producer: self.producer,
            recipient,
            sequence,
            mode: self.mode,
            payload,
        })
    }
    pub fn finish(mut self) -> Result<BfsCompletion> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        if !self.exhausted {
            return Err("BFS emission has not reached EOF".into());
        }
        self.emitted
            .set(self.work)
            .map_err(|_| "BFS emission already completed")?;
        self.completed = true;
        Ok(BfsCompletion {
            phase: self.phase.clone(),
            origin: self.origin,
            producer: self.producer,
            mode: self.mode,
            sequences: std::mem::take(&mut self.sequences),
            _ownership: self.ownership.clone(),
        })
    }
}
impl Drop for BfsEmissionCursor {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.resources.execution.cancel();
        }
    }
}

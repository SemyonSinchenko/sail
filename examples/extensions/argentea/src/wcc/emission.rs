//! Owned snapshot cursors never hold the partition lock while emitting.
use super::*;
use grust_procedures::WorkMeter;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WccPayload {
    Topology {
        source: i64,
        target: i64,
    },
    Label {
        source: i64,
        target: i64,
        root: i64,
    },
    Member {
        vertex: i64,
        root: i64,
        candidate: Option<i64>,
    },
    Assignment {
        vertex: i64,
        old_root: i64,
        new_root: i64,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccMessageValues {
    pub origin: WccOrigin,
    pub producer: usize,
    pub recipient: usize,
    pub sequence: u64,
    pub mode: WccMode,
    pub payload: WccPayload,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccCompletionValues {
    pub origin: WccOrigin,
    pub producer: usize,
    pub mode: WccMode,
    pub sequence: u64,
    pub total_messages: u64,
}
#[derive(Clone, Debug)]
pub struct WccMessage {
    pub phase: Arc<Round>,
    pub origin: WccOrigin,
    pub producer: usize,
    pub recipient: usize,
    pub sequence: u64,
    pub mode: WccMode,
    pub payload: WccPayload,
    _ownership: Arc<Ownership>,
    _cursor: Arc<Ownership>,
}
impl WccMessage {
    pub fn values(&self) -> WccMessageValues {
        WccMessageValues {
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
pub struct WccCompletion {
    pub phase: Arc<Round>,
    pub origin: WccOrigin,
    pub producer: usize,
    pub mode: WccMode,
    pub sequences: Vec<u64>,
    _ownership: Arc<Ownership>,
}
pub struct WccEmissionCursor {
    phase: Arc<Round>,
    origin: WccOrigin,
    producer: usize,
    mode: WccMode,
    adjacency: Arc<Adjacency>,
    incoming: Option<Arc<Adjacency>>,
    values: Arc<Values>,
    candidates: Option<Arc<protocol::Candidates>>,
    routed: Option<Arc<Routed>>,
    resources: Resources,
    sequences: Vec<u64>,
    ownership: Arc<Ownership>,
    meter: WorkMeter,
    emitted: Arc<OnceLock<WccWork>>,
    work: WccWork,
    position: usize,
    edge: usize,
    side: usize,
    entered: bool,
    exhausted: bool,
    completed: bool,
}
impl WccPartition {
    pub fn start_emission(&mut self, phase: &Round) -> Result<WccEmissionCursor> {
        self.check_phase(phase)?;
        let result = self.emission_inner(phase);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn emission_inner(&mut self, phase: &Round) -> Result<WccEmissionCursor> {
        let g = self.global_statistics()?;
        let mode = self.decision(&g)?;
        let State::Statistics(stats) = &self.state else {
            unreachable!()
        };
        let inbox = Inbox::new(
            mode,
            &stats.slots,
            self.adjacency.vertices.len(),
            &self.resources,
        )?;
        let ownership = Ownership::new(&self.resources, self.operation.partitions * 8 + 2048)?;
        let result = WccEmissionCursor {
            phase: Arc::new(phase.clone()),
            origin: self.origin,
            producer: self.partition,
            mode,
            adjacency: self.adjacency.clone(),
            incoming: self.incoming.clone(),
            values: self.values.clone(),
            candidates: self.candidates.clone(),
            routed: self.routed.clone(),
            resources: self.resources.clone(),
            sequences: filled(self.operation.partitions, 0)?,
            ownership,
            meter: self.resources.execution.work_meter(),
            emitted: inbox.emitted.clone(),
            work: WccWork::default(),
            position: 0,
            edge: 0,
            side: 0,
            entered: false,
            exhausted: false,
            completed: false,
        };
        self.state = State::Receiving(Box::new(inbox));
        Ok(result)
    }
}
impl WccEmissionCursor {
    pub fn mode(&self) -> WccMode {
        self.mode
    }
    pub fn next_update(&mut self) -> Result<Option<WccMessage>> {
        let result = self.next_values().and_then(|x| {
            x.map(|m| {
                Ok(WccMessage {
                    phase: self.phase.clone(),
                    origin: m.origin,
                    producer: m.producer,
                    recipient: m.recipient,
                    sequence: m.sequence,
                    mode: m.mode,
                    payload: m.payload,
                    _ownership: Ownership::new(&self.resources, 512)?,
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
    /// Caller holds admitted Arrow output while serializing these borrowed scalars.
    pub fn next_values(&mut self) -> Result<Option<WccMessageValues>> {
        let result = self.next_inner();
        if result.is_err() {
            let _ = self.resources.execution.cancel();
        }
        result
    }
    fn next_inner(&mut self) -> Result<Option<WccMessageValues>> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        match self.mode {
            WccMode::Topology | WccMode::Reference | WccMode::Neighbors => {
                while self.position < self.adjacency.vertices.len() {
                    let i = self.position;
                    let graph = if self.side == 0 {
                        &self.adjacency
                    } else {
                        self.incoming
                            .as_ref()
                            .ok_or("WCC missing incoming adjacency")?
                    };
                    if !self.entered {
                        self.edge = graph.offsets[i];
                        self.entered = true;
                        if self.side == 0 {
                            self.work.examined_vertices += 1;
                            self.meter.charge(1).map_err(|e| e.to_string())?;
                        }
                    }
                    if self.edge < graph.offsets[i + 1] {
                        self.meter.charge(1).map_err(|e| e.to_string())?;
                        self.work.examined_edges += 1;
                        let target = graph.targets[self.edge];
                        self.edge += 1;
                        let source = self.adjacency.vertices[i];
                        let payload = if self.mode == WccMode::Topology {
                            WccPayload::Topology { source, target }
                        } else {
                            WccPayload::Label {
                                source,
                                target,
                                root: self.values.roots[i],
                            }
                        };
                        return self
                            .message(self.phase.operation.owner(target), payload)
                            .map(Some);
                    }
                    self.entered = false;
                    if self.side == 0 && self.mode != WccMode::Topology {
                        self.side = 1;
                    } else {
                        self.side = 0;
                        self.position += 1;
                    }
                }
            }
            WccMode::HookRoute | WccMode::NormalizeRoute => {
                if let Some(&vertex) = self.adjacency.vertices.get(self.position) {
                    self.meter.charge(1).map_err(|e| e.to_string())?;
                    self.work.examined_vertices += 1;
                    let root = self.values.roots[self.position];
                    let candidate = if self.mode == WccMode::HookRoute {
                        self.candidates
                            .as_ref()
                            .ok_or("WCC missing candidates")?
                            .heads[self.position]
                    } else {
                        None
                    };
                    self.position += 1;
                    return self
                        .message(
                            self.phase.operation.owner(root),
                            WccPayload::Member {
                                vertex,
                                root,
                                candidate,
                            },
                        )
                        .map(Some);
                }
            }
            WccMode::HookReturn | WccMode::NormalizeReturn => {
                let routed = self.routed.as_ref().ok_or("WCC missing routed members")?;
                if let Some(&(root, vertex)) = routed.members.values.get(self.position) {
                    self.meter
                        .charge(1 + self.adjacency.vertices.len().max(1).ilog2() as usize)
                        .map_err(|e| e.to_string())?;
                    self.work.examined_vertices += 1;
                    self.position += 1;
                    let i = self
                        .adjacency
                        .vertices
                        .binary_search(&root)
                        .map_err(|_| "WCC root disappeared")?;
                    let new_root = routed.choices[i];
                    return self
                        .message(
                            self.phase.operation.owner(vertex),
                            WccPayload::Assignment {
                                vertex,
                                old_root: root,
                                new_root,
                            },
                        )
                        .map(Some);
                }
            }
            WccMode::Done => {}
        }
        self.exhausted = true;
        Ok(None)
    }
    fn message(&mut self, recipient: usize, payload: WccPayload) -> Result<WccMessageValues> {
        let sequence = self.sequences[recipient];
        self.sequences[recipient] = add(sequence, 1)?;
        self.work.emitted_messages = add(self.work.emitted_messages, 1)?;
        Ok(WccMessageValues {
            origin: self.origin,
            producer: self.producer,
            recipient,
            sequence,
            mode: self.mode,
            payload,
        })
    }
    pub fn finish(mut self) -> Result<WccCompletion> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        if !self.exhausted {
            return Err("WCC emission has not reached EOF".into());
        }
        self.emitted
            .set(self.work)
            .map_err(|_| "WCC emission already completed")?;
        self.completed = true;
        Ok(WccCompletion {
            phase: self.phase.clone(),
            origin: self.origin,
            producer: self.producer,
            mode: self.mode,
            sequences: std::mem::take(&mut self.sequences),
            _ownership: self.ownership.clone(),
        })
    }
}
impl Drop for WccEmissionCursor {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.resources.execution.cancel();
        }
    }
}

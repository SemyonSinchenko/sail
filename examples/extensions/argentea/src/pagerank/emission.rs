//! Owned emission snapshots allow downstream consumers to drain bounded shuffle
//! channels without taking a lock held by an upstream producer.
use super::*;
use grust_procedures::WorkMeter;

pub struct EmissionCursor {
    adjacency: Arc<Adjacency>,
    ranks: Arc<Ranks>,
    round: Arc<Round>,
    producer: usize,
    ownership: Arc<EmissionOwnership>,
    resources: Resources,
    meter: WorkMeter,
    sequences: Vec<u64>,
    vertex: usize,
    edge: usize,
    entered: bool,
    exhausted: bool,
    completed: bool,
    dangling_mass: f64,
    messages: u64,
}

impl PageRankPartition {
    /// Freeze immutable native owners, then release the partition lock before
    /// pulling updates or waiting on shuffle backpressure. No rank vector is
    /// copied. Dropping a partially drained cursor cancels the local operation.
    pub fn start_emission(&mut self, round: &Round) -> Result<EmissionCursor> {
        self.check_round(round)?;
        let State::Receiving(inbox) = &mut self.state else {
            return self.fail("round is not receiving");
        };
        if inbox.emitted {
            return self.fail("producer already emitted this round");
        }
        inbox.emitted = true;
        let result = self.emission_snapshot(round);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }

    fn emission_snapshot(&self, round: &Round) -> Result<EmissionCursor> {
        let admission = self
            .resources
            .execution
            .reserve(self.operation.partitions * 8 + 2048)
            .map_err(|e| e.to_string())?;
        let mut sequences = reserve_vec(self.operation.partitions)?;
        sequences.resize(self.operation.partitions, 0);
        Ok(EmissionCursor {
            adjacency: self.adjacency.clone(),
            ranks: self.ranks.clone(),
            round: Arc::new(round.clone()),
            producer: self.partition,
            ownership: Arc::new(EmissionOwnership {
                _admission: admission,
                _lease: self.resources.lease.clone(),
            }),
            resources: self.resources.clone(),
            meter: self.resources.execution.work_meter(),
            sequences,
            vertex: 0,
            edge: 0,
            entered: false,
            exhausted: false,
            completed: false,
            dangling_mass: 0.0,
            messages: 0,
        })
    }
}

impl EmissionCursor {
    pub fn next_update(&mut self) -> Result<Option<Contribution>> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        while self.vertex < self.ranks.values.len() {
            if !self.entered {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                if self.adjacency.offsets[self.vertex] == self.adjacency.offsets[self.vertex + 1] {
                    self.dangling_mass += self.ranks.values[self.vertex];
                }
                self.entered = true;
            }
            if self.edge < self.adjacency.offsets[self.vertex + 1] {
                self.meter.charge(1).map_err(|e| e.to_string())?;
                let target = self.adjacency.targets[self.edge];
                let owner = self.round.operation.owner(target);
                let update = Contribution {
                    round: self.round.clone(),
                    _ownership: self.ownership.clone(),
                    producer: self.producer,
                    sequence: self.sequences[owner],
                    target,
                    value: self.ranks.values[self.vertex]
                        / (self.adjacency.offsets[self.vertex + 1]
                            - self.adjacency.offsets[self.vertex]) as f64,
                };
                self.edge += 1;
                self.sequences[owner] += 1;
                self.messages += 1;
                return Ok(Some(update));
            }
            self.vertex += 1;
            self.entered = false;
        }
        self.exhausted = true;
        Ok(None)
    }

    pub fn finish(mut self) -> Result<Emission> {
        self.meter.checkpoint().map_err(|e| e.to_string())?;
        if !self.exhausted {
            return Err("producer stream has not been exhausted".into());
        }
        self.completed = true;
        Ok(Emission {
            dangling_mass: self.dangling_mass,
            messages: self.messages,
            sequences: std::mem::take(&mut self.sequences),
            _ownership: self.ownership.clone(),
        })
    }
}

impl Drop for EmissionCursor {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.resources.execution.cancel();
        }
    }
}

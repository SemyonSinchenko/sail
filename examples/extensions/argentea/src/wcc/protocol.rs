//! Typed inboxes publish only after all producer completions and input EOF.
use super::*;
#[derive(Debug)]
pub(super) struct Candidates {
    pub heads: Vec<Option<i64>>,
    _admission: MemoryReservation,
}
impl Candidates {
    fn new(heads: &[Option<i64>], r: &Resources) -> Result<Self> {
        let admission = r
            .execution
            .reserve(std::mem::size_of_val(heads) + 128)
            .map_err(|e| e.to_string())?;
        let mut values = reserve_vec(heads.len())?;
        values.extend_from_slice(heads);
        Ok(Self {
            heads: values,
            _admission: admission,
        })
    }
}
#[derive(Debug)]
pub(super) struct Inbox {
    mode: WccMode,
    expected: Vec<WccStatisticsValues>,
    sequences: Vec<u64>,
    finished: Vec<bool>,
    labels: Vec<Option<i64>>,
    pairs: Option<Buffer<(i64, i64)>>,
    pub emitted: Arc<OnceLock<WccWork>>,
    crossing: u64,
    received: u64,
    _admission: MemoryReservation,
}
impl Inbox {
    pub fn new(
        mode: WccMode,
        reports: &[Option<WccStatisticsValues>],
        n: usize,
        r: &Resources,
    ) -> Result<Self> {
        // Topology accumulates pairs; Done only validates completion barriers.
        // Neither phase reads per-vertex labels, including during publication.
        let n = if matches!(mode, WccMode::Topology | WccMode::Done) {
            0
        } else {
            n
        };
        let bytes = n
            .checked_mul(size_of::<Option<i64>>())
            .and_then(|x| {
                x.checked_add(reports.len() * (size_of::<WccStatisticsValues>() + 16) + 512)
            })
            .ok_or("WCC inbox admission overflow")?;
        let admission = r.execution.reserve(bytes).map_err(|e| e.to_string())?;
        let mut expected = reserve_vec(reports.len())?;
        for s in reports {
            expected.push(s.ok_or("incomplete WCC statistics")?);
        }
        Ok(Self {
            mode,
            expected,
            sequences: filled(reports.len(), 0)?,
            finished: filled(reports.len(), false)?,
            labels: filled(n, None)?,
            pairs: None,
            emitted: Arc::new(OnceLock::new()),
            crossing: 0,
            received: 0,
            _admission: admission,
        })
    }
    fn pair(&mut self, pair: (i64, i64), r: &Resources) -> Result<()> {
        if self.pairs.is_none() {
            self.pairs = Some(Buffer::new(r)?);
        }
        self.pairs.as_mut().unwrap().push(pair, r)
    }
}
impl WccPartition {
    pub fn receive(&mut self, m: &WccMessage) -> Result<()> {
        self.receive_values(&m.phase, m.values())
    }
    pub fn receive_values(&mut self, phase: &Round, m: WccMessageValues) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.receive_inner(m);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn receive_inner(&mut self, m: WccMessageValues) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return Err("WCC is not receiving updates".into());
        };
        let p = m.producer;
        if p >= self.operation.partitions
            || m.recipient != self.partition
            || self.origins[p] != Some(m.origin)
            || m.mode != inbox.mode
            || inbox.finished[p]
            || inbox.sequences[p] != m.sequence
        {
            return Err("foreign, replayed, misrouted or late WCC update".into());
        }
        self.resources
            .execution
            .charge_work(1 + self.adjacency.vertices.len().max(1).ilog2() as usize)
            .map_err(|e| e.to_string())?;
        match m.payload {
            WccPayload::Topology { source, target } => {
                if inbox.mode != WccMode::Topology || self.operation.owner(source) != p {
                    return Err("invalid WCC topology source or mode".into());
                }
                self.adjacency
                    .vertices
                    .binary_search(&target)
                    .map_err(|_| "unknown WCC topology destination")?;
                inbox.pair((target, source), &self.resources)?;
            }
            WccPayload::Label {
                source,
                target,
                root,
            } => {
                if !matches!(inbox.mode, WccMode::Reference | WccMode::Neighbors)
                    || self.operation.owner(source) != p
                {
                    return Err("invalid WCC label source or mode".into());
                }
                let i = self
                    .adjacency
                    .vertices
                    .binary_search(&target)
                    .map_err(|_| "unknown WCC label destination")?;
                let own = self.values.roots[i];
                if own != root {
                    inbox.crossing = add(inbox.crossing, 1)?;
                }
                if inbox.mode == WccMode::Reference {
                    if root > source {
                        return Err("reference WCC label exceeds its source ID".into());
                    }
                    inbox.labels[i] = Some(inbox.labels[i].map_or(root, |v| v.min(root)));
                } else if own != root
                    && !wcc_head(self.options.seed, self.rounds, own)
                    && wcc_head(self.options.seed, self.rounds, root)
                {
                    inbox.labels[i] = Some(inbox.labels[i].map_or(root, |v| v.min(root)));
                }
            }
            WccPayload::Member {
                vertex,
                root,
                candidate,
            } => {
                if !matches!(inbox.mode, WccMode::HookRoute | WccMode::NormalizeRoute)
                    || self.operation.owner(vertex) != p
                {
                    return Err("invalid WCC member source or mode".into());
                }
                let i = self
                    .adjacency
                    .vertices
                    .binary_search(&root)
                    .map_err(|_| "unknown WCC root")?;
                if self.values.roots[i] != root {
                    return Err("WCC membership addressed to non-root".into());
                }
                if let Some(head) = candidate {
                    if inbox.mode != WccMode::HookRoute
                        || head == root
                        || wcc_head(self.options.seed, self.rounds, root)
                        || !wcc_head(self.options.seed, self.rounds, head)
                    {
                        return Err("invalid WCC tail-to-head candidate".into());
                    }
                    inbox.labels[i] = Some(inbox.labels[i].map_or(head, |v| v.min(head)));
                }
                if inbox.mode == WccMode::NormalizeRoute {
                    inbox.labels[i] = Some(inbox.labels[i].map_or(vertex, |v| v.min(vertex)));
                }
                inbox.pair((root, vertex), &self.resources)?;
            }
            WccPayload::Assignment {
                vertex,
                old_root,
                new_root,
            } => {
                if !matches!(inbox.mode, WccMode::HookReturn | WccMode::NormalizeReturn)
                    || self.operation.owner(old_root) != p
                {
                    return Err("invalid WCC assignment origin or mode".into());
                }
                let i = self
                    .adjacency
                    .vertices
                    .binary_search(&vertex)
                    .map_err(|_| "unknown WCC assignment vertex")?;
                if self.values.roots[i] != old_root || inbox.labels[i].is_some() {
                    return Err("duplicate or stale WCC member assignment".into());
                }
                if inbox.mode == WccMode::HookReturn
                    && new_root != old_root
                    && (wcc_head(self.options.seed, self.rounds, old_root)
                        || !wcc_head(self.options.seed, self.rounds, new_root))
                {
                    return Err("invalid WCC star hook".into());
                }
                if inbox.mode == WccMode::NormalizeReturn
                    && (new_root > vertex || new_root > old_root)
                {
                    return Err("invalid WCC minimum component label".into());
                }
                inbox.labels[i] = Some(new_root);
            }
        }
        inbox.sequences[p] = add(m.sequence, 1)?;
        inbox.received = add(inbox.received, 1)?;
        Ok(())
    }
    pub fn finish_producer(&mut self, c: &WccCompletion) -> Result<()> {
        if c.sequences.len() != self.operation.partitions {
            return self.fail("invalid WCC completion partition count");
        }
        let total = match c.sequences.iter().try_fold(0u64, |a, &b| add(a, b)) {
            Ok(total) => total,
            Err(e) => return self.fail(e),
        };
        self.finish_producer_values(
            &c.phase,
            WccCompletionValues {
                origin: c.origin,
                producer: c.producer,
                mode: c.mode,
                sequence: c.sequences[self.partition],
                total_messages: total,
            },
        )
    }
    pub fn finish_producer_values(&mut self, phase: &Round, c: WccCompletionValues) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.finish_producer_inner(c);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn finish_producer_inner(&mut self, c: WccCompletionValues) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return self.fail("WCC is not receiving updates");
        };
        let p = c.producer;
        if p >= self.operation.partitions
            || self.origins[p] != Some(c.origin)
            || c.mode != inbox.mode
            || inbox.finished[p]
            || c.sequence != inbox.sequences[p]
            || c.sequence > c.total_messages
        {
            return self.fail("incomplete, foreign or repeated WCC completion");
        }
        let s = inbox.expected[p];
        let expected = match inbox.mode {
            WccMode::Topology => s.arcs,
            WccMode::Reference | WccMode::Neighbors => add(s.arcs, s.incoming_arcs)?,
            WccMode::HookRoute | WccMode::NormalizeRoute => s.vertices,
            WccMode::HookReturn | WccMode::NormalizeReturn => s.members,
            WccMode::Done => 0,
        };
        if expected != c.total_messages {
            return self.fail("WCC completion count differs from producer statistics");
        }
        inbox.finished[p] = true;
        Ok(())
    }
    /// Invoke only after EOF; completion markers cannot exclude later rows.
    pub fn finish(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("WCC is not receiving updates".into());
        };
        let mut inbox = *inbox;
        if !inbox.finished.iter().all(|x| *x) {
            return Err("incomplete WCC update barrier".into());
        }
        let mut work = *inbox
            .emitted
            .get()
            .ok_or("WCC local emission not complete")?;
        work.received_messages = inbox.received;
        let mut incoming = self.incoming.clone();
        let mut values = None;
        let mut candidates = None;
        let mut routed = None;
        let mut rounds = self.rounds;
        let mut changed = 0;
        let crossing = inbox.crossing;
        match inbox.mode {
            WccMode::Topology => {
                let edges = inbox
                    .pairs
                    .as_ref()
                    .map_or(&[][..], |x| x.values.as_slice());
                incoming = Some(Adjacency::build(
                    &self.operation,
                    self.partition,
                    &self.adjacency.vertices,
                    edges,
                    &self.resources,
                )?);
            }
            WccMode::Reference => {
                let mut next = Values::new(&self.values.roots, &self.resources)?;
                for (i, label) in inbox.labels.iter().enumerate() {
                    self.resources
                        .execution
                        .charge_work(1)
                        .map_err(|e| e.to_string())?;
                    if let Some(label) = label {
                        next.roots[i] = next.roots[i].min(*label);
                    }
                    changed += u64::from(next.roots[i] != self.values.roots[i]);
                }
                values = Some(Arc::new(next));
                rounds = add(rounds, 1)?;
            }
            WccMode::Neighbors => {
                candidates = Some(Arc::new(Candidates::new(&inbox.labels, &self.resources)?));
            }
            WccMode::HookRoute | WccMode::NormalizeRoute => {
                let mut members = match inbox.pairs.take() {
                    Some(x) => x,
                    None => Buffer::new(&self.resources)?,
                };
                let len = members.values.len();
                self.resources
                    .execution
                    .charge_work(len.saturating_mul(len.max(1).ilog2() as usize + 1))
                    .map_err(|e| e.to_string())?;
                members.values.sort_unstable_by_key(|&(_, vertex)| vertex);
                if members.values.windows(2).any(|x| x[0].1 == x[1].1) {
                    return Err("duplicate WCC member routed to root owner".into());
                }
                let admission = self
                    .resources
                    .execution
                    .reserve(self.adjacency.vertices.len() * 8 + 128)
                    .map_err(|e| e.to_string())?;
                let mut choices = reserve_vec(self.adjacency.vertices.len())?;
                choices.extend(
                    self.adjacency
                        .vertices
                        .iter()
                        .enumerate()
                        .map(|(i, &id)| inbox.labels[i].unwrap_or(id)),
                );
                routed = Some(Arc::new(Routed {
                    members,
                    choices,
                    _admission: admission,
                }));
            }
            WccMode::HookReturn | WccMode::NormalizeReturn => {
                if inbox.labels.iter().any(Option::is_none) {
                    return Err("incomplete WCC member assignments".into());
                }
                let mut next = Values::new(&self.values.roots, &self.resources)?;
                for (i, label) in inbox.labels.iter().enumerate() {
                    self.resources
                        .execution
                        .charge_work(1)
                        .map_err(|e| e.to_string())?;
                    next.roots[i] = label.unwrap();
                    changed += u64::from(next.roots[i] != self.values.roots[i]);
                }
                values = Some(Arc::new(next));
                if inbox.mode == WccMode::HookReturn {
                    rounds = add(rounds, 1)?;
                }
            }
            WccMode::Done => {}
        }
        let next = StatisticsInbox::new(self.operation.partitions, &self.resources)?;
        let next_phase = add(self.next_phase, 1)?;
        if let Some(values) = values {
            self.values = values;
        }
        self.incoming = incoming;
        self.candidates = candidates;
        self.routed = routed;
        self.rounds = rounds;
        self.changed = changed;
        self.crossing = crossing;
        self.completed = inbox.mode;
        self.work = work;
        self.next_phase = next_phase;
        self.state = State::Statistics(next);
        Ok(())
    }
}

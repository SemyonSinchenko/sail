//! Producer-complete integer barriers. EOF is required before any decision.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WccStatisticsValues {
    pub options: WccOptions,
    pub origin: WccOrigin,
    pub completed: WccMode,
    pub rounds: u64,
    pub vertices: u64,
    pub arcs: u64,
    pub incoming_arcs: u64,
    pub changed: u64,
    pub crossing: u64,
    pub members: u64,
}
#[derive(Clone, Debug)]
pub struct WccStatistics {
    pub phase: Arc<Round>,
    pub producer: usize,
    pub values: WccStatisticsValues,
    _ownership: Arc<Ownership>,
}
#[derive(Debug)]
pub(super) struct StatisticsInbox {
    pub slots: Vec<Option<WccStatisticsValues>>,
    closed: bool,
    _admission: MemoryReservation,
}
impl StatisticsInbox {
    pub fn new(p: usize, r: &Resources) -> Result<Self> {
        let admission = r
            .execution
            .reserve(p * size_of::<Option<WccStatisticsValues>>() + 128)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            slots: filled(p, None)?,
            closed: false,
            _admission: admission,
        })
    }
}
#[derive(Default)]
pub(super) struct Totals {
    pub vertices: u64,
    pub arcs: u64,
    pub incoming: u64,
    pub changed: u64,
    pub crossing: u64,
    pub members: u64,
}
impl WccPartition {
    fn local_statistics(&self) -> WccStatisticsValues {
        WccStatisticsValues {
            options: self.options,
            origin: self.origin,
            completed: self.completed,
            rounds: self.rounds,
            vertices: self.adjacency.vertices.len() as u64,
            arcs: self.adjacency.targets.len() as u64,
            incoming_arcs: self.incoming.as_ref().map_or(0, |x| x.targets.len() as u64),
            changed: self.changed,
            crossing: self.crossing,
            members: self
                .routed
                .as_ref()
                .map_or(0, |x| x.members.values.len() as u64),
        }
    }
    pub fn statistics(&self) -> Result<WccStatistics> {
        if !matches!(self.state, State::Statistics(_)) {
            return Err("WCC is not collecting statistics".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(WccStatistics {
            phase: Arc::new(Round {
                operation: self.operation.clone(),
                number: self.next_phase,
            }),
            producer: self.partition,
            values: self.local_statistics(),
            _ownership: Ownership::new(&self.resources, 2048)?,
        })
    }
    pub fn receive_statistics(&mut self, s: &WccStatistics) -> Result<()> {
        self.receive_statistics_values(&s.phase, s.producer, s.values)
    }
    pub fn receive_statistics_values(
        &mut self,
        phase: &Round,
        producer: usize,
        s: WccStatisticsValues,
    ) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.receive_statistics_inner(producer, s);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn receive_statistics_inner(&mut self, producer: usize, s: WccStatisticsValues) -> Result<()> {
        if producer >= self.operation.partitions
            || s.options != self.options
            || s.completed != self.completed
            || s.rounds != self.rounds
            || s.vertices > self.operation.vertices
            || s.changed > s.vertices
            || s.members > self.operation.vertices
            || s.crossing
                > s.arcs
                    .checked_add(s.incoming_arcs)
                    .ok_or("WCC arc count overflow")?
            || s.origin.adjacency_id == 0
            || self.origins[producer].is_some_and(|x| x != s.origin)
            || self.shapes[producer].is_some_and(|x| x != (s.vertices, s.arcs))
            || (s.vertices == 0 && (s.arcs != 0 || s.incoming_arcs != 0 || s.members != 0))
            || (self.next_phase == 0
                && (s.incoming_arcs != 0 || s.changed != 0 || s.crossing != 0 || s.members != 0))
            || (!matches!(s.completed, WccMode::HookRoute | WccMode::NormalizeRoute)
                && s.members != 0)
            || (producer == self.partition && s != self.local_statistics())
        {
            return self.fail("invalid or inconsistent WCC statistics/origin");
        }
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("WCC is not collecting statistics");
        };
        if inbox.closed || inbox.slots[producer].is_some() {
            return self.fail("duplicate or late WCC statistics");
        }
        inbox.slots[producer] = Some(s);
        self.origins[producer] = Some(s.origin);
        self.shapes[producer] = Some((s.vertices, s.arcs));
        Ok(())
    }
    pub fn finish_statistics(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("WCC is not collecting statistics");
        };
        if inbox.closed || inbox.slots.iter().any(Option::is_none) {
            return self.fail("incomplete or repeated WCC statistics EOF");
        }
        inbox.closed = true;
        if let Err(e) = self.global_statistics() {
            return self.fail(e);
        }
        Ok(())
    }
    pub(super) fn global_statistics(&self) -> Result<Totals> {
        let State::Statistics(inbox) = &self.state else {
            return Err("WCC is not collecting statistics".into());
        };
        if !inbox.closed {
            return Err("WCC statistics input has not reached EOF".into());
        }
        let mut g = Totals::default();
        let mut meter = self.resources.execution.work_meter();
        for s in &inbox.slots {
            meter.charge(1).map_err(|e| e.to_string())?;
            let s = s.ok_or("incomplete WCC statistics")?;
            g.vertices = add(g.vertices, s.vertices)?;
            g.arcs = add(g.arcs, s.arcs)?;
            g.incoming = add(g.incoming, s.incoming_arcs)?;
            g.changed = add(g.changed, s.changed)?;
            g.crossing = add(g.crossing, s.crossing)?;
            g.members = add(g.members, s.members)?;
        }
        if g.vertices != self.operation.vertices
            || (self.next_phase > 0 && g.incoming != g.arcs)
            || (matches!(self.completed, WccMode::HookRoute | WccMode::NormalizeRoute)
                && g.members != g.vertices)
        {
            return Err(
                "WCC graph cardinality or complete topology/member count inconsistent".into(),
            );
        }
        Ok(g)
    }
}

//! Integer, producer-complete decision and termination barrier.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BfsStatisticsValues {
    pub options: BfsOptions,
    pub origin: BfsOrigin,
    pub completed: BfsMode,
    pub levels: u64,
    pub vertices: u64,
    pub arcs: u64,
    pub source_count: u64,
    pub reached: u64,
    pub frontier: u64,
    pub frontier_edges: u64,
    pub remaining_edges: u64,
}
#[derive(Clone, Debug)]
pub struct BfsStatistics {
    pub phase: Arc<Round>,
    pub producer: usize,
    pub values: BfsStatisticsValues,
    _ownership: Arc<Ownership>,
}
#[derive(Debug)]
pub(super) struct StatisticsInbox {
    pub slots: Vec<Option<BfsStatisticsValues>>,
    closed: bool,
    _admission: MemoryReservation,
}
impl StatisticsInbox {
    pub fn new(p: usize, resources: &Resources) -> Result<Self> {
        let admission = resources
            .execution
            .reserve(p * size_of::<Option<BfsStatisticsValues>>() + 128)
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
    pub source_count: u64,
    pub reached: u64,
    pub frontier: u64,
    pub frontier_edges: u64,
    pub remaining: u64,
}
impl BfsPartition {
    fn local_statistics(&self) -> Result<BfsStatisticsValues> {
        let mut frontier_edges = 0u64;
        let mut meter = self.resources.execution.work_meter();
        for &i in &self.values.frontier {
            meter.charge(1).map_err(|e| e.to_string())?;
            frontier_edges = frontier_edges
                .checked_add((self.adjacency.offsets[i + 1] - self.adjacency.offsets[i]) as u64)
                .ok_or("BFS frontier edge overflow")?;
        }
        Ok(BfsStatisticsValues {
            options: self.options,
            origin: self.origin,
            completed: self.completed,
            levels: self.levels,
            vertices: self.adjacency.vertices.len() as u64,
            arcs: self.adjacency.targets.len() as u64,
            source_count: u64::from(
                self.adjacency
                    .vertices
                    .binary_search(&self.options.source)
                    .is_ok(),
            ),
            reached: self.values.reached,
            frontier: self.values.frontier.len() as u64,
            frontier_edges,
            remaining_edges: self.values.remaining,
        })
    }
    pub fn statistics(&self) -> Result<BfsStatistics> {
        if !matches!(self.state, State::Statistics(_)) {
            return Err("BFS is not collecting statistics".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(BfsStatistics {
            phase: Arc::new(Round {
                operation: self.operation.clone(),
                number: self.next_phase,
            }),
            producer: self.partition,
            values: self.local_statistics()?,
            _ownership: Ownership::new(&self.resources, 2048)?,
        })
    }
    pub fn receive_statistics(&mut self, report: &BfsStatistics) -> Result<()> {
        self.receive_statistics_values(&report.phase, report.producer, report.values)
    }
    pub fn receive_statistics_values(
        &mut self,
        phase: &Round,
        producer: usize,
        report: BfsStatisticsValues,
    ) -> Result<()> {
        self.check_phase(phase)?;
        if producer >= self.operation.partitions
            || report.options != self.options
            || report.levels != self.levels
            || report.completed != self.completed
            || report.vertices > self.operation.vertices
            || report.reached > report.vertices
            || report.frontier > report.reached
            || report.source_count > 1
            || report.source_count > report.reached
            || (report.frontier == 0 && report.frontier_edges != 0)
            || report
                .frontier_edges
                .checked_add(report.remaining_edges)
                .is_none_or(|n| n > report.arcs)
            || (report.source_count == 1 && self.operation.owner(self.options.source) != producer)
            || self.shapes[producer]
                .is_some_and(|shape| shape != (report.vertices, report.arcs, report.source_count))
            || report.origin.adjacency_id == 0
            || report.remaining_edges > report.arcs
            || report.frontier_edges > report.arcs
            || (report.vertices == 0
                && (report.arcs != 0
                    || report.frontier_edges != 0
                    || report.remaining_edges != 0
                    || report.source_count != 0))
            || self.origins[producer].is_some_and(|origin| origin != report.origin)
        {
            return self.fail("invalid or inconsistent BFS statistics/origin");
        }
        if producer == self.partition && report != self.local_statistics()? {
            return self.fail("local BFS statistics replaced");
        }
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("BFS is not collecting statistics");
        };
        if inbox.closed || inbox.slots[producer].is_some() {
            return self.fail("duplicate or late BFS statistics");
        }
        inbox.slots[producer] = Some(report);
        self.origins[producer] = Some(report.origin);
        self.shapes[producer] = Some((report.vertices, report.arcs, report.source_count));
        Ok(())
    }
    /// Caller invokes only after input EOF. Late reports poison the operation.
    pub fn finish_statistics(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("BFS is not collecting statistics");
        };
        if inbox.closed || inbox.slots.iter().any(Option::is_none) {
            return self.fail("incomplete or repeated BFS statistics EOF");
        }
        inbox.closed = true;
        if let Err(e) = self.global_statistics() {
            return self.fail(e);
        }
        Ok(())
    }
    pub(super) fn global_statistics(&self) -> Result<Totals> {
        let State::Statistics(inbox) = &self.state else {
            return Err("BFS is not collecting statistics".into());
        };
        if !inbox.closed {
            return Err("BFS statistics input has not reached EOF".into());
        }
        let mut g = Totals::default();
        let mut meter = self.resources.execution.work_meter();
        for slot in &inbox.slots {
            meter.charge(1).map_err(|e| e.to_string())?;
            let s = slot.ok_or("incomplete BFS statistics")?;
            g.vertices = add(g.vertices, s.vertices)?;
            g.source_count = add(g.source_count, s.source_count)?;
            g.reached = add(g.reached, s.reached)?;
            g.frontier = add(g.frontier, s.frontier)?;
            g.frontier_edges = add(g.frontier_edges, s.frontier_edges)?;
            g.remaining = add(g.remaining, s.remaining_edges)?;
        }
        if g.vertices != self.operation.vertices || g.source_count != 1 {
            return Err("BFS source missing or graph cardinality inconsistent".into());
        }
        Ok(g)
    }
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| "BFS global integer overflow".into())
}

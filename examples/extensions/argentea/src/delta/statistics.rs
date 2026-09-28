//! The P-wide statistics barrier is separate from contribution completion.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Scalars {
    pub vertices: u64,
    pub mass: f64,
    pub residual_l1: f64,
    pub min_score: f64,
    pub metrics: Metrics,
}
#[derive(Debug)]
struct Ownership {
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}
#[derive(Clone, Debug)]
pub struct DeltaStatistics {
    pub phase: Arc<Round>,
    pub producer: usize,
    pub options: DeltaOptions,
    pub completed: DeltaMode,
    pub pushes: u64,
    pub certificate_passes: u64,
    pub vertices: u64,
    pub mass: f64,
    pub residual_l1: f64,
    pub min_score: f64,
    pub active_vertices: u64,
    pub active_edges: u64,
    pub reactivated_vertices: u64,
    _ownership: Arc<Ownership>,
}
/// Borrowed-wire fields. The caller owns its Arrow batch; this copy contains
/// no native ownership token and allocates nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeltaStatisticsValues {
    pub options: DeltaOptions,
    pub completed: DeltaMode,
    pub pushes: u64,
    pub certificate_passes: u64,
    pub vertices: u64,
    pub mass: f64,
    pub residual_l1: f64,
    pub min_score: f64,
    pub active_vertices: u64,
    pub active_edges: u64,
    pub reactivated_vertices: u64,
}
impl DeltaStatistics {
    pub fn values(&self) -> DeltaStatisticsValues {
        DeltaStatisticsValues {
            options: self.options,
            completed: self.completed,
            pushes: self.pushes,
            certificate_passes: self.certificate_passes,
            vertices: self.vertices,
            mass: self.mass,
            residual_l1: self.residual_l1,
            min_score: self.min_score,
            active_vertices: self.active_vertices,
            active_edges: self.active_edges,
            reactivated_vertices: self.reactivated_vertices,
        }
    }
}
impl DeltaStatisticsValues {
    fn scalars(self) -> Scalars {
        Scalars {
            vertices: self.vertices,
            mass: self.mass,
            residual_l1: self.residual_l1,
            min_score: self.min_score,
            metrics: Metrics {
                active_vertices: self.active_vertices,
                active_edges: self.active_edges,
                reactivated_vertices: self.reactivated_vertices,
            },
        }
    }
}
#[derive(Debug)]
pub(super) struct StatisticsInbox {
    pub slots: Vec<Option<Scalars>>,
    _admission: MemoryReservation,
}
impl StatisticsInbox {
    pub(super) fn new(p: usize, resources: &Resources) -> Result<Self> {
        let bytes = p
            .checked_mul(size_of::<Option<Scalars>>())
            .and_then(|n| n.checked_add(128))
            .ok_or("statistics admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            slots: filled(p, None)?,
            _admission: admission,
        })
    }
}

impl DeltaPartition {
    fn local_scalars(&self) -> Result<Scalars> {
        let mut mass = 0.0;
        let mut residual_l1 = 0.0;
        let mut min_score = f64::INFINITY;
        let mut meter = self.resources.execution.work_meter();
        for (&x, &r) in self.values.scores.iter().zip(&self.values.residual) {
            meter.charge(1).map_err(|e| e.to_string())?;
            if !x.is_finite() || x < 0.0 || !r.is_finite() {
                return Err("invalid residual score/state".into());
            }
            mass += x;
            residual_l1 += r.abs();
            min_score = min_score.min(x);
        }
        if !mass.is_finite() || !residual_l1.is_finite() {
            return Err("residual statistic overflow".into());
        }
        Ok(Scalars {
            vertices: self.adjacency.vertices.len() as u64,
            mass,
            residual_l1,
            min_score,
            metrics: self.metrics,
        })
    }
    pub fn statistics(&self) -> Result<DeltaStatistics> {
        if !matches!(self.state, State::Statistics(_)) {
            return Err("phase is not collecting statistics".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        let s = self.local_scalars()?;
        let admission = self
            .resources
            .execution
            .reserve(2048)
            .map_err(|e| e.to_string())?;
        Ok(DeltaStatistics {
            phase: Arc::new(Round {
                operation: self.operation.clone(),
                number: self.next_phase,
            }),
            producer: self.partition,
            options: self.options,
            completed: self.completed,
            pushes: self.pushes,
            certificate_passes: self.certificates,
            vertices: s.vertices,
            mass: s.mass,
            residual_l1: s.residual_l1,
            min_score: s.min_score,
            active_vertices: s.metrics.active_vertices,
            active_edges: s.metrics.active_edges,
            reactivated_vertices: s.metrics.reactivated_vertices,
            _ownership: Arc::new(Ownership {
                _admission: admission,
                _lease: self.resources.lease.clone(),
            }),
        })
    }
    pub fn receive_statistics(&mut self, report: &DeltaStatistics) -> Result<()> {
        self.receive_statistics_values(&report.phase, report.producer, report.values())
    }
    pub fn receive_statistics_values(
        &mut self,
        phase: &Round,
        producer: usize,
        report: DeltaStatisticsValues,
    ) -> Result<()> {
        self.check_phase(phase)?;
        let s = report.scalars();
        if producer >= self.operation.partitions
            || report.options != self.options
            || report.completed != self.completed
            || report.pushes != self.pushes
            || report.certificate_passes != self.certificates
            || s.vertices > self.operation.vertices
            || !s.mass.is_finite()
            || s.mass < 0.0
            || !s.residual_l1.is_finite()
            || s.residual_l1 < 0.0
            || (s.vertices > 0 && (!s.min_score.is_finite() || s.min_score < 0.0))
            || (s.vertices == 0
                && (s.min_score != f64::INFINITY || s.mass != 0.0 || s.residual_l1 != 0.0))
            || s.metrics.active_vertices > s.vertices
            || s.metrics.reactivated_vertices > s.metrics.active_vertices
        {
            return self.fail("invalid or inconsistent residual statistics");
        }
        if producer == self.partition && s != self.local_scalars()? {
            return self.fail("local residual statistics were replaced");
        }
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("phase is not collecting statistics");
        };
        if inbox.slots[producer].is_some() {
            return self.fail("duplicate residual statistics");
        }
        inbox.slots[producer] = Some(s);
        Ok(())
    }
    pub(super) fn global_statistics(&self) -> Result<Scalars> {
        let State::Statistics(inbox) = &self.state else {
            return Err("phase is not collecting statistics".into());
        };
        let mut global = Scalars {
            vertices: 0,
            mass: 0.0,
            residual_l1: 0.0,
            min_score: f64::INFINITY,
            metrics: Metrics::default(),
        };
        let mut meter = self.resources.execution.work_meter();
        // Index order, never arrival order. Every owner must make the same choice.
        for slot in &inbox.slots {
            meter.charge(1).map_err(|e| e.to_string())?;
            let s = slot.ok_or("incomplete residual statistics barrier")?;
            global.vertices = global
                .vertices
                .checked_add(s.vertices)
                .ok_or("vertex count overflow")?;
            global.mass += s.mass;
            global.residual_l1 += s.residual_l1;
            global.min_score = global.min_score.min(s.min_score);
            global.metrics.active_vertices = global
                .metrics
                .active_vertices
                .checked_add(s.metrics.active_vertices)
                .ok_or("active count overflow")?;
            global.metrics.active_edges = global
                .metrics
                .active_edges
                .checked_add(s.metrics.active_edges)
                .ok_or("edge count overflow")?;
            global.metrics.reactivated_vertices = global
                .metrics
                .reactivated_vertices
                .checked_add(s.metrics.reactivated_vertices)
                .ok_or("reactivation count overflow")?;
        }
        if global.vertices != self.operation.vertices
            || !global.mass.is_finite()
            || global.mass <= 0.0
            || !global.residual_l1.is_finite()
            || !global.min_score.is_finite()
            || global.min_score < 0.0
        {
            return Err("invalid global residual statistics or cardinality".into());
        }
        Ok(global)
    }
    pub(super) fn decision(&self, global: &Scalars) -> Result<DeltaMode> {
        match self.completed {
            DeltaMode::Initial => Ok(DeltaMode::Certify),
            DeltaMode::Done => Ok(DeltaMode::Done),
            DeltaMode::Certify if global.residual_l1 <= self.options.tolerance => {
                Ok(DeltaMode::Done)
            }
            DeltaMode::Certify if self.pushes >= self.options.max_pushes => {
                Err("residual PageRank did not converge at the push cap".into())
            }
            DeltaMode::Certify => Ok(DeltaMode::Push),
            DeltaMode::Push
                if self.pushes >= self.options.max_pushes
                    || 2.0 * global.residual_l1 / global.mass <= self.options.tolerance =>
            {
                Ok(DeltaMode::Certify)
            }
            DeltaMode::Push => Ok(DeltaMode::Push),
        }
    }
    pub(super) fn convergence(&self, global: &Scalars) -> Result<Convergence> {
        let bound = global.residual_l1 / (1.0 - self.options.damping);
        if !bound.is_finite() {
            return Err("stationary error bound overflow".into());
        }
        Ok(Convergence {
            residual_l1: global.residual_l1,
            stationary_error_bound: bound,
            mass: global.mass,
            pushes: self.pushes,
            certificate_passes: self.certificates,
        })
    }
}

//! Validated CSR ownership boundary before dense residual state allocation.
use super::*;

/// Owns the admitted CSR independently of the borrowed input arrays.
#[derive(Debug)]
pub struct DeltaInitialization {
    pub(super) operation: Operation,
    pub(super) partition: usize,
    pub(super) options: DeltaOptions,
    // Storage and admission must drop before the host memory lease.
    pub(super) adjacency: Arc<Adjacency>,
    pub(super) resources: Resources,
}
impl DeltaInitialization {
    /// Consume this preparation after releasing temporary input storage.
    pub fn finish(self) -> Result<DeltaPartition> {
        let Self {
            operation,
            partition,
            options,
            resources,
            adjacency,
        } = self;
        let mut values = Values::allocate(adjacency.vertices.len(), &resources)?;
        values.scores.fill(1.0 / operation.vertices as f64);
        let statistics = StatisticsInbox::new(operation.partitions, &resources)?;
        Ok(DeltaPartition {
            operation,
            partition,
            adjacency,
            resources,
            options,
            values: Arc::new(values),
            next_phase: 0,
            completed: DeltaMode::Initial,
            pushes: 0,
            certificates: 0,
            metrics: Metrics::default(),
            state: State::Statistics(statistics),
            terminal: None,
        })
    }
}

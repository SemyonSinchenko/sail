//! Validated CSR ownership boundary before root and protocol-state allocation.
use super::*;

/// Owns the admitted CSR independently of the borrowed input arrays.
#[derive(Debug)]
pub struct WccInitialization {
    pub(super) operation: Operation,
    pub(super) partition: usize,
    pub(super) worker_id: u64,
    pub(super) options: WccOptions,
    // Storage and admission must drop before the host memory lease.
    pub(super) adjacency: Arc<Adjacency>,
    pub(super) resources: Resources,
}
impl WccInitialization {
    /// Consume this preparation after releasing temporary input storage.
    pub fn finish(self) -> Result<WccPartition> {
        let Self {
            operation,
            partition,
            worker_id,
            options,
            resources,
            adjacency,
        } = self;
        let origin = WccOrigin {
            worker_id,
            adjacency_id: adjacency.identity,
        };
        let admission = resources
            .execution
            .reserve(
                operation.partitions
                    * (size_of::<Option<WccOrigin>>() + size_of::<Option<(u64, u64)>>())
                    + 256,
            )
            .map_err(|e| e.to_string())?;
        let mut origins = filled(operation.partitions, None)?;
        origins[partition] = Some(origin);
        let shapes = filled(operation.partitions, None)?;
        let values = Arc::new(Values::new(&adjacency.vertices, &resources)?);
        let state = State::Statistics(StatisticsInbox::new(operation.partitions, &resources)?);
        Ok(WccPartition {
            operation,
            partition,
            origin,
            options,
            adjacency,
            incoming: None,
            resources,
            values,
            candidates: None,
            routed: None,
            origins,
            shapes,
            _origin_admission: admission,
            state,
            next_phase: 0,
            rounds: 0,
            completed: WccMode::Topology,
            changed: 0,
            crossing: 0,
            work: WccWork::default(),
            terminal: None,
        })
    }
}

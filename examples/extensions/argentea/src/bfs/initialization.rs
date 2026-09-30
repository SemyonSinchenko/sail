//! Validated CSR ownership boundary before per-vertex state allocation.
use super::*;

/// Prepared partition that owns its admitted CSR but has no labels or frontier.
/// Dropping it abandons initialization and releases the retained CSR.
#[derive(Debug)]
pub struct BfsInitialization {
    pub(super) operation: Operation,
    pub(super) partition: usize,
    pub(super) worker_id: u64,
    pub(super) options: BfsOptions,
    // CSR storage and its admission must drop before the host memory lease.
    pub(super) adjacency: Arc<Adjacency>,
    pub(super) resources: Resources,
}

impl BfsInitialization {
    /// Allocate initial state after the caller has released temporary inputs.
    /// This consumes the preparation; it cannot publish two states for one CSR.
    pub fn finish(self) -> Result<BfsPartition> {
        let Self {
            operation,
            partition,
            worker_id,
            options,
            resources,
            adjacency,
        } = self;
        let origin = BfsOrigin {
            worker_id,
            adjacency_id: adjacency.identity,
        };
        let origin_admission = resources
            .execution
            .reserve(
                operation.partitions
                    * (size_of::<Option<BfsOrigin>>() + size_of::<Option<(u64, u64, u64)>>())
                    + 128,
            )
            .map_err(|e| e.to_string())?;
        let mut origins = filled(operation.partitions, None)?;
        let shapes = filled(operation.partitions, None)?;
        origins[partition] = Some(origin);
        let mut values = Values::new(adjacency.vertices.len(), &resources)?;
        values.remaining = adjacency.targets.len() as u64;
        if let Ok(i) = adjacency.vertices.binary_search(&options.source) {
            values.depths[i] = Some(0);
            values.parents[i] = Some(options.source);
            values.frontier.push(i);
            values.reached = 1;
            values.remaining -= (adjacency.offsets[i + 1] - adjacency.offsets[i]) as u64;
        }
        let state = State::Statistics(StatisticsInbox::new(operation.partitions, &resources)?);
        Ok(BfsPartition {
            operation,
            partition,
            origin,
            options,
            adjacency,
            incoming: None,
            resources,
            values: Arc::new(values),
            origins,
            shapes,
            _origin_admission: origin_admission,
            state,
            next_phase: 0,
            levels: 0,
            completed: BfsMode::Topology,
            work: BfsWork::default(),
            terminal: None,
        })
    }
}

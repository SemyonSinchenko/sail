//! Validated CSR ownership boundary before per-vertex state allocation.
use super::*;

/// Prepared partition that owns its admitted CSR but has no labels or frontier.
/// Dropping it abandons initialization and releases the retained CSR.
#[derive(Debug)]
pub struct SsspInitialization {
    pub(super) operation: Operation,
    pub(super) partition: usize,
    pub(super) worker_id: u64,
    pub(super) options: SsspOptions,
    // CSR storage and its admission must drop before the host memory lease.
    pub(super) adjacency: Arc<WeightedAdjacency>,
    pub(super) resources: Resources,
}

impl SsspInitialization {
    /// Allocate initial state after the caller has released temporary inputs.
    /// This consumes the preparation; it cannot publish two states for one CSR.
    pub fn finish(self) -> Result<SsspPartition> {
        let Self {
            operation,
            partition,
            worker_id,
            options,
            resources,
            adjacency,
        } = self;
        let origin = SsspOrigin {
            worker_id,
            adjacency_id: adjacency.identity(),
        };
        let admission = resources
            .execution
            .reserve(
                operation.partitions
                    * (size_of::<Option<SsspOrigin>>() + size_of::<Option<(u64, u64, u64)>>())
                    + 128,
            )
            .map_err(|e| e.to_string())?;
        let mut origins = filled(operation.partitions, None)?;
        origins[partition] = Some(origin);
        let shapes = filled(operation.partitions, None)?;
        let mut values = Values::new(adjacency.vertices().len(), &resources)?;
        if let Ok(i) = adjacency.vertices().binary_search(&options.source) {
            values.labels[i] = Some(SsspLabel::source(options.source));
            values.active.push(i);
            values.reached = 1;
            values.reachable_edges = adjacency.outgoing_at(i).len() as u64;
        }
        let state = State::Statistics(StatisticsInbox::new(operation.partitions, &resources)?);
        Ok(SsspPartition {
            operation,
            partition,
            origin,
            options,
            adjacency,
            resources,
            values: Arc::new(values),
            origins,
            shapes,
            _origin_admission: admission,
            state,
            next_phase: 0,
            rounds: 0,
            completed: SsspMode::Topology,
            work: SsspWork::default(),
            terminal: None,
        })
    }
}

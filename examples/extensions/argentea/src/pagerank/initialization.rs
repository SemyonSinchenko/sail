//! Validated CSR ownership boundary before per-vertex rank allocation.
use super::*;

/// Owns the admitted CSR independently of the borrowed input arrays.
#[derive(Debug)]
pub struct PageRankInitialization {
    pub(super) operation: Operation,
    pub(super) partition: usize,
    // Storage and admission must drop before the host memory lease.
    pub(super) adjacency: Arc<Adjacency>,
    pub(super) resources: Resources,
}
impl PageRankInitialization {
    /// Consume this preparation after releasing temporary input storage.
    pub fn finish(self) -> Result<PageRankPartition> {
        let Self {
            operation,
            partition,
            resources,
            adjacency,
        } = self;
        let n = adjacency.vertices.len();
        let ranks_admission = resources
            .execution
            .reserve(n * 8 + 128)
            .map_err(|e| e.to_string())?;
        let mut ranks = reserve_vec(n)?;
        ranks.resize(n, 1.0 / operation.vertices as f64);
        Ok(PageRankPartition {
            operation,
            partition,
            adjacency,
            ranks: Arc::new(Ranks {
                values: ranks,
                _admission: ranks_admission,
            }),
            next_round: 0,
            state: State::Ready,
            resources,
        })
    }
}

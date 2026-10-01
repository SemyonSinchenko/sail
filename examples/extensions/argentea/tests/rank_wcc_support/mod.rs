#![allow(dead_code)]
#[path = "../delta_support/mod.rs"]
mod shared;
use grust_procedures::MemoryAccount;
use sail_argentea_core::*;
pub use shared::{operation, resources};

pub const LIMIT: usize = 256 << 20;
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    PageRank,
    Residual,
    Reference,
    Star,
}
pub const KINDS: [Kind; 4] = [Kind::PageRank, Kind::Residual, Kind::Reference, Kind::Star];
impl Kind {
    pub fn options(self) -> WccOptions {
        WccOptions {
            algorithm: if matches!(self, Self::Star) {
                WccAlgorithm::StarContraction
            } else {
                WccAlgorithm::Reference
            },
            seed: 42,
            max_rounds: 128,
        }
    }
}
pub struct Raw {
    pub ids: Vec<i64>,
    pub edges: Vec<(i64, i64)>,
    pub admission: MemoryAccount,
}
impl Raw {
    pub fn new(n: usize, degree: usize, resources: &Resources) -> Self {
        let mut admission = resources.execution.memory_account();
        admission.charge(n * 8 + n * degree * 16).unwrap();
        Self {
            ids: (0..n as i64).rev().map(|i| i * 3).collect(),
            edges: (0..n * degree).map(|i| ((i % n) as i64 * 3, 0)).collect(),
            admission,
        }
    }
}
pub enum Prepared {
    PageRank(PageRankInitialization),
    Residual(DeltaInitialization),
    Wcc(WccInitialization),
}
pub enum Partition {
    PageRank(PageRankPartition),
    Residual(DeltaPartition),
    Wcc(WccPartition),
}
impl Prepared {
    pub fn new(kind: Kind, op: Operation, raw: &Raw, resources: Resources) -> Result<Self> {
        match kind {
            Kind::PageRank => PageRankPartition::prepare(op, 0, &raw.ids, &raw.edges, resources)
                .map(Self::PageRank),
            Kind::Residual => {
                DeltaPartition::prepare(op, 0, &raw.ids, &raw.edges, shared::options(), resources)
                    .map(Self::Residual)
            }
            _ => WccPartition::prepare(op, 0, 7, &raw.ids, &raw.edges, kind.options(), resources)
                .map(Self::Wcc),
        }
    }
    pub fn finish(self) -> Result<Partition> {
        match self {
            Self::PageRank(p) => p.finish().map(Partition::PageRank),
            Self::Residual(p) => p.finish().map(Partition::Residual),
            Self::Wcc(p) => p.finish().map(Partition::Wcc),
        }
    }
}
impl Partition {
    pub fn build(kind: Kind, op: Operation, raw: &Raw, resources: Resources) -> Result<Self> {
        match kind {
            Kind::PageRank => {
                PageRankPartition::build(op, 0, &raw.ids, &raw.edges, resources).map(Self::PageRank)
            }
            Kind::Residual => {
                DeltaPartition::build(op, 0, &raw.ids, &raw.edges, shared::options(), resources)
                    .map(Self::Residual)
            }
            _ => WccPartition::build(op, 0, 7, &raw.ids, &raw.edges, kind.options(), resources)
                .map(Self::Wcc),
        }
    }
    pub fn check(&self, n: usize) {
        match self {
            Self::PageRank(p) => {
                assert_eq!(p.next_round(), 0);
                assert_eq!(p.receiving_round(), None);
                assert_eq!(p.ranks().count(), n);
                for (i, (id, rank)) in p.ranks().enumerate() {
                    assert_eq!(id, i as i64 * 3);
                    assert_eq!(rank.to_bits(), (1.0 / n as f64).to_bits());
                }
            }
            Self::Residual(p) => {
                assert_eq!(p.collecting_phase(), Some(0));
                assert_eq!(p.completed_mode(), DeltaMode::Initial);
                assert_eq!(p.state_rows().count(), n);
                for (i, (id, score, residual)) in p.state_rows().enumerate() {
                    assert_eq!(id, i as i64 * 3);
                    assert_eq!(score.to_bits(), (1.0 / n as f64).to_bits());
                    assert_eq!(residual.to_bits(), 0.0f64.to_bits());
                }
            }
            Self::Wcc(p) => {
                assert_eq!(p.collecting_phase(), Some(0));
                assert_eq!(p.completed_mode(), WccMode::Topology);
                assert_eq!(p.rounds(), 0);
                assert_eq!(p.state_rows().count(), n);
                for (i, row) in p.state_rows().enumerate() {
                    assert_eq!(row.id, i as i64 * 3);
                    assert_eq!(row.component, row.id);
                }
            }
        }
    }
}

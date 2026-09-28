//! Weighted storage and exact candidate order for distributed SSSP.
//! The worker protocol is implemented separately from these admitted primitives.
mod adjacency;
use crate::Result;
pub use adjacency::WeightedAdjacency;

/// Finite nonnegative path distance, minimum hops, then numeric predecessor.
/// Fields are private so NaN cannot enter the candidate order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspLabel {
    distance: f64,
    hops: u64,
    parent: i64,
}
impl SsspLabel {
    pub fn source(id: i64) -> Self {
        Self {
            distance: 0.0,
            hops: 0,
            parent: id,
        }
    }
    pub fn from_parts(distance: f64, hops: u64, parent: i64) -> Result<Self> {
        if !distance.is_finite() || distance < 0.0 {
            return Err("SSSP distance must be finite and nonnegative".into());
        }
        Ok(Self {
            distance: if distance == 0.0 { 0.0 } else { distance },
            hops,
            parent,
        })
    }
    /// Overflow refuses the operation even if another path dominates this arc,
    /// matching the existing Banda all-edge delta-star contract.
    pub fn extend(self, source: i64, weight: f64) -> Result<Self> {
        if !weight.is_finite() || weight < 0.0 {
            return Err("SSSP weight must be finite and nonnegative".into());
        }
        let distance = self.distance + weight;
        if !distance.is_finite() {
            return Err("SSSP distance overflow".into());
        }
        let hops = self.hops.checked_add(1).ok_or("SSSP hop overflow")?;
        Self::from_parts(distance, hops, source)
    }
    pub fn distance(self) -> f64 {
        self.distance
    }
    pub fn hops(self) -> u64 {
        self.hops
    }
    pub fn parent(self) -> i64 {
        self.parent
    }
    pub fn precedes(self, other: Self) -> bool {
        (self.distance, self.hops, self.parent) < (other.distance, other.hops, other.parent)
    }
    /// A parent-only improvement changes this row's witness, but cannot change
    /// an outgoing candidate: that candidate names this vertex as predecessor.
    pub fn changes_outgoing(self, previous: Self) -> bool {
        (self.distance, self.hops) != (previous.distance, previous.hops)
    }
    pub fn bucket(self, delta: f64) -> Result<f64> {
        if !delta.is_finite() || delta <= 0.0 {
            return Err("SSSP delta must be finite and positive".into());
        }
        let bucket = (self.distance / delta).floor();
        if !bucket.is_finite() {
            return Err("SSSP bucket overflow; choose larger delta".into());
        }
        Ok(bucket)
    }
}

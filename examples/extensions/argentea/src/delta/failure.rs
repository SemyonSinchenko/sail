//! Typed non-convergence evidence; cancellation is never classified as a cap.
use super::{statistics::Scalars, *};
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeltaCapFailure {
    pub pushes: u64,
    pub max_pushes: u64,
    pub certificate_passes: u64,
    pub residual_l1: f64,
    pub tolerance: f64,
}
impl std::fmt::Display for DeltaCapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "residual PageRank did not converge at the push cap")
    }
}
impl DeltaPartition {
    /// Inspect the same typed cap predicate used by emission and sealing after
    /// a validated, complete global statistics barrier. This allows an adapter
    /// to emit authoritative failure evidence before its error guard cancels
    /// peers. It does not infer a cause from an error string or partial reports.
    pub fn cap_failure(&mut self, phase: &Round) -> Result<Option<DeltaCapFailure>> {
        self.check_phase(phase)?;
        let global = self.global_statistics()?;
        Ok(self.cap_failure_for(&global))
    }
    pub(super) fn cap_failure_for(&self, global: &Scalars) -> Option<DeltaCapFailure> {
        (self.completed == DeltaMode::Certify
            && global.residual_l1 > self.options.tolerance
            && self.pushes >= self.options.max_pushes)
            .then_some(DeltaCapFailure {
                pushes: self.pushes,
                max_pushes: self.options.max_pushes,
                certificate_passes: self.certificates,
                residual_l1: global.residual_l1,
                tolerance: self.options.tolerance,
            })
    }
}

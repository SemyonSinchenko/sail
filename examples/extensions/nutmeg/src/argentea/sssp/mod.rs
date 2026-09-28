//! Weighted SSSP protocol v5, with exact setup and complete producer barriers.
mod input;
mod output;
pub(super) mod plan;
pub(super) mod request;
mod result_batch;
pub(super) mod state;
mod wire;

#[cfg(test)]
mod tests;

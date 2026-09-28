//! Integer WCC protocol v4, with exact setup and complete component barriers.
mod input;
mod output;
pub(super) mod plan;
pub(super) mod request;
mod result_batch;
pub(super) mod state;
#[cfg(test)]
mod tests;
mod wire;

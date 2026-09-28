//! Residual protocol v2; the v1 reference parser and positive checks stay strict.
mod input;
mod output;
pub(super) mod plan;
pub(super) mod request;
pub(super) mod state;
#[cfg(test)]
mod tests;
mod wire;

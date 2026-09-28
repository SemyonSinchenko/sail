//! Compile the actual generic worker boundary without Sail's unrelated workspace
//! dependencies. This is a source probe, not the full Sail integration gate.
#[path = "../../../../../crates/sail-common-datafusion/src/extension.rs"]
pub mod extension;
#[path = "../../../../../crates/sail-common-datafusion/src/worker_extension.rs"]
pub mod worker_extension;

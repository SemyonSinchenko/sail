//! Source-included scheduler probes avoid a multi-gigabyte DataFusion build.
//! Fail if the source layout changes; never silently substitute a copied model.
use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../../..");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let scheduler = root.join("crates/sail-execution/src/driver/job_scheduler/core.rs");
    let source = fs::read_to_string(&scheduler).unwrap();
    let group = source
        .split_once("struct StageGroup {")
        .expect("StageGroup source missing")
        .1;
    let group = group
        .split_once("\n#[cfg(test)]")
        .expect("StageGroup boundary changed")
        .0;
    fs::write(
        out.join("stage_group.rs"),
        format!("struct StageGroup {{{group}"),
    )
    .unwrap();
    println!("cargo:rerun-if-changed={}", scheduler.display());
    let graph = root.join("crates/sail-execution/src/job_graph/mod.rs");
    let source = fs::read_to_string(&graph).unwrap();
    let variants = source
        .split_once("pub enum TaskPlacement {")
        .expect("TaskPlacement source missing")
        .1
        .split_once("\n}")
        .expect("TaskPlacement boundary changed")
        .0;
    fs::write(out.join("placement.rs"), format!("#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\npub enum TaskPlacement {{{variants}\n}}")).unwrap();
    println!("cargo:rerun-if-changed={}", graph.display());
}

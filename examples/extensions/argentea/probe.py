#!/usr/bin/env python3
"""Reproduce Argentea core and actual-source scheduler probes with evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
HOST_FILES = [
    "crates/sail-execution/src/id.rs",
    "crates/sail-execution/src/task/scheduling.rs",
    "crates/sail-execution/src/job_graph/mod.rs",
    "crates/sail-execution/src/driver/job_scheduler/core.rs",
    "crates/sail-execution/src/driver/task_assigner/core.rs",
    "crates/sail-execution/src/driver/task_assigner/mod.rs",
    "crates/sail-execution/src/driver/task_assigner/options.rs",
    "crates/sail-execution/src/driver/task_assigner/state.rs",
    "crates/sail-native-resource-ffi/src/lib.rs",
    "crates/sail-native-resource-ffi/Cargo.toml",
]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()


def sources():
    files = [REPO / name for name in HOST_FILES]
    files.extend(p for p in HERE.rglob("*") if p.is_file() and (
        p.suffix == ".rs" or p.name in {"Cargo.toml", "Cargo.lock", "probe.py"}
    ))
    return {str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(files)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--target-dir", required=True, type=Path)
    parser.add_argument("--allow-working-tree", action="store_true",
                        help="development check only; receipt is not a detached commit gate")
    args = parser.parse_args()
    branch = subprocess.run(["git", "symbolic-ref", "-q", "HEAD"], cwd=REPO,
                            capture_output=True, text=True)
    dirty = git("status", "--porcelain")
    if (branch.returncode == 0 or dirty) and not args.allow_working_tree:
        parser.error("run a detached clean worktree, or explicitly label a development check")
    output = args.output.resolve()
    target = args.target_dir.resolve()
    if output.is_relative_to(REPO) or target.is_relative_to(REPO):
        parser.error("use evidence and target directories outside the checkout")
    output.mkdir(parents=True, exist_ok=False)
    target.mkdir(parents=True, exist_ok=True)
    receipt = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "native partition core and source-included scheduler probes; no Sail runtime or transport",
        "source_sha": git("rev-parse", "HEAD"),
        "detached_clean_gate": branch.returncode != 0 and not dirty,
        "source_files": sources(),
        "host": os.uname().nodename,
        "machine": os.uname().machine,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "commands": [],
    }
    env = dict(os.environ, CARGO_INCREMENTAL="0", CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="4")
    prefix = ["cargo"]
    manifest = ["--manifest-path", str(HERE / "Cargo.toml")]
    checks = [
        ("format", prefix + ["fmt"] + manifest + ["--", "--check"]),
        ("clippy", prefix + ["clippy"] + manifest + ["--locked", "--all-targets", "--", "-D", "warnings"]),
        ("release", prefix + ["test"] + manifest + ["--locked", "--release", "--", "--nocapture"]),
    ]
    passed = True
    for name, command in checks:
        with (output / f"{name}.log").open("w") as log:
            status = subprocess.run(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        receipt["commands"].append({"name": name, "command": command, "exit_code": status})
        print(f"{name}: {'PASS' if status == 0 else 'FAIL'}", flush=True)
        passed = passed and status == 0
        if status:
            break
    receipt["unchanged"] = receipt["source_files"] == sources() and receipt["source_sha"] == git("rev-parse", "HEAD")
    receipt["finished_utc"] = datetime.now(timezone.utc).isoformat()
    receipt["outcome"] = "passed" if passed and receipt["unchanged"] else "failed"
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"ARGENTEA_CORE_PROBE {receipt['outcome'].upper()} {receipt['source_sha']} detached_clean={receipt['detached_clean_gate']}")
    raise SystemExit(0 if receipt["outcome"] == "passed" else 1)


if __name__ == "__main__":
    main()

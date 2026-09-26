#!/usr/bin/env python3
"""Record immutable identities after the verification script has passed."""
import argparse
from datetime import datetime, timezone
import hashlib
from importlib import metadata
import json
from pathlib import Path
import platform
import re
import runpy
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--target", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[3]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command(*argv):
    return subprocess.check_output(argv, cwd=repo, text=True).strip()


def native_files(distribution):
    package = metadata.distribution(distribution)
    return {str(path): sha256(Path(package.locate_file(path)))
            for path in package.files or [] if str(path).endswith((".so", ".dylib", ".pyd"))}


identity = runpy.run_path(str(repo / "crates/sail-session/src/extensions/package_identity.py"))["identity"]
identities = {}
for entry in metadata.entry_points(group="pysail.extensions"):
    factory = entry.load()
    if callable(factory):
        factory = factory()
    manifest = factory.manifest()
    identities[manifest["name"]] = identity(entry, manifest)
process_logs = list((args.target / "pytest-process-cluster").rglob("server.log"))
processes = []
for path in process_logs:
    content = path.read_text()
    for worker, driver in re.findall(r"extension process worker \d+: pid=Some\((\d+)\), driver_pid=(\d+)", content):
        processes.append({"log": str(path.relative_to(args.target)), "worker_pid": int(worker), "driver_pid": int(driver)})
if not processes or not all(row["worker_pid"] != row["driver_pid"] for row in processes):
    raise RuntimeError("missing separate-process worker evidence")

receipt = {
    "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "host": platform.node(),
    "platform": platform.platform(),
    "sail_commit": command("git", "rev-parse", "HEAD"),
    "rust": command("rustc", "--version"),
    "python": sys.version,
    "modes": ["local", "local-cluster", "process-cluster"],
    "native_plugins": "separate installed wheels; driver-resident Nutmeg, independently loaded worker Sedona",
    "package_identities": identities,
    "worker_processes": processes,
    "extension_sources": {
        "sedonadb_commit": command("git", "-C", "examples/extensions/sedona/.deps/sedona-db", "rev-parse", "HEAD"),
        "sedonadb_patch_sha256": sha256(repo / "examples/extensions/sedona/patches/datafusion-55.patch"),
        "nutmeg_upstream_commit": "f267b03659dd536981f98420944f911b667632b7",
        "nutmeg_local_changes": "vendored source belongs to sail_commit",
    },
    "input_partition_fixture": {"nodes": 4, "edges": 4, "rows_each": 3, "includes_empty_partition": True},
    "host_binary_sha256": sha256(args.target / "host/debug/sail"),
    "source_locks": {str(path.relative_to(repo)): sha256(path) for path in
                     [repo / "Cargo.lock", repo / "examples/extensions/sedona/Cargo.lock",
                      repo / "examples/extensions/nutmeg/Cargo.lock",
                      repo / "examples/extensions/vendor/nutmeg-graph/Cargo.lock",
                      repo / "examples/extensions/requirements.lock"]},
    "wheels": {path.name: sha256(path) for path in sorted((args.target / "wheels").glob("*.whl"))},
    "python_distributions": {name: metadata.version(name) for name in
                             ["pyspark", "pyarrow", "apache-sedona", "sail-sedona-extension", "sail-nutmeg"]},
    "installed_native_files": {name: native_files(name) for name in
                               ["sail-sedona-extension", "sail-nutmeg"]},
    "cancellation_stress": json.loads((args.target / "cancellation-stress.json").read_text()),
    "outcome": "passed local and distributed native extension gates",
    "boundaries": {
        "distributed_extensions": "local-cluster actors and separate Sail worker processes on one host; multi-host/Kubernetes deployment not qualified",
        "sedona_indexed_join": "not implemented; baseline spatial SQL correctness tested",
        "geometry_client_udt": "not qualified; results collected as scalars/text",
        "kernel_cancellation": "blocked-output native FFI lifetime tested; fresh projection build not interruptible",
        "mutation_replay": "driver-native regions have one attempt; acknowledgement loss is indeterminate; a new request is a new operation",
    },
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(receipt, indent=2) + "\n")
print(f"Evidence: {args.output}")

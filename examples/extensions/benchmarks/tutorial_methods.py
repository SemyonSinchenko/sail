#!/usr/bin/env python3
"""Exercise every graph method with existing validation and timing machinery.

This is a functional tutorial, not a benchmark matrix: each case gets a fresh
Sail server, but the outer host/container is shared across cases. For publishable
comparisons use run_matrix.py, which creates a fresh container per trial.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

from graph_fixtures import prepare


ENGINES = ("pecan", "nutmeg-native", "nutmeg-datafusion")
ALGORITHMS = ("pagerank", "wcc")
VARIANTS = ("reference", "optimized")


def utc():
    return datetime.now(timezone.utc).isoformat()


def selection(value, choices):
    return choices if value == "all" else (value,)


def display(value, *, mib=False):
    if value is None:
        return "unavailable"
    return f"{value / 1024**2 if mib else value:.3f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sail-binary", type=Path, required=True)
    parser.add_argument("--runtime-source-sha", required=True)
    parser.add_argument("--native-source-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("local", "process-cluster"), default="local")
    parser.add_argument("--engine", choices=("all", *ENGINES), default="all")
    parser.add_argument("--algorithm", choices=("all", *ALGORITHMS), default="all")
    parser.add_argument("--variant", choices=("all", *VARIANTS), default="all",
                        help="optimized is the retained CLI name for advanced methods")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="development smoke only; records the dirty source in each receipt")
    args = parser.parse_args()
    args.sail_binary = args.sail_binary.resolve()
    if not args.sail_binary.is_file():
        parser.error("--sail-binary must name an existing executable")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    dataset = args.output / "dataset"
    manifest = prepare(dataset, vertices=128, degree=4, block_size=32)
    private_container = Path("/.dockerenv").exists() and Path("/proc/stat").exists()
    summary = {
        "started_utc": utc(),
        "purpose": "functional tutorial; not isolated per-cell benchmark measurements",
        "mode": args.mode,
        "dataset_counts": manifest["counts"],
        "memory_scope": (
            "all visible container processes; private PID namespace required; "
            "outer container reused across cases, so cgroup peaks are cumulative"
            if private_container else
            "process memory is unavailable in this tutorial table outside a private Linux container"
        ),
        "cells": [],
    }
    print(json.dumps({"fixture": summary["dataset_counts"], "purpose": summary["purpose"]}), flush=True)
    print("engine | algorithm | flavor | outcome | call seconds | sampled execution RSS MiB | PSS MiB", flush=True)
    failed = False
    script = Path(__file__).with_name("graph_cell.py")
    for engine in selection(args.engine, ENGINES):
        for algorithm in selection(args.algorithm, ALGORITHMS):
            for variant in selection(args.variant, VARIANTS):
                name = f"{engine}-{algorithm}-{variant}"
                output = args.output / name
                command = [
                    sys.executable, str(script), "--sail-binary", str(args.sail_binary),
                    "--runtime-source-sha", args.runtime_source_sha,
                    "--native-source-sha", args.native_source_sha,
                    "--dataset", str(dataset), "--output", str(output),
                    "--engine", engine, "--algorithm", algorithm, "--variant", variant,
                    "--mode", args.mode, "--partitions", "4", "--threads", "4",
                    "--worker-task-slots", "32", "--sail-pool-bytes", str(16 * 1024**3),
                    "--native-quota", str(8 * 1024**3),
                    "--max-iterations", "1000", "--tolerance", "1e-8", "--seed", "42",
                    "--allow-unisolated",
                ]
                if args.allow_dirty:
                    command.append("--allow-dirty")
                with (args.output / f"{name}.log").open("w") as log:
                    completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
                receipt_path = output / "receipt.json"
                receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
                outcome = receipt.get("outcome", "incomplete_record")
                if completed.returncode != 0 or outcome != "passed":
                    failed = True
                peaks = receipt.get("memory", {}).get("phase_peaks", {}).get("execute", {})
                rss = peaks.get("rss_bytes") if private_container else None
                pss = peaks.get("pss_bytes") if private_container else None
                row = {
                    "engine": engine, "algorithm": algorithm, "variant": variant,
                    "outcome": outcome, "returncode": completed.returncode,
                    "end_to_end_seconds": receipt.get("end_to_end_seconds"),
                    "sampled_execution_rss_bytes": rss, "sampled_execution_pss_bytes": pss,
                    "receipt": str(receipt_path.relative_to(args.output)),
                    "log": f"{name}.log", "command": command,
                }
                summary["cells"].append(row)
                summary["updated_utc"] = utc()
                (args.output / "tutorial-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                flavor = "advanced" if variant == "optimized" else "reference"
                print(f"{engine} | {algorithm} | {flavor} | {outcome} | "
                      f"{display(row['end_to_end_seconds'])} | {display(rss, mib=True)} | {display(pss, mib=True)}",
                      flush=True)
    summary.update(finished_utc=utc(), outcome="failed" if failed else "passed")
    (args.output / "tutorial-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Receipts, result Parquet, server logs and summary: {args.output}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

# Run the bounded Argentea PageRank prototype

Argentea runs Nutmeg native graph partitions inside Sail workers. The example
client builds `init -> round -> ... -> result` into one query. Sail supplies
planning, partition routing, shuffle and task placement; each worker retains
its native adjacency between rounds. This is a fixed-round PageRank prototype,
not a converged PageRank implementation or a performance result.

The Python client and qualification checks are implemented. A passing Python
unit suite is not a runtime verdict: use the exact Sail and Nutmeg artifacts
from a successful combined build, and retain the qualification receipt.
The old `sail-extensions-1` tag predates this worker implementation. Use the
reviewed commit on `work/extensions-traversal-bench` for all commands below.

## 1. Build matching artifacts

The [source tutorial](../TUTORIAL.md#3-prepare-a-build-machine) lists system
prerequisites, including Python 3.12, Rust, protobuf and native libraries. From
the reviewed branch checkout, create the environment and build the installed
wheels and Sail together:

```bash
git rev-parse HEAD
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
export CARGO_BUILD_JOBS=4
export CARGO_INCREMENTAL=0
bash examples/extensions/scripts/build.sh
.venv/bin/python - <<'PY'
from importlib.metadata import entry_points
for entry in entry_points(group="pysail.extensions"):
    if entry.name == "argentea":
        print(entry.load()().manifest())
PY
```

This builds both existing extension wheels and installs Pecan, whose owned
snapshot/result lifecycle the example client reuses. Argentea's server entry
point is shipped inside the Nutmeg wheel. It requires the new Sail worker-role
loader; an old Sail host rejects this manifest. Install real wheels rather than
editable packages, because the embedded interpreter does not process `.pth`
files. Pure Python client tests can be run separately:

```bash
.venv/bin/python -m pytest -q examples/extensions/argentea/python
```

## 2. Run one host with two worker processes

The qualifier starts a fresh driver and two worker processes, invokes the tiny
PageRank fixture, shuts them down and audits their logs:

```bash
candidate=$(git rev-parse HEAD)
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python \
  examples/extensions/argentea/python/qualify.py \
  --mode process-cluster \
  --sail-binary target/extensions-poc/host/debug/sail \
  --runtime-source-sha "$candidate" --native-source-sha "$candidate" \
  --iterations 2 --partitions 3 \
  --output /tmp/argentea-process-evidence
```

The output directory must not exist. The checkout must be clean; explicitly
adding `--allow-working-tree` labels the result as a development check. The
source arguments identify the artifact builds and must be taken from their
build receipts. The qualifier also records binary and installed-package hashes;
declaring a source SHA is not, by itself, proof that those bytes were built from
it. Keep the build receipts with the result.

Defaults are 32 task slots per worker, a 2 GiB Sail memory pool and a 256 MiB
Argentea quota per worker/job/operation. The native reservation participates in
the actual worker pool. Process RSS also includes runtime and other allocations,
so this is not an RSS limit. Change the envelope explicitly with
`--worker-task-slots`, `--sail-pool-bytes`, `--native-quota` and `--threads`.

The six-vertex fixture includes negative IDs, parallel edges, a self-loop,
dangling vertices, disconnected components and an empty owner partition. A pass
requires the full rank vector to match an independent fixed-step reference,
both workers to execute native work, every owner to reuse its adjacency across
rounds, one native job, completed task receipts and native close receipts.
`receipt.json` preserves outcomes and errors; `server.log`, `exercise.json`,
`client-plan.pb` and `explain.txt` retain supporting evidence. These are
functional checks, not publishable timing or memory measurements.

To verify the explicit local-only refusal, run the same command with
`--mode local` and a new output directory. Its expected success outcome means
the worker-native relation was rejected. Ordinary local Sail queries and the
existing Banda/Grenada paths remain separate functionality.

## 3. Use the client against a running worker cluster

Configure that server with extensions enabled and the same installed Nutmeg
wheel. Provide `SAIL_GRAPH_UTILS_ROOT` for owned staging. On one host a local
file URI works; every process must be able to read it. The client uses ordinary
DataFrames and returns an owned materialized result:

```bash
PYTHONPATH=examples/extensions/argentea/python \
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python - <<'PY'
from pyspark.sql.connect.session import SparkSession
from argentea_client import Argentea

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    nodes = spark.createDataFrame([(0,), (1,), (2,), (3,)], "id long")
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], "src long, dst long")
    with Argentea(spark).pagerank(nodes, edges, iterations=4, partitions=3) as result:
        result.frame.orderBy("id").show()       # id, pagerank
        result.native_frame.show()             # also worker/CSR provenance
        assert result.converged is None
        # Persist elsewhere before leaving this context if needed:
        # result.write_parquet("s3://review-results/argentea-run-1")
finally:
    spark.stop()
PY
```

Inputs have unique non-null BIGINT IDs and existing edge endpoints by the
valid-graph contract. The client creates separately owned input snapshots using
the existing Pecan service, without graph-data validation jobs.
It assigns `owner = pmod(id, P)` and edge ownership by source. Parallel edges and
loops remain. Ranks start at `1/N`; every update redistributes dangling mass
uniformly and uses damping `1 - reset_probability` (default `0.85`). All native
updates execute in one DAG; input snapshots, required cardinality and reads of
the materialized result are separate ordinary Sail jobs. Leaving the context releases the owned
result. It is not a reusable cross-job native graph handle.

The prototype accepts 1–32 fixed rounds, 1–64 partitions and nonempty graphs.
Empty partitions participate in completion and cleanup; an entirely empty graph
is explicitly unsupported. These are bounded qualification limits. There is no
convergence claim, cross-job residency, transparent retry of mutated native
state, adaptive iteration loop or Argentea WCC path in this client.

## 4. Run across two physical hosts

Start from [two-host.example.json](../scripts/two-host.example.json). Set absolute
paths for the driver and two workers, reachable advertised IPs and optional
`ssh` for a remote target. Install exactly the same Sail binary and extension
wheel bytes on both hosts. For Capitola and Intel Morrobay, a shared x86_64 macOS
artifact can run under Rosetta on Capitola; separately built arm64/x86_64 wheels
do not satisfy this exact-package gate.

Each target must use the same clean source commit. Add an `environment_file`
path to every target for host-local JSON configuration of the shared staging
store, for example `SAIL_GRAPH_UTILS_ROOT: "s3://review-bucket/argentea"` and
the required object-store settings. The files stay on their hosts; do not put
credentials in the tracked configuration. All processes must access the same
staging objects. Private local directories on different hosts are not shared
storage. The existing supervisor admits only object-store settings and the
staging root from these files.

```bash
candidate=$(git rev-parse HEAD)
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python \
  examples/extensions/argentea/python/qualify.py \
  --mode two-host --two-host-config /absolute/path/to/argentea-two-host.json \
  --runtime-source-sha "$candidate" --native-source-sha "$candidate" \
  --iterations 2 --partitions 5 \
  --output /tmp/argentea-two-host-evidence
```

This adds supervised hostname/PID evidence to the native receipts. It requires
nonempty vertices on each physical host and at least one edge crossing between
hosts, then verifies that the supervised Sail processes have exited. The five
owners retain an empty owner for the completion check; this fixture's three-owner
layout puts every nonempty owner on one worker and cannot satisfy the stronger
two-host graph check. The process-only default remains three owners. Actual
placement is checked from validated rank rows and supervised worker/PID identities,
with per-host vertex/edge counts and crossing edges retained in `native_host_graph`.
A pair of distinct worker IDs alone is not two-host graph evidence. The driver
collects only this tiny fixture's results for validation.
Large-graph scaling, cancellation during an active round, worker loss, quota
refusal and retained Arrow-buffer release require their own gates before a
broader distributed-support claim.

For a development network that cannot reach Sail's direct LAN ports, explicit
SSH local/reverse forwards can carry the same driver and worker gRPC connections.
Record the tunnel endpoints and remote SSH hostnames with the run configuration.
The host check still uses actual supervised hostnames and PIDs, not advertised
loopback addresses. Such a run qualifies execution through SSH forwarding; it
is not evidence of direct LAN reachability or network performance.

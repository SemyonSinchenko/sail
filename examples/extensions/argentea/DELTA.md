# Bounded Argentea residual PageRank

Version 2 adds signed residual/frontier PageRank to the Argentea worker factory
in the Nutmeg wheel. It reuses native adjacency across its phases, skips edges of
inactive sources, redistributes signed dangling updates and requires a fresh
global normalized residual certificate. Version 1 fixed-round PageRank remains
available through `Argentea`; version 2 uses `ArgenteaDelta`.

The implementation has core and in-process DataFusion tests. Those tests use a
placement shim, not Sail worker processes. A baseline version-1 Sail receipt does
not qualify this version-2 path. Retain a successful matching wheel/runtime build
and worker/two-host qualification before reporting distributed support or results.

## Build and configure

Follow [PYTHON.md](PYTHON.md) to build Sail and the combined Nutmeg wheel, install
Pecan, and configure a worker-mode Sail cluster with an owned staging root. Use
one reviewed source candidate and identical wheel bytes on every participant.
The older reference-only frozen wheel has no v2 relation type. Confirm the
installed manifest before starting a run:

```bash
.venv/bin/python - <<'PY'
from importlib.metadata import entry_points
for entry in entry_points(group="pysail.extensions"):
    if entry.name == "argentea":
        manifest = entry.load()().manifest()
        urls = {item["type_url"] for item in manifest["relation_types"]}
        assert "type.googleapis.com/nutmeg.v2.ArgenteaDeltaApi" in urls
        print(manifest)
PY
```

The reference `qualify.py` currently exercises version 1. It must not be used as
a v2 success receipt merely because both types are in the same wheel. Local-only
Sail remains unsupported for this worker relation. Each worker's existing
`SAIL_ARGENTEA_MEMORY_BYTES` quota covers its v2 state, statistics, early input
queues, snapshots and output buffers through the actual Sail memory lease.

## Run a small certified fixture

Against the running cluster:

```bash
PYTHONPATH=examples/extensions/argentea/python \
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python - <<'PY'
from pyspark.sql.connect.session import SparkSession
from argentea_delta_client import ArgenteaDelta

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    vertices = spark.createDataFrame([(0,), (1,), (2,)], "id long")
    edges = spark.createDataFrame([(0, 1), (1, 1)], "src long, dst long")
    with ArgenteaDelta(spark).pagerank(
        vertices, edges, max_pushes=7, tolerance=1e-3,
        partitions=3, max_phase_budget=32,
    ) as result:
        result.frame.orderBy("id").show()
        print(result.pushes, result.certificate_passes)
        print(result.residual, result.error_bound, result.converged)
        assert result.converged and result.residual <= 1e-3
        result.native_frame.show()  # provenance and repeated global diagnostics
finally:
    spark.stop()
PY
```

Vertex 2 is dangling. Uniform initialization produces negative residual at
vertices 0 and 2, exercising both signed edge and signed dangling updates. The
looser tolerance is deliberate for this bounded fixture. IDs are non-null unique
BIGINT values and endpoints name existing vertices by contract; the client
retains isolates, parallel edges and loops without input-audit jobs. Empty owner partitions still complete every barrier.

`iterations` and `pushes` count actual residual pushes, excluding full certificate
passes. `residual` is `||T(x)-x||1` on normalized output. `error_bound` is
`residual/(1-damping)` in exact arithmetic. A successful result has
`converged=True`; an exhausted cap raises and does not return uncertified ranks.
The client reduces diagnostics from the stored result into one scalar row;
that is an additional ordinary Sail job, not another native iteration job.

To test the cap, run the same nonstationary fixture with `max_pushes=0` and
`tolerance=1e-12`. The native failure is a failed fresh certificate at the push cap. Parallel
peer cancellation may reach the client before that cause, so the functional
qualifier requires a typed `pagerank_push_cap` native failure receipt, the
complete failing global-certificate barrier, a failed query, no result or retry,
and owner cleanup. A cancellation or quota error without that cause is a
different outcome. A stationary cycle or all-dangling graph can pass
its initial certificate with zero pushes.

## What the bound means

The current adapter/client guard allows `max_phase_budget` from 4 through 32,
with `4*max_pushes+4 <= max_phase_budget`. Thus K is currently at most seven.
**This is a first-qualification guard, not a protocol or Sail hard limit.** The
core calculates larger checked phase counts, but larger plans need their own
real-worker capacity and live-buffer gates before this guard or default changes.
Seven pushes do not make general PageRank converge at `1e-8`; failure at that cap
is expected on many graphs. This is not yet a general large-graph benchmark API.

The public client registers each phase in its own UUID-named session temporary
view using `createTempView`, without replacing existing views. Each registration
contains one extension envelope and shallow references to earlier views. Sail
stores the resolved logical plan; registration performs no native graph work.
The final materialization expands those views into one native query/job. This
keeps every protobuf request within the existing nesting guard, without changing
Sail or introducing a cross-job native handle. There are at most 32 view
registrations and corresponding catalog cleanup commands; these are additional
client/server operations, not additional native iterations.

All confirmed views stay alive through terminal materialization and are dropped
before the result is returned. Cleanup attempts every confirmed alias after
errors or cancellation. A failed registration can have an uncertain server
outcome; deleting its alias could remove a preexisting view. Such an alias is
left session-owned and exposed as `uncertain_view_names` on the original error.
Failed drops are exposed as `view_cleanup_errors`; either sets
`view_cleanup_deferred=True`. Session teardown is the fallback. This is separate
from Pecan's uncertain-write cleanup of result files.

The raw `build_plan` helper still describes a nested DAG for serialization and
protocol tests. Larger K can exceed the host protobuf nesting limit when that
raw frame is submitted directly; production callers should use
`ArgenteaDelta.pagerank`. No guard is disabled or increased.

One lazy native query contains initialization, `2K+1` decide/apply pairs, and result:
`4K+4` native stages. Initialization performs no push. The initial certificate,
K pushes and up to K later certificates fit the reserved slots. Convergence
switches remaining slots to validated DONE relays, which preserve the result
without traversing adjacency. Those relays still incur scheduling and shuffle
costs. Ordinary host scan, exchange and sink stages are additional.

The current core traversal is sequential. Fixed-source-block parallel combining,
larger static budgets and their performance effects remain separate work. There
is no adaptive Sail iteration runtime, cross-job native handle or hidden restart.
A future checkpoint continuation would rebuild CSR in a fresh job and must
include that cost in comparisons.

## Reproduce the source gates

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/argentea-core-target \
  cargo test --manifest-path examples/extensions/argentea/Cargo.toml --locked --release
PYO3_PYTHON="$(uv python find 3.12)" CARGO_INCREMENTAL=0 \
CARGO_TARGET_DIR=/tmp/argentea-native-target \
  cargo test --manifest-path examples/extensions/nutmeg/Cargo.toml --locked
PYTHONPATH=examples/extensions/argentea/python:examples/extensions/graph-algorithms/src:examples/extensions/nutmeg/python \
  .venv/bin/python -m pytest -q examples/extensions/argentea/python
```

Embedded-Python tests may require `PYTHONHOME` and the platform library search
path from the same interpreter used at build time; see the source tutorial's
Python setup. Preserve exact source and artifact hashes with test logs.

Native tests cover real DataFusion range exchanges at P=2,3,11, signed updates,
empty-owner infinity, DONE relays, certification, malformed statistics, wrong
metadata, cap/quota failure, both pending barriers, origin stability and retained
Arrow slices after close. These complement the independent dense core tests;
they do not replace Sail process and two-host qualification.

The separate `python/qualify_delta.py` checks v2 answers and actual native
execution. Use a clean source checkout, fresh output directories and matching
installed artifacts. Its first candidate uses native source
`f11fe6e8a091e4be56a21712850d2a1a205c3e6c` and unchanged host source
`038c9b9597d3fcf7e0b8c30c1253d7d77563f012`. Use actual build-receipt identities
for other builds; declaring a source SHA is not proof of binary provenance.
This functional gate makes no elapsed-time or memory-performance claim.

## Live worker qualification

Set paths and identities for the artifacts being tested, then run:

```bash
export SAIL_BINARY=/absolute/path/to/sail
export RUNTIME_SOURCE_SHA=038c9b9597d3fcf7e0b8c30c1253d7d77563f012
export NATIVE_SOURCE_SHA=f11fe6e8a091e4be56a21712850d2a1a205c3e6c

.venv/bin/python examples/extensions/argentea/python/qualify_delta.py \
  --mode process-cluster --case residual \
  --sail-binary "$SAIL_BINARY" \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" \
  --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-delta-residual
```

The output directory must not exist. The qualifier starts and stops a fresh
server and two workers. A same-session readiness check waits for two distinct
running worker endpoints before the graph starts; physical placement is still
audited after native execution. Defaults are five owners, 32 asynchronous task slots
per worker, a 2 GiB Sail pool per process and a 256 MiB native allowance per
worker/job/operation. The pool reservation is not an RSS limit.

The fixture has vertices `0,1,2` and arcs `0→1, 1→1`: vertex 2 is dangling and
owners 3 and 4 are empty. Uniform initialization requires negative edge and
dangling residual pushes. The gate uses tolerance `1e-3` and at most seven
pushes. Its static DAG contains **32 native stages**: initialization, fifteen
decide/apply pairs and a result stage. Ordinary scan, shuffle and sink stages
are additional. Actual pushes and certificate passes are recorded separately;
remaining transport slots relay DONE after convergence. Seven pushes do not
promise convergence for arbitrary graphs or normal `1e-8` workloads.

A positive result requires every vertex exactly once, nonnegative normalized
ranks, independently recomputed true fixed-point L1 residual within tolerance,
agreement with the reported certificate, and a full-vector bound against an
independent converged power reference. The tiny-fixture roundoff slack is
`1e-12`. Native receipts must prove one job, all five owners, stable worker/PID
and adjacency per owner, every statistics/emit phase, a terminal certificate,
exactly one close per owner and attempt-zero successful native tasks.

### Separate boundary cases

Run each case into a new directory with the same artifact arguments:

```bash
.venv/bin/python examples/extensions/argentea/python/qualify_delta.py \
  --mode process-cluster --case stationary --sail-binary "$SAIL_BINARY" \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-delta-stationary

.venv/bin/python examples/extensions/argentea/python/qualify_delta.py \
  --mode process-cluster --case cap --sail-binary "$SAIL_BINARY" \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-delta-cap

.venv/bin/python examples/extensions/argentea/python/qualify_delta.py \
  --mode local --case residual --sail-binary "$SAIL_BINARY" \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-delta-local-refusal
```

`stationary` is the directed cycle `0→1→2→0`: it must certify with zero pushes
and one certificate pass, while still completing the planned DONE transports.
`cap` uses the nonstationary fixture with zero allowed pushes and tolerance
`1e-12`. Passing this negative gate requires a failed query and an actual native
`failure` receipt with code `pagerank_push_cap`, outcome `nonconverged`, matching
operation/phase/counters and a fresh global residual above tolerance. The audit
checks the complete certificate barrier after all owner initializations, no
result receipts, no native retry and owner cleanup. The observed RPC error is
retained separately; a generic cancellation alone cannot pass. The failed write remains session-owned until teardown. The local-only
gate must reject worker-native execution explicitly; it is not a local fallback.

### Two physical hosts

Follow the [v1 shared-storage and supervisor setup](PYTHON.md#4-run-across-two-physical-hosts).
Every participant needs identical clean source, Sail binary and v2 wheel bytes.
Private storage credentials remain in host-local configuration files.

```bash
.venv/bin/python examples/extensions/argentea/python/qualify_delta.py \
  --mode two-host --case residual --partitions 5 \
  --two-host-config /absolute/path/to/argentea-two-host.json \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-delta-two-host
```

The positive gate additionally maps validated rank rows through supervised
worker/PID identities to actual hostnames. Both physical hosts must hold
nonempty graph vertices and at least one arc must cross hosts; empty-owner
participation alone is insufficient. Any SSH forwarding must be disclosed as
such, rather than represented as a direct LAN or network-performance result.
The cap case has no rank output and therefore does not establish this nonempty
host-graph proof. Verify that the dedicated shared staging prefix is empty after
the supervised server/session shutdown; retain that storage check with the run.

`receipt.json`, `exercise.json`, the shallow terminal `client-plan.pb`, individual
`view-plan-NN.pb` registration plans and aliases, native receipts, actual stage
inventories, task statuses and server logs preserve the evidence. A top-level
pass for a negative case retains `expected-cap` or `expected-local-rejection`
in `checks.outcome`. Unexpected errors remain failures. Result context exit
removes owned Parquet, phase-view absence is checked, session teardown handles
uncertain registrations/writes, and process
group cleanup is checked separately. Retained Parquet is not a cross-job native
handle or evidence that all process memory returned to zero.

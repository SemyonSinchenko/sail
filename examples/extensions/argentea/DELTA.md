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
BIGINT values; the client validates edge endpoints and retains isolates,
parallel edges and loops. Empty owner partitions still complete every barrier.

`iterations` and `pushes` count actual residual pushes, excluding full certificate
passes. `residual` is `||T(x)-x||1` on normalized output. `error_bound` is
`residual/(1-damping)` in exact arithmetic. A successful result has
`converged=True`; an exhausted cap raises and does not return uncertified ranks.
The client reduces diagnostics from the stored result into one scalar row;
that is an additional ordinary Sail job, not another native iteration job.

To test the cap, run the same nonstationary fixture with `max_pushes=0` and
`tolerance=1e-12`. The expected native failure contains `push cap`. A different
error is a different outcome. A stationary cycle or all-dangling graph can pass
its initial certificate with zero pushes.

## What the bound means

The current adapter/client guard allows `max_phase_budget` from 4 through 32,
with `4*max_pushes+4 <= max_phase_budget`. Thus K is currently at most seven.
**This is a first-qualification guard, not a protocol or Sail hard limit.** The
core calculates larger checked phase counts, but larger plans need their own
real-worker capacity and live-buffer gates before this guard or default changes.
Seven pushes do not make general PageRank converge at `1e-8`; failure at that cap
is expected on many graphs. This is not yet a general large-graph benchmark API.

One lazy query contains initialization, `2K+1` decide/apply pairs, and result:
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

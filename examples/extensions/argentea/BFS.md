# Bounded Argentea BFS

Argentea v3 provides three unweighted BFS methods in the Nutmeg worker factory:
`reference`, `frontier`, and `direction`. All return exact integer distances and
the minimum numeric external ID among preceding-level neighbors as the parent.
The source has distance/hops zero and parent equal to itself. Unreachable
vertices have null distance, hops, and parent. Parallel arcs, self-loops, signed
BIGINT IDs, and isolated vertices are supported.

This adapter is a bounded distributed prototype. Python protocol/reference tests
and native core tests are separate from live Sail worker qualification. Retain a
matching successful process/two-host receipt before claiming that an artifact
has passed distributed qualification. These examples make no performance claim.

## Build and install

Use the current reviewed source candidate and the [source build and installation
instructions](PYTHON.md). Build Sail, install Pecan, and build/install the combined
Nutmeg wheel on every participating machine. All participants must use identical
wheel bytes. A v1/v2-only wheel does not contain BFS. Check the installed manifest:

```bash
.venv/bin/python - <<'PY'
from importlib.metadata import entry_points
manifests = [entry.load()().manifest()
             for entry in entry_points(group="pysail.extensions")
             if entry.name == "argentea"]
assert any("type.googleapis.com/nutmeg.v3.ArgenteaBfsApi" in
           {relation["type_url"] for relation in manifest["relation_types"]}
           for manifest in manifests)
PY
```

Start worker-mode Sail with the owned staging root and memory settings described
in [PYTHON.md](PYTHON.md). Local-only Sail explicitly rejects this worker relation.
The existing native allowance covers outgoing adjacency, frontier state, queues,
statistics, retained output, and the additional incoming CSR for `direction`.
The Sail memory pool reservation is not an operating-system RSS limit.

## Run all three methods

Against the running server, use the source client and installed Pecan package:

```bash
PYTHONPATH=examples/extensions/argentea/python \
.venv/bin/python - <<'PY'
from pyspark.sql.connect.session import SparkSession
from argentea_bfs_client import ArgenteaBfs

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    vertices = spark.createDataFrame(
        [(node,) for node in [-5, 0, 1, 6, 10, 11, 20, 21]], "id long")
    edges = spark.createDataFrame(
        [(-5, 0), (-5, 1), (-5, 1), (0, 6), (1, 6),
         (6, 10), (10, 10), (20, 21)], "src long, dst long")
    for method in ("reference", "frontier", "direction"):
        with ArgenteaBfs(spark).bfs(
            vertices, edges, source=-5, method=method,
            partitions=5, max_levels=14,
        ) as result:
            result.frame.orderBy("id").show()
            assert result.levels == 4 and result.reached == 5
            print(method, result.levels, result.reached, result.converged)
            # result.write_parquet("caller-owned-output-" + method)
finally:
    spark.stop()
PY
```

All methods must produce:

| id | distance | hops | parent |
|---:|---:|---:|---:|
| -5 | 0 | 0 | -5 |
| 0 | 1 | 1 | -5 |
| 1 | 1 | 1 | -5 |
| 6 | 2 | 2 | 0 |
| 10 | 3 | 3 | 6 |
| 11 | null | null | null |
| 20 | null | null | null |
| 21 | null | null | null |

Vertex 6 has two preceding-level neighbors; parent 0 is chosen numerically.
The duplicate arc and self-loop do not change the answers. The unreachable
component `20→21` is still validated during topology setup.

`reference` scans all outgoing rows during each expansion. `frontier` visits
active source rows. `direction` can alternate frontier push and incoming-neighbor
pull using integer thresholds `alpha=14` and `beta=24`. Pull requires an incoming
CSR, built during topology setup and retained in the same worker operation.
The qualifier records actual modes and examined-work counters; choosing the
method does not itself prove a particular speedup.

For undirected input, pass `directed=False`. The client expands every input edge
to both arcs server-side before routing by source owner. It does not collect
edges or vertices. Starting the fixture at source 10 then reaches five vertices;
in directed mode source 10 reaches only itself.

## Bounds and lifecycle

A maximum depth D needs D+1 expansions: the last proves the next frontier is
empty. Even an isolated source needs one expansion. `max_levels=0` therefore
fails after topology setup; it never returns partial distances. The initial
bound is `max_levels<=14` and `2*max_levels+4<=max_phase_budget<=32`.

One native query contains init, `K+1` decide/apply pairs, and result: `2K+4` native
stages. The phase-zero pair validates every normalized arc and constructs incoming
adjacency when required. After convergence, remaining phases relay DONE without
additional graph work. Actual `levels` are separate from the reserved phase count.

The client uses [bounded lazy session views](DELTA.md#what-the-bound-means) to keep
each request shallow under the unchanged host wire guard. View registration is
catalog work, not native execution. One terminal materialization runs the native
DAG; input snapshots, source-presence validation, and scalar result diagnostics
are ordinary jobs. Views are dropped after the terminal action. Uncertain
registration/drop errors expose deferred cleanup metadata; session teardown is
the fallback. Pecan separately owns staging and result files. Closing a result
invalidates its frames; `write_parquet` creates caller-owned output.

## Qualify matching artifacts

Set `SAIL_BINARY`, `RUNTIME_SOURCE_SHA`, and `NATIVE_SOURCE_SHA` to absolute build
paths and full source identities from retained build receipts. Native BFS needs
its own wheel; declaring a SHA does not prove the installed bytes.

```bash
for method in reference frontier direction; do
  .venv/bin/python examples/extensions/argentea/python/qualify_bfs.py \
    --mode process-cluster --case graph --method "$method" \
    --sail-binary "$SAIL_BINARY" \
    --runtime-source-sha "$RUNTIME_SOURCE_SHA" \
    --native-source-sha "$NATIVE_SOURCE_SHA" \
    --output "/tmp/argentea-bfs-$method"
done
```

Each output directory must be new. The qualifier launches a fresh server and two
workers, waits for two registered endpoints in the same session, and uses five
owners, 32 asynchronous task slots per worker, a 2 GiB Sail pool per process,
and a 256 MiB native allowance. Owners 2, 3, and 4 are empty in the fixture.

Separate cases are `undirected`, `source-only`, and `cap`. The undirected case
uses source 10. The source-only case checks empty CSR/owner behavior and one
actual expansion. The cap case uses K=0 and requires a failed query plus a typed
native `bfs_level_cap` cause after complete topology, no result/retry, and all
owner closes. A generic cancellation or quota error alone cannot pass. Use
`--mode local --case graph` to verify explicit local-mode refusal.

For two physical hosts, use the [shared-storage supervisor setup](PYTHON.md#4-run-across-two-physical-hosts):

```bash
.venv/bin/python examples/extensions/argentea/python/qualify_bfs.py \
  --mode two-host --case graph --method direction --partitions 5 \
  --two-host-config /absolute/path/to/argentea-two-host.json \
  --runtime-source-sha "$RUNTIME_SOURCE_SHA" \
  --native-source-sha "$NATIVE_SOURCE_SHA" \
  --output /tmp/argentea-bfs-two-host
```

The positive gate requires graph vertices on both actual supervised hosts and
an arc crossing hosts; executing only empty owners on one host is insufficient.
Disclose SSH forwarding if used. Source-only is intentionally excluded from that
physical graph proof. Retain an independent empty-prefix check for shared storage.

Receipts preserve exact integer answers, an independent queue reference, the
all-edge shortest-path/reachability certificate, minimum-ID parents, all native
phases, stable worker/PID/adjacency identities, local/global counters, direction
choices, task attempts, view absence, and process/storage cleanup. The client
collects only scalar diagnostics; the qualifier collects its small known fixture
to check every answer. No claimed RSS-zero or large-graph performance result
follows from this functional gate.

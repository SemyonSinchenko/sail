# Relational PageRank and weakly connected components

This pure Python client runs graph algorithms through ordinary Spark Connect
queries. Joins, aggregations and Parquet writes run in Sail/DataFusion; the
client advances iterations and receives only scalar reductions and filesystem
receipts. It does not build a local graph representation.

The first implementation provides probability-normalized PageRank and exact
minimum-label WCC. WCC is **not** the randomized contraction algorithm from
graphframes-rs. It requires no affine hash, and has no prime-field fallback.
This is a new API, not a GraphFrames wire or behavioral compatibility layer.

## Install and run

Clone the implementation branch:

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git
cd sail
```

Follow the source tutorial's [prerequisites](../TUTORIAL.md#3-prepare-a-build-machine)
and [build instructions](../TUTORIAL.md#4-build-and-install), keeping this branch
checked out. The tutorial's older `sail-extensions-1` tag does not contain these
algorithms or their host utils service. Use this branch's binary and its pinned
Python environment: PySpark Connect 4.0.1 and protobuf 7.36.2. A released Sail
wheel does not contain the new service either.

```bash
uv pip install --python .venv/bin/python --no-deps ./examples/extensions/graph-algorithms

# Set these on the Sail server, in addition to its usual embedded-Python setup.
export SAIL_EXPERIMENTAL_EXTENSIONS=1
mkdir -p /absolute/path/to/graph-staging
export SAIL_GRAPH_UTILS_ROOT=file:///absolute/path/to/graph-staging
target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

For multiple hosts, configure a shared object store or filesystem visible under
the same URI to every worker. A driver's private local directory is insufficient.
The service uses Sail's existing object-store registry and credentials.
The local root must already exist and be exclusively managed by Sail. Symlinks
inside owned runs are rejected; concurrent external filesystem modification is
outside this storage contract.

In another terminal:

```bash
.venv/bin/python examples/extensions/graph-algorithms/examples/run.py \
  --remote sc://localhost:50051 --partitions 4
```

## API

```python
from pyspark.sql.connect.session import SparkSession
from pyspark_graph_algorithms import GraphAlgorithms

spark = SparkSession.builder.remote("sc://localhost:50051").create()
vertices = spark.createDataFrame([(0,), (1,), (2,), (9,)], "id long")
edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], "src long, dst long")
graph = GraphAlgorithms(spark)

with graph.pagerank(vertices, edges, max_iterations=20) as result:
    result.frame.show()                 # id, pagerank
    # Optional persistent export, owned and cleaned up by the caller:
    # result.write_parquet("s3://my-bucket/results/pagerank")

with graph.wcc(vertices, edges) as result:
    result.frame.show()                 # id, component
    assert result.converged
spark.stop()
```

Inputs require unique, non-null BIGINT vertex `id` and BIGINT edge `src`/`dst`.
Every endpoint must refer to an existing vertex. Invalid graphs fail explicitly.
Properties are ignored; results contain the structural columns shown above.
Duplicate edges and self-loops are permitted. Isolated vertices are preserved.
The client separately snapshots the two input relations into Parquet; this is
not an atomic snapshot across mutable input sources.

| Option/result | PageRank | WCC |
|---|---|---|
| Algorithm | Directed probability-normalized PageRank | Undirected minimum-label propagation |
| Initial state | `1 / number_of_vertices` | Each vertex's own ID |
| Dangling vertices | Their probability is redistributed uniformly | Isolated vertices retain their own ID |
| Duplicate edges | Count separately in outgoing degree and contributions | Do not change component membership |
| Label/score | `pagerank: double`, total approximately 1 | `component: bigint`, minimum ID in the component |
| Stopping | Exactly `max_iterations` when `tolerance=None`; otherwise L1 change <= tolerance | No label changes |
| Limit without convergence | Raises `ConvergenceError` when a tolerance was requested | Raises `ConvergenceError` |
| Defaults | Reset 0.15; 20 iterations; no tolerance | At most 100 iterations |

Each PageRank step is
`reset / N + (1 - reset) * (incoming_probability + dangling_probability / N)`.
`reset_probability` must lie in `(0, 1]`. When running a fixed number of steps,
`result.converged` is `None`, not a convergence claim. An empty graph returns an
empty result with zero iterations. WCC needs at most component diameter + 1
iterations to detect its fixed point; a larger diameter may require raising
`max_iterations`. No performance equivalence to contraction is claimed.

## Staging, cancellation and ownership

Every iteration writes a new Parquet generation and reads it back before
removing its predecessor. The client checks schema and vertex row count. It
does not equate the requested partition count with the number of output files.
At completion only the result generation remains in the run directory.

`GraphResult` owns that directory. `close()` or leaving its context removes it
and invalidates its DataFrame. `touch()` keeps the owning server session active;
results do not survive session expiration or `spark.stop()`. `write_parquet()`
exports to an independently owned path before closing the result. Do not keep
the returned DataFrame beyond its result context unless you have exported it.

Validation failures, convergence failures and cancellation between completed
writes remove the run immediately. A failed or interrupted write RPC has an
uncertain outcome: its remote writers may still be stopping, so the client
**does not delete that run immediately**. The exception exposes
`cleanup_deferred=True` and `run_path`; cancellation raises
`GraphCancelledError` with the original engine error as its cause. Other
failures retain their original exception type.

Sail retains ownership of uncertain runs and attempts cleanup at session
shutdown or expiration. This also covers a client that dies or cannot reach
the server. Teardown cleanup is best effort: it is not a universal barrier
proving every nested writer or remote storage operation has finished, and
persistent storage failures can require administrative cleanup. Close the
session when finished; keeping it alive also retains deferred runs. This first
protocol has no independent run TTL
(`lease_seconds=0`). It does not delete an active run on a separate timer.

```python
from pyspark_graph_algorithms import CancellationToken

token = CancellationToken()
# A UI or another thread can call token.cancel(). It interrupts only queries
# tagged by this algorithm; the controller checks cancellation between actions.
with graph.pagerank(vertices, edges, cancellation=token) as result:
    result.frame.show()
```

Cancellation is cooperative. `InterruptTag` reaches operations already
registered on the server; a cancellation racing the registration of a new
operation is best effort, not an atomic guarantee. Untagged operations and
schema-analysis RPCs are outside that interruption mechanism.

For an optional progress callback, construct
`GraphAlgorithms(spark, observer=callback)`. It receives dictionaries containing
`kind` (`iteration_start` or `iteration_end`), `algorithm`, `iteration`, and
`run_path`. Exceptions raised by the callback abort the run and trigger cleanup.

## Required server contract

The canonical [protobuf schema](../../../crates/sail-session/proto/gf/utils/v1/utils.proto) specifies
`gf.utils.v1.Request`, carried by a zero-input `Relation.extension` with type URL
`type.googleapis.com/gf.utils.v1.Request`. The operations are `Ping`, `Mkdir`,
`Exists`, bounded `Ls`, and `Rm`. Their responses are typed Arrow columns, not
serialized protobuf receipt blobs. Every request is executed eagerly by
collecting its bounded receipt.

The checked-in Python messages were generated with `protoc 36.1` (Python
gencode 7.36.1). To regenerate after changing the canonical schema, run
`python examples/extensions/graph-algorithms/generate_proto.py` from the repo.

The client requires protocol version 1 and capabilities `fs` and
`owned_runs_v1`. `Mkdir` accepts a client-generated request UUID and returns an
opaque run token. Subsequent filesystem operations require that token and the
same session. A path under the root is not sufficient authority; root deletion
is prohibited. Retrying allocation is idempotent; retrying removal is safe.
PageRank and this WCC implementation require no native function capability.
Other graph algorithms are not exposed by this initial API.

The client uses the same protocol for any compatible Spark Connect engine;
cross-engine portability requires an engine's utils implementation and semantic
tests. This Sail implementation alone does not establish support on Spark or
Snowpark. No Python UDF, JVM, RDD, `cache`, `persist`, or Spark checkpoint API is
used by these algorithms.

## Test

```bash
PYTHONPATH=examples/extensions/graph-algorithms/src \
  SAIL_GRAPH_TEST_REMOTE=sc://localhost:50051 \
  .venv/bin/python -m pytest -q examples/extensions/graph-algorithms/tests
```

The small correctness fixtures deliberately collect their final answers for
comparison. Algorithm implementations collect only scalar values, never graph
rows. Local, process-worker and two-host execution run the same client.

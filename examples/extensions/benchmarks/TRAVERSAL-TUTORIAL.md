# Run BFS and shortest paths through Pecan, Banda and Grenada

This tutorial builds the traversal branch and checks all eighteen path/method
combinations locally and with two Sail worker processes. It produces complete
result checks and receipts. For isolated performance measurements, use the matrix
in the last step. Commands run from the repository root in Bash.

## 1. Freeze and build the source

```bash
git clone --branch work/extensions-traversal-bench https://github.com/querygraph/sail.git sail-traversal
cd sail-traversal
git switch --detach
export REVIEW_SOURCE_SHA="$(git rev-parse HEAD)"
```

Install the [native macOS or Linux prerequisites](TUTORIAL.md#2-prepare-and-build):
Python 3.12 with its shared library, Rust 1.97.1, C/C++ tools, uv, GEOS 3.12+,
and protobuf compiler plus standard headers. Use matching architectures for
Python, Sail and native wheels. For Linux, the existing
`examples/extensions/scripts/Dockerfile.linux` supplies the build environment;
clone this traversal branch inside its persistent Linux volume and detach at the
same SHA. The older tag and released wheels do not contain these kernels.

```bash
rustup toolchain install 1.97.1 --profile minimal --component rustfmt,clippy
rustup override set 1.97.1
uv python install 3.12
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
export CARGO_BUILD_JOBS=4
df -h .
bash examples/extensions/scripts/build.sh
```

The script installs locked Python dependencies, both native wheels and Pecan,
then builds `target/extensions-poc/host/debug/sail`. Its development binaries are
suitable for functional review; use the release build procedure from the
[benchmark guide](README.md) for performance measurements. Record the exact
commit and installed wheel hashes, not just their `0.1.0` version labels.

## 2. Check every method on a local server

```bash
traversal_output=$(mktemp -d "$PWD/target/traversal-review.XXXXXX")
.venv/bin/python examples/extensions/benchmarks/tutorial_traversals.py \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --runtime-source-sha "$REVIEW_SOURCE_SHA" \
  --native-source-sha "$REVIEW_SOURCE_SHA" \
  --mode local --output "$traversal_output/local"
```

Require eighteen passed rows and exit code zero. The helper creates a weighted
fixture with an isolate, zero-weight cycles, duplicate edges and skew. It starts
a fresh server for each case, compares every distance with independent Python
BFS/heap-Dijkstra references, and retains output Parquet, logs and receipts.
When a method exposes parent/hop columns, it checks the entire rooted parent tree.
Native reference BFS and native SSSP expose distances only; the receipt states
that parent-tree validation is unavailable for those methods.

| Operation | Variant | Pecan and Grenada | Banda |
| --- | --- | --- | --- |
| BFS | `reference` | Full reached-set relaxation | Existing native `bfs` |
| BFS | `frontier` | Changed vertices only | Same existing native `bfs` |
| BFS | `push_pull` | Frontier/unvisited relational joins | `bfsDirection`, native push/pull with pull early exit |
| SSSP | `reference` | Full reached-set Bellman–Ford | `bellmanFord` |
| SSSP | `frontier` | Changed-label relaxation | `dijkstra` |
| SSSP | `delta_star` | Lowest-bucket, all-edge relaxation | Parallel `ssspDeltaStar` |

The two native BFS reference/frontier rows deliberately exercise the same kernel;
they are not independent implementations. Delta-star relaxes all outgoing edges
of active vertices; it is not classical light/heavy delta-stepping. Grenada uses
Pecan's controller through Nutmeg graph tables. A relational pull join does not
promise the native kernel's first-neighbor early exit.

To select one case, add—for example—
`--engine nutmeg-native --algorithm sssp --variant delta_star`, with a fresh
output directory. The accepted engines are `pecan`, `nutmeg-native` and
`nutmeg-datafusion`.

## 3. Check separate worker processes

```bash
.venv/bin/python examples/extensions/benchmarks/tutorial_traversals.py \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --runtime-source-sha "$REVIEW_SOURCE_SHA" \
  --native-source-sha "$REVIEW_SOURCE_SHA" \
  --mode process-cluster --output "$traversal_output/process-cluster"
```

Again require eighteen passed rows and zero exit status. Pecan/Grenada joins,
aggregations and materializations execute through the distributed engine. Banda
still stages and computes its native graph on the driver; extra Sail workers do
not pool native graph memory. [Argentea](../../../docs/development/extensions/argentea-plan.md)
is the separately scoped distributed native path and is not qualified by this test.

For two machines, use the existing
[two-host deployment recipe](../../../docs/development/extensions/portable-graph-validation.md#reproduction)
with the traversal branch and matching installed artifacts on both hosts. Input,
staging and output URIs must be accessible to both workers. The local tutorial
helper itself launches processes on one host; it does not provision remote hosts.

## 4. Call the APIs

On a running, correctly configured branch server:

```python
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphAlgorithms
from sail_nutmeg import Nutmeg
from pyspark.sql.connect import functions as F

spark = SparkSession.builder.remote("sc://localhost:50051").create()
v = spark.createDataFrame([(0,), (1,), (2,), (9,)], "id long")
e = spark.createDataFrame([(0, 1, 4.), (0, 2, 1.), (2, 1, 1.)],
                          "src long, dst long, weight double")

# Pecan; methods are explicit, and result ownership is a context manager.
graph = GraphAlgorithms(spark)
with graph.bfs(v, e, source=0, method="push_pull") as result:
    result.frame.orderBy("id").show()
with graph.sssp(v, e, source=0, method="delta_star", delta=4.) as result:
    result.frame.orderBy("id").show()  # distances: 0, 2, 1, null

# Grenada: preserve the weight property while adapting Nutmeg graph tables.
nm = Nutmeg(spark)
tables = nm.tables(v, e, node_id="id", source="src", target="dst")
gv = tables.nodes.select(F.col("node_id").alias("id"))
ge = tables.edges.select(F.col("source").alias("src"),
                         F.col("target").alias("dst"), "weight")
with graph.sssp(gv, ge, source=0, method="delta_star", delta=4.) as result:
    result.frame.orderBy("id").show()

# Banda: native staging and kernel invocation, followed by explicit drop.
nodes = v.select(F.col("id").cast("string").alias("node_id"))
edges = e.select(F.col("src").cast("string").alias("source"),
                 F.col("dst").cast("string").alias("target"), "weight")
nm.stage("traversal", nodes, edges)
try:
    nm.run("traversal", "bfsDirection", source="0", orientation="outgoing",
           concurrency=4, maxIterations=1000).show()
    nm.run("traversal", "ssspDeltaStar", source="0", weightProperty="weight",
           orientation="outgoing", delta=4., concurrency=4,
           maxIterations=1000).show()
finally:
    nm.drop("traversal")
spark.stop()
```

Use BIGINT vertex IDs in Pecan, and finite nonnegative DOUBLE weights for SSSP.
The source must exist. Unreachable distances are null. Portable parents prefer
fewer hops and then the smaller parent ID; zero-weight cycles cannot produce
parent cycles. Iteration exhaustion and floating-point overflow are errors,
not partially successful results. Portable methods accept a `CancellationToken`;
result context managers release their owned staging data.

## 5. Run isolated Linux measurements

Copy `traversal-matrix.example.json` outside the checkout. Set the actual image,
volume, container paths, source SHAs, output directory, and a CPU/memory envelope
that fits the dedicated host. The example contains 36 cells: eighteen methods
in local mode and eighteen in process-worker mode. Increase repetitions only
after qualification; keep every planned outcome, including errors and timeouts.

```bash
.venv/bin/python examples/extensions/benchmarks/run_matrix.py \
  --config /absolute/path/traversal-config.json --dry-run
.venv/bin/python examples/extensions/benchmarks/run_matrix.py \
  --config /absolute/path/traversal-config.json
```

The [benchmark guide](README.md#timing-and-memory-boundaries) defines the timing
and memory fields. Tutorial times are functional observations on a shared outer
environment, not isolated benchmark results. Memory is unavailable in the tutorial
table outside a private Linux container. No zero is substituted for missing PSS.

Start with the bounded fixture, then the
[large Graph Kernels traversal workloads](GRAPH-KERNELS-TRAVERSAL.md).
[Graph500 input preparation](GRAPH500.md) scales
input generation separately; generating a large graph does not certify its
algorithm results or make this an official Graph500 submission. Preserve the
[capacity ladder and external-control plan](TRAVERSAL-PLAN.md) before large runs.

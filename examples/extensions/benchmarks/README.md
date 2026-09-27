# Pecan and Nutmeg: PageRank/WCC benchmark

This harness measures complete algorithm calls through Spark Connect against
the same Sail executable, input Parquet files, and resource envelope. It retains
successes, mismatches, errors, nonconvergence, timeouts, and memory-limit failures.

| CLI engine | Name | Execution |
| --- | --- | --- |
| `pecan` | Pecan | Python controller; Sail/DataFusion relational iterations |
| `nutmeg-native` | Nutmeg Banda | Native Grust kernels over a driver-resident staged graph/CSR |
| `nutmeg-datafusion` | Nutmeg Grenada | Nutmeg `GraphTables`, adapted to Pecan's relational controller |

Grenada currently shares Pecan's PageRank/WCC implementation. It measures the
graph-table entry path and is not an independent third algorithm implementation.
It does not stage a native graph or build CSR. The names distinguish execution
paths; the installed native package remains `sail-nutmeg`.

## Algorithms and accuracy

Every existing method remains available. `--variant` chooses the implementation:

| Algorithm | Variant | Pecan / Grenada method | Banda kernel |
| --- | --- | --- | --- |
| PageRank | `reference` | `power` | `pagerank` (Grust 0.23.0) |
| PageRank | `optimized` | `delta` | `pagerankDelta` (local addition) |
| WCC | `reference` | `min_label` | `wcc` (Grust 0.23.0 union-find) |
| WCC | `optimized` | `randomized` | `wccRandomized` (local addition) |

"Optimized" identifies the new implementation, not a claim that it is faster
on a particular workload. The two original WCC methods are different algorithms.
The new native kernels live in the vendored Nutmeg integration; no published
Grust crate was changed. See the [kernel contracts](../vendor/nutmeg-graph/OPTIMIZED_ALGORITHMS.md).

All PageRank methods use float64, damping 0.85, uniform initialization/restart,
uniform dangling redistribution, and tolerance `1e-8`. Duplicate edges contribute
separately. Reference methods stop on successive-iterate L1 change. Delta methods
retain signed residuals at inactive vertices, push an active frontier, and allow
reactivation. They normalize the output and certify its true fixed-point L1
residual before reporting convergence. The benchmark independently recomputes
that same final residual for **every** method and requires it to be at most the
tolerance. Native `converged=false` and client convergence exceptions are retained
as nonconverged outcomes. Native parallelism is explicitly requested.

Delta PageRank follows the frontier motivation of
[GraphX](https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala),
with an explicit global certificate for probability-normalized scores. It is not
GraphX's local-threshold stopping rule. Native delta traverses active adjacency;
the relational frontier join can still scan all Parquet edges. `active_edges`
counts propagated messages, not physical reads avoided. All materializations,
frontier statistics and certificates remain inside the measured algorithm call.

Minimum-label WCC needs up to the component diameter in propagation rounds,
followed by a no-change check. An ordered chain of V vertices takes V rounds
including that check. Randomized WCC instead contracts closed neighborhoods using
seeded GF64 affine priorities, retains representative maps, and expands final
labels. Its expected query-round bound is logarithmic; each run still has an
explicit cap. See [Bögeholz et al.](https://arxiv.org/abs/1802.09478).
Both new paths share the coefficient stream and seed, with original vertex IDs
kept as representatives. The benchmark records every contraction's graph size.

Native reference WCC labels reflect lexicographic staging order of string IDs; Pecan labels
use numeric IDs. Outside the timed interval, normalize each output partition to
its minimum numeric vertex ID and compare every vertex with independent
union-find results. Casting the native label to BIGINT alone is insufficient.

Independent NumPy PageRank reference scores are also generated before measurement.
Validation checks every output ID, uniqueness, finite nonnegative scores,
normalization and full-vector L1/max-absolute errors. Against the precomputed
power reference, the conservative comparison bound is `2*d*t/(1-d)` for power
and `(1+d)*t/(1-d)` for delta, plus `1e-12` for numerical roundoff. The separate
true-residual certificate bounds stationary error by `residual/(1-d)` for every
method. Receipts retain actual errors and residuals, not only pass/fail.

## Timing and memory boundaries

Each trial starts a fresh Sail server and, in `process-cluster` mode, two worker
processes. Session creation and a `SELECT 1` readiness query precede timing.
The timer starts with input DataFrame handles and ends when the complete output
Parquet write returns. It includes input reads, API validation, representation
conversion, native staging/CSR construction when applicable, algorithm execution,
and result writing. Correctness validation and cleanup follow the timer.

Native `run()` is lazy: the output write executes the kernel exactly once.
Pecan returns an already materialized result, which is then exported to an
independent output path. Both paths therefore deliver a caller-owned Parquet
result. Pecan's own intermediate materialization remains part of its cost.
`output_action_seconds` includes native execution for Banda, so it is not a
kernel-only or directly comparable export-only number.

Dataset checksum reads precede timing and warm the OS page cache. No cache flush
is performed. Fresh Sail state does not mean cold filesystem cache. Binary and
dataset identities, source commits, package versions, settings, and outcomes are
recorded in every receipt.

Run one trial per fresh Linux container, with a private PID namespace, `--init`,
8 assigned CPUs, 32 GiB RAM and no swap. Run trials sequentially without builds
or other benchmark trials in parallel. Measurements include the Python
controller, driver, workers, sampler, and container init:

- Sampled RSS sums count shared mappings once per process.
- Sampled PSS apportions shared mappings. Missing PSS is unavailable, never zero.
- Kernel cgroup `memory.peak` includes the container's entire lifetime, imports,
  checksum reads, verification and filesystem page cache. It is not an
  algorithm-only allocation count. `cgroup_execution_after.memory.peak`, captured
  before verification, separately reports the lifetime peak through result delivery.
- Baseline and execution-phase samples are separate. Samples spanning phase
  changes are excluded from phase peaks. Sampled peaks can miss brief spikes;
  an unsampled phase has no reported process-memory peak.
- `/proc/stat` steal is measured across the whole Linux VM, not just the trial's
  assigned CPUs. Record physical-host background load and the VM configuration.

All three paths load the same installed Sedona and Nutmeg extensions. Every Sail
session prepays Nutmeg's 4 GiB allowance from its 10 GiB participating memory pool,
even in Pecan/Grenada trials. This reservation is not 4 GiB of allocated RSS.
It leaves a nominal 6 GiB for other participating operators in each such process.
Separate processes have separate Sail pools; the container's 32 GiB limit is the
aggregate hard bound. Native diagnostics are recorded separately from RSS/PSS.

## Generate inputs

Use the locked extension Python environment. Generation and reference calculation
must run outside the measured container. Choose a fresh directory:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_fixtures.py \
  --output /absolute/path/data/sparse-100000 --vertices 100000 \
  --family sparse --degree 8 --block-size 1024 --seed 20260927
```

The sparse family has component-separated directed graphs, a random recursive
tree in each component, skewed source selection, additional random edges,
isolates, dangling vertices, self-loops, and duplicate edges. IDs follow generation
order: early vertices have higher source-selection weights; IDs are not randomly
permuted. Components are bounded by `--block-size`, which matters for WCC diameter
and contraction work. Preserve these parameters in comparisons. This synthetic
family does not characterize arbitrary ID assignments or giant connected graphs.

For a separate diameter stress test:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_fixtures.py \
  --output /absolute/path/data/chain-512 --vertices 512 --family chain
```

Run large sparse cases and bounded chains as distinct result groups. A chain
iteration limit or timeout is retained, not replaced by a shorter successful
chain or silently removed from summaries.

## Run a cell

Build an optimized Sail executable and Nutmeg wheel from a recorded immutable
source revision. Install Pecan and both native wheels in the locked environment.
Inside a fresh container, mount immutable source, those artifacts, inputs, and
a writable results directory. For example:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_cell.py \
  --sail-binary /absolute/path/to/release/sail \
  --runtime-source-sha SOURCE_SHA_USED_TO_BUILD_SAIL \
  --native-source-sha SOURCE_SHA_USED_TO_BUILD_NUTMEG \
  --dataset /absolute/path/data/sparse-100000 \
  --output /absolute/path/results/pecan-pr-repeat-0 \
  --engine pecan --algorithm pagerank --variant optimized --mode process-cluster \
  --threads 8 --partitions 8 --repeat 0
```

Use `nutmeg-native` or `nutmeg-datafusion` for the other entry paths, and `wcc`
for connected components. Keep identical settings and inputs. Use at least
three repetitions, vary order reproducibly, and retain every cell. Report median
and range, with outcomes separate from performance statistics. Local and
process-worker modes are distinct execution classes.

## Run all methods

Copy [`matrix.example.json`](matrix.example.json) outside the frozen checkout and
fill in the immutable image, source, binary, wheel-source and host paths. The
runner validates the configuration and creates a reproducible shuffled order:

```bash
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --dry-run
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --prepare-only
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --skip-prepare
# After interruption, retain finished cells, including failures:
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --skip-prepare --resume
```

The example covers all three entry paths and both variants of both algorithms:
10k, 100k and 1m vertices in process-worker mode, 100k in local mode, three repeats,
plus a separate 512-vertex WCC chain at a 100-round cap (150 cells total). The
reference min-label chain cells are expected to be nonconverged; they remain
failures to finish within that envelope. Native reference WCC has no round cap.
Randomization order, graph seed and algorithm seed are independent recorded
parameters. Run pilots in a separate output root; do not replace measured failures.

Export every cell plus tables of median/range statistics for passed cells:

```bash
python examples/extensions/benchmarks/summarize.py \
  --evidence /absolute/path/to/pecan-review-evidence \
  --output /absolute/path/to/new-summary-directory
```

`cells.csv` retains failures and missing cells, `summary.json` records all outcome
counts and metric sample counts, and `tables.md` presents the comparisons. Missing
memory values remain unavailable. Incorrect or unfinished results never enter
successful time or memory statistics. An apparent pass whose receipt disagrees
with its planned cell, source, or artifact identities becomes `integrity_error`;
CSV and JSON retain its original outcome and explicit reasons. Conflicting Sail
binary or installed native-file hashes invalidate every affected passed trial;
dataset hashes must agree across trials of the same dataset. A receipt without
its matrix summary is `incomplete_record`, not a successful or never-run cell.
The exporter writes the complete report and exits nonzero when it finds an
integrity error.

Each cell emits `receipt.json`, `memory-samples.jsonl`, `server.log`, and complete
result Parquet files on success. An outer container timeout/memory failure must
also be recorded even if the process cannot write its receipt. `--allow-dirty`
and `--allow-unisolated` exist only for functional harness development; those runs
must not be published as benchmark measurements.

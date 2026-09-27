# Sail on the published large graph fixtures

This experiment runs Pecan, Nutmeg Banda and Nutmeg Grenada on the hub and
uniform inputs from adversari.al's Graph Kernels campaign. The input edge files
must match the published B9 SHA256 values in `large-fixtures.json`.
At 2,097,152 vertices they contain about 16.8 million edges; at 4,194,304
vertices they contain about 33.6 million. WCC at these sizes extends the
published campaign, which measured PageRank on its large inputs.

The Sail measurement is a complete algorithm call, from lazy input handles to
written output. It includes loading, validation, graph staging and intermediate
materialization. It is not the existing kernel-only measurement, and its time
must not be divided by a historical kernel time and described as a speedup.
PageRank is float64 with uniform restart and dangling redistribution, damping
0.85 and an independently checked L1 fixed-point residual at tolerance 1e-8.
WCC checks every vertex's component against independent union-find.

## Prepare the exact inputs

Build and install the Sail runtime and extension wheels using [the tutorial](TUTORIAL.md).
Keep the runtime, native wheel and harness source identities separate.
Obtain the upstream generator at its pinned commit:

```sh
git clone https://github.com/querygraph/adversarial-graph-algorithms
git -C adversarial-graph-algorithms checkout fe50ea8f112a5970214c619ed244eb38917250f1
python3 adversarial-graph-algorithms/docker/simple-rust-algo-bench/fixtures.py \
  --output large-inputs --families hub uniform --sizes 65536 2097152 4194304
```

Run generation outside timed trials. Place these inputs in the benchmark's
Docker volume, or mount them at the absolute `edge_file` paths in the config.
The importer refuses a hash mismatch, incorrect header, invalid endpoint,
missing edge or extra column. It preserves edge order, duplicates, loops and
isolated vertices, and records the source hash in each dataset manifest.

## Qualify and measure

Copy `large-matrix.example.json` outside the checkout. Set the operator paths,
Docker image and context, source SHAs and output directory as in tutorial
section 7. The example requests 16 CPUs, 32 GiB with no swap, a driver and two
workers on one host, 16 partitions and 64 task slots. Banda remains driver-local.
The same limits apply to every path, including the driver-native accounting
allowance. These are a new experiment's limits, not the historical kernel run.

First qualify each method on the hash-pinned 65,536-vertex inputs, then on the
largest inputs. Give pilots separate run IDs and evidence directories. Retain
errors and timeouts; do not overwrite them when revising a capacity limit.

```sh
python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --dry-run
python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --prepare-only
python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --skip-prepare
python3 examples/extensions/benchmarks/summarize.py --help
```

The main matrix has 180 trials: four graphs, three paths, five methods and
three repetitions. PageRank retains reference power iteration and delta/frontier;
WCC retains reference, randomized contraction and fused randomized contraction.
Every trial runs in a fresh container. Preserve the full result vectors,
receipts, process memory samples, cgroup peaks, source and binary hashes, and
all unsuccessful outcomes. Use `--resume --skip-prepare` to resume a matrix;
completed failures stay completed failures. Do not run other work during timing.

The example is the parallel Sail campaign. A one-CPU local campaign requires a
separate run ID, CPU quota/cpuset, thread and partition counts, and `mode=local`.
Do not merge its results with the parallel campaign or imply either reproduces
the original in-memory benchmark's execution boundary.

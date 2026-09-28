# BFS and SSSP on the large Graph Kernels inputs

This adds traversal workloads to the same four pinned topologies used by the
[large PageRank/WCC campaign](https://github.com/querygraph/sail/blob/8ca99a286247c300e59f05aaf65b28bed7de2837/docs/development/extensions/pecan-nutmeg-large-benchmark.md).
It does not modify that campaign's input files or results. Build and qualify the
three paths using [the traversal tutorial](TRAVERSAL-TUTORIAL.md) first.

| Topology | Vertices | Directed input arcs |
| --- | ---: | ---: |
| Hub | 2,097,152 | 16,762,845 |
| Uniform | 2,097,152 | 16,777,173 |
| Hub | 4,194,304 | 33,525,729 |
| Uniform | 4,194,304 | 33,554,395 |

Input SHA-256 values are in [large-fixtures.json](large-fixtures.json) and the
[matrix example](graph-kernels-traversal-matrix.example.json). The example plans
216 trials: four inputs, three paths, three variants per algorithm, BFS and SSSP,
and three repetitions, with randomized order within each suite/repetition.
These are planned trials; this page reports no performance results.

## Topology and weights

The source `.edges` files are unweighted. Each starts with `V E`, followed by
exactly E lines containing two nonnegative integer endpoints. The converter
retains every arc in its original order, including loops and duplicates, and
creates every vertex in `0..V-1`, including isolates. It rejects malformed rows,
incorrect counts, out-of-range endpoints and changed input hashes.
Each physical line must fit in 128 bytes including its newline; an unterminated
final line must be shorter than 128 bytes. Overlong whitespace cannot split one
malformed line into multiple accepted arcs.

BFS ignores the weight column. SSSP needs an explicit weight policy:

- `unit`: every arc has weight 1. SSSP distances then equal BFS hop distances.
- `splitmix64-1-16`: the example's **derived weighted workload**, assigning each
  original arc an integer weight from 1 through 16 using its zero-based row index
  and `weight_seed` (42 in the example). These weights were not present in the
  original Graph Kernels files.

The complete formula is below. All arithmetic before the final mask wraps
modulo 2^64. Thus changing Parquet chunk sizes cannot change the graph. Duplicate
arcs remain separate and can receive different weights.

```python
mask = (1 << 64) - 1
z = (edge_index + weight_seed + 0x9E3779B97F4A7C15) & mask
z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask
z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask
weight = 1 + ((z ^ (z >> 31)) & 15)
```

All paths read the same prepared Parquet data. `directed: true` and `source: 0`
are explicit in the example. Unreachable vertices remain in the result with
null distance; the harness does not silently substitute a more connected source.
Changing orientation, source or weights defines another workload and needs a
separate dataset/configuration and output directory.

## Prepare one input

Run from the repository root with the tutorial's environment. Supply an actual
downloaded file whose bytes match the pinned input. The converter uses bounded
chunks rather than loading the whole graph or building a Python adjacency list.
Preparation and checksum reads are outside algorithm timing.

```bash
.venv/bin/python examples/extensions/benchmarks/imported_traversal_fixture.py \
  --edge-file /absolute/path/hub-2097152.edges \
  --edge-sha256 60841becebebd8b41e30443f8a5a89355345189e49bf204e1bc4f05d080b4d33 \
  --vertices 2097152 --weight-policy splitmix64-1-16 --weight-seed 42 \
  --source 0 --directed --chunk-edges 262144 --chunk-vertices 262144 \
  --output /absolute/path/new-hub-traversal
```

The manifest records the original input digest and a separate canonical digest
of the weighted records: little-endian int64 source, int64 destination and
float64 weight, 24 bytes per original arc in original order. Vertex identity is
the ascending little-endian int64 sequence. Every Parquet file is also hashed.
Failed conversions retain `failure.json` and never write a final manifest.
Argument/header/input-pin errors before output creation leave no output directory.

`--reference` produces independent Python BFS and heap-Dijkstra answers only for
inputs of at most 100,000 vertices and 1,000,000 arcs. The large inputs use the
existing distributed distance certificate: all-edge inequalities, source and
unreachable conditions, rooted tight-edge reachability, and parent-tree checks
where that method returns parents. Validation failure or a certificate round cap
is retained as a failure; elapsed time alone is not an accepted result.

## Run the matrix on Morrobay

Copy the matrix example outside the clean checkout. Replace the zero source
SHAs and example paths with the exact **release** host, native wheel and harness
identities, and point each `edge_file` at the pinned input in the mounted volume.
Use the [benchmark guide](README.md#timing-and-memory-boundaries) for release
builds, container setup and time/memory definitions. A completed build or
functional gate does not itself supply benchmark measurements.

```bash
.venv/bin/python examples/extensions/benchmarks/run_matrix.py \
  --config /absolute/path/graph-kernels-traversal.json --dry-run
.venv/bin/python examples/extensions/benchmarks/run_matrix.py \
  --config /absolute/path/graph-kernels-traversal.json --prepare-only
```

Inspect all prepared manifests. Add each `canonical.edges.sha256` to that
dataset's `expected_edge_sha256` before the measured campaign; use a fresh
campaign host output directory for the finalized configuration. Then, after
other builds and qualification tasks have stopped on the dedicated host:

```bash
.venv/bin/python examples/extensions/benchmarks/run_matrix.py \
  --config /absolute/path/graph-kernels-traversal-final.json --skip-prepare
```

Even with `--skip-prepare`, every cell checks the original topology hash, vertex
count, weight policy/seed, orientation/source and optional canonical weighted
hash against its configuration, then verifies the exact file inventory and
Parquet hashes. Added partitions, missing files and input symlinks are rejected.
Preparation
receipts should be retained beside the final configuration.

Keep all cells, including unsupported, error, timeout and certificate-failure
outcomes. Report the observed reachable count, iterations/work, wall time and
memory together with the path's execution boundary: Pecan/Grenada use Sail's
distributed relational execution; Banda's native graph remains on the driver.
Argentea's separate distributed native qualification is not part of these 216
cells. Run this capacity step before the larger [Graph500 inputs](GRAPH500.md).

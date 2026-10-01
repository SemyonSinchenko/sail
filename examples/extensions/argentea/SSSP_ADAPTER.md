# Argentea SSSP adapter and client

`nutmeg.v5.ArgenteaSsspApi` connects the [weighted SSSP core](SSSP_CORE.md) to
Sail's existing worker-extension execution path. It reuses the job-bound owner,
integer range routing, task-group placement, shared memory admission and
close/cancel callbacks used by Argentea PageRank, BFS and WCC. No additional Sail
host change is required. The Python client is `python/argentea_sssp_client.py`.

Both methods pass bounded ARM Sail process-cluster qualification, including
the 128-stage boundary, signed IDs, isolates, skew with empty owners, typed cap
failures and local-mode rejection. [Archived evidence](../../../docs/development/extensions/argentea-validation/README.md#sssp-process-cluster-and-cap-qualification)
pins the source, wheel, native events, scheduler tasks and cleanup. Linux and
physical two-host SSSP qualification remain outstanding. These are functional
results, not performance measurements.

## Input and algorithm contract

Vertices require BIGINT `id`. Edges require BIGINT `src,dst` and DOUBLE `weight`.
Weights must be finite, non-null and nonnegative. Native construction checks all
weights, including unreachable edges. The complete topology exchange validates
all destinations and exactly one source. Duplicates, loops, isolates, empty
owners and signed BIGINT extremes are supported. Ownership is `id mod P`, with
a nonnegative remainder. Edges initially belong to their source owner.

`method="reference"` performs synchronous Bellman–Ford relaxation.
`method="delta_star"` selects the globally smallest active distance bucket and
relaxes all outgoing edges of its active vertices. Vertices can reactivate in
the same bucket. This is all-edge delta-star, not classical light/heavy
delta-stepping. It shares the suite's advanced semantics; Banda's native
Dijkstra reference is a different algorithm. Distance/hop/parent order is exact;
no tolerance is used to manufacture agreement. Overflow fails the operation.

Both methods use `2K+4` native stages for cap K: initialization, K+1 pairs of
`decide/apply` (including topology), and result certification. The static query
supports at most 128 stages, so K is at most 62. Completed partitions relay
empty barriers through unused stages. Hitting the cap with active work fails
without partial distances. The stage bound does not guarantee convergence or
adequate worker task slots for every graph.

## Protocol and resource ownership

Strict JSON requests specify version 5, algorithm, verb, operation/snapshot IDs,
generation, partition/vertex counts, source, delta, round cap, phase budget,
phase and output batch size. Unknown fields are rejected. Algorithm and options
cannot change under one bound worker owner.

Each Arrow channel has schema metadata for protocol, algorithm, operation,
snapshot, generation, phase, partition/vertex counts and channel. Eleven
non-null BIGINT columns carry owner, kind, producer, sequence, target, source,
hops, mode, auxiliary count, worker identity and adjacency identity. Two DOUBLE
columns carry distance and bucket. A bucket of -1 denotes absence; other
buckets must be finite, nonnegative integers. IDs never travel through doubles.

Each statistics barrier contains eight scalar fields and a completion from
every producer. Each contribution barrier contains sequenced topology or path
candidates and a per-recipient completion count plus the producer's scalar
total. Source, predecessor and destination ownership, phase, origin, sequence,
mode, selected bucket and count must agree. Input is drained through EOF before
publishing a successor. Missing/late/replayed data fails the operation.

Weighted input buffers, early messages, statistics, CSR, successor state and
Arrow buffers share the admitted resource domain. Cursors retain immutable
snapshots and release partition locks before output backpressure. Sliced result
arrays retain their memory lease after the worker closes. See the core document
for the remaining O(local vertices) publication work per exchange.

## Calling the client

Build/install the branch's Nutmeg wheel and Pecan package in Sail's Python
environment using the [shared setup](PYTHON.md). Start the branch's distributed
Sail runtime with worker extensions enabled. Add the client modules to the
reviewer's Python path:

```sh
export PYTHONPATH="$PWD/examples/extensions/argentea/python:$PYTHONPATH"
```

Given a Spark Connect session `spark` attached to that runtime:

```python
from argentea_sssp_client import ArgenteaSssp

vertices = spark.createDataFrame([(-5,), (0,), (1,), (9,)], "id long")
edges = spark.createDataFrame(
    [(-5, 0, 5.0), (-5, 1, 1.0), (1, 0, 0.25)],
    "src long, dst long, weight double",
)
with ArgenteaSssp(spark).sssp(
    vertices, edges, source=-5, method="delta_star", delta=4.0,
    partitions=2, max_rounds=14,
) as result:
    result.frame.orderBy("id").show()  # tiny reviewer fixture only
    print(result.rounds, result.reached, result.converged)
```

Expected `(id, distance, hops, parent)` rows are `(-5,0,0,-5)`,
`(0,1.25,2,1)`, `(1,1,1,-5)`, and `(9,null,null,null)`. Use
`method="reference"` for the reference path. Use `directed=False` to add each
reverse weighted arc before ownership routing. The default is directed.

The client uses Pecan's owned snapshots and retained-result lifecycle. Inputs
obey the valid-graph contract, including a present source and finite
nonnegative weights with finite path sums; the Python client runs no graph-data
validation jobs.
It registers shallow phase views, performs one native result materialization,
validates scalar termination/reachability diagnostics, then releases the views.
Production clients can write `result.frame` without collecting graph vectors.
The returned context manager owns result cleanup. Runtime errors and uncertain
writes preserve the existing deferred-cleanup contract.

## Reproducing current tests

```sh
cargo test --manifest-path examples/extensions/nutmeg/Cargo.toml --lib argentea::sssp
PYTHONPATH=examples/extensions/argentea/python:examples/extensions/nutmeg/python:examples/extensions/graph-algorithms/src \
  python -m pytest -q examples/extensions/argentea/python/test_argentea_sssp_client.py
```

Native tests use real DataFusion range exchanges and an in-process owner
dispatcher. They check exact full vectors over 1, 2, 3 and 11 owners, the
128-stage boundary, cap refusal, signed extrema, retained Float64 result slices,
invalid weights and strict statistics encoding. Client tests check serialization,
shallow views, weighted snapshot columns, directed/symmetrized construction,
scalar diagnostics and cleanup. These are functional checks, not benchmark or
physical deployment results.

## Reproducing the process-cluster qualification

Run each case in a fresh Python process and a new evidence directory after
building the branch and installing its wheels as above:

```sh
python examples/extensions/argentea/python/qualify_sssp.py \
  --mode process-cluster --method delta_star --max-rounds 62 --case graph \
  --partitions 5 --worker-task-slots 512 \
  --sail-binary /absolute/path/to/sail \
  --runtime-source-sha "$SAIL_SOURCE_SHA" \
  --native-source-sha "$NUTMEG_SOURCE_SHA" \
  --output /absolute/path/to/new-sssp-evidence
```

Use `--method reference` for the other 128-stage boundary. Other fixtures are
`--case extremes`, `--case skew`, and `--case isolates`; use `--max-rounds 14`
for these. The skew fixture puts every vertex on one owner at five partitions,
so four empty owners must still complete every barrier. Expected cap controls
use `--case graph --max-rounds 0 --expect-cap` for either method, and
`--method delta_star --max-rounds 1 --expect-cap`. The zero-round cases still
finish topology and report one active source; the one-round case reports two
active vertices and three reached vertices. Caps return no result rows.

The qualifier uses an independent Dijkstra oracle and checks complete weighted
vectors, predecessor witnesses, the globally selected bucket, expected edge
relaxation counts, every producer barrier, fixed worker/CSR identity, P-wide
worker stages in one slot group, attempt-zero task placement, owned views,
staging and process cleanup. Failed-cap audits require a FAILED job, the typed
cause with measured work, and terminal stored tasks for every native stage and
owner. A generic query error does not qualify the cap.

The frozen ARM cases use the development-profile wheel from `00ebb7ac9`, host
`d9c6381a`, five owners, 512 worker task slots, a 2 GiB Sail pool and a 256 MiB
native admission per worker. This deliberately generous functional envelope
must not silently become a performance-comparison envelope.

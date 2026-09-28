# BFS/SSSP implementation and qualification

The traversal branch adds reference and advanced BFS/SSSP to Pecan, Nutmeg Banda
and Nutmeg Grenada. The existing PageRank/WCC campaign remains frozen under its
original source identities. This document records functional qualification;
isolated Linux time/memory comparisons and large capacity runs remain pending.

Build and run every method with the
[traversal tutorial](../../../examples/extensions/benchmarks/TRAVERSAL-TUTORIAL.md).
The [input guide](../../../examples/extensions/benchmarks/GRAPH500.md) builds the
pinned official Graph500 generator and streams its output to partitioned Parquet.
The [external-control guide](../../../examples/extensions/benchmarks/traversal-controls/README.md)
provides fixed-source GAPBS and Parallel-SSSP checks on exact integer fixtures.

## Implemented paths

Pecan and Grenada share relational full-set, frontier, and advanced methods.
BFS's advanced method changes push/pull join orientation; it does not claim
native adjacency early exit. SSSP's advanced method is all-edge delta-star
stepping, explicitly distinguished from classical light/heavy delta-stepping.
Both return distance, hop count and rooted parents, including zero-weight cycles.

Banda retains its existing BFS, Bellman–Ford and Dijkstra kernels. New native
`bfsDirection` builds forward/reverse adjacency and switches between sparse push
and early-exit pull. `ssspDeltaStar` uses parallel all-edge relaxation and an
indexed bucket queue. Its output is distances only. Native graph computation
remains driver-local, including when the surrounding Sail server has workers.
[Argentea](argentea-plan.md) is a separate distributed-native workstream.

Query-owned adjacency and scratch use the existing native resource accounting.
Weighted snapshots validate matching graph revision and entry identity, including
drop/recreate races. Retained output arrays hold their reservations. Input and
work quotas, cancellation, invalid weights, overflow and iteration exhaustion
have explicit failure behavior.

## Findings incorporated before qualification

The original delta-star queue read mutable atomic distances directly as heap
keys. A parallel wave could change several queued priorities before sequential
repairs and invalidate the heap order. That violated lowest-bucket selection;
it was not evidence of a demonstrated wrong final distance. The fixed queue
owns admitted priority keys and applies decreases sequentially after each wave.
The regression fails against the original production queue and passes against
the replacement; the negative-control log is retained below.

Native pull BFS originally checked work/cancellation only while visiting edges.
Long scans over isolated or already visited vertices could postpone cancellation.
It now charges fixed destination blocks as well as examined edges. A deterministic
work-limit test exercises the empty-adjacency scan. Parallel fixtures cross the
actual execution threshold and verify requested widths 1, 2 and 8.

Portable SSSP now rejects overflowing explored relaxations even when another
shorter path dominates that candidate, matching the new native kernel contract.
Cancellation tests cover every portable traversal method and verify owned-stage
removal. Partition-count checks include an isolated source.

## Recorded functional evidence

The core qualification candidate is
`b2e7e0faf86b64b8c7a54fb14210db7485c9fe99`, tested in detached worktrees. The
Mac functional server uses the unchanged Sail runtime source
`70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`; each receipt pins its binary and
installed native package files separately. Rust tests are native arm64 on
Capitola. Server/Python integration uses matching x86_64 artifacts through
Rosetta. These checks do not establish Linux compatibility or performance.

| Check | Observed result |
| --- | --- |
| Native unit and admission tests | 85 unit tests and one admission integration test passed |
| Native Clippy | All targets passed with warnings denied |
| Native release stress | Four idle and 80 targeted runs with ten CPU-saturating processes passed; every load process was cleaned up |
| Portable traversal, existing reference algorithms and certificate tests | 51 passed locally and 51 with process workers |
| Full entry-path matrix | 36 passed: eighteen methods in each Sail mode |
| Graph500 input plus certificate integration | Six passed: BFS and SSSP across all three paths, with process workers |
| Frozen harness tests | 126 passed; nine live-server certificate tests skipped here and run in the preceding integration gate |
| Pinned generator and external controls | 58 passed in their separate frozen source gate |

The six Graph500 cases use 64 vertices, 256 original edge tuples, and 48 vertices
reachable from the selected source. The checks therefore exercise traversal,
including unreachable vertices, rather than succeeding on an isolated root.

[Functional evidence](traversal-validation/functional-evidence.json) records all
42 full-call cases, output hashes, source identities, correctness results and
cleanup. Their raw receipts remain at the recorded local paths. Output hashes
and zero remaining owned staging files were independently rechecked while
writing this evidence. The artifact deliberately omits unisolated timing and
memory observations from comparison tables.

## Large-result validation

The default gate compares every distance with independently generated reference
vectors. Large inputs may instead explicitly request a distributed certificate:
all-edge triangle/reachability inequalities plus source-rooted reachability
through tight edges. The latter condition rejects disconnected zero-weight
cycles with fabricated finite distances, which local inequalities alone miss.

BFS uses exact integer distances. Floating-point SSSP records local slack and a
conservative accumulated absolute-distance error bound; it does not claim the
same relative-error guarantee as a full-vector oracle. Parent/hop columns are
checked when a method exposes them. Certificate work runs after the measured
algorithm call, has its own round cap, and cannot turn incomplete validation
into success. Its joins and materializations remain server-side.

## Remaining qualification

Linux gates, isolated measurements, and
large Graph500/real-graph capacity runs remain separate requirements. External
integer controls do not yet cover the official generator's fractional weights;
those inputs must not be silently quantized for comparison. Native SSSP lacks
parent output, so these checks do not establish official Graph500 compliance.

Argentea's worker registration, scoped native state, routing, cancellation and
two-host execution require their own integration gates before any distributed
native claim. Its source-level scheduler probes and partition-core tests are
not a substitute for executing native work on both machines.

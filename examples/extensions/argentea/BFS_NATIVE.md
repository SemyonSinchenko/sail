# Argentea BFS worker adapter (v3)

The Nutmeg wheel's existing Argentea worker factory now also implements integer
BFS. It reuses the worker-role host hooks, job scope, leased memory domain,
partition-preserving input bridge, host range exchanges, topology validation,
fail-fast lifecycle, and atomic receipt writer. No Sail host changes or new
transport are introduced for BFS. The user tutorial is [BFS.md](BFS.md); the
algorithm and resource contract is [BFS_CORE.md](BFS_CORE.md).

The adapter lives in `../nutmeg/src/argentea/bfs/`. The first gate runs genuine
DataFusion range exchanges through an **in-process ownership shim**. This proves
native Arrow integration, not Sail scheduling, worker placement, physical hosts,
or a useful large-graph phase budget. Combined-source runtime qualification is a
separate requirement before claiming distributed BFS support.

## Request and static plan

Type URL: `type.googleapis.com/nutmeg.v3.ArgenteaBfsApi`.

The strict JSON object contains `version:3`, `algorithm` (`bfs_reference`,
`bfs_frontier`, or `bfs_direction`), `verb` (`init`, `decide`, `apply`, `result`),
`operation_id`, `snapshot_id`, `generation`, `partitions`, `vertices`, `source`,
`max_levels`, `alpha`, `beta`, `max_phase_budget`, `phase`, and `batch_rows`.
Source IDs are signed 64-bit integers. Counts are nonnegative integers; alpha and
beta must be positive. Unknown fields and incompatible repeated operation
parameters are rejected. The adapter guard is at most 128 native phases,
P<=64, and 1<=batch_rows<=65,536. These are qualification bounds, not new Sail or
algorithm limits.

For level cap K, compose:

```text
init(0, vertices, source-owned directed arcs)
  -> decide(0) -> apply(0)         # exact endpoint setup, incoming CSR for pull
  -> decide(1) -> apply(1)         # first BFS expansion
  ...
  -> decide(K) -> apply(K)
  -> result(K+1)
```

This is 2K+4 native stages, so the guard permits K<=62. Defaults retain K=14 and a32-stage budget. The client composes
these through existing lazy temporary views to keep each protobuf plan shallow;
the graph and iteration state still belong to one Sail job. There is no action
per level. Early convergence relays DONE through the remaining static phases;
the reported `levels` counts actual expansions. A source at maximum reachable
distance D needs D+1 expansions to prove an empty frontier. K=0 therefore fails
for every valid source, including an isolated source.

Every input receives the existing explicit integer range routing on `owner` with
split points `1..P-1`. Driver planning is schema-only. At runtime the factory
binds to the same authoritative job/package/operation and actual worker resource
pool. An operation cannot mix v1 reference PageRank, v2 residual PageRank, and v3
BFS. Init builds source-owned state; the adapter checks the prepared child
partition count and every actual owner value.

## Integer wire

All update/statistics fields are non-null Int64, in this order:

```text
owner, kind, producer, sequence, target, parent, value, mode, aux,
producer_worker, adjacency_id
```

Schema metadata pins protocol, algorithm, operation, snapshot, generation, phase,
P, N, and statistics/update channel. The native bound owner additionally pins the
complete request options. No v1/v2 float validation is relaxed.

Kinds are topology=0, candidate=1, membership=2, statistic=3, completion=4. Modes
are topology=0, reference=1, push=2, pull=3, done=4. Topology uses target/parent for
destination/source; candidates use target/parent/value for destination/parent/
integer distance; membership uses target for the frontier ID. Unused fields must
be zero. Per-owner sequences and pinned producer origins reject replay, drift,
misrouting, or channel mixing.

Each statistics producer emits exactly eight fields indexed by `target`:
vertices, arcs, source_count, reached, frontier, frontier_edges, remaining_edges,
levels. Values use `aux`. Completion sequence is eight and `aux` repeats the
producer vertex count. The adapter drains the input before submitting complete
reports and explicit statistics EOF to the core.

Update completion carries only this recipient's sequence and the producer's
**total** emitted count in `aux`. Topology totals must equal the pinned arc count;
pull recipient counts must equal the producer frontier count and total must equal
P times that count; DONE requires zero. The cursor computes total once and emits
one fixed-width marker per destination. Its internal P-element accounting vector
is never serialized. Completion control traffic is therefore O(P²) scalar
records rather than O(P³) scalar words.

Typed `BfsMessageValues` and `BfsCompletionValues` decode borrowed Arrow fields;
there is no per-row Vec/Arc allocation or 512-byte owned-message reservation in
the native wire path. Input rows arriving before the local phase is ready are
kept in an admitted queue. Own-origin rows require actual local state in the
correct barrier. The adapter drains both channels to EOF and finishes the core
barrier before publishing the next phase.

## Results, diagnostics, and close

Result columns are Int64 in the following order; `?` marks nullable:

```text
id, distance?, hops?, parent?, owner, worker_id, pid, adjacency_id,
incoming_adjacency_id, phase, levels, reached, converged
```

Distance and hops are identical integer values. Unreachable rows have null
traversal fields. Source parent equals source. `reached` is global; `converged` is
one only after the global zero-frontier certificate. Empty owners emit receipts
without adding fake vertex rows. Non-directional modes use incoming identity
zero; directional setup gives every owner, including an empty owner, a positive
immutable incoming identity.

Nullable result builders reserve both value/validity storage before allocation.
Each Arrow array retains the same reservation and Sail lease through slicing,
stream/plan/owner drop, and close. The existing explicit bound `close()` first
cancels the shared execution domain and then clears algorithm-owned partitions.
It is idempotent and rejects late state resurrection. No native lock is held
while a returned stream waits on downstream backpressure.

Receipts use the existing atomic bounded JSON-line writer and common flat scope
fields. Audit mode names are strings. Init/apply report local vertices,
`local_reached`, `frontier_vertices`, `frontier_edges`, `remaining_edges`, levels,
output_phase, and incoming identity. Apply includes local examined_edges,
examined_vertices, emitted_messages, received_messages. Decide reports mode,
levels, local_reached, frontier_vertices, and incoming identity. Result reports
converged, levels, **global** reached, local_reached, and incoming identity. Close
reports levels, local_reached, and incoming identity. Summing local counters is
explicitly separate from interpreting repeated global diagnostics.

A typed `BfsCapFailure` requires completed topology and complete global
statistics with frontier>0 and levels>=max_levels. Before its execution guard
cancels peers, the failing stage writes `event:"failure"`,
`code:"bfs_level_cap"`, `outcome:"nonconverged"`, levels, max_levels, **global**
frontier_vertices and reached. Missing source, unknown endpoint, quota, or plain
cancellation does not produce this receipt. A runtime negative gate must require
the matching typed cause, failed query, no result/retry, and complete cleanup;
the RPC may report peer cancellation first.

## Validation boundaries

Native gates exercise all three variants through real DataFusion range exchanges
for P=2,3,11 and both directed/normalized-undirected inputs. They include negative
IDs, parallel/self edges, unreachable vertices, numeric parent ties, empty
owners, static DONE tails, immutable CSR IDs, nullable slice lifetime after
close, source/endpoint/quota/cap failures, pending-channel cancellation, dropped
streams, malformed statistics, and false completion counts. The core additionally
has a nonvacuous dense fixture proving pull/hysteresis and early-exit work.

Run native tests and Clippy against a frozen combined checkout, with matching
Python/DataFusion/Arrow versions and an isolated Cargo target. Keep the known
pre-existing `mutation.rs` large-enum Clippy exception disclosed separately from
new warnings. Current development wheels use the dev profile and are functional
qualification artifacts; they are not benchmark binaries. Physical Sail gates,
Linux qualification, practical larger phase budgets, and performance measurements
remain separate evidence. WCC/SSSP follow this BFS work.

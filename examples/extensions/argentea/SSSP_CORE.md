# Argentea distributed SSSP

SSSP uses the existing job-bound worker factory, integer owner routing,
producer-complete barriers and admitted native storage. It adds no host transport,
graph scheduler or general iteration engine. The implemented core includes weighted CSR, path candidate ordering and the
producer-complete distributed state machine. The [Arrow adapter and Python client](SSSP_ADAPTER.md) are implemented; Sail
runtime qualification is still pending. Core tests exchange
messages between partition objects in one process; they are not evidence of
multiworker or two-host execution.

## Weighted storage and answers

`WeightedAdjacency` stores source-owned arcs with finite nonnegative FLOAT64
weights. It preserves duplicate arcs, self-loops, arbitrary signed BIGINT vertex
IDs and each input arc's target/weight pairing. Local vertices are sorted for
lookup; arcs retain input order within a source. Negative zero is canonicalized
to positive zero. A constructor validates local ownership and weights, but only
the subsequent distributed topology exchange can prove that remote destinations
exist. That exchange must cover unreachable components as well as reachable ones.

Build admission covers weighted CSR and sorting scratch before allocation. Work
charges cover sorting and binary searches. The finished CSR retains its admission
and host lease independently of the builder. Quota refusal, cancellation and
invalid input unwind the admission. Retaining an Arc retains graph storage;
releasing its last owner releases that storage and lease, not necessarily RSS.

`SsspLabel` admits only finite nonnegative distances and orders candidates by
(distance, hops, numeric predecessor). A zero-weight cycle cannot improve a
source's zero-hop label. A parent-only improvement changes the row's witness but
not any outgoing candidate, which names the current vertex as predecessor.
Unreachable vertices will use an absent label, not infinity as an output value.
Every emitted relaxation checks floating distance and hop overflow. An overflow
is an error even when another path dominates it, matching the current Banda
all-edge delta-star implementation. Bucket division rejects overflow and invalid
delta, rather than saturating or silently changing groups.

## Distributed algorithm contract

1. Build source-owned weighted partitions and exchange complete topology,
   proving one source vertex and every destination before relaxation.
2. Reference execution performs synchronous full-edge Bellman–Ford relaxation.
   It needs a complete zero-change barrier to certify termination.
3. Advanced execution uses the existing suite's **all-edge delta-star** semantics.
   Global statistics select the smallest active distance bucket. A bounded
   exchange relaxes all outgoing edges of active vertices in that bucket;
   improvements can reactivate vertices in the same bucket. Every owner,
   including empty owners, completes the barrier before selecting the next
   bucket. This is distinct from classical light/heavy delta-stepping.
4. Candidate reduction is an exact minimum over the admitted label order; it
   needs no floating-point sum. Distance additions remain ordinary FLOAT64 path
   arithmetic. Do not substitute epsilon comparisons or different weight types
   to claim agreement across implementations.
5. A cap fails the whole operation after a complete barrier. It never emits
   partial distances. Reports must distinguish candidate work, active vertices,
   bucket closure and certificate work. A zero frontier requires global evidence,
   not one empty partition.
6. Retain immutable output snapshots, prepaid leases, replay rejection and
   EOF-sensitive publication as in BFS/WCC. Validate source presence, ownership,
   missing targets, delivery counts, skew, cancellation and worker loss. Compare
   full vectors with an independent tiny-graph oracle before any benchmark.

Reference Bellman–Ford and advanced all-edge delta-star are the implemented core
methods. Existing Banda also has a driver-native Dijkstra reference; that is an
algorithm difference to disclose. Classical delta-stepping and rho-stepping are
comparison controls in the [traversal plan](../benchmarks/TRAVERSAL-PLAN.md), not
implemented Argentea methods. No fastest-implementation claim follows from these
core tests or from any one topology.

## Core validation and cost boundary

Run the standalone core tests from the Sail checkout:

```sh
CARGO_INCREMENTAL=0 cargo test --release \
  --manifest-path examples/extensions/argentea/Cargo.toml
```

The SSSP tests compare complete distance/hop/parent vectors with an independent
sequential Dijkstra oracle across owner counts and bucket widths. They cover
zero-weight cycles, duplicate arcs, signed ID extremes, unreachable vertices,
same-bucket reactivation, topology rejection, phase/origin/sequence replay,
truncated producer barriers, cap refusal, arithmetic overflow, successor-state
admission failure, incomplete-cursor cancellation and retained result leases.

The advanced cursor scans active vertices and emits only edges in the selected
global bucket. Later-bucket vertices remain active. Each completed exchange
currently copies the local label vector and scans the local candidate vector
to publish an immutable successor. This is O(local vertices) state work per
exchange even when the selected frontier is small. `examined_vertices` and
`examined_edges` count emission traversal, not this publication work, statistics
scans or allocation. Memory admission includes predecessor and successor state
while both coexist; it does not promise that the allocator returns pages to
the operating system immediately.

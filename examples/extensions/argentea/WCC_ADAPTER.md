# Argentea WCC native adapter

The Nutmeg wheel's `nutmeg.v4.ArgenteaWccApi` relation connects the
[WCC core](WCC_CORE.md) to Arrow execution plans. The adapter uses the same
job-bound worker owner, integer range routing, task group placement, memory
lease and close/cancel callbacks as Argentea PageRank and BFS. It introduces no
Sail host change. This is an implementation description; Python client and live
Sail qualification are still required before deployment instructions are complete.

## Request and execution

The strict JSON request specifies `version=4`, `algorithm=wcc_reference` or
`wcc_star`, `verb`, operation and snapshot identity, generation, partition and
vertex counts, `max_rounds`, `seed`, `max_phase_budget`, `phase`, and `batch_rows`.
Unknown fields are rejected. Owners are `id.rem_euclid(P)`; IDs retain signed
64-bit values. Initial inputs have BIGINT `(id, owner)` and `(src, dst, owner)`
columns. Edges are source-owned and interpreted as undirected by the core's
incoming topology exchange. Duplicates, loops, isolates and empty owners remain
represented.

The static plan emits initial statistics, followed by pairs of `decide` and
`apply` relations, then `result`. Each decide consumes a complete P-producer
statistics barrier and emits the core-selected message channel. Each apply
consumes all messages and completions through EOF before publishing successor
state. Core decisions select topology, reference propagation, neighbor labels,
root membership routing, assignments, normalization or completed-state relay.
The host continues to execute an ordinary bounded query DAG.

A reference cap K needs `2K+4` native stages; seeded star contraction needs
`6K+10`, including the final neighbor certificate and minimum-ID normalization.
The adapter permits at most128 stages (K<=62 reference, K<=19 star). This ceiling
is a validation target, not a claim that live Sail or the existing32-view Python
composer has already qualified that depth. Seed42 on the eight-vertex test graph
exceeds three contraction rounds; that case is retained as a failure control.
No seed or cap guarantees convergence for arbitrary graphs.

## Wire invariants

Statistics and messages use distinct schema metadata carrying protocol,
algorithm, operation, snapshot, generation, phase, partition and vertex counts.
Rows contain eleven non-null BIGINT fields: `owner`, `kind`, `producer`,
`sequence`, `target`, `source`, `value`, `mode`, `aux`, `producer_worker`, and
`adjacency_id`. Statistics carry seven scalar fields followed by one completion
per producer/recipient. Message completions carry the recipient's sequence and
a scalar producer total, avoiding a P-wide vector in every marker.

Membership messages distinguish an absent head candidate from the valid vertex
ID0 with `aux=0/1`. Root and label IDs use signed fields, including the full
BIGINT extrema. Counts and sequence numbers must fit nonnegative BIGINT values.
Producer origin remains fixed across phases. Duplicate fields, late rows,
missing completions, wrong phase/channel, replayed stage claims and changed
operation parameters fail the operation. Algorithms cannot be mixed under one
bound owner.

The result contains `id`, minimum-ID `component`, owner/worker/PID and adjacency
identities, phase, round count and `converged=1`. A cap failure emits a structured
`wcc_round_cap` audit with measured unresolved work and never returns partial
components. The first surfaced error can be a peer cancellation after the
original cap; audit evidence retains the original cause.

## Resource and test boundary

Input buffers, early message queues, producer reports, native adjacency and
state, and Arrow result storage share the admitted resource domain. Output
cursors retain snapshots and leases without holding partition locks during
backpressure. A retained Arrow slice keeps its lease after the worker closes;
failed range exchanges release leases after their asynchronous tasks terminate.

Adapter tests execute actual DataFusion integer range exchanges with an
in-process worker dispatcher, compare complete component vectors over multiple
owner counts, check signed extrema and retained slices, reject incomplete
statistics, and retain the three-round cap failure. They are not evidence of
physical two-host execution, a Linux wheel, or benchmark performance. Those gates
must use frozen source identities and separate receipts.

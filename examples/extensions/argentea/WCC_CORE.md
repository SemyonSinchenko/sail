# Argentea WCC core

This extension-owned core implements reference min-label propagation and seeded
head/tail star contraction. It supplies no scheduler, transport, Arrow adapter,
Python API or new Sail hook. Runtime qualification and performance are separate.

The advanced method differs from Banda/Pecan's GF64 closed-neighborhood
contraction. Those methods pick one neighbor by random priority, contract the
edge table, retain history and expand labels backward. Here every original
vertex retains a component root, and every original edge is scanned each round.
A round assigns deterministic heads/tails to roots. A tail chooses the smallest
numeric adjacent head; heads stay. Every member receives its old root's choice
before the next round. The resulting components remain stars, so no recursive
pointer-jumping or cross-job state is required. No-hook rounds consume the cap.
There is no claim of equivalent work, fewer physical edge reads or greater speed.

Input is source-owned directed arcs interpreted as undirected. Topology exchange
validates every destination, including disconnected components, and constructs
an admitted incoming CSR. Original outgoing and incoming arcs retain duplicates,
self-loops and all signed BIGINT IDs. Vertices belong to id.rem_euclid(P), including
isolates and empty owners. Initial roots are vertex IDs.

Advanced rounds use three producer-complete exchanges:

1. Neighbors: send each current root across outgoing and incoming arcs. Count
   crossing arcs and retain one minimum eligible head candidate per local vertex.
2. Route: each vertex sends its root, own ID and optional candidate to the root
   owner. The receiver verifies that the target is a current root, reduces the
   choice and retains admitted member pairs. This costs O(local vertices) at
   senders and up to O(component vertices) at a skewed root owner, not O(crossing
   edges) candidate storage.
3. Return: each root returns its single choice to every member. Every original
   vertex must receive exactly one response from its old root owner. Publish
   only after all producers complete and input reaches EOF; then count one round.

Coins use the low bit of SplitMix64's finalizer applied to
`seed XOR (round * 0x9e3779b97f4a7c15) XOR root_bits`, with wrapping u64 arithmetic;
rounds are zero-based and bit1 means head. This pins behavior across owner counts
and message arrival order, without claiming the coefficient stream used by
Banda/Pecan. A hook is nominated only by a member adjacent through an original
arc to the chosen head's component. Tail-to-head restrictions exclude cycles.

Termination requires a fresh complete neighbor exchange with zero crossing arcs.
This proves all original edges lie within components; every prior hook joins
connected components, so disconnected components cannot merge. A final Route
and Return pair independently reduces minimum original vertex ID at the root
owner and sends that minimum to every member. The representative need not be
that minimum. Reference propagation instead performs complete neighbor min-label
updates, including the final zero-change propagation. Its labels stay connected
to a source vertex and never increase; the zero-change barrier certifies the
minimum label on every connected component.

A static deployment needs init, topology decide/apply, bounded work and result.
Reference K propagations requires `2K+4` native stages. Advanced K contractions,
plus the final neighbor certificate and normalization exchanges, requires
`6K+10` native stages. A separately qualified 32-stage client would therefore
allow K<=14 reference or K<=3 advanced. These are bounds, not convergence claims.
After certified completion, remaining static phases relay DONE markers without
adjacency work. They still allocate and initialize admitted local inbox arrays
and P-wide control state; zero graph-work counters do not mean zero CPU or
allocation work. The counters also exclude snapshot-copy and sorting costs,
which remain subject to resource admission and work checks. Cap errors require a complete global barrier and never return
partial labels. Reference K=0 reports a pending, unattempted convergence
certificate with the measured change count zero; it does not invent an unresolved
vertex. Other reference caps report changed vertices, while advanced caps report
measured crossing arcs. Larger bounded DAGs or checkpoint continuation need separate
qualification; this core does not establish them.

All reports pin operation/snapshot/generation, options, phase, producer and
worker/adjacency origin. Every owner supplies one report and completion,
including empty owners. Duplicate, missing, wrong-phase, wrong-mode, changed
origin and late rows poison the state. A candidate is committed only after EOF
and successful admission of its successor. Cursors own immutable snapshots,
release owner locks before emitting, retain the host lease and cancel the shared
execution domain when dropped before completion. Incoming CSR construction,
root vectors, per-owner barriers, member buffers, retained messages and result
snapshots are charged to the existing execution context and prepaid Sail lease.
No RSS-zero claim follows from releasing those admissions.

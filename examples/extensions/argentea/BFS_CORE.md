# Argentea BFS core

This extension-owned core implements three integer BFS variants over retained
native partitions. This core alone does not provide a Sail relation, client, network transport,
or distributed execution qualification. The separate native adapter is described
in [BFS_NATIVE.md](BFS_NATIVE.md). The current kernels
are sequential within each partition. They reuse Argentea's admitted `Adjacency`,
`Operation`, `Resources`, and Sail memory lease; no host source changes are needed
for these primitives.

## Traversal contract

`BfsPartition::build` receives unique signed 64-bit vertex IDs and directed arcs,
partitioned by `id.rem_euclid(P)` and source owner respectively. Normalize an
undirected edge `(u,v)` to both `(u,v)` and `(v,u)` **before** source-owner routing,
as Pecan's traversal preparation already does. Parallel arcs and self loops are
retained. A self loop in undirected input consequently becomes two arcs.

The source must occur exactly once globally. A setup exchange visits **every
arc**, including disconnected components, and checks each destination against its
owner's vertices. This rejects unknown endpoints even when BFS would never reach
them. The sum of owner vertex counts must match `Operation.vertices`. This extra
full-graph validation cost belongs to setup and must remain in end-to-end reports.

Final `BfsRow` contains `id`, `distance: Option<u64>`, and `parent: Option<i64>`.
The source has distance zero and itself as parent. Unreachable vertices have both
fields absent; a future Arrow adapter can expose distance and hops from the same
integer value. Every reachable non-source vertex chooses the **smallest numeric
external ID** among its previous-level neighbors, irrespective of producer order.
The core never stores distances in floats.

The existing comparison contracts are in
`../graph-algorithms/src/pyspark_pecan/traversal_bfs.py`,
`../vendor/nutmeg-graph/src/optimized/bfs.rs`, and
`../benchmarks/traversal_reference.py`. The semantic gate uses an independent
queue BFS plus a separate full-edge minimum-parent pass. A second certificate
checks source/rooted tight parent paths and all directed edge inequalities;
neither condition alone proves shortest paths.

## Three variants

- **Reference:** scan every local outgoing arc each level, emitting only arcs
  whose source distance equals the current level. This is a deliberately simple
  level-synchronous full-scan reference.
- **Frontier:** traverse outgoing CSR rows of the current sparse frontier only.
  Target owners take the minimum parent for previously unvisited destinations.
- **DirectionOptimizing:** select sparse push or real pull using complete global
  integer statistics. Defaults `alpha=14`, `beta=24` match Banda. Enter pull when
  `frontier_edges * alpha > remaining_unvisited_outgoing_edges`. Continue pull
  while `frontier_vertices * beta >= total_vertices`; otherwise perform one push
  level before considering entry again. Products use `u128`, so no floating
  reduction or threshold rounding is needed.

Directional setup retains an additional admitted incoming CSR. Incoming source
IDs are sorted numerically within each destination. Pull scans unvisited local
vertices and stops at the first incoming parent present in the complete frontier;
sorting makes that early exit also the minimum-parent witness. Outgoing and
incoming CSR identities remain unchanged after setup through all later levels.

The first pull protocol broadcasts frontier IDs to **worker owners**, never to
the driver. Each receiver retains membership only for its local incoming ghost
IDs. It still receives `P * |frontier|` records in aggregate and performs binary
searches into the ghost index. This is a bounded correctness implementation,
**not** a claim of optimal communication. Source-to-consumer subscriptions can
later suppress irrelevant destinations using the same owner routing. Membership
allocation is bounded by local distinct incoming sources, not the entire global
vertex set. A byte per ghost is currently admitted conservatively for `Vec<bool>`.

Dense labels and a candidate label snapshot are retained per owner. Every active
level currently copies and scans local dense labels to publish atomically, even
in frontier mode. This `O(V_local)` work and repeated allocation must be included
in later performance analysis. Parallel source blocks, compact bitsets, targeted
membership delivery, and avoiding dense publication copies are future measured
optimizations; none are asserted by these tests.

`BfsWork` reports outgoing/pull edges examined, emission/pull vertices examined,
records emitted, and records received for the last completed phase. The counters
do not include sorting, CSR construction, dense snapshot copies, or all lookup
instructions. Topology has its own counter record. Tests require distinct work
for all three modes, including a dense layer where pull really exits early.

## Barrier protocol and lifecycle

Each partition starts in statistics phase zero. Its local source label is
provisional and cannot be exposed as a result before setup and convergence.

1. `statistics()` emits one admitted report per owner, including empty owners.
   `receive_statistics()` checks operation, snapshot, generation, phase, options,
   level, previous mode, immutable counts, and host-scoped worker/CSR origin.
   The receiver pins each origin at setup; later substitution is rejected.
2. After **input EOF**, `finish_statistics()` validates exactly all P reports,
   global vertex/source counts, and checked integer sums. Merely receiving P
   records does not permit a decision.
3. `start_emission()` returns an owned cursor with no partition lock. Phase zero
   emits topology arcs. Later phases emit target-owner integer candidates for
   reference/push or frontier membership for pull. Sequences are per
   producer-to-recipient stream. Pull IDs are unique and numerically ordered.
4. Every producer emits completion for every owner, including zero-message and
   empty owners. Receivers verify exact sequences, origin, and mode. Topology
   completions also match the producer's declared arc count; pull completions
   match its complete frontier count at every destination.
5. The adapter drains input to EOF, then calls `finish()`. Publication requires
   all P completions **and this owner's completed output cursor**. Pull only
   reads membership after this barrier. Next barrier/state memory is admitted
   before publishing labels or the incoming CSR.
6. A later complete statistics barrier proves convergence only when the sum of
   frontier sizes is zero. `seal()` then allows `row_cursor()`. An active
   frontier at `max_levels` errors and exposes no partial result. The expansion
   that proves emptiness counts: an isolate needs one level, and a path of
   distance D needs D+1 levels. `Done` relays allow a static tail after early
   convergence, retaining levels/results without touching adjacency.

The candidate native plan would use schema-only init, setup decide/apply, K
level decide/apply pairs, then result: **2K+4 native stages**, plus Sail stages.
`BfsOptions::native_phase_bound()` checks arithmetic, not deployment capacity.
No BFS phase-budget default is qualified yet. A future client should compose
stages using existing lazy temporary views, as the PageRank client does; it must
still gate actual stage placement, capacity, buffers, and cancellation.

These are fail-fast trusted-extension consistency checks, not an authenticated
or Byzantine-tolerant transport. Typed fields do not prove that a hostile
producer computed truthful labels. Actual worker scope and package/operation
binding remain the existing host's responsibility. The core contains no retries,
worker assignment, RPCs, storage changes, or new scheduler.

One `Resources` domain can cover all partitions in a worker operation. All native
CSR, ghost state, labels, inboxes, cursor metadata, reports, and retained messages
are admitted to the prepaid shared Sail lease. Incoming construction growth
admits the replacement while the old buffer is still live. Emitted messages own
admission independently of the cursor, and retain its identity allocation.
Dropping an unfinished cursor cancels the shared execution. Admission failure
poisons emission, so returning quota cannot resume after skipping an arc.
Committed labels survive a failed phase for diagnostics but remain uncertified.
Final row cursors retain graph/labels/lease after the partition owner drops;
cancellation stops further cursor reads without invalidating already copied rows.

## Core gates and remaining integration

Run from the repository root with an isolated target directory:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/sail-argentea-bfs-core-target \
  cargo test --manifest-path examples/extensions/argentea/Cargo.toml --locked --release -j 2
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/sail-argentea-bfs-core-target \
  cargo clippy --manifest-path examples/extensions/argentea/Cargo.toml --locked --all-targets -j 2 -- -D warnings
```

The BFS gates exercise P=1,2,3,11; empty owners; directed/normalized undirected
input; negative IDs; numeric parent ties; isolates/self/parallel arcs; unknown
unreachable endpoints; source/count checks; strict caps; DONE; stale/replayed/
foreign phases and origins; missing/late completion; incomplete or duplicate
membership; quota/work/cancellation; lease retention; and a one-message channel
that drains topology and real pull without holding the owner lock. A 400-vertex,
10,203-arc layered fixture asserts Push/Push/Pull/Pull/Push and measures fewer
than 250 examined edges on a 10,000-arc dense layer, with exactly 11*100 emitted
membership records. These are deterministic core counters, not benchmark timings.

The separate native adapter supplies borrowed Arrow fields, scalar completions,
retained nullable Arrow results, and typed cap receipts. Its gates are distinct
from these core gates. Actual optimized Sail job topology, strict native receipts,
worker failure/cap/cancel behavior, physical two-host execution, and measured
larger phase budgets require runtime qualification of the combined source. Core
tests alone establish none of those integration claims. WCC and SSSP remain
subsequent algorithm work.

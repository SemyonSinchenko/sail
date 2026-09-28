# Argentea: residual PageRank and BFS

This is the extension-owned continuation after the bounded reference PageRank
qualification. It adds no Sail iteration runtime, transport, allocator ABI or
cross-job native handle. The existing worker scope, placement validation, range
exchanges, memory lease and job cleanup remain the host boundary.

The initial reference wheel is frozen at
`e420cafc1ea03e889d6db11f212c9659c374346b`. The residual wheel at
`f11fe6e8a091e4be56a21712850d2a1a205c3e6c` and public client at
`010b9e065ad2adc33a7169de73191a039ce8d6d0` now pass bounded residual execution
on actual workers and both physical hosts. Existing lazy temporary views keep
each client registration shallow while the final materialization runs one
native job; no host depth-limit increase is needed. Typed cap failure reporting
remains under repair after one run surfaced peer cancellation. The
[evidence index](argentea-validation/README.md) retains every outcome.
BFS core qualification remains separate from native adapter and distributed
qualification; larger phase budgets and performance are not established here.

## Existing contracts

| Implementation | Relevant source | Contract to retain |
| --- | --- | --- |
| Banda residual PageRank | [`optimized/pagerank.rs`](../../../examples/extensions/vendor/nutmeg-graph/src/optimized/pagerank.rs), especially lines 50–99 and 163–229 | Signed residual pushes, tolerance-scaled activation, reactivation, uniform dangling redistribution and a freshly recomputed normalized certificate. At the cap it returns normalized rows with `converged=false`. |
| Pecan residual PageRank | [`pagerank_delta.py`](../../../examples/extensions/graph-algorithms/src/pyspark_pecan/pagerank_delta.py) | The same residual equations and activation policy. A failed certificate rebases state; exhaustion raises `ConvergenceError`. The public result exposes residual and its stationary error bound. |
| Reference PageRank | [`algorithms.py`](../../../examples/extensions/graph-algorithms/src/pyspark_pecan/algorithms.py), lines 117–197; [`argentea/src/pagerank.rs`](../../../examples/extensions/argentea/src/pagerank.rs) | Full power updates from uniform ranks, duplicate arcs/self-loops retained, dangling rank redistributed. Fixed K is not convergence. Pecan's optional power stopping criterion is successive-iterate L1 difference, not the advanced certificate. |
| Banda direction-optimizing BFS | [`optimized/bfs.rs`](../../../examples/extensions/vendor/nutmeg-graph/src/optimized/bfs.rs) | Sparse outgoing push; dense incoming pull with early exit; alpha/beta switching and hysteresis; exact hop distances and a valid parent tree. |
| Pecan BFS | [`traversal.py`](../../../examples/extensions/graph-algorithms/src/pyspark_pecan/traversal.py), [`traversal_bfs.py`](../../../examples/extensions/graph-algorithms/src/pyspark_pecan/traversal_bfs.py) | Reference expands all reached vertices; frontier expands changed vertices. Push/pull uses relational joins without native adjacency early exit. Source exists and parents itself; unreachable fields are null; the cap includes the empty-change certificate. |

Banda and Pecan do not promise identical BFS parents. Banda push minimizes the
dense parent index within a level; pull uses the first matching incoming parent.
Pecan minimizes numeric parent ID. Cross-implementation validation must compare
exact distances and a valid parent tree, unless parent ordering is explicitly
standardized. Banda's local PageRank reduction order is fixed across thread
widths; Pecan does not promise bitwise-stable distributed sums.

## Residual PageRank equations

Let `d` be damping, `a=1-d>0`, `N` the global vertex count, and `P` the
column-stochastic transition including uniform dangling columns. Define
`T(x)=a/N+d*P*x`. Keep local slices of scores `x` and **signed** pending residual
`r=T(x)-x`. Start with uniform scores and a full transition/certificate.

At each push, all owners use the same global scalars
`m=sum(x)` and `R=sum(abs(r))`. Set

```
theta = min(R/(2*N), tolerance*m/(4*N))
push_i = r_i if abs(r_i) > theta else 0
x_i += push_i
r_i -= push_i
r += d*P*push
```

An active source sends `d*push_i/out_degree_i` along each outgoing arc. For
dangling sources, reduce `D=sum(push_i)` over the active dangling vertices and
add `d*D/N` to every local residual. Both contributions and D can be negative.
There is no new restart term on a residual push; restart belongs to T during
initialization and certification. Inactive residual is retained. Later incoming
updates can reactivate a vertex; monotonic frontier shrinkage is not promised.

The relative cutoff leaves at most R/2 inactive, giving
`||r_next||1 <= ((1+d)/2)*R` in exact arithmetic. The normalized residual of
`y=x/m` is at most `2*R/m`: since `sum(r)=a*(1-m)`,
`T(y)-y=(r+(m-1)*a/N)/m`. This bound only **triggers** certification.

Certification normalizes with the actual global m, executes a full transition,
and reduces `C=sum(abs(T(y)-y))` over every vertex. Convergence requires
`C<=tolerance`. The stationary L1 error bound is `C/a` in exact arithmetic.
A failed certificate sets `x=y` and `r=T(y)-y` before further pushes. Check
finite scores/residuals, positive global mass, unchanged global cardinality,
and nonnegative scores. Signed residual must never be clamped to zero.

Pecan explicitly checks the minimum score; Banda's internal loop checks mass
and residual finiteness, while benchmark validation separately checks its output.
Argentea should enforce the stronger score check in its own typed state.

## Required global barriers and bounded plan

Post-update R and m cannot be attached to contributions emitted *before* their
receiver applies that update. Advanced execution therefore needs an additional
P-wide statistics exchange. Reusing the old dangling marker alone would leave
the next cutoff based on incomplete or stale global state.

The first implementation uses P-wide typed statistics/completion streams.
This is not the only possible global reduction: the host placement repair
separates operation stage groups, so an ordinary Sail aggregate followed by
broadcast can be evaluated later without assuming that an intervening scalar
stage moves native state. Actual planned topology must still pass validation.

Use two worker phases per work operation, all at the same P:

1. **Decide and emit:** consume exactly one statistics set from every owner;
   reduce it in producer order; choose PUSH, CERTIFY or DONE consistently;
   acquire an owned snapshot and emit contributions plus completion markers.
2. **Apply and report:** consume every producer to EOF, validate sequences and
   identities, apply the complete update or certificate locally, and broadcast
   local statistics to all P owners through ordinary owner-range exchanges.

Initialization builds CSR and emits initial mass/cardinality statistics.
The first work operation is a full certificate. A PUSH reports its updated
local mass, residual L1, minimum score, active vertices/edges and reactivations.
A CERTIFY reports freshly recomputed certificate L1 and normalized mass.
Each statistics kind has a declared reduction: sums for mass/L1/counts,
minimum for score minimum. Store producer-indexed scalar slots and reduce in
the same order on every owner so floating summation cannot cause participants
to choose different work modes within the same job.

The terminal result phase receives the final global certificate decision before
emitting rank rows. A stationary uniform graph uses zero frontier pushes.
`max_pushes` counts only pushes; initialization and certificate passes have
separate counters. Force a full certificate at the push cap. The default public
operation raises explicit nonconvergence if it fails, following Pecan. If a
future diagnostic API returns capped rows like Banda, it must mark them
`converged=false`; it is a separate, explicit policy.

The static DAG reserves slots for the worst case: one initial certificate, K
pushes and up to K subsequent certificate attempts. With initialization and
result, this is at most **4K+4 native phase stages**, before ordinary host scan,
shuffle and sink stages. A certificate failure is followed by another push,
not an unbounded sequence of certificate retries.

The existing reference limit of 32 fixed iterations must not silently become
32 residual pushes: K=32 needs up to 132 native phase stages. For the first
advanced gate, use an explicit 32-native-phase budget, permitting K<=7, and
qualify small K first. This does not claim normal `1e-8` workloads converge
within seven pushes. Reject an over-budget plan before execution; measure task
and live-buffer costs before increasing the bound.

After global convergence, all remaining planned slots relay validated DONE
markers and preserve the certified snapshot. They perform no adjacency work,
but still incur scheduling/shuffle costs. They must not cancel the shared
execution context or skip a producer: either would break downstream barriers.
Native iteration counters remain fixed while transport slots advance.

If bounded plans are insufficient, the smallest continuation without new Sail
hooks is an explicit checkpoint to Sail-owned tables, followed by a fresh job.
Persist x, r, actual push count, activity flags, snapshot/options and a new
checkpoint generation; recompute/validate global statistics when resuming.
Each new job rebuilds and readmits CSR. That is O(V) checkpoint I/O plus graph
reconstruction, loses cross-job native adjacency reuse, and must be included in
its execution class and measurements. It is not an implicit completion of the
persistent-state design. A distributed feedback loop inside the current DAG is
not established by local DataFusion recursion support.

## Wire and native state changes

Preserve version-1 reference requests and their positive-contribution checks.
Use a separately versioned, algorithm-tagged worker payload and message schema
for advanced execution. Current
[`input.rs`](../../../examples/extensions/nutmeg/src/argentea/input.rs) and
`PageRankPartition::receive_inner/finish_producer` reject negative values;
globally relaxing those checks would weaken the reference protocol.

Retain owner, producer, sequence, vertex/target, value, worker and adjacency
identity. Add a signed BIGINT auxiliary field for integer statistics or a BFS
parent. Distinguish update, floating statistic, integer statistic and completion
kinds. In particular, preserve BIGINT parents/counts without encoding them in
DOUBLE. Phase metadata pins algorithm, protocol, transport slot, operation,
snapshot, generation, P and N. Completion announces the selected work mode,
final sequence and local cardinality; mode disagreement fails the operation.
PR push updates/dangling values allow finite signed numbers; full certificate
contributions remain nonnegative. Every producer, including empty owners,
must supply its required statistics and completion before a decision.

Extract the existing immutable admitted CSR into an extension-local module,
shared by reference PR, residual PR and BFS. Add a focused residual partition
state with x, r, activity flags, phase/counter state and admitted inboxes. Reuse
the existing owned-cursor, lease, cancellation and early-message-buffer patterns.
Do not add a generic algorithm framework or another published graph library.
Factor the affine full transition used by reference PR and certification; a
certificate retains y rather than replacing it with T(y).

The Banda implementation supplies the equations, fixed-source-block contribution
combining and regression fixtures. It is not directly callable on a distributed
partition: its `GraphProjection` assumes a complete local graph. Port that state
transition explicitly and compare against it; do not rename full power PageRank
or claim a call to the local kernel makes execution distributed.

## Residual adapter implementation boundary

Keep version-1 reference parsing and execution intact. Register a second relation
type, `type.googleapis.com/nutmeg.v2.ArgenteaDeltaApi`, in the existing Argentea
worker entry point. This changes the combined wheel identity; install the same
new wheel on both hosts after qualification. It adds no host extension API.

The version-2 request pins algorithm/options, graph identity, owner count,
`max_pushes`, phase budget, batch size, phase number and one of four verbs:

| Verb | Input | Core action | Output |
| --- | --- | --- | --- |
| `init` | Owned vertices and source-owned edges | Build and publish `DeltaPartition` before output | Statistics for phase 0 |
| `decide` | Complete statistics for phase j | Producer-ordered decision and owned emission cursor | Contributions/completions for phase j |
| `apply` | Complete contributions for phase j | Finish and commit candidate after EOF | Statistics for phase j+1 |
| `result` | Complete final statistics | `seal`; require a passed certificate | Certified rank rows and diagnostics |

Build one init, `2K+1` decide/apply pairs, and one result: exactly `4K+4` native
stages. A converged intermediate decision uses DONE relays; only the result verb
seals. If the final `seal` returns no certificate, fail explicitly even if a
programming error produced a shorter plan. The initial client budget of 32
native stages permits K<=7. It does not promise convergence for normal workloads.

The core's owned `DeltaStatistics` deliberately keeps its admission private.
Arrow decoding therefore needs a borrowed, typed statistics-values entry point,
mirroring `receive_values`; it must not fabricate ownership tokens. Add cheap
collecting/receiving phase accessors for the early-buffer readiness guard.
These are extension-core additions, not Sail hooks.

Use separate v2 input/output modules with shared admitted batch ownership and
input-cancellation helpers. The new message schema can retain one FLOAT64 value
column and add BIGINT `mode` and `aux` columns. Nine scalar records describe one
producer report: three floating values (mass, residual L1, minimum score) and
six integer values (vertices, pushes, certificate passes, active vertices,
active edges, reactivations). A final completion closes that report. Decode
exactly one of every required scalar; validate sequence, mode, cardinality and
origin before submitting it to the core. Drain the entire input before deciding.
All records use ordinary owner-range routing, including empty-owner reports.

Only the minimum-score statistic may carry positive infinity, and only when its
producer's vertex count is zero. Arrow supports that value. Requests and JSON
audit records must not silently use nonfinite JSON numbers; represent an empty
minimum explicitly as null in diagnostic JSON. Negative finite values are valid
only for push contributions and push dangling mass. Certificates retain the
nonnegative transition checks.

Retain the v1 shared execution domain, job-scoped owner, late-close rejection,
owned cursors and Arrow lease attachment. Registry configuration must forbid
mixing reference and residual algorithms under one operation token. Claim keys
include protocol, verb, phase and partition. The early queue must distinguish
collecting statistics from receiving contributions at the same phase number;
matching only `next_phase` is insufficient. Pin producer worker/adjacency origins
across phases, and verify the local producer against the actual local state.

Result diagnostics should carry actual push count, certificate-pass count,
certified residual and its stationary error bound, with `converged=true` only
after sealing. Additional floating result columns need a v2 result builder:
the current `BatchBuilder` assumes exactly one FLOAT64 column. Empty owners emit
no fake vertex rows; their audit receipts retain the same certificate metadata.
Keep transport phases, logical pushes and certificate passes distinct in audit
and client result APIs. Public rank columns can remain `id,pagerank`.

Adapter gates must add signed row round trips, split/reordered statistics
batches, empty-owner infinity, missing/duplicate fields, wrong phase/channel,
late rows after completion, quota refusal for queues, cancellation while waiting
at either barrier, retained Arrow slices after close, and the actual static
stage-count assertion. Then repeat worker-process and two-host provenance gates.

## BFS after residual PageRank

The first BFS slice reuses outgoing CSR with visited/depth/parent arrays and a
sparse local frontier. In level l, send `(destination, source_parent, l+1)` only
from frontier sources. Owners combine all same-level candidates, choose the
minimum numeric parent, and commit newly reached vertices only after complete
input. Source depth is zero and parent is itself; unreachable fields are null.
Support directed graphs first; undirected mode explicitly adds reverse arcs.
Duplicates and self-loops cannot cause repeated discovery.

Retain a reference mode that emits from all reached vertices, alongside the
frontier mode. Report active sources, visited arcs, wire rows and discoveries
separately. A frontier implementation is not yet direction-optimizing BFS.

Pure frontier BFS needs no global cutoff before its next local expansion.
Piggyback the newly discovered frontier count on outgoing completion records.
Global stopping may be recognized one exchange later; count actual BFS levels,
not that metadata-only exchange. Reserve a final **seal** stage that consumes
the Kth expansion, emits only updated discovery statistics, and performs no
K+1 edge expansion. A result stage reduces those statistics: zero new frontier
certifies completion; a nonzero frontier at the cap is explicit nonconvergence.
This shape uses K+2 native stages. Drain and validate all input even when a
preceding summary indicates completion. Source-presence and vertex-count totals
must also be globally verified.

Direction optimization is the next BFS slice. It requires incoming CSR for each
owned destination in addition to outgoing CSR. Supply a separately range-routed
incoming-edge input through the existing input envelope; changing the package's
input arity from two to three requires no host ABI change. Validate the resulting
actual job topology rather than assuming the extra branch preserves placement.

For pull, send frontier membership to owners containing that source's outgoing
neighbors. Build admitted, deduplicated destination-owner lists per source, and
an incoming-source/ghost-ID index at each receiver. This avoids broadcasting
every frontier ID to every owner. It still has worst-case replication up to P
and must account for it. Once complete membership arrives, scan unvisited local
destinations' incoming adjacency and stop at the first active source. Sorting
incoming IDs numerically during construction permits both early exit and the
declared minimum-parent rule; include that construction cost.

Use a P-wide statistics phase before choosing direction from global current
frontier edge volume and remaining outgoing volume, preserving Banda's alpha=14,
beta=24 and one-push-round hysteresis initially. These are heuristics, not a
claim of universally fastest thresholds. Use one initialization/statistics stage, K pairs of decide/emit and
apply/report stages, and one final result stage: **2K+2 native stages** for K
expansions, before ordinary host stages. Each apply/report stage emits statistics
without expanding the next frontier; the final result requires its global new
frontier count to be zero. Pin this exact builder count in tests before exposing
its limit; do not reuse the push-only stage budget unchanged.

## Admission, ownership and parallel work

Keep every partition in the existing job/operation owner. Its single execution
domain shares the prepaid `MemoryLease` admitted from the actual worker's
DataFusion pool. Admit CSR, incoming/ghost indices, dense vectors, old snapshots,
statistics slots, early queues, contribution combining, output buffers and
per-phase traces before allocation. A graph fitting reference PR may correctly
fail advanced admission because it needs more live state.

Use one bounded kernel pool per worker operation, not one full-machine pool per
partition or phase. Reuse Banda's fixed source-block grouping for floating
combining; pool width may change disjoint work, not reduction grouping. Limit
concurrent scratch against the same execution domain. Thread stacks and RSS
remain distinct from tracked data-buffer admission. Shared-pool contention with
ordinary DataFusion must be tested, not inferred from isolated counters.

No owner lock spans backpressure or input awaits. Immutable cursors retain
admitted snapshots; dropped unfinished streams cancel the operation. CloseJob
and late publication retain the current tombstone contract. Arrow slices keep
their own admission and lease after close. A DONE algorithm state is distinct
from cancellation and cannot invalidate a retained certified result.

## Implementation and validation order

1. **Baseline gate:** real Sail reference PR, actual planned topology, owner
   routing, empty owners, stable adjacency identities, two worker PIDs and then
   two physical hosts. Preserve failures as evidence; do not edit the frozen
   wheel while repairing host routing.
2. **Residual core:** shared adjacency/transition, signed pushes, producer-indexed
   statistics, normalization, certificate/rebase and explicit cap. Compare with
   an independent dense implementation and existing Banda/Pecan fixtures.
3. **Advanced adapter/client:** versioned messages, bounded phase builder, actual
   stage-count assertion, terminal relays, strict cap handling and diagnostics.
   Run the same DataFusion exchange/lease tests, then actual Sail workers.
4. **BFS core and adapter:** reference/frontier modes, seal/global-stop protocol,
   exact distance and parent-tree validation; then incoming CSR, membership
   exchange and measured push/pull switching.

Minimum regressions include:

- PR graph `0->1, 1->1` with vertex 2 dangling: uniform initialization produces
  negative residual at 0 and 2. Exercise negative cross-owner edge and dangling
  pushes rather than a fixture whose residual happens to be nonnegative.
- The existing nine-vertex Banda fixture with reactivation, plus its settling
  components fixture. Require actual inactive-edge avoidance and reactivation
  counters; compare freshly certified normalized output, not equal push counts.
- Stationary cycle and all-dangling graphs with zero pushes; nonfinite/negative
  score rejection; a cap that fails certification; and a deliberately perturbed
  maintained residual proving the full certificate prevents a false success and
  correctly rebases before continuation.
- Uneven partitions and empty owners at P=1,2,3,11; identical global cutoff and
  mode within each job under reordered scalar arrival; duplicate, missing,
  wrong-phase and late-after-completion rows; wrong N and source-presence totals.
- BFS cross-owner diamond with competing parents, signed/extreme BIGINT IDs,
  disconnected vertices, self-loops, duplicates and directed/undirected cases.
  Compare distances exactly and independently validate every returned parent.
  A chain of depth K tests that the extra empty-frontier certificate is required;
  a cap is never accepted just because no more transport slots remain.
- Pull fixtures large enough to exercise real parallel blocks, with transitions
  push→pull→push, a remote frontier parent and an isolate-heavy scan. Assert
  actual early exit and examined-edge counts, not an algorithm name or timing.
- Quota refusal for additional vectors/indices, DataFusion/native pool
  contention, close while awaiting statistics, stream drop during certification,
  and retained Arrow slices. Run concurrency checks in release under CPU load
  and on a second host before treating local successes as distributed evidence.

Performance comparison comes after these gates. Keep reference and advanced
variants separate, include initialization/certificates/checkpoint costs, and
report unsupported, nonconverged, failed and timed-out cells distinctly.

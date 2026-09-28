# Argentea integration: the smallest first Sail change

Status: the bounded reference PageRank path passes its first live two-worker
process qualification. Native CSR partitions persist across two rounds; the
result, empty-owner participation, actual task placement and job cleanup are
audited. Live quota reuse also passes after successful and failed operations.
The reference path additionally passes across Capitola and Morrobay using
explicit SSH-forwarded Sail connections. Certified residual PageRank also
passes bounded worker-process and physical two-host tests using existing
temporary views. BFS passes physical two-host qualification. Worker-loss and
active-cancellation controls pass on two Capitola worker processes after a focused
failed-job task-state cleanup repair; WCC and weighted SSSP pass process-cluster
qualification. This is a functional result,
not a distributed performance measurement.
This is the gap inventory for [the Argentea scope](argentea-plan.md), with an
initial route that reuses Sail's stage grouping for worker placement.

## Upstream foundation: the session factory hook

[Merged PR #2630](https://github.com/lakehq/sail/pull/2630), commit
`e976c8b317e2b920fca0ed05f9a2efa92a0cff1c`, is already an ancestor of the
extension branch. Its `serve_with_session_factory`,
`create_spark_session_manager_with_factory` and `SparkSessionMutator` interfaces
remain intact. Argentea needs no additional embedding API.

The prototype's package loader currently runs inside
`ServerSessionFactory::create_session_state`, rather than being supplied by an
out-of-tree factory caller. The upstream hook establishes session customization;
it does not load packages on workers, decode foreign native regions, admit their
memory or give their state a job lifetime. Those are the focused gaps below.
Worker bootstrap reuses the existing `WorkerSessionFactory` and creates bindings
only after the actual worker `RuntimeEnv` is available. Prior extension-branch
shutdown/lifecycle changes in Spark Connect are separate from factory selection.

## Start with one bounded job

The first two-host spike unrolls two full PageRank rounds into one Sail
physical job. Use the existing Flight shuffle and keep all graph stages at the
same partition count and operation group. Sail assigns matching partitions of
equal-width stages in one pipelined region and slot-sharing group to the same
task set and worker. Ordinary stages must be excluded from that group: their
different widths can shift its task-set offsets. This is a
smaller starting point than preserving state across independently submitted
queries.

The evidence is executable, not an assumption about hash partitioning:

- `JobScheduler::build_task_region` and `StageGroup` in
  `crates/sail-execution/src/driver/job_scheduler/core.rs` group equal-placement,
  equal-group stages into task sets. Equal partition counts give offset zero.
- `examples/extensions/argentea/tests/scheduler_probe.rs` compiles the actual
  `TaskAssigner` source and extracts the actual `StageGroup` implementation at
  build time. It proves equal-width grouping at 2, 3 and 16 partitions.
- A negative control puts a one-partition stage in the same slot group between
  two two-partition stages: partition zero changes task-set bucket. Any wider
  stage or intervening width in that group can also shift ownership. The host
  now isolates operation-bearing stages with Sail's existing `Stage.group`
  field. Topology validation still compares every stateful occurrence; scalar
  reductions that move the native operation itself or create a blocking region
  remain invalid.
- A second negative control submits independent queries with unrelated work in
  between: logical partition zero moves from worker 2 to worker 1. Ordinary
  cross-query repartitioning supplies no persistent owner affinity.

These focused scheduler-source tests establish the placement prerequisites.
The live integration gate additionally inspects the actual job graph and
retains worker PIDs, partition IDs, native adjacency identities and round
receipts; its first successful result is described below.

The host now also has a pre-admission topology validator. It examines the
completed `JobGraph` and `JobTopology` before creating the output task or
inserting a job. Every worker descriptor must match its containing worker
stage's partition count. For all occurrences of the same package and operation,
each logical partition must map to the same region, slot group and task-set
bucket. Validation and scheduling call the same `StageGroup` construction and
bucket method, including the effects of ordinary intervening stages. Independent
operations remain independent unless their descriptors share a physical stage;
those operations share a slot group transitively. Ordinary jobs receive no
placement restriction. Forward-only regions may remain sliced by partition when ownership
is preserved. Actual-graph tests cover aligned widths, a scalar or wider
intermediate stage, a blocking boundary, nested descriptors and independent
operation/package identities. These host tests pass at
`0d3a0b753b47da9901f014a47d5db92bc330021a`; native allocation/worker receipts
are still required for runtime qualification.

## Round shape and barrier

Each partition retains its outgoing CSR and rank vector. Its round emits native
contributions tagged with destination owner. At the end it sends a completion
marker, final producer-to-destination sequence number, local vertex count and dangling mass
to **every** destination, including destinations receiving no contributions.
Every receiver consumes its entire input, requires all producers' completion
markers and verifies their total vertex count against N before applying the
round. It sums the dangling scalars in producer order and uses the ordinary
probability-normalized PageRank update. The scalar traffic is O(P²) for P
partitions; adjacency remains partitioned.

This avoids a separate one-partition reduction stage in the first spike. The
messages travel through Sail's shuffle, not a new network protocol. Existing
`Partitioning::Range` / `OutputDistribution::Range` can internally express routing on an
explicit destination-owner column with split points 1 through P−1. The worker relation bootstrap must request this routing explicitly; the host
constructs its own visible range repartition around partition-preserving child
plans. This uses Sail's existing `ExplicitRepartitionExec`: protocol routing
must survive physical optimization before `RewriteExplicitRepartition` lowers
it to the ordinary DataFusion exchange. A focused experiment on host candidate
`866d84903fa03abe6c8fa8c4b044ed27929dd96f` showed that DataFusion's
`EnsureRequirements` removes an ordinary `RepartitionExec` beneath the worker
wrapper, changing its input from two partitions to one. The regression runs
the complete Sail physical optimizer sequence with target width eight and
protocol width three, then checks exact owner routing and an empty channel.
The nested source fix and regression pass the combined host gate at
`5d38d3e16ad25dc63f572c0a8d38ff06ff84e46d`. No new
optimizer rule is introduced. A foreign FFI repartition is not automatically
visible to Sail's planner.
Qualification must execute the real `BatchPartitioner` and shuffled plan for boundary values,
empty channels and duplicates. Hashing an owner column is **not** equivalent to
routing owner p to task partition p.

The extension core checks operation, package, session, snapshot, generation and
round identity; sequence numbers; exactly one completion per producer; valid
owned destination IDs; and complete input before rank publication. Emission
failure poisons the round. These checks detect replay/truncation at the typed
boundary, but the Sail adapter must check identities against host-issued scope, preserve each
producer's order and invalidate the whole operation on any failure. A valid
marker must be emitted only after successful upstream exhaustion.

## Focused host gaps

The baseline below is commit `9c9ea46c88acb9e81354df6c0b2e979c30f114db`,
before the Argentea hooks. The inventory records why each hook is necessary;
passing isolated tests does not close its runtime qualification requirement.


| Gap | Baseline evidence | Smallest first change and gate |
| --- | --- | --- |
| Worker native relation decoding | `extensions/mod.rs::register_extensions` rejects distributed non-driver relations. `load_worker_extensions` installs scalar functions only. `proto/codec.rs::RemoteExecutionCodec` handles `DriverExtensionExec` and built-ins, with no package worker relation decoder. | Add one generic serializable worker-native wrapper and an identity-checked worker factory registry. Reuse the existing entry point, pinned manifest checks, Python binding and DataFusion FFI plan capsules. Test local/remote decode, absent package, wrong identity/version, child schema/arity, and that no driver pointer is serialized. |
| Worker native admission | `session_factory/worker.rs::WorkerSessionFactory` already has an explicit shared `MemoryResourceDomain`; the manifest permits `memory_bytes` only for driver placement. | Admit a worker operation quota from the exact worker runtime pool and pass the existing `MemoryLease` capsule through `bind_with_resources`. Reuse `NativeResourceTracker`; do not introduce a second allocator ABI. Test contention with a DataFusion allocation and final-owner release. |
| Job-owned native state | `TaskRunnerActor::handle_close_job` cancels task streams and removes local shuffle streams; it has no callback for extension-retained partitions. | Give the worker factory a registry scoped to host-issued job/session identity and a job-local operation label, attach it to existing job close/cancellation/shutdown, and call the bound owner's required `close()` method before dropping registry references. Active readers retain admitted storage until final release. No cross-job persistence is needed in the first spike. Test cancellation during build/round, worker disconnect, output retention and final quota release. |
| Retry safety | `JobScheduler::update_task_regions` forces one attempt only for regions containing `DriverExtensionExec`; ordinary worker tasks use configured retries. | Disable retries for every region of a job containing worker-native state, including ordinary upstream regions that could otherwise replay its input. A lost acknowledgement aborts the whole job. Test that a configured retry count above one does not replay an Argentea round, while jobs without worker-native state retain current behavior. |
| Placement and validation | `TaskPlacement` supports only Driver/Worker; `TaskSlotAssigner::next` balances available slots. Existing `Stage.group` isolates slot-sharing groups, but the planner leaves it empty. The real two-input plan interleaves an ordinary source between native stages in breadth-first region order. | Assign operation-bearing stages to existing slot groups after graph construction; unify operations that share a stage transitively. Keep ordinary groups unchanged. Validate actual region, width, placement and task-set bucket before execution. Test the original negative layout, two inputs, transitive groups, slot refusal, blocking boundaries and mixed native widths. No new worker-owner map or task scheduler is needed. |

The new `sail-common-datafusion::worker_extension` module contains the generic
`WorkerDescriptor`, `WorkerExtensionExec`, `WorkerExtensionRegistry` and async
`WorkerExtensionFactory` contract. Its codec checks exact package identity before
native layout access. `WorkerTaskScope` carries the actual host task/job identity,
not identifiers decoded from extension payloads. The registry tombstones closed
jobs before invoking factory cleanup; the factory must also reject publication
that races with closure. Five isolated boundary tests cover malformed descriptors,
wrong package identity, authoritative scope, output schema/partition mismatches
and a delayed-materialization/close race. The host gate at the revision above
passed 253 tests across the three affected crates and all-target Clippy. This
verdict predates the nested-routing repair described below and does not cover
native execution on separate workers.

Worker-role bindings must provide a callable, idempotent `close()` method. Job
closure first tombstones the job and removes its owners under the registry
mutex, then calls every owner outside that mutex. The callback cancels producers
and wakes pending work; it must not invalidate retained Arrow buffers. Errors
are logged with package, job and operation identity while other owners are
still closed. Repeated job closure does not invoke a removed owner twice.
Dropping registry references alone is insufficient because a live plan or
stream can retain the owner. Lifecycle tests hold a foreign plan, stream and
Arrow output across closure, observe the callback before their release, and
check that the final Arrow owner returns the quota. A separate test covers a
failing callback alongside another operation. These host tests pass; they do
not establish cancellation of a native algorithm across two workers.
Existing driver/scalar owner finalization is unchanged. Worker task scope and
its registry are installed only when the session has worker-role relations.

The worker wrapper exposes ordinary host children for Sail stage planning
and rebuild its native region only after `TaskPreparation::rewrite_shuffle`
substitutes real input streams. The existing driver wrapper demonstrates this
FFI boundary pattern. Registering a foreign Rust plan type directly in Sail's
hard-coded codec or relaxing driver placement checks would not solve it.

Package loading must stay process-lifetime; mutable graph state must stay
job/operation-lifetime. Reuse `retain_package` for callbacks that outlive a query,
while the worker operation registry owns only its own partitions and leases.
A package digest identifies code compatibility; it is not a session capability.

## Why not keep the client loop unchanged?

Pecan currently submits successive jobs. Keeping that shape for Argentea would
require an operation-scoped owner map `(operation, partition) -> worker`, a
worker-liveness pin between rounds, authenticated state lookup, explicit abort
on owner loss, and cleanup on close/session expiry. The existing idle check only
sees task slots and local streams; a retained native graph is invisible after
job cleanup. The source probe verifies that behavior.

Such affinity needs to last only for one algorithm invocation. It must never
bind a vertex partition permanently across sessions or silently recreate state
on a replacement worker. This can be a later focused addition if adaptive rounds
cannot be implemented acceptably inside an existing job lifetime.

Unrolling is a bounded integration experiment, not a solution for thousands of
rounds. Every round adds tasks, live plan objects and potential shuffle buffers.
A logical task slot can contain several stages; that does not eliminate their
memory or scheduling cost. The gate must measure these costs and reject an
oversized plan. The first acceptance target is two rounds and two worker
processes, then the same job across Capitola and Morrobay.

Existing `RecursiveQueryExec`/`WorkTableExec` serialization alone is not evidence
of distributed feedback, shared native state or global convergence. Do not turn
that into a new iteration framework merely to avoid the bounded experiment.
The full Argentea goal still includes adaptive advanced PageRank/BFS/WCC/SSSP;
those algorithms require a separately qualified continuation strategy.

## Implemented extension core

`examples/extensions/argentea` contains a small Rust crate with no Sail scheduler
or network implementation. `PageRankPartition` constructs immutable outgoing
CSR once, stores ranks between rounds, and emits through an owned `EmissionCursor`. The adapter acquires the cursor
under its partition lock and releases that lock before waiting for shuffle
backpressure. Immutable CSR/rank snapshots and their admissions stay alive with
the cursor; dropping an incomplete cursor cancels the local operation. A
one-message channel test proves that a consumer can reacquire the partition lock
while its producer is backpressured. It uses the same `grust-procedures` 0.23.0 resource
accounting as Banda and the existing `sail-native-resource-ffi::MemoryLease`.
Adjacency, rank/inbox state and emitted ownership are admitted before allocation.
The Arrow adapter separately admits its input staging, early-message queue,
producer metadata and output batches under that same native execution domain.

This first primitive implements **full power PageRank**: initialize 1/N,
redistribute dangling mass uniformly, preserve duplicate arcs/self-loops and
isolate vertices, and return the local L1 change. It matches Pecan's reference
contract. It does not implement Banda's residual/frontier threshold, reactivation,
normalization certificate or convergence controller. Advanced work must reuse or
extract those extension-owned primitives and qualify distributed equivalence;
renaming this full-round kernel “delta” would be incorrect.

Tests compare complete rank vectors for 20 rounds at multiple partition counts
against an independent dense reference. They cover negative/sparse IDs, empty
partitions, duplicates, self-loops, isolates, disconnected graphs, bad identity,
stale rounds, replay, missing completion, quota refusal, cancellation and final
lease ownership. The in-memory test harness deliberately gathers tiny fixture
messages; it is not the production transport or a distributed benchmark.

No Connect endpoint, two-worker runtime, two-host run or published performance
result is claimed by the core tests. The core is a path dependency of the
Nutmeg wheel rather than an independent extension distribution.

## Native worker adapter

`examples/extensions/nutmeg/src/argentea` implements a separate worker role in
the existing `sail-nutmeg` wheel. The entry point
`sail_nutmeg.argentea_factory:extension` requires the new Sail worker runtime;
older Sail builds reject its manifest rather than execute it on the driver.
Frozen benchmark wheels and their runtimes remain paired and unchanged.

The driver bootstrap imports input placeholders, validates schemas and returns
an opaque operation label, P output partitions and explicit owner range routing.
It neither reads graph rows nor creates native state. The worker binds the
host-issued session/job/worker/package scope and the prepaid memory lease.
`init` reads only prepared partition p of nodes and edges, builds one immutable
CSR and emits round 0. `round(r)` consumes round r−1 and emits round r.
`result(r)` consumes round r and emits final rows. Thus K fixed power iterations
use init, K−1 intermediate rounds and one result stage in a single lazy query.
The result is fixed-round output; no convergence claim is inferred from K.

The message schema has `owner`, `kind`, `producer`, `sequence`, `target`,
`value`, `producer_worker` and `adjacency_id`. Update rows use target vertex ID
and contribution value. Completion rows use local vertex count in `target` and
dangling mass in `value`. Schema metadata pins operation, snapshot, generation,
round, P and N. Per-producer sequence and identity checks detect replay,
truncation and changed origins. The receiver verifies its own producer ran on
the same host-scoped worker and retained the same native adjacency identity.
These are correctness checks, not a new authentication protocol.

A downstream partition can receive another producer's rows before its local
CSR or next inbox is ready. The adapter buffers these early messages under the
native quota. Its own producer marker proves that its state must already exist;
missing or misplaced state fails immediately at that point. This buffering can
consume quota under skew and is part of the bounded spike's memory envelope.
No mutex spans an input await, a shuffle backpressure wait or output delivery.
Immutable emission and final-rank cursors retain their admitted snapshots.

Each partition stage is claimed at most once. Any unfinished stream drop cancels
the native operation. Host `close()` tombstones before clearing partition state
and wakes cancellation subscribers; later planning/publication is rejected.
Output Arrow buffers, including slices retained without their original batch,
keep the lease and their native admission after the stream and job owner close.

Native receipts record `init`, `emit`, `consume`, `result` and `close`, with
operation/snapshot/generation, host session/job/worker, PID, partition, round and
a monotonic process-local adjacency identity. An empty owner still emits all
completion markers and receipts, but no fake rank row. Receipts go to stderr
and optionally `SAIL_ARGENTEA_AUDIT_PATH`; `{pid}` expands to the native PID.
They support [the runtime qualification tutorial](../../../examples/extensions/argentea/PYTHON.md).

## First live integration finding

The initial live check used host source
`0d3a0b753b47da9901f014a47d5db92bc330021a` and native/client source
`e420cafc1ea03e889d6db11f212c9659c374346b`. Local mode rejected the worker-native
relation as required. The process-cluster attempt started two workers but
failed the adapter's input-partition check before constructing any native CSR.
Its analyzed physical plan retained only the outer range exchange.

A minimal negative control reproduced the cause: Connect children are already
physically optimized when composed into their parent. A second optimization
then removes their previously lowered ordinary range exchanges. Protecting only
the outermost input was insufficient. The repair restores routing declared by
every nested worker descriptor at `WorkerTableProvider::scan`, before the
enclosing optimization. Matching explicit exchanges are reused; matching
lowered exchanges are replaced, so repeated scans do not accumulate exchanges.
This stays inside worker-extension planning and adds no global optimizer rule
or job-graph operator. Its regression composes four nested provider scans and
checks exact range exchanges, both initial inputs, duplicates and empty channels.
The repair passes 255 host tests, all-target Clippy with warnings denied, and
the CLI build at `5d38d3e16ad25dc63f572c0a8d38ff06ff84e46d`. The live rerun's
analyzed plan retains all four required range exchanges, including the different
owner-column positions in the initial vertex and edge inputs.

Both first-run process groups exited and their staging files were removed.
That observation establishes process/staging cleanup only: with no native
partition built, it cannot establish successful graph execution or return of a
native quota while a worker remains alive. The failed attempt remains evidence.

The rerun reaches the ownership validator and is rejected at stage 3. The same
failure occurs with P=2 and with a direct tiny-result collection that omits Pecan
staging and result writes. No native graph is initialized in any of these
attempts. This rules out the materialized-result write as the specific trigger;
it does not establish successful native execution. A frozen actual-job-graph
test at `8c49adfad277a1596b5909fb3d1bd193f63b3fcc` reproduces widths
`[1,1,3,3,3]`, breadth-first stage order `[0,2,1,3,4]`, and different buckets
for the initial and subsequent native stages. A focused 76-line
`job_graph/worker_groups.rs` module now assigns the existing `Stage.group`
field after graph construction. It joins stages by package/operation identity,
including transitive co-occurrence in a stage, and keeps ordinary groups intact.
The completed graph must still pass the unchanged ownership validator.

Separating groups consumes existing task slots. The P=3 two-input fixture needs
four slots: three for the native operation and one for its ordinary sources.
Tests execute the real task assigner, require refusal at three slots, and verify
stable owner/slot assignment at four. The qualifier's existing two workers with
32 slots each need no configuration change. The new grouping tests pass at
`038c9b9597d3fcf7e0b8c30c1253d7d77563f012`. That host revision passes
258 library tests across the three affected crates, all-target Clippy with
warnings denied, and a CLI build.

## First successful worker-process qualification

The successful run uses host source
`038c9b9597d3fcf7e0b8c30c1253d7d77563f012`, native source
`e420cafc1ea03e889d6db11f212c9659c374346b`, and client/auditor source
`a952564a4892c1bbd87a38243262ca130d05bd72`. These identities are separate:
the host placement repair does not change the previously built native wheel.

The fixture has six vertices, a negative ID, parallel edges, a self-loop and
an isolate. With P=3, owner 1 is empty. Two full normalized PageRank rounds
match an independent Python reference. Both worker processes execute native
code, including the empty owner; each owner's native adjacency identity stays
unchanged through initialization, rounds and result production. The driver
performs no native partition work.

The audit joins 21 native events to the actual session/job and three native
stages. All nine native stage/partition tasks succeed once, on attempt zero,
with the same worker for each owner. Each native stage is pipelined, has three
partitions and shares the operation's slot group. Ordinary scans and result
writers remain separately recorded; they need not have the native width.
An earlier audit incorrectly applied that width requirement to all worker
stages. Its failed receipt is retained; focused negative tests now distinguish
ordinary stages from missing, retried or misplaced native tasks.

Every owner emits a close receipt. Post-run inspection confirms that the driver
and both worker processes exited and no staging files remain. This proves
successful reference execution and final cleanup on one physical host.

A separate live resource gate uses client source
`a033ca1471b3dc7ae9796a8866b985446435bb88` with the same host and native pins.
Each worker has a 48 MiB Sail pool and a 32 MiB native quota. A leaked full
quota reservation would prevent the next operation from binding. In one
session, the same two worker PIDs perform a valid operation, an intentional
global-vertex-count mismatch after all three owners initialize, and three more
valid operations. Every operation closes all native owners before the next
begins; none of its native tasks is retried. All four successful materialized
results remain readable until their explicit close. Final process and staging
checks pass. This establishes live quota reuse under that envelope, not zero
RSS or general leak freedom. The retained results are Parquet files; separate
native tests cover Arrow-buffer owners that outlive their streams.

The physical two-host gate uses client source
`8d43c6f4868eb43d748c48210f90d999d2f494c9` with the same host/native pins.
P=5 preserves one empty owner while placing four vertices on Capitola and two
on Morrobay. Three directed input arcs cross between hosts. Its independent
audit checks all 35 native events and all 15 native stage/owner completions,
unchanged adjacency through two rounds, and the complete reference vector.
Every task succeeds once on attempt zero. All supervised Sail PIDs exit, and
a separate object-store listing finds no remaining staging objects.

The gate uses identical x86 executable/wheel bytes, with Rosetta on Capitola.
Direct LAN attempts timed out before the first graph operation; a transport
probe confirmed HTTP/2 replies on loopback and timeouts on Capitola's LAN
address. Explicit SSH port forwards carry the existing Sail gRPC/Flight
connections for this functional test. This is neither direct-LAN qualification
nor a performance measurement, and no new transport is added to Sail.

An independent x86 process run also returned correct ranks but failed its strict
audit because concurrent formatted stderr writes interleaved two JSON records.
The native receipt writer now constructs each complete line before one write.
Eight-process tests cover 1,024 distinct records; a separate release helper test
covers 260,096 records under CPU saturation. The parser stays strict. The fixed
wheel's version-1 resource regression passes at native/client source
`f11fe6e8a091e4be56a21712850d2a1a205c3e6c` with host `038c9b9597d3`.

The fixed wheel also passes the physical two-host gate with qualification
source `941b5e50b90e5fbf7f5184c733f7d9d522aa42c5`. An earlier attempt returned
correct ranks but failed the two-host audit: all native work finished on
Capitola before Morrobay's worker registered. The qualifier now waits for two
distinct running workers in the graph's own session using the existing
`system.cluster.workers` table. It still separately proves native execution,
nonempty graph ownership and cross-host edges afterwards. This readiness
check changes the test harness, not Sail's scheduler or the production client.

## Compose deeper plans with existing temporary views

The first residual PageRank plan failed Sail's existing protobuf nesting guard
before any native allocation. Each nested extension envelope contains its
previous phase, so a 32-phase client tree exceeded the depth limit despite its
small byte size. Raising the guard is unnecessary: the client can register one
uniquely named temporary view per phase and reference that view in the next
phase. Sail stores each resolved logical plan without running the child.
One final materialization still submits the complete graph as one native job.

A private diagnostic on the unchanged host `038c9b9597d3` and wheel source
`f11fe6e8a091` verified no native allocation during registration or explain,
32 native phases across five owners, stable CSR identities, six residual pushes
and two certificates. Its independently recomputed normalized residual was
`0.00039088808203777137`, below `1e-3`. All 32 owned views, native owners,
processes and staging objects were released.

The public client at `010b9e065ad2adc33a7169de73191a039ce8d6d0` repeats that
result on worker processes and across Capitola/Morrobay. The two-host run has
nonempty graph data on both hosts, a crossing arc, 170 native events in one job,
zero native events before terminal materialization, and all 32 views removed.
Its host and native wheel are unchanged. Stationary zero-push/DONE transport
and local-mode refusal also pass. A cap failure run surfaced cancellation;
an identical fresh control surfaced the expected push-cap cause. Both attempts
are retained. The subsequent native repair at `50195d14aafc`, with combined
client source `277ae341c90c`, records a typed `pagerank_push_cap` cause only after
the fresh global certificate completes. Three fresh ARM cap cases and the
physical two-host cap case pass: failed query, residual above tolerance at the
push cap, no result, no retry, all owners closed and staging removed. One ARM
case still exposes peer cancellation as its first RPC error; the audited native
record establishes the cause without promising a global first-error ordering.
Positive residual and stationary controls also pass with the repaired wheel.
The [cap repair evidence](argentea-validation/README.md#typed-cap-failure-repair)
is separate from the unchanged archives of the original attempts. Larger phase
budgets and performance remain unqualified.

See [the advanced execution plan](argentea-advanced-plan.md) for signed residual
PageRank, its additional statistics barriers and certificate, and the subsequent
reference/frontier and direction-optimizing BFS work. Each implementation has
its own qualification; a bounded reference query does not establish the others.

## Bounded BFS qualification

The extension/client at `b3c543171c6c64df0ef53677394abc24e9a2de99` adds reference,
frontier and direction-switching BFS without further Sail host changes. All
three pass on ARM workers and across Capitola/Morrobay using host `038c9b9597d3`.
The physical fixture has eight vertices, five reached from source `-5`, five
cross-host arcs including a duplicate, and four vertices owned on each host.
Each positive query executes 32 native phases in one job, completes four
expansions, and checks every distance and parent against an independent BFS.
Direction switching actually performs four Pull expansions. A separate zero-cap
case fails with a typed `bfs_level_cap` record after complete topology setup.

The [BFS evidence](argentea-validation/README.md#bfs-worker-execution) retains
19 runtime cases: 15 ARM cases and four physical two-host cases. All owned views,
native owners and supervised processes close; independent post-stop listings
find empty owned storage prefixes. The existing SSH/Rosetta functional boundary
still applies. The [tutorial](../../../examples/extensions/argentea/BFS.md)
describes the initial 14-level/32-phase deployment bound. This proof establishes
neither arbitrary graph diameter nor distributed performance.

## Remaining executable integration gates

The [fault evidence](argentea-validation/faults/README.md) retains the original
RUNNING-state failure and the repaired-host outcomes. The change terminates task
records before unassignment for failed/canceled jobs, without changing success
ordering. Both observed first-error paths pass the same no-replay and cleanup
requirements. The [WCC adapter](../../../examples/extensions/argentea/WCC_ADAPTER.md)
and [SSSP adapter](../../../examples/extensions/argentea/SSSP_ADAPTER.md) add
extension-owned graph stages and no further Sail host hook.

1. Extend active-cancellation, worker-loss and post-initialization quota controls
   to each additional algorithm and to physical two-host execution. Bind-time
   refusal does not prove post-initialization allocation cleanup.
2. Complete Linux and physical WCC/SSSP qualification after the measured
   Morrobay campaign. Process-cluster gates cover128-stage reference WCC and
   both SSSP methods,124-stage star WCC, signed extrema, isolates, SSSP skew,
   scheduler placement, typed caps and local-mode rejection.
3. Apply the same correctness, ownership, resource and cleanup gates to WCC and
   SSSP, and qualify larger BFS phase budgets. Keep reference and advanced
   variants separately identified.
4. Run performance comparisons only after functional qualification, with the
   same graph, semantics and disclosed resource envelope. Report per-host and
   aggregate memory, including communication and staging costs.

See the [probe instructions](../../../examples/extensions/argentea/README.md).

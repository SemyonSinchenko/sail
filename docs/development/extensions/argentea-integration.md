# Argentea integration: the smallest first Sail change

Status: extension-owned PageRank partition primitives, scheduler probes and the
generic worker descriptor/codec boundary are implemented in the working tree.
The combined host binding and remote execution are **not qualified**.
This is the gap inventory for [the Argentea scope](argentea-plan.md), with an
initial route that may avoid changing worker placement.

## Start with one bounded job

The first two-host spike should unroll two full PageRank rounds into one Sail
physical job. Use the existing Flight shuffle and keep all graph stages at the
same partition count. Sail already assigns matching partitions of equal-width
stages in one pipelined region to the same task set and worker. This is a
smaller starting point than preserving state across independently submitted
queries.

The evidence is executable, not an assumption about hash partitioning:

- `JobScheduler::build_task_region` and `StageGroup` in
  `crates/sail-execution/src/driver/job_scheduler/core.rs` group equal-placement,
  equal-group stages into task sets. Equal partition counts give offset zero.
- `examples/extensions/argentea/tests/scheduler_probe.rs` compiles the actual
  `TaskAssigner` source and extracts the actual `StageGroup` implementation at
  build time. It proves equal-width grouping at 2, 3 and 16 partitions.
- A negative control inserts a one-partition stage between two two-partition
  stages: partition zero changes task-set bucket. A global scalar aggregation
  therefore cannot be inserted casually into this plan. Any wider stage or
  intervening widths that shift the cumulative bucket offset can also break
  ownership; actual job-topology inspection must compare all stateful stages.
- A second negative control submits independent queries with unrelated work in
  between: logical partition zero moves from worker 2 to worker 1. Ordinary
  cross-query repartitioning supplies no persistent owner affinity.

These are focused scheduler-source tests. They do not prove that a finished
Argentea physical plan has the required topology or that workers ran a kernel.
The integration gate must inspect the actual job graph and retain worker PIDs,
partition IDs, native adjacency identities and round receipts.

## Round shape and barrier

Each partition retains its outgoing CSR and rank vector. Its round emits native
contributions tagged with destination owner. At the end it sends a completion
marker, final producer-to-destination sequence number and local dangling mass
to **every** destination, including destinations receiving no contributions.
Every receiver requires all producers' completion markers before applying the
round. It sums the dangling scalars in producer order and uses the ordinary
probability-normalized PageRank update. The scalar traffic is O(P²) for P
partitions; adjacency remains partitioned.

This avoids a separate one-partition reduction stage in the first spike. The
messages travel through Sail's shuffle, not a new network protocol. Existing
`Partitioning::Range` / `OutputDistribution::Range` can internally express routing on an
explicit destination-owner column with split points 1 through P−1. The worker relation bootstrap must request this routing explicitly; the host
constructs its own visible range repartition around partition-preserving child
plans. A foreign FFI repartition is not automatically visible to Sail's planner.
Qualification must execute the real `BatchPartitioner` and shuffled plan for boundary values,
empty channels and duplicates. Hashing an owner column is **not** equivalent to
routing owner p to task partition p.

The extension core checks operation, package, session, snapshot, generation and
round identity; sequence numbers; exactly one completion per producer; valid
owned destination IDs; and complete input before rank publication. Emission
failure poisons the round. These checks detect replay/truncation at the typed
boundary, but the Sail adapter must authenticate identities, preserve each
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
| Job-owned native state | `TaskRunnerActor::handle_close_job` cancels task streams and removes local shuffle streams; it has no callback for extension-retained partitions. | Give the worker factory a registry scoped to host-issued job/session identity and a job-local operation label, attach it to existing job close/cancellation/shutdown, and release native state after active readers finish. No cross-job persistence is needed in the first spike. Test cancellation during build/round, worker disconnect, output retention and final quota release. |
| Retry safety | `JobScheduler::update_task_regions` forces one attempt only for regions containing `DriverExtensionExec`; ordinary worker tasks use configured retries. | Extend the existing no-retry marker to worker-native state mutations. A lost acknowledgement aborts the whole job. Test that a configured retry count above one does not replay an Argentea round, while ordinary tasks retain current behavior. |
| Placement validation | `TaskPlacement` supports only Driver/Worker; `TaskSlotAssigner::next` balances available slots. Equal-width one-region task grouping can avoid new affinity machinery for the first spike. | Validate the completed Argentea job topology before execution: one pipelined region, identical partition width/group/placement for stateful stages, no rebind after worker loss. Add a narrowly scoped affinity hook only if the real plan cannot satisfy these existing conditions. |

The new `sail-common-datafusion::worker_extension` module contains the generic
`WorkerDescriptor`, `WorkerExtensionExec`, `WorkerExtensionRegistry` and async
`WorkerExtensionFactory` contract. Its codec checks exact package identity before
native layout access. `WorkerTaskScope` carries the actual host task/job identity,
not identifiers decoded from extension payloads. The registry tombstones closed
jobs before invoking factory cleanup; the factory must also reject publication
that races with closure. Five isolated boundary tests cover malformed descriptors,
wrong package identity, authoritative scope, output schema/partition mismatches
and a delayed-materialization/close race. Full Sail gates are still required.

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
The future Arrow adapter must account for its own batch/queue allocations.

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

No wheel, Connect endpoint, two-worker runtime, two-host run or published
performance result is claimed by these tests. This core will be consumed by the
Nutmeg worker adapter rather than registered as an independent graph library.

## Next executable integration gate

1. Export the core through the native worker adapter and add the generic codec
   and quota bindings. Verify a stateless identity/partition receipt remotely.
2. Build a two-round, fixed-width Flight plan. Inspect its actual task sets before
   admission; test owner-column routing with the actual shuffle.
3. Retain one CSR per owner and run both rounds; prove that each owner's native
   allocation identity is unchanged and two worker PIDs executed native code.
4. Exercise wrong package, incomplete producer, lost worker, cancellation and
   quota refusal. Whole-query failure must close every other partition and leave
   no retained native lease after final output owners drop.
5. Repeat with workers on Capitola and Morrobay after the frozen benchmark/audit
   finishes. Then qualify more rounds and assess adaptive execution costs.

See the [probe instructions](../../../examples/extensions/argentea/README.md).

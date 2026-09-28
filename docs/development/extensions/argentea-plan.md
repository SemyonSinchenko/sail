# Argentea: distributed native graph execution

Status: active implementation. Partition primitives and focused host hooks are
under qualification; distributed native execution is not yet established.
Repository: `querygraph/sail`, branch `work/extensions-traversal-bench`.

Argentea is distributed Banda: native Rust graph partitions execute on Sail
workers and exchange updates through Sail. Pecan and Grenada retain their
relational execution path; Banda retains its single-process native path.
Argentea adds an execution path, not a fourth independent algorithm library.

## Design constraint: reuse Sail

Keep graph partitioning, adjacency formats, kernels, frontier management and
algorithm policies in the extension. Reuse existing Sail/DataFusion execution,
shuffle transport, registration and identity checks, native resource domains,
cancellation, and owned staging. Do not introduce a graph scheduler, a second
transport, or a general iteration framework for this prototype.

Existing driver-native placement and handles remain driver-only. Argentea needs
an explicit worker-capable contract; removing the current placement checks is
not a valid implementation. Worker registration for scalar extensions does not
by itself prove support for stateful native physical operators.

| Existing foundation | Intended reuse | Feasibility check before a Sail change |
| --- | --- | --- |
| Connect extensions and Pecan input/staging code | Submit the operation plan; retain bounded control metadata | Can the operation use one existing job lifetime? |
| DataFusion plans and Sail remote codecs | Native partition input/output as Arrow batches | Can extension-owned physical nodes round-trip with package identity? |
| Sail scheduling and shuffles | Route updates by destination partition | Can successive rounds reliably reach the owner of retained state? |
| Native resource domains and leases | Reserve adjacency, scratch, messages and retained output | Are domains and final-owner lifetimes available on remote workers? |
| Owned staging and graph tables | Input snapshot, intermediates, result publication and cleanup | Does storage visibility work across both hosts? |
| Job cancellation and task failure | Abort the complete algorithm and release all partitions | Are cleanup and retry controls sufficient for stateful rounds? |

These are integration targets, not assertions that every needed hook exists.
For each gap, record the failing minimal experiment, existing interfaces tried,
smallest generic change, and focused regression test. Keep each host change
separately reviewable from graph code. Avoid a new public protocol or ABI unless
that experiment demonstrates the need.

## Execution contract

Use a deterministic vertex-to-partition mapping, independent of worker count.
Stage every vertex, including isolates, and route source adjacency to its owner.
Keep stable vertex IDs at the boundary and compact local indices internally.
A vertex-to-owner mapping also routes cross-partition updates. Freeze partition
count and membership for an operation; repartitioning is a later feature.

Retain immutable adjacency and mutable algorithm state on each owning worker
between rounds. Distinguish logical partitions from worker processes: ordinary
hash repartitioning does not guarantee that the next task lands on the same
process. Prove affinity through existing scheduling before relying on it. If a
small placement hook is necessary, constrain it to operation-scoped ownership;
do not add a general distributed object store. A materialized-state fallback may
help correctness experiments but must disclose adjacency rebuild costs.

Identify state and messages by operation, graph snapshot, partition, generation
and round. Validate package identity and reject stale or foreign references.
Never send raw pointers or current driver-local graph handles to workers.
Apply a round once, expose its output only on successful completion, and require
all expected partitions to finish before convergence is declared. Empty local
frontiers alone do not establish global completion.

The first integration uses two bulk-synchronous rounds unrolled into one query
and one existing Sail job lifetime. Equal-width stages reuse Sail's task-set
placement; Arrow update batches and completion markers use existing shuffles.
The [integration design](argentea-integration.md) explains why separate client
queries do not currently preserve native ownership. This bounded experiment
does not yet provide adaptive convergence over arbitrarily many rounds.

Combine updates locally where algorithm semantics allow. A later adaptive
controller may receive bounded scalar reductions, never the full graph or
frontier. Its continuation strategy must preserve operation ownership explicitly.
Keep network queues bounded and include their buffers in admission/backpressure
accounting.

Admit graph construction and round scratch before allocation, with a budget per
worker. Reuse native leases and DataFusion reservations without double counting;
account separately for transport and staging. In-process integration does not
make native adjacency spillable. Refuse oversized partitions explicitly until a
spill design exists. Report aggregate and maximum per-host memory separately.

Initial failure behavior is whole-operation abort. Disable automatic retries for
state-mutating work, or demonstrate an existing mechanism that makes replay safe.
Worker loss, memory refusal, cancellation, and round failure publish no partial
success and invalidate operation state. Use existing job/session teardown for
remote cleanup, with tests for lost connections. Durable recovery, elastic worker
membership and checkpoint restart are outside the first implementation.

## Delivery sequence

1. **Integration spike.** Run two partitions on separate worker processes,
   exchange one update round, reuse adjacency in a second round, and prove
   ownership, resource release and package mismatch rejection. Record exact Sail
   gaps before choosing host changes. A driver-local execution is not a pass.
2. **Reference PageRank.** Implement full synchronous iterations with declared
   damping, dangling-vertex handling, normalization and stopping rules matching
   the comparison contract. Validate complete vectors on adversarial fixtures.
3. **Advanced PageRank and BFS.** Add delta/frontier PageRank, then distributed
   BFS. Reuse Banda primitives where valid. Treat distributed push/pull as a
   separate choice requiring frontier communication and work measurements;
   native single-process heuristics do not automatically transfer.
4. **WCC and SSSP.** Preserve reference methods and add distributed contraction
   and bucketed shortest paths. Specify cross-partition contraction, component
   ownership, global bucket selection and termination before optimization.
   Reusing single-process code is insufficient evidence of algorithm equivalence.
5. **Qualification and measurement.** Exercise local, process-worker and
   Capitola/Morrobay execution, then join the existing benchmark matrix as
   `argentea`. Publish standalone build/run instructions and raw evidence.

PageRank is the first acceptance milestone, not a claim that the whole path is
finished. WCC/SSSP remain explicit later milestones in the same goal. Finish the
frozen large PageRank/WCC campaign and its audit before loading Morrobay with new
builds or experiments. Keep existing traversal work and experiment identities.

## Acceptance and comparison

- Demonstrate real native kernel work on both hosts, persistent adjacency reuse,
  partition ownership, and cross-host update traffic using retained receipts.
- Check full answers against independent references and the existing paths.
  Include isolates, disconnected components, duplicates, self-loops, directed
  graphs, hubs, zero-weight cycles, skew and empty partitions as appropriate.
- Test missing/mismatched workers, worker loss between rounds, stale generations,
  cancellation during construction and exchange, quota refusal, retained result
  lifetime, and complete cleanup. Run concurrency tests beyond parallel cutoffs.
- Preserve algorithm parameters and distinguish floating-point tolerance from
  bitwise reproducibility. Specify reduction order before claiming determinism.
- Measure graph staging, CSR construction, each round, exchange, output, total
  elapsed time, per-host peak memory and aggregate resource envelope. Report
  bytes exchanged and partition imbalance where instrumentation permits.
- Compare with Pecan/Grenada on equal total CPU/memory and identical graph semantics.
  Compare Banda within its single-host capacity; mark capacity refusal separately
  from error or timeout. Retain slower cases and all unsuccessful outcomes.
- Separate strong scaling, capacity scaling, and kernel-only controls. Do not
  assume network cost or two-host speedup from a local timing.

The review deliverables are this scope, a measured integration-gap inventory,
focused Sail patches with regression tests, extension-owned implementation,
standalone tutorial, and reproducible benchmark evidence. The architectural
success criterion is a small justified host surface with explicit ownership and
failure semantics, rather than a predetermined speedup.

# Portable graph algorithms through Spark Connect

Status: implementation in progress on `work/extensions-datafusion-graphs`.

The first distributed graph algorithm implementation is now a pure PySpark client
with a small Sail utilities service. This adopts Semyon Sinchenko's
`pyspark-graph-algorithms` project draft and develops the client-loop alternative
from the [graphframes-rs plan](graphframes-rs-plan.md). The compiled-in
server-side algorithm controller remains a later option, not a prerequisite.

## Architecture and scope

```text
Python algorithm loop
  -> ordinary Spark Connect joins / aggregates / Parquet writes
  -> Sail planning, distributed stages and worker memory/spill
  -> Parquet stage read by the next iteration

Python lifecycle helper
  -> gf.utils.v1.Request relation -> eager typed receipt
  -> driver-owned storage operations using Sail's object-store registry
```

The Python process controls iterations and collects only bounded control results.
Graph rows remain in engine relations. The initial implementation provides
PageRank and exact minimum-label WCC. WCC label propagation is an explicit
initial algorithm; it does not claim to implement graphframes-rs's randomized
contraction. Native `gf_axpb` is a separately tested primitive for that future
implementation, without a prime-field fallback.

PageRank defines reset probability, dangling-node redistribution, normalization,
convergence norm and iteration-limit behavior in its client API. WCC treats edges
as undirected for connectivity and chooses the minimum original integer ID as
component label. Both preserve isolated vertices and reject invalid IDs or
endpoints. Duplicate-edge and self-loop semantics must be documented and tested.
Algorithms beyond the implemented set are explicitly unsupported.

## Relationship to the existing extensions

| Surface | Role |
| --- | --- |
| Sedona wheel | Native spatial scalar functions, including worker execution. |
| Nutmeg graph-table helpers | Degrees, triplets and bounded walks using ordinary Sail plans, with no CSR. |
| Nutmeg native wheel | Explicit graph staging and driver-native kernels; host-funded quota and shared CSR snapshots. |
| Portable graph client and utils | Iterative relational algorithms, distributed through ordinary queries, with engine-owned storage utilities. |

The portable client is a new graph API implementation, not another CSR engine.
It can later be exposed through Nutmeg's graph-table API with an explicit backend
choice. It does not change existing Nutmeg algorithm semantics or silently fall
back to its kernels. Relational execution avoids a separate native topology but
still creates shuffle buffers, spill files and successive Parquet generations.
No single-copy or total-RSS guarantee is made.

## Sail integration boundary

Reuse the existing zero-input relation extension and function-by-name paths.
Filesystem mutations execute when their receipt is consumed, never during
planning or schema analysis. Use one canonical protobuf schema and explicitly
defined Arrow receipt columns. A protobuf message definition alone is not the
DataFrame result schema.

The first branch implementation uses compiled-in utils registration under an
explicit opt-in. This is a deliberate packaging deviation from the draft's wheel:
the current wheel binding exposes neither the host object-store registry nor
host configuration, and a single manifest cannot combine driver-only relations
with worker scalar functions in cluster mode. Compiled-in registration preserves
host ownership without inventing a new FFI resource API.

A later wheel version can split filesystem and scalar registrations into two
entry points, but still needs a narrow host storage capability. Do not expose
Rust SessionContext or ObjectStore trait-object layouts across the native ABI.
Custom aggregate loading is outside the first PageRank/WCC milestone: their
reductions use builtin sum/min.

[Upstream PR #2670](https://github.com/lakehq/sail/pull/2670), reviewed at
`8f8ba9d8`, provides a related Python storage bridge through Sail's host registry.
Its private proxies are scoped to Python datasource callbacks, not Connect
extension callbacks or graph-run lifetimes. Reuse the common `sail-object-store`
resolution layer and align URI handling with that work. The utils service adds
run ownership, bounded receipts and teardown above this storage foundation; it
must not construct competing Python storage clients. Integrating the callback
bridge itself is unnecessary for the initial Rust host utility implementation.

## Storage and lifecycle contract

Use a server-configured trusted root and Sail's existing object-store registry;
the client supplies neither credentials nor an unrestricted filesystem root.
The initial configuration is `SAIL_GRAPH_UTILS_ROOT`; this does not imply that
Spark's `spark.checkpoint.dir` is already wired into Sail's plugin bindings.
For multi-host execution, the driver and every worker must reach the same store.
For `file://`, precreate a dedicated directory. Sail canonicalizes its root and
rejects existing symlinks in run paths before storage operations. Administrators
must keep the directory exclusively managed by Sail; this check does not defend
against concurrent hostile filesystem changes.

Allocate a server-owned namespace per run, with retry identity and a run token.
Scope deletion to that run, forbid root deletion and cross-run access, normalize
URIs and reject traversal. Define bounded listing behavior explicitly.
Checkpoint storage and operator spill are separate storage consumers.

The client materializes input relations and each iteration, then reads the
materialized result. Publish a new stage only after a successful write. Delete
an older stage only when no remaining relation depends on it. A result handle
retains its final stage until closed or copied to caller-owned output.
Separately materializing vertices and edges does not create an atomic snapshot
across arbitrary changing sources; callers must supply stable inputs.

Use finally/context cleanup after completed writes, including validation errors
and cancellation between iterations. An interrupted or failed write leaves its
namespace owned by the session: the exception exposes `run_path` and
`cleanup_deferred = True`. A Connect interrupt acknowledgment does not guarantee
that every worker writer has terminated, so immediate deletion would race those
writers. On session close or idle expiry, request job shutdown, then attempt
cleanup with bounded retries. Teardown cleanup is best effort: there is no
universal writer-drain barrier or crash-persistent orphan catalog in this POC.
Persistent storage failures and abrupt server termination require administrator
cleanup after confirming the writers have stopped.

Runs belong to the host session incarnation. There is no independent per-run TTL in v1:
`lease_seconds = 0` denotes session ownership. A killed client therefore leaves
files until session teardown. Validate observed orphan cleanup with an explicit
short session-expiry fixture and cancellation during an actual write. These
tests do not establish a general termination barrier. A new session cannot
adopt an old session’s run token. Root/run ownership and commit state are
correctness requirements, not optional cleanup polish.

Do not infer committed file count from `repartition(n)`. Validate schema, rows
and algorithm invariants; list files only for bounded diagnostics and cleanup.

## Delivery sequence

1. Freeze the versioned utils request and receipt contract, capability names,
   ownership rules and graph algorithm semantics.
2. Implement Sail driver storage handlers and worker scalar registration, with
   pure planning, explicit configuration and boundary tests.
3. Implement the portable Python client, stage ownership and PageRank/WCC;
   add runnable examples and exact small-graph reference tests.
4. Qualify local and process-worker execution, failures, cancellation and orphan
   cleanup. Run Linux gates on Morrobay and native macOS checks.
5. Run a genuine Capitola–Morrobay graph workload with shared storage and retain
   evidence that algorithm joins and aggregations execute on both hosts.
6. Publish concise installation/API documentation and exact-revision validation.

A Scala/Spark utils implementation and cross-engine semantic qualification are
subsequent milestones. Common PySpark APIs are the portability target, not proof
that every Spark Connect implementation already supports the client. Pin the
client environment and disclose the engines/modes actually tested.

## Acceptance criteria

- No graph rows are collected into the Python control loop.
- Schema analysis and EXPLAIN do not allocate/delete a run or execute algorithms.
- PageRank and WCC match independently specified small-graph results, including
  sinks, isolates, loops, duplicates, disconnected and empty graphs.
- Invalid graph data/options and missing capabilities fail explicitly. Reaching
  an iteration limit is not reported as convergence.
- Driver filesystem operations reuse host storage; worker functions resolve after
  plan serialization in fresh workers. `gf_axpb` is bit-exact with GF(2^64).
- Run roots cannot escape configured storage or delete another run. Retries,
  partial writes, explicit close, exceptions and client termination are covered.
- Distributed tests exercise algorithm stages, not only input preparation.
- Existing Sedona and Nutmeg local/distributed behavior remains covered.

## Deferred server-side option

The [graphframes-rs integration plan](graphframes-rs-plan.md) retains the design
for reusing its Rust algorithm implementation inside Sail. Revisit it when lazy
query composition or server-owned iteration justifies the extra execution
interface, preparation hook and child-job lifecycle. The portable client provides
an independent semantic reference, but a min-label WCC result is not evidence
that randomized contraction uses the same intermediate algorithm.

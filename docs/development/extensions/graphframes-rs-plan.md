# Running graphframes-rs inside Sail

**Status: proposed implementation, for review.** No integration described here
has been built or qualified yet.

Implement PageRank and weakly connected components (WCC) over Sail tables using
`graphframes-rs`. Execute their DataFusion joins and aggregations inside Sail,
first locally and then across Sail workers. Keep graph data as relations;
this path requires no CSR conversion.

Review scope: `querygraph/sail`, branch `work/extensions-datafusion-graphs`,
starting at `088a1d540`; graphframes-rs source pinned to
[`b4da56da`](https://github.com/SemyonSinchenko/graphframes-rs/tree/b4da56dabe20bba8e29563e06acc5179b2113ce3).
This proposal extends the existing [graph-table work](datafusion-graph-plan.md).
The native Nutmeg path remains a separate execution choice.

## What can be reused

The library already builds graph algorithms from DataFusion DataFrames.
[PageRank](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/algorithm/centrality/pagerank.rs)
uses its Pregel engine;
[WCC](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/algorithm/connectivity/connected_components.rs)
uses randomized contraction followed by backward propagation. Both materialize
intermediate state and write their final results to Parquet. Their `run` methods
return iteration counts, rather than result DataFrames.

The library's Cargo requirement is `datafusion = "55.0.1"`; its reviewed lockfile
resolves 55.1.0, matching the Sail branch. Compile with one resolved DataFusion
version and verify Arrow dependencies before treating this as compatibility.
Disable the library's default CLI feature when embedding it.

The current Sail native relation interface supplies physical input plans and
places native graph execution on the driver. Simply wrapping graphframes-rs in
that interface would not distribute its iterations. The library also executes
physical partitions directly in its specialized checkpoint writer.

## Proposed execution model

```text
Connect relation request: algorithm + options + vertices + edges
                            |
                   Sail graph adapter
                            |
             driver iteration controller
                            |
          Sail planning and JobService / JobRunner
                            |
       local execution OR distributed worker stages
         joins -> aggregations -> checkpoint writes
                            |
           committed checkpoint + scalar status
                            |
                next iteration / final scan
```

The controller handles iteration state and convergence. Graph rows remain in
worker stages and checkpoint storage; only bounded control results return to the
controller. A local run uses the same interface with Sail's local job runner.

Begin with an optional, compiled-in `sail-graphframes` adapter. Keep the algorithm
library independent of Sail. This avoids inventing a native plugin ABI while the
execution interface is still being established. A separately installed wheel is
a later packaging decision; do not pass Rust `SessionContext` objects through
the existing FFI boundary.

## Library interface to extract

Introduce a small execution interface in graphframes-rs, retaining a standalone
DataFusion implementation. These are proposed responsibilities, not existing API
names or final Rust signatures:

| Responsibility | Contract |
| --- | --- |
| Execute a control query | Run a logical plan and return a bounded scalar result, such as active-vertex or remaining-edge count. |
| Materialize | Execute a relation and return a committed checkpoint handle with its schema. |
| Read checkpoint | Produce a relation over a live checkpoint; advertise only verified partitioning and ordering. |
| Release checkpoint | Release an ownership handle; delete storage after the last dependent operation or result releases it. |
| Cancel | Propagate query cancellation to active jobs, writers and the iteration loop. |

The Sail implementation plans these operations with the host session and submits
them through its existing
[`JobRunner`](../../../crates/sail-common-datafusion/src/session/job.rs).
Route all execution sites used by PageRank, Pregel, WCC and checkpoint handling
through the interface, including counts, final normalization, cache operations
and output writes. Replacing only `collect()` would leave bypasses.

Preserve the host planner, function registry, object stores and session services.
The library's current
[`scoped_ctx`](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/utils/options.rs)
shares the runtime but constructs fresh session state. Sharing the runtime alone
is insufficient to preserve Sail's execution behavior.

## Sail integration and client contract

Reuse the Connect relation envelope with exactly two input relations and a
versioned algorithm/options payload. Add a host-side handler that retains resolved
logical inputs, before the existing native path lowers and gathers them.
Keep that handler separate from the physical-plan wheel interface.

The adapter should provide a lazy result relation: analysis validates options and
returns the schema without starting jobs or writing files. At execution, a
controller prepares the final checkpoint, which becomes a normal scan usable by
subsequent Sail operators. Determine the precise execution-preparation hook in
the first implementation stage. The controller must not hold a worker task slot
while waiting for jobs that require those workers.

Initial contract:

- Vertices have unique, non-null `Int64 id`; edges have `Int64 src` and `dst`
  referencing existing vertices. Preserve isolated vertices. Validate data at
  execution, not during schema analysis.
- PageRank returns `id, pagerank`; WCC returns `id, component`. Define duplicate
  edge and self-loop behavior explicitly and retain the library's documented
  options only where verified.
- Reject unsupported algorithms and options explicitly. Do not silently route
  them to the native Nutmeg backend.
- Expose iteration limits/deadlines and defined non-convergence behavior. Review
  reset probability, tolerance, personalization and normalization before claiming
  GraphFrames API equivalence.
- Start with integer IDs. String-ID support can later reuse the library's ingest
  mapping while preserving original IDs in results; it needs its own distributed
  execution and lifetime review.

## Memory and checkpoint ownership

Use Sail's existing runtime and memory pool for local execution; workers use
their configured process resources. Do not create a second independently budgeted
runtime for the graph library. Sharing a process alone does not prevent competing
memory pools from overcommitting it. DataFusion accounting also does not bound
all allocations or total RSS.

Assign each execution a unique storage namespace. Keep checkpoints and final
results alive through explicit ownership handles. Publish an iteration only after
all its writes succeed; failed attempts must not appear as committed input to the
next iteration. Cancellation must stop child jobs and clean abandoned files.
Final result files survive until their consumers release them.

For multiple hosts, use storage accessible to all workers. Initially read plain
Parquet checkpoints and let Sail repartition as necessary. Defer the library's
specialized hash-partitioned/sorted checkpoint optimization until its physical
properties, serialization and file ownership are proven across workers. Bound
checkpoint storage separately from operator spill storage.

## Implementation stages

| Stage | Changes | Completion condition |
| --- | --- | --- |
| 1. Local integration | Pin dependencies; add the optional adapter and lazy request/result contract; connect PageRank and WCC to host-owned execution and storage. | Both algorithms run over Sail input tables, compose with a subsequent query, and perform no work during analysis. |
| 2. Execution interface | Extract execution and checkpoint operations into the library interface; preserve standalone behavior. This can be developed together with stage 1. | All execution sites for the two algorithms use the interface, including final output and cleanup. |
| 3. Distributed stages | Implement Sail job submission, checkpoint commit/lifetime, cancellation and required worker function registration/codecs. | Iteration joins, aggregations and writes run on workers without gathering the graph to the driver. |
| 4. Qualification and examples | Verify semantics, constrained resources and failure handling; add small local and distributed examples. | Linux and two-host runs demonstrate correct results and actual algorithm-stage execution on both hosts. |

Keep changes reviewable in separate PRs: the library execution interface, Sail's
optional graph adapter, distributed orchestration/checkpoints, and client examples.
Prefer reusing existing Sail job and checkpoint services; identify missing
capabilities before proposing new generic infrastructure. WCC's custom scalar
functions must be available and serializable on every worker.

Validation should cover empty graphs, sinks, isolates, disconnected components,
loops, duplicate edges and invalid endpoints. Check PageRank numerical semantics
against an independently specified reference, and WCC membership plus the chosen
component-label convention. Include spill pressure, concurrent sessions,
cancellation, worker failure and partial checkpoint writes. Record non-convergence
and failures explicitly. Distributed floating-point reductions need justified
tolerances; do not assume bitwise equality across partition counts.

## Decisions requested in review

1. Is an execution/checkpoint interface acceptable upstream in graphframes-rs,
   with its standalone DataFusion implementation retained?
2. Is a compiled-in optional Sail adapter the right first integration boundary,
   before designing an independently installed plugin interface?
3. Which PageRank options and WCC labeling convention should the initial client
   guarantee? Should the first API be library-native or a GraphFrames subset?
4. Can the first distributed implementation use ordinary Parquet checkpoints,
   deferring partition-preserving checkpoint optimization?
5. Which existing Sail preparation and checkpoint hooks can support a lazy
   algorithm result without executing during planning or blocking worker slots?

The first deliverable is two executable examples over Sail tables—PageRank and
WCC—with the same request shape in local and distributed mode, accompanied by
short build/run instructions. Other algorithms remain explicitly unsupported
until their execution paths and semantics are reviewed.

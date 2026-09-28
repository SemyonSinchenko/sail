# BFS and shortest-path benchmark

Build on branch `work/extensions-traversal-bench`. The existing PageRank/WCC
campaign remains frozen; new trials receive new source pins and run identities.

## Algorithm choices

Straightforward implementations are correctness references, not the performance
claim. Retain all methods and every outcome.

| Operation | Reference | Advanced candidates |
| --- | --- | --- |
| BFS | Full reached-set relational relaxation; existing native BFS | Direction-optimizing push/pull BFS, compact frontier membership, parallel adjacency traversal |
| Nonnegative weighted SSSP | Bellman–Ford and native Dijkstra | All-edge delta-star stepping; assess classical delta-stepping and rho-stepping controls on the same inputs and hardware |

Pecan and Grenada share the relational algorithms. Banda executes native code
inside the Sail driver. Algorithm names alone do not establish equivalence or
speed: record implementation, tuning, work, execution placement and boundary.
A relational pull join does not automatically have a native bottom-up BFS
kernel's early-exit behavior. Frontier joins may still scan every edge.

Use established implementations as performance controls:

- [GAP Benchmark Suite](https://github.com/sbeamer/gapbs), pinned at
  `2972aeb2703165bafd921222f4ed7196f542d3a8`: direction-optimizing BFS and
  delta-stepping SSSP.
- [Parallel-SSSP](https://github.com/ucrparlay/Parallel-SSSP), pinned at
  `a160e5eaf5bed40e2c2626d6d46e526129b2f274`: rho-stepping, delta-stepping
  and Bellman–Ford. See its
  [SPAA 2021 paper](https://arxiv.org/abs/2105.06145).

These are established research implementations, not a claim that either is the
fastest available implementation on every graph. Check weight types, duplicate
handling, directedness, source selection and output semantics before comparison.
Do not compare an integer-weight control with a floating-weight experiment as
though they were the same workload. Disclose conversion and preprocessing costs.

## Acceptance gates

1. Exact BFS distances, independently computed shortest-path distances, and
   valid rooted parent trees. Parents may differ between implementations.
   Include disconnected vertices, directed edges, duplicates, self-loops,
   zero-weight cycles, ties, missing sources, invalid weights, overflow and
   iteration exhaustion. An unreachable distance is null, never a finite sentinel.
2. Local and distributed Sail execution. Check cancellation, memory admission,
   output lifetime and cleanup. Parallel native tests must cross the actual
   parallel threshold and run with multiple worker counts.
3. Demonstrate that advanced paths execute: BFS push-to-pull and pull-to-push
   transitions, SSSP bucket closure and reactivation, and skewed high-degree
   frontiers. Record examined edges, rounds, active vertices, shuffle/spill
   information when available, and algorithm-specific tuning parameters.
4. Run fixed inputs and sources under equal CPU/memory limits. Tune on separate
   training fixtures, freeze parameters before measured runs, retain slower
   cases and failures. No universal "fastest" claim based on one graph family.
5. Report complete Sail calls separately from kernel-only controls: staging,
   transpose/CSR construction, input conversion, algorithm and output writing
   must have explicit boundaries. Include peak process PSS/RSS and cgroup memory;
   preserve raw receipts and independent validation evidence.

## Capacity ladder and real graphs

Finish current PageRank/WCC measurements first. Replace whole-file preparation
with streaming partitioned conversion before billion-edge trials. Start with
Graph500-generated scale 24, then 25 and 26 at edge factor 16, subject to capacity
pilots. Add Twitter-2010 and UK-2007-05; use declared deterministic synthetic
weights when an input has no weights, retaining the original topology.

Capitola and Morrobay can supply distributed Sail workers. Banda's current native
kernels remain driver-local. Do not imply that their memory is pooled.

Using Graph500 inputs is not an official Graph500 result. A later compliance
campaign must implement its exact source-selection, parent-tree validation,
weight generation, timing and TEPS rules from the
[specification](https://graph500.org/?page_id=12).

## Argentea follow-on in the active goal

[Argentea](../../../docs/development/extensions/argentea-plan.md) extends Banda
with persistent native partitions on Sail workers. Its first milestone is a
two-host PageRank prototype using existing scheduling, shuffles and resource
contracts; advanced PageRank/BFS and then WCC/SSSP follow. Current Banda trials
remain driver-local. Argentea receives a distinct matrix identity and source pins
after qualification; it does not change the frozen measurements.

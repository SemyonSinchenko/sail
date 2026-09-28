# Three kinds of nut graphs

Pecan, Banda, and Grenada all calculate graph results such as **PageRank**
(which vertices are important) and **weakly connected components**
(which vertices belong to the same connected group, ignoring edge direction).

They differ in how the graph is supplied and who does the work.

| Path | Simple explanation | Where the work happens |
| --- | --- | --- |
| **Pecan** | Builds graph algorithms from table operations: joins, grouping, and filtering, like a sequence of SQL queries. | Python controls the iterations; Sail's DataFusion engine executes the table operations and can distribute them across workers. |
| **Nutmeg Banda** | Converts tables into a specialized graph structure, then runs dedicated Rust graph algorithms. | Inside the Sail driver process. The graph computation currently uses one machine, even when Sail has distributed workers. |
| **Nutmeg Grenada** | Takes graph tables through Nutmeg's interface and passes them to Pecan's algorithm controller. | The same Python controller and Sail/DataFusion operations as Pecan, with distributed execution available. |

## Pecan and Grenada: two entrances to the same machinery

With Pecan, the caller gives the algorithm vertex and edge tables directly.
With Grenada, the caller starts with Nutmeg's `GraphTables` interface, which is
adapted to Pecan's `GraphAlgorithms` interface.

**Grenada reuses Pecan's algorithm implementation.** It is a different entry
path, not an independent third algorithm engine. Neither path needs to build
Banda's specialized native graph structure.

## Banda: specialized graph machinery

Banda first stages the graph into a compact adjacency structure called CSR:
essentially, a convenient way to look up each vertex's neighbors. Dedicated
Rust kernels then operate on that structure.

This work happens inside Sail, in the driver process. It is not sent to a
separate external graph service. Staging and kernel memory must fit within the
driver's admitted memory budget; adding worker machines does not pool their
memory for Banda's current native computation.

## Comparing them fairly

The names describe execution paths, not different mathematical definitions of
PageRank or connected components. Each path offers reference and advanced
algorithm choices, although their implementations can differ—for example,
Banda's reference WCC uses union-find, while Pecan and Grenada use minimum-label
propagation.

Performance depends on the graph, algorithm choice, and execution setup.
Comparisons should include both elapsed time and memory, and state whether
they include graph staging and writing the results.

For implementation details and measurements, see the
[Pecan, Banda, and Grenada benchmark report](development/extensions/pecan-nutmeg-benchmark.md).

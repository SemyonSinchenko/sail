# Vendored Nutmeg graph integration

Source: https://github.com/querygraph/nutmeg

Revision: `f267b03659dd536981f98420944f911b667632b7`.

Only `crates/nutmeg-graph/src` is included as implementation, with the upstream
MIT and Apache licenses and the upstream README as a catalog-test fixture. The standalone manifest reproduces its dependencies with the Sail
PoC's exact DataFusion 55.1.0 and Arrow 59.3.0 pins. This is a source dependency,
not a second implementation of Grust's algorithms.

Local changes add `SessionRegistry` with a separate graph store and memory pool,
atomic two-part replacement, stable graph snapshots for read providers, and
session-specific read accounting. Existing process-global APIs remain available
for the upstream test suite. The extension exclusively uses session APIs.

Schema probes now use a separate fixed 16 MiB setup store. `prepare_output_schemas`
warms the finite catalog before user planning; session read constructors use
cache-only schema lookup. The one upstream test that constructs `AlgorithmTable`
directly is bound to the setup store explicitly.

The DataFusion graph follow-up shares each committed session revision and its
projection cache across independently planned readers. `GraphSnapshot` exposes
the staged normalized node and edge columns as ordinary Arrow table providers,
without constructing CSR. Empty staged schemas remain available. Snapshot
buffers retain their row admission until the final consumer releases them.
The cache belongs to the shared published entry and projection-options key;
name/revision strings are diagnostics, and revision numbering restarts after
drop/recreate. These providers scan one driver-resident partition.

`SessionRegistry::new_with_owner` accepts an opaque host admission lease; stores,
background readers, and exported algorithm/graph buffers retain it. This allows
the Sail bridge to reserve the whole native session budget from Sail's pool
while the existing Grust pool accounts for graph allocations within that quota.
It is conservative admission, not allocator-level RSS accounting or native
spill support. Standalone `SessionRegistry::new` keeps its independent budget.

Normalization and canonicalization reserve conservative buffer/build bounds
before converting IDs, filling missing columns, encoding sort keys, or copying
sorted rows. They shrink excess admission after measuring the retained buffers.
An isolated allocator regression fixture checks that rejecting an expanding
integer-ID batch does not allocate the normalized buffers first, and that the
accepted fixture's transient allocation peak fits its admission peak. Bounds
cover Arrow buffers/builders and admitted graph workspace, not every Rust
metadata allocation. They include empty casts, nested encoded casts and repeated
view values, which can expand beyond their physical backing buffers.

Query cancellation remains cooperative within kernels. Initial CSR construction
and the final canonical staging sort are synchronous store operations and cannot
be preempted by query interruption. No native spill or automatic cache eviction
is introduced.

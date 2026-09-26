# Native Nutmeg Sail extension proof of concept

Start with the [source-distribution tutorial](../TUTORIAL.md) for a fresh install,
local and distributed deployment, and executable review examples.

This package provides ordinary Sail graph-table plans and independently compiled
Nutmeg/Grust native graph kernels. The native wheel has no Sail engine dependency;
it shares only a dependency-free resource ABI definition with the host.
The host discovers `sail_nutmeg:extension`, validates its manifest and calls
`bind_with_resources` with a host-issued memory lease. Each session has its own
graph store/read log and a default 256 MiB cap prepaid from Sail's memory pool.
A recycled session ID receives a new store.

Binding prepares the finite algorithm-schema catalog once per library: tiny
kernels run on an isolated three-node setup graph under a fixed 16 MiB budget.
Defaults plus `f32`/`f64` for the algorithms declaring precision are the entire
cache key space. User relation planning uses cache-only schema lookup, and
unsupported precision values fail without running a probe. Setup work and its
budget are separate from every user's graph store.

## Build

From the Sail branch root, with its Python environment active:

```sh
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/tmp/nutmeg-extension-target \
  maturin build --profile dev --manifest-path examples/extensions/nutmeg/Cargo.toml \
    --out examples/extensions/nutmeg/dist
uv pip install --python .venv/bin/python \
  examples/extensions/nutmeg/dist/sail_nutmeg-0.1.0-cp312-cp312-macosx_11_0_arm64.whl
```

Install the actual wheel: the embedded server does not process editable `.pth`
files. Select the wheel matching the build platform when its filename differs.
Use the committed lock file and the matching host manifest versions. The
vendored Nutmeg source and local changes are described in
[`../vendor/nutmeg-graph/PROVENANCE.md`](../vendor/nutmeg-graph/PROVENANCE.md).

## Client

```python
from sail_nutmeg import Nutmeg
nm = Nutmeg(spark)
nodes = spark.createDataFrame([("a",), ("b",), ("c",)], "node_id string")
edges = spark.createDataFrame([("a", "b"), ("b", "c")], "source string, target string")
# Ordinary distributed Sail plans; also works with extensions disabled.
graph = nm.tables(nodes, edges).validate()
graph.degrees().show()
graph.walks(2).show()
# Explicit native snapshot/kernel execution requires extensions enabled.
receipt = nm.stage("g", nodes.repartition(4), edges.repartition(4))
nm.nodes("g").filter("node_id = 'a'").show()
nm.run("g", "pagerank").orderBy("nodeId").show()
print(nm.status())
nm.drop("g")
```

Staging eagerly executes and returns one receipt (`graph`, `nodeCount`,
`edgeCount`, `revision`). `run` returns a normal lazy Spark DataFrame. A run
provider pins graph rows when Sail resolves it, so subsequent overwrite/drop
cannot change that provider's read. A new analysis or execution request may
resolve a new provider and therefore pin a newer revision.

`tables` accepts DataFrames or table names and retains lazy references to normal
Sail relations. It preserves identifier types, properties, duplicate edges and
self-loops. `validate()` checks unique/non-null node IDs and valid endpoints.
`out_degrees`, `in_degrees`, `degrees`, `triplets`, `walks(hops)` and
`closed_walks(hops)` build ordinary joins/aggregations, with no staging or CSR.
Walks may revisit vertices/edges. These references follow underlying table
consistency; they are not immutable native snapshots.

`nodes` and `edges` expose normalized staged Arrow rows. These scans build no CSR.
Native algorithm readers of the same revision/options share one synchronized
projection cache, while overwrite/drop preserves already-pinned revisions.
Each provider pins at server resolution; two separately resolved providers are
not an atomic pair of reads across a concurrent overwrite. The Rust
`SessionRegistry::snapshot` API can obtain both scans from one pinned revision.
The displayed revision increases on overwrite but restarts after drop/recreate;
the shared published entry, not the name/revision strings alone, identifies the
cache. Snapshots are in-memory ownership objects, not durable snapshot IDs.

Staging casts structural IDs to UTF8 and normalizes supported properties to
`property.*`/`present.*` columns; it does not preserve every original field type.
Raw node scans return explicitly staged nodes, without synthesizing nodes from
edge endpoints. Canonical staging retains empty schemas and fills absent
properties. The lower-level legacy `asStaged` mode still accepts heterogeneous
batches, but relational scans require a common schema. Staged sources have one
driver partition; downstream relational operators may redistribute their output.

## Memory admission

`SAIL_NUTMEG_MEMORY_BYTES` selects the native per-session quota. With experimental
extensions enabled, a server manager explicitly shares one resource domain with
its sessions and actor workers. Independent managers remain isolated even with
equal configurations; separate processes have separate pools. Configure `SAIL_RUNTIME__MEMORY_POOL__TYPE=greedy` and
`SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE` for a finite bound; an unbounded
configuration remains unbounded. Other sessions and participating DataFusion
operators contend with the prepaid native quotas.

Admission is coarse: the full session allowance is reserved up front, even when
idle, and cannot spill. A versioned C callback lease retains the host reservation
through native snapshots, producers and exported Arrow buffers. The final owner
releases it. Native normalization and canonicalization reserve conservative
buffer/scratch bounds before allocation. These participating reservations are
not a total RSS limit; runtime, transport, Rust metadata and other untracked
allocations need headroom. Bounds include Arrow builder growth and can refuse a
write whose eventual retained arrays alone would fit. Excess reservation is
returned after conversion; no dynamic lending or automatic CSR eviction is added.
The fixed schema-probe budget described above is outside user session quotas.

## Wire contract

The outer `Any` uses `type.googleapis.com/nutmeg.v1.NutmegApi`; its value is UTF-8
JSON, a deliberately limited PoC encoding rather than the proposal's final
library protobuf. Unknown fields and unsupported versions fail.

- Stage: `{"version":1,"verb":"stage","graph":"g"}` inside the Sail envelope
  `type.googleapis.com/sail.extension.v1.SailExtensionRequest`, version 1, with
  exactly two plan inputs in `[nodes, edges]` order. Optional `nodeMapping` and
  `edgeMapping` dictionaries accept Nutmeg's `idColumn`, `labelColumn`,
  `sourceColumn`, `targetColumn`, `typeColumn`, `edgeIdColumn` keys.
- Run: `{"version":1,"verb":"run","graph":"g","algorithm":"pagerank","options":{},"columnNames":"grust"}`.
  Input-free, accepted as a bare `Any`. `gds` column names and Nutmeg's query
  limits are supported.
- Drop: `{"version":1,"verb":"drop","graph":"g"}`. Input-free and lazy on the
  server; the client collects the `graph`/`dropped` receipt.
- Scans: `{"version":1,"verb":"nodes","graph":"g"}` or `"verb":"edges"`.
  Input-free providers pin the current staged revision during planning.
- Diagnostics: `{"version":1,"verb":"diagnostics","graph":"__session__"}`.
  Returns execution-time memory, graph revision/cache and actual kernel-state
  data as JSON in a `status` column; the client decodes this in `status()`.

Every partition is streamed into the admitted staging transaction. Nodes and
edges become visible together as one revision, after both streams finish.
Abandonment, invalid input or admission failure before commit preserves the
previous graph. Overwrite is the only write mode in this PoC.

Mutation providers do no work during schema analysis or scan planning. A
completed provider replay returns its recorded receipt without another write.
A concurrent, interrupted or failed attempt cannot be retried through that
same provider: it reports an explicit indeterminate-attempt error. There is no
cross-request idempotency key; a newly planned stage is a new overwrite.

The exported table provider and physical plans retain their FFI task-context
provider strongly; constructor locals, capsules and Python bindings can be
released without invalidating a later scan.

Algorithm execution uses Nutmeg's existing lazy `AlgorithmExec`, bounded
channel, work/memory admission and stream-drop cancellation. Native schema and
actual batches share the same Grust/GDS naming rules. The host retains its task
context for input plans rather than executing them under a reconstructed
foreign context. Local and distributed modes use explicit native driver placement
and codecs. Relational graph helpers run through worker plans; staged graph state
and native kernels remain driver-resident. Driver-native regions do not retry;
cross-request mutation idempotency remains outside this interface.
Projection construction and staging's final canonical sort use the store's
synchronous admission path. Query interruption cannot preempt that region;
staging interruption around the commit boundary can leave an unacknowledged
outcome indeterminate. The cancellation test proves stopping a blocked output
producer, not preemption within projection construction.

## Verification

```sh
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/tmp/nutmeg-extension-target \
  cargo test --manifest-path examples/extensions/nutmeg/Cargo.toml
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=/tmp/nutmeg-extension-target \
  cargo test --manifest-path examples/extensions/vendor/nutmeg-graph/Cargo.toml
```

Keep the vendored graph crate's tests separate from Sail tests: its SQL
feature is a test-only DataFusion dependency.

On this macOS host, uv's CPython has a `/install/lib/libpython3.12.dylib`
install name. Native Rust tests therefore also set `PYO3_PYTHON` to the branch
`.venv/bin/python` and `DYLD_LIBRARY_PATH` to that interpreter's `LIBDIR`
(`python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))'`).
The Python extension itself uses Python's already-loaded symbols and needs no
such runtime override.

The release cancellation stress driver is
[`scripts/stress_cancel.py`](scripts/stress_cancel.py). Pass `--output` for its JSON receipt and optionally `--target-dir` for isolated
artifacts. It builds with `cargo test --release --no-run --locked`; a prebuilt
release executable can instead be supplied via `--test-binary`. It runs once without
added load, then ten times with one `yes` process per logical CPU; all load
processes are stopped even on failure. Each outcome and the exact command are
retained in the JSON receipt, together with the before/after commit and source
fingerprint. A moving commit or source invalidates the run.

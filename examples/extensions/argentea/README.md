# Argentea: native graph partitions on Sail workers

Argentea is Nutmeg Banda's distributed execution path. Its bounded reference
and certified residual PageRank implementations pass worker-process and physical
two-host functional tests. Native CSR partitions persist within one Sail job;
ordinary Sail shuffles carry their updates. Larger iteration budgets, further
failure scenarios and performance remain under qualification.

Start with the [reference PageRank tutorial](PYTHON.md) or the
[certified residual PageRank tutorial](DELTA.md). Both describe build commands,
the Python API and exact-source qualification. The
[evidence index](../../../docs/development/extensions/argentea-validation/README.md)
retains successful and failed attempts, including a cap failure whose cause was
obscured by peer cancellation. The subsequent typed-cause repair passes fresh
worker-process and physical two-host checks. A failed query alone does not
qualify that cause; the native certificate and failure record are also audited.
The [integration inventory](../../../docs/development/extensions/argentea-integration.md)
explains each focused host change, its upstream foundation and remaining gates.

The core keeps an immutable outgoing CSR across full PageRank rounds, exchanges
typed contributions and producer-completion records, and uses Banda's existing
resource-accounting crate and Sail memory-lease ABI. It has no scheduler,
transport or separate graph API. The core unit harness uses tiny in-memory
fixtures; its results remain separate from the actual Sail execution evidence.

## Run the current probes

From the Sail checkout, with the repository Rust toolchain available:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/argentea-development-target \
  cargo test --manifest-path examples/extensions/argentea/Cargo.toml --locked -- --nocapture
```

The scheduler tests compile Sail's actual `TaskAssigner` and `StageGroup`
implementations. They show why cross-query partition ownership is not guaranteed,
why native state alone does not pin an idle worker, and when equal-width stages
share a worker within one job. A one-partition scalar stage is a negative control
because it can change task-set ownership.

For a commit gate, use a detached clean worktree and a separate target directory:

```sh
candidate=$(git rev-parse HEAD)
git worktree add --detach /tmp/argentea-gate "$candidate"
python3 /tmp/argentea-gate/examples/extensions/argentea/probe.py \
  --output /tmp/argentea-gate-evidence \
  --target-dir /tmp/argentea-gate-target
```

The evidence directory must not exist. The script checks formatting, clippy and
release tests, records exact source hashes and the tested commit, and checks that
sources did not change during execution. `--allow-working-tree` permits a
**development check**, explicitly labelled as such, before committing a candidate.

These probes do not include DataFusion plan execution, shuffle routing, wheel
loading, worker teardown or two-host behavior. Those remain mandatory integration
gates; a passing core receipt is not a distributed Argentea verdict.

## Generic worker boundary probe

`worker-probe` compiles the actual common worker descriptor/codec wrapper and its
unit tests without unrelated Sail workspace dependencies:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/argentea-worker-probe-target \
  cargo test --manifest-path examples/extensions/argentea/worker-probe/Cargo.toml --locked
```

This is useful before the combined Sail build. It does not exercise the remote
codec dispatcher, session factory, task runner or real shuffle; their host gates
must pass separately. The worker adapter must use an owned `EmissionCursor`
outside its native owner lock so bounded shuffle backpressure cannot prevent a
consumer from acquiring that same lock.

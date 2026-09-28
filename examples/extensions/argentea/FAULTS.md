# Qualify Argentea worker faults

`python/qualify_faults.py` checks the first failing call, native task and owner
cleanup, temporary-view cleanup, and final process/staging cleanup. It starts a
fresh Sail server with two worker processes on one POSIX host for each case.
This is functional evidence, with no performance or two-host fault claim.

Use the matching Sail worker-role branch and Nutmeg wheel described in
[the client tutorial](PYTHON.md). The qualifier imports the Python packages from
its own environment; Sail and both workers inherit that same environment. Freeze
a clean source checkout before the final gate. Record the actual runtime and
native wheel source SHAs separately from the qualifier source.

```bash
fault_output=$(mktemp -d "$PWD/target/argentea-faults.XXXXXX")
for case in cancel worker-loss quota; do
  .venv/bin/python examples/extensions/argentea/python/qualify_faults.py \
    --case "$case" --output "$fault_output/$case" \
    --sail-binary /absolute/path/to/sail \
    --runtime-source-sha "$SAIL_RUNTIME_SHA" \
    --native-source-sha "$NATIVE_WHEEL_SHA"
done
```

For explicit placement coverage, repeat `worker-loss` in new output directories
with `--victim-owner 0` and `--victim-owner 1`. Selection resolves the initialized
native owner to the already supervised worker; it never signals a PID from a
native receipt alone. Omitting this option retains the lowest-worker-ID default.
Receipts retain requested/selected owner and supervised identity, and independently
record the final output stage's task placement. Neither owner is assumed to
guarantee a particular first-error path; keep every attempt and its observed path.

Require exit status zero and `outcome: passed` for each receipt. A failed case
remains failed evidence; use a new output directory for any subsequent control.
`--allow-working-tree` marks a development check and is not the final source gate.
The qualifier never retries the Spark Connect call, although Sail's configured
ordinary task-attempt limit is three. Every native task must remain on attempt
zero, in one job and one P=2 worker stage group. The existing worker heartbeat
settings are one second between heartbeats and ten seconds to declare loss;
post-fault diagnostic SQL waits for the driver to retire the killed worker.

## Cancellation and worker loss

The graph contains 4096 vertices and 524288 directed edges: a cycle with 128
parallel copies of each edge, crossing the two owners. The initial full PageRank
certificate must stream these arcs. With `batch_rows=1`, this leaves substantial
work after CSR initialization even on a fast machine; the test does not rely on
an algorithm failing to converge or on a fixed sleep.

The controller waits for both atomic native `init` receipts, then sends SIGSTOP
to the two supervised worker PIDs. It confirms both are stopped in the driver's
process group and rejects a preceding result, close, or typed algorithm failure.
Only that observed held window permits fault injection. If the workload completes
first, the test fails. Worker PIDs come from the driver's launch log, never from
an arbitrary native receipt alone. A `finally` block resumes surviving workers.

- `cancel` calls Pecan's public `CancellationToken.cancel()`. The tag interruption
  must target a registered RPC; the client must receive `GraphCancelledError`.
  Both native owners must emit exactly one close receipt while workers stay alive.
- `worker-loss` sends SIGKILL to one stopped worker and resumes its peer. The
  whole native job must fail with automatic retry disabled. The surviving owner
  must emit one close receipt; a killed process cannot emit a close receipt.
  The process manager may retain its dead child as a zombie until session stop;
  the live check requires it to be absent or zombie, and final teardown requires
  that PID to be absent after reaping, just like every other worker.

The audit checks native stage width, placement, group, complete task inventory,
attempt zero, fixed worker/adjacency identity, and whole-job failure. It retains
all task outcomes and the original error. Session shutdown and process-group
shutdown must remove every observed worker and every staging file.

The receipt names the first RPC error path as `scheduler_failure` or
`bare_h2_transport`. Sail's `driver/output.rs::forward_job_output` forwards an
observed stream error unchanged, then requests failed-job cleanup. The separate
`job_scheduler/core.rs::refresh_job` failure action adds the text "automatic
retry disabled". Either can reach the caller first; that phrase is not a
guaranteed property of the transport error. The bare branch accepts only the
observed `SparkRuntimeException` with the exact HTTP/2 body-read error. Unrelated
errors remain failures.

Both worker-loss paths require an authoritative `FAILED` job, the exact killed
native owner matched to the supervised worker and held process window, SIGKILL
evidence, every job task terminal on attempt zero, and the complete native
stage/owner audit. Client retries must be disabled. No result, duplicate init,
replayed native job, missing survivor close, or failed final cleanup is accepted.
This proves the no-replay behavior independently of which error arrived first.
The earlier frozen qualifier's bare-HTTP/2 failure remains failed evidence; a
new qualifier source and fresh runtime attempt are required for this audit.

## Bind-time quota refusal

`quota` uses three vertices and a one-byte native quota. The native worker bind
must fail with the explicit `procedure memory budget exceeded (limit 1)` cause,
before any partition initializes. All native tasks must stop without retry and
session-owned staging must be removed. This gate does not establish cleanup of
an already initialized graph after an allocation refusal. An allocation failure
that surfaces only peer cancellation is not accepted as proof of a quota cause.

Native Arrow-buffer retention after close remains covered by separate native
and host lifetime tests; this process gate does not retain those buffers. It also
does not establish zero RSS or universal leak freedom.


## WCC and SSSP fault cases

`--algorithm` selects `pagerank_delta` (the existing default), `wcc_reference`,
`wcc_star`, `sssp_reference`, or `sssp_delta_star`. WCC and SSSP use a three-round
cap: 10 native stages for reference WCC and both SSSP methods, 28 for star WCC.
The fixture has 4,096 vertices and 524,288 cross-owner arcs; SSSP assigns each
arc weight 1. Native batches contain one row. Fault acceptance requires the
observed stopped-process window after both owners initialize, before a result
or another typed failure. This does not depend on the graph failing to converge.

For example, after the shared build/install setup:

```sh
python examples/extensions/argentea/python/qualify_faults.py \
  --algorithm sssp_delta_star --case worker-loss --victim-owner 1 \
  --sail-binary /absolute/path/to/sail \
  --runtime-source-sha "$SAIL_SOURCE_SHA" \
  --native-source-sha "$NUTMEG_SOURCE_SHA" \
  --output /absolute/path/to/new-sssp-worker-loss-evidence
```

Use `--case cancel` without `--victim-owner` for cancellation, or owner 0 for the
other loss case. Owner selection is explicit; it does not guarantee which RPC
error arrives first.

[The graph-fault archive](../../../docs/development/extensions/argentea-validation/README.md#graph-algorithm-cancellation-and-worker-loss)
retains 13 passing cases at qualifier `d03579496`, host `d9c6381a` and native
wheel `00ebb7ac9`: three cases for each WCC/SSSP method, plus a residual PageRank
cancellation control. Both first-error paths occurred for every WCC/SSSP method.
The envelope is two owners/processes, 32 worker task slots, a 2 GiB Sail pool and
256 MiB native admission per worker. These are functional process tests, not
physical two-host or performance measurements.

## Preserving later memory-refusal causes

WCC and SSSP now write `failure` records with `code=native_memory_budget` for an
exact local memory-budget error from their pinned resource-accounting dependency.
The core currently returns string errors, so the adapter matches the exact
local error and this operation's memory limit. It does not classify a generic
cancellation, a nested remote error, or the configured quota alone as a refusal.
The original execution error is returned unchanged; audit failure never replaces
it. This requires no new Sail host hook.

Records include worker/operation/adjacency identity, whether native state exists,
its phase, the limit, and live/peak reservation counters after the failed call
unwinds. Those counters are not RSS and do not state the refused allocation's
size. A peer cancellation can still be the first RPC error. Full post-init quota
qualification must independently require initialized owners, this causal record,
terminal tasks, cleanup and subsequent quota reuse on the same live workers.
That formal quota/reuse gate remains outstanding.

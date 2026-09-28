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

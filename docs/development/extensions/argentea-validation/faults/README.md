# Argentea worker failure evidence

These immutable archives retain the failure that motivated the Sail cleanup
repair, the original qualifier's error-ordering failure, and fresh qualification
of the repaired host. [archives.json](archives.json) pins every archive's bytes
and SHA256. Each archive contains its own decompressed-file manifest.

| Archive | Host / qualifier | Observed outcome |
| --- | --- | --- |
| `sail-argentea-fault-9a7d-evidence.tar.gz` | `038c9b959` / `9a7d0ffea` | Worker-loss gate failed: failed-job task records remained RUNNING after unassignment. Earlier probes and successful controls retained. |
| `sail-argentea-fault-d9c-evidence.tar.gz` | `d9c6381a` / `9a7d0ffea` | Four controls passed; one failed the qualifier's literal error-text requirement despite terminal tasks and cleanup. |
| `sail-argentea-fault-a37e-evidence.tar.gz` | `d9c6381a` / `a37e1dc8` | Thirteen controls passed; worker-loss runs observed the scheduler-wrapped error path only. |
| `sail-argentea-fault-4717-evidence.tar.gz` | `d9c6381a` / `47176805` | Six controls passed: cancellation, bind-time quota refusal, three worker-loss selections and positive residual PageRank. Both wrapped and bare HTTP/2 paths observed. |

The focused host repair marks nonterminal attempts canceled and reports their
state before unassigning workers during failed/canceled job cleanup. It preserves
existing terminal states and does not alter successful-job cleanup: output EOF
can precede a final success callback. The retained deterministic scheduler test
fails without this repair; the repaired host passes262 host tests and strict
Clippy.

The final qualifier requires a FAILED job, the exact supervised native victim,
a stopped-process injection window, SIGKILL, terminal tasks on attempt zero,
no result or replay, one surviving owner close, and final process/staging cleanup.
It records the first RPC error separately: output-stream forwarding can surface
bare HTTP/2 before the scheduler's contextual failure. Accepting that exact typed
error requires the same independent evidence as the wrapped path. An arbitrary
query error is insufficient.

These are two worker processes on Capitola, not a physical two-host fault test.
Quota refusal is at bind time, before initialization. Retained Arrow buffers,
post-initialization quota failure, arbitrary workloads, zero RSS and performance
are outside these process gates. See the [fault tutorial](../../../../../examples/extensions/argentea/FAULTS.md).

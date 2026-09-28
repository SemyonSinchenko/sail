# Argentea functional evidence

This directory retains functional qualification and failed attempts separately.
It contains no distributed performance comparison. The
[integration design](../argentea-integration.md) explains the host changes; the
[runtime tutorial](../../../../examples/extensions/argentea/PYTHON.md) gives
build and execution commands.

## Source and artifact boundaries

| Component | Source |
| --- | --- |
| Sail host hooks | `038c9b9597d3fcf7e0b8c30c1253d7d77563f012` |
| Initial native reference wheel | `e420cafc1ea03e889d6db11f212c9659c374346b` |
| First process qualifier | `a952564a4892c1bbd87a38243262ca130d05bd72` |
| Live resource qualifier | `a033ca1471b3dc7ae9796a8866b985446435bb88` |
| Nonempty two-host graph proof | `8d43c6f4868eb43d748c48210f90d999d2f494c9` |
| Reference receipt fix and residual implementation | `f11fe6e8a091e4be56a21712850d2a1a205c3e6c` |

The host passes 258 tests across the three affected crates, strict all-target
Clippy and a CLI build on ARM macOS, Intel macOS and Linux in Colima. The Intel
supervisor's original output pipe failed after tests and Clippy; its resumed
receipt retains that attempt and records the subsequent CLI build. Old protected
runtime artifact hashes are unchanged. Binary bytes are not included here;
receipts pin the binaries and their build source separately from client code.

## Included attempts

The [archive](reference-functional-evidence.tar.gz) contains 59 original
artifacts plus its manifest. Its SHA256 is
`e9e1ec9a4d0a9be639a52d9ae1d32d843ffe9b589c78d4012d2a83a3e30ba6a6`.
The [manifest](reference-functional-manifest.json) lists every artifact hash;
the [bundle receipt](reference-functional-bundle.json) records each outcome.

| Archive directory | Outcome and coverage |
| --- | --- |
| `process-a952/` | Passed: two native workers, P=3 including an empty owner, two reference rounds, complete vector, stable adjacency, final cleanup. |
| `resource-a033/` | Passed: same workers and session through success, post-initialization cardinality failure and three later successes; 32 MiB native quota within a 48 MiB pool. |
| `resource-f11/` | Passed: the same version-1 resource gate with the wheel containing the receipt fix and version-2 implementation; this is not version-2 qualification. |
| `two-host-8d43-forwarded/` | Passed: actual Capitola/Morrobay native PIDs, nonempty graph data on both, three crossing arcs, P=5 with an empty owner, 35 native events and 15 native task completions. |
| `x86-a952-log-interleaving/` | Failed audit after correct graph output: concurrent formatted stderr writes interleaved native receipt lines. |
| `two-host-a952-direct-failure/` | Failed before graph work: HTTP/2 connection handshake timeout on Capitola's LAN address. |
| `two-host-a952-direct-repeat/` | Same direct-address failure in an unchanged fresh attempt. |
| `two-host-http2-diagnostic/` | Diagnostic interrupted after recording loopback success/LAN timeout; all supervised processes stopped. It is not a qualification pass. |
| `transport/` | Exact SSH forwarding command and direct HTTP/2 observations. |
| `host/` | ARM, Intel and Linux host-gate receipts. |
| `receipt-writer/` | Exact helper-source release stress under ten CPU burners: 260,096 complete distinct records; all owned burners stopped. |

The physical two-host test used the same x86 binary/wheel bytes, with Rosetta on
Capitola. Explicit SSH loopback forwards carried Sail gRPC/Flight traffic.
Direct LAN operation and native two-host performance are not established by
this test. It used existing Sail scheduling and shuffles; no transport was
added to Sail. A separate post-stop object-store listing found no remaining
objects under the owned staging root.

The resource gate retained all four successful results while testing reuse.
Those results were Parquet files, not retained native Arrow buffers. It proves
that a full native quota reservation did not survive operation close under the
tested envelope, not zero RSS or general leak freedom. Arrow slice ownership
has separate native tests. Worker loss and active cancellation require their
own runtime gates.

Every outcome remains in its original receipt; none is replaced by a later
success. The initial resource invocation with an incorrectly expanded runtime
SHA is excluded from accepted evidence and retained locally with an explicit
invalid-provenance verdict. Its replacement qualifier checks source existence
and the successful run supplies the actual full source identity.

## Residual PageRank and worker readiness

The separate [residual archive](residual-functional-evidence.tar.gz) contains
207 unchanged artifacts and its manifest. SHA256:
`781f5034a7e9d7dd7a9adfa13f3497178e4ba18d5daf3f012d4ba37f52a665ab`.
The [manifest](residual-functional-manifest.json),
[bundle receipt](residual-functional-bundle.json) and
[decompressed scan](residual-functional-scan.json) describe every byte and
attempt. The original reference archive above remains unchanged.

| Archive directory | Outcome and coverage |
| --- | --- |
| `nested-plan-depth-failure/` | Failed before native allocation: a nested 32-phase client plan exceeded the existing protobuf depth guard. |
| `lazy-view-private-probe/` | Passed diagnostic: existing temporary views composed 32 phases into one native job without changing the host guard. The diagnostic script is included. |
| `raw-cap-9ea/` | Passed negative gate: zero-push nonstationary input failed with the cap cause and all owners closed. |
| `raw-stationary-k0-private/` | Passed diagnostic: stationary input certified with zero pushes and four native phases. |
| `v1-two-host-readiness-failure/` | Correct ranks, failed physical distribution audit: all native work ran before the second worker registered. |
| `v1-two-host-ready/` | Passed fixed-wheel reference gate after explicitly waiting for registered workers; nonempty graph data on both physical hosts. |
| `public-client-gates/` | Public view composer: 209 unit tests, residual convergence, stationary DONE transport and local-mode refusal pass. A cap run failed its strict cause check; an identical fresh cap control passed. Both remain included. |
| `v2-two-host/` | Passed public residual client: 32 phases, P=5, two physical hosts, six pushes and two certificates; all owned views, native owners, processes and staging released. |

The public client source is `010b9e065ad2adc33a7169de73191a039ce8d6d0`;
readiness-only reference qualification is `941b5e50b90e5fbf7f5184c733f7d9d522aa42c5`.
Both use host `038c9b9597d3fcf7e0b8c30c1253d7d77563f012` and native wheel
source `f11fe6e8a091e4be56a21712850d2a1a205c3e6c`. The residual fixture's
independent normalized residual is `0.00039088808203777137 <= 1e-3`. This does
not establish convergence for arbitrary graphs within the seven-push budget.
All pre-terminal view registrations and explain calls produced zero native
events. Physical execution retains the SSH/Rosetta boundary described above.

The cap cause check is outstanding at this source: one run surfaced peer
cancellation, while a fresh unchanged run surfaced push-cap exhaustion. A
cancelled query by itself is insufficient evidence of the intended cap failure.
Typed native cause evidence is the next repair; this archive is not an overall
clean qualification verdict for every failure path.

## Typed cap failure repair

The separate [cap repair archive](cap-repair-functional-evidence.tar.gz) contains
164 unchanged artifacts plus its manifest. SHA256:
`90a78e0c4311649606fafbcff316d8328bbbfbd9f6087ebbdd9bf7333d629e88`.
The [manifest](cap-repair-functional-manifest.json),
[bundle receipt](cap-repair-functional-bundle.json) and
[decompressed scan](cap-repair-functional-scan.json) record its contents. Both
earlier archives remain byte-for-byte unchanged.

The host remains `038c9b9597d3fcf7e0b8c30c1253d7d77563f012`. The repaired
native wheel source is `50195d14aafca6559aace887bf699d2d18c62c6f`; the combined
client source is `277ae341c90cad66e118a667565c0699ece3d77b`. Wheel build and
source receipts are under `provenance/`.

| Archive directory | Outcome and coverage |
| --- | --- |
| `arm-gates/` | Three fresh cap cases, residual convergence and stationary DONE transport all pass with the repaired wheel. |
| `two-host-cap/` | Passed negative gate: complete fresh certificate, residual above tolerance, typed cap cause, failed query, no result/retry and all owners closed. |
| `two-host-residual/` | Passed positive control: 32 phases, six pushes, two certificates, nonempty graph data on both physical hosts and a crossing arc. |

The native `pagerank_push_cap` record is emitted only after the complete fresh
global certificate establishes nonconvergence at the push limit. The qualifier
requires that causal record and certificate as well as query failure and
cleanup. The first ARM cap case still surfaces peer cancellation through the
RPC; that error alone is not accepted as evidence. This fixes cause attribution
without asserting deterministic ordering of concurrent RPC failures.

Both physical runs retain independent post-stop listings of their owned storage
prefixes: empty. All supervised processes are absent. The original SSH/Rosetta
and bounded functional boundaries still apply. This evidence does not qualify
injected worker loss, active cancellation, larger graphs or performance.

## BFS worker execution

The [BFS archive](bfs-functional-evidence.tar.gz) contains 525 original artifacts
plus its manifest. SHA256:
`092e4b4ef0cfa1cf1c98131015e7db3baaa98f5e988f5c1afd8fbf7e89e03dc0`.
The [manifest](bfs-functional-manifest.json), [bundle receipt](bfs-functional-bundle.json)
and [scan](bfs-functional-scan.json) pin every included byte; earlier archives
remain unchanged.

The native wheel and client source are
`b3c543171c6c64df0ef53677394abc24e9a2de99`, with the unchanged `038c9b9597d3`
host. Two additional pre-execution input controls use Python-only helper source
`8b42fbe7a4dcc80212aee9be105d9b7d5f118cac` with that same native wheel. The
combined source passes 54 core release tests, 32 native adapter tests and 386
Python tests; the helper extends the Python gate to 398. Core Clippy is strict;
native Clippy retains only the disclosed pre-existing `mutation.rs` enum-size
exception. A separate core-only release stress runs 5,748 checks with ten owned
CPU burners, all reaped afterward; it is not a distributed stress verdict.

| Archive directory | Outcome and coverage |
| --- | --- |
| `arm-gates/` | 15 passed cases: reference/frontier/direction across directed, undirected, source-only and cap fixtures; local refusal; missing-source prevalidation; unknown request-field registration refusal. |
| `two-host-reference/` | Passed: exact distances/parents, 32 native phases, four expansions, nonempty graph data on both physical hosts and five cross-host arcs. |
| `two-host-frontier/` | Passed the same full-vector and ownership checks. |
| `two-host-direction/` | Passed the same checks, with four actual Pull expansions. |
| `two-host-cap/` | Passed negative gate: complete topology, typed `bfs_level_cap` cause, failed query, no result/retry and all owners closed. |

All positive calls allocate zero native state during temporary-view registration
and explain. Every owned view and native owner closes; all supervised processes
are absent afterward. The physical runs retain independent empty listings of
their owned storage prefixes. The helper's first rejected CLI invocation is
also retained: its unsupported argument prevented server startup, and it is not
counted as a runtime case. Physical tests retain the SSH/Rosetta boundary above.
See [BFS.md](../../../../examples/extensions/argentea/BFS.md) for reproduction.

## Verify delivered bytes

From the repository root, using Python 3.12 or later:

```bash
python3 - <<'PY'
import hashlib, json, tarfile
from pathlib import Path

root = Path("docs/development/extensions/argentea-validation")
for name in ("reference-functional", "residual-functional", "cap-repair-functional", "bfs-functional", "wcc-functional", "sssp-functional", "graph-fault-functional"):
    receipt = json.loads((root / f"{name}-bundle.json").read_text())
    archive = root / receipt["archive"]
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == receipt["sha256"]
    manifest = json.loads((root / f"{name}-manifest.json").read_text())
    with tarfile.open(archive, "r:gz") as bundle:
        names = bundle.getnames()
        assert len(names) == len(set(names)) == receipt["included_files"]
        assert set(names) == {item["path"] for item in manifest["files"]} | {"manifest.json"}
        assert json.load(bundle.extractfile("manifest.json")) == manifest
        for item in manifest["files"]:
            data = bundle.extractfile(item["path"]).read()
            assert len(data) == item["size_bytes"]
            assert hashlib.sha256(data).hexdigest() == item["sha256"]
    print(f"Argentea {name}: bytes verified")
PY
```

The [scan receipt](reference-functional-scan.json) records the actual patterns
and configured credential fields checked across all decompressed files, with
zero matches. Credential values are not included. This scan is not a guarantee
of exhaustive secret detection. Source gates and new runtime attempts must
produce their own receipts; validating these bytes does not rerun the tests.

## WCC process-cluster and cap qualification

[wcc-functional-bundle.json](wcc-functional-bundle.json) pins
[wcc-functional-evidence.tar.gz](wcc-functional-evidence.tar.gz), its956 files,
source/artifact identities and all14 recorded attempts. The
[manifest](wcc-functional-manifest.json) hashes decompressed bytes; the
[scan](wcc-functional-scan.json) records the credential/token checks.

The five frozen positive/refusal cases at `605a42299` and four cap/control cases
at `c063649ae` passed against host `d9c6381a` and the ARM Nutmeg wheel from
`b831464ea`. Reference WCC uses synchronous min-label propagation; advanced WCC
uses seeded head/tail star contraction with retained original adjacency. It is
not Banda/Pecan's GF64 contraction and makes no equivalent-work claim.

The positive gates cover128 native reference stages,124 star stages, signed
BIGINT extrema, disconnected vertices, empty owners and local-mode rejection.
Every phase is audited for P-wide worker placement, a shared task-slot group,
complete owner/task inventory, attempt zero, fixed worker/adjacency identity,
message conservation, minimum-ID components against independent union-find, and
view, staging and process cleanup. The test envelope is512 worker task slots,
2GiB Sail pool and256MiB native admission per worker, with five owners. These are
development-profile functional results, not performance or physical two-host
WCC evidence.

Three expected failures require the typed `wcc_round_cap` cause and a complete
pre-cap barrier. Reference K=0 reports an unattempted certificate and measured
change count0. Star K=0 reports14 crossing arcs; star K=3 reports8. No partial
components return. All stored job tasks are terminal at attempt zero and every
owner closes. The original positive star fixture converges in five rounds.

The initial multi-case probe retains its failed fourth attempt: the PySpark
client raised a thread-pool shutdown error during worker readiness before WCC
execution. The same star19 case passed in a fresh client process. Formal cases
use one fresh Python process each; the failure was not reclassified. Native
adapter development logs also retain earlier launch, fixture and Clippy failures.

See [WCC execution and qualification](../../../../examples/extensions/argentea/WCC_ADAPTER.md)
for commands and the bounded-plan contract. Physical two-host qualification,
algorithm-specific post-initialization quota/skew and fault controls, and
performance measurements remain separate gates.


## SSSP process-cluster and cap qualification

[sssp-functional-bundle.json](sssp-functional-bundle.json) pins the
[archive](sssp-functional-evidence.tar.gz), its 545 files and all 12 attempts:
ten formal cases from `00ebb7ac9` and two earlier positive probes from
`7c2770130`. The [manifest](sssp-functional-manifest.json) hashes every included
file; the [scan](sssp-functional-scan.json) records the decompressed-byte scan.
The probes retain their narrower answer/native-event/cleanup boundary.

All ten formal cases passed against host `d9c6381a` with the ARM development
wheel built from `00ebb7ac9`. Reference is synchronous Bellman–Ford; advanced is
all-edge delta-star. The latter selects the globally smallest active bucket,
including repeated same-bucket work; it is not classical light/heavy
stepping. Both 128-stage plans pass complete independent Dijkstra vector and
predecessor checks, native event/barrier/message audits and Sail stage/task
placement audits. Signed extrema, isolates and a skew fixture with all vertices
on one of five owners also pass. Worker/CSR identities stay fixed, and views,
staging and supervised processes are absent after cleanup.

Zero-round caps for both methods finish topology, report one active source and
return no partial distances. The one-round delta-star cap reports two active
vertices and three reached vertices. Each cap has a typed `sssp_round_cap`
cause, complete pre-cap barriers, a FAILED job, terminal stored task records,
no native retry and all owners closed. Local execution is explicitly rejected.
The positive graph takes four relaxation rounds for either method; the skew
case takes five delta-star rounds, and isolates take one reference round.

The disclosed envelope is five owners, 512 worker task slots, a 2 GiB Sail pool
and 256 MiB native admission per worker. These are functional results. Linux,
physical two-host execution, SSSP-specific post-initialization quota/fault gates
and time/memory comparisons remain separate requirements. See the
[SSSP tutorial](../../../../examples/extensions/argentea/SSSP_ADAPTER.md) for
build context, API examples and exact-source qualification commands.


## Graph algorithm cancellation and worker loss

[graph-fault-functional-bundle.json](graph-fault-functional-bundle.json) pins
[the archive](graph-fault-functional-evidence.tar.gz), its 73 files, the
[manifest](graph-fault-functional-manifest.json) and
[scan](graph-fault-functional-scan.json). All 13 cases passed at qualifier
`d03579496`, host `d9c6381a`, and ARM development wheel `00ebb7ac9`.

Each reference/star WCC and reference/delta-star SSSP method was canceled once
and run twice with loss of an explicitly selected owner (0 and 1). A residual
PageRank cancellation control also passed. The gate observes native
initialization and a supervised stopped-process window before injecting any
fault. It requires no result, no replay, complete terminal task inventories,
fixed owner identity, surviving-owner closure and final process/staging cleanup.
Every WCC/SSSP method observed both the scheduler-wrapped and bare HTTP/2 first
RPC error paths; neither arrival order nor a particular victim-to-error mapping
is promised.

Reference WCC and both SSSP methods have 10 native stages and 20 native tasks;
star WCC has 28 stages and 56 tasks. Each cancellation closes two owners; each
worker loss closes the survivor. The PageRank control has 32 stages and 64 tasks.
The disclosed envelope is two owners/workers, 32 task slots, a 2 GiB Sail pool
and 256 MiB native admission per worker. No additional host change was needed.
These are two processes on Capitola. Physical two-host faults and post-init
quota/reuse remain separate gates. See [FAULTS.md](../../../../examples/extensions/argentea/FAULTS.md)
for commands and the causal-evidence contract.


## Graph memory refusal and same-worker reuse

[Graph resource evidence](graph-resource-functional-evidence.tar.gz) preserves
four clean-source qualifications at `e246f3d26c82728a14e6b1dffd3bf2448e549be0`,
two development sequences, and six earlier causal diagnostics. The
[bundle](graph-resource-functional-bundle.json),
[manifest](graph-resource-functional-manifest.json), and
[byte scan](graph-resource-functional-scan.json) identify every artifact.

Reference/star WCC and reference/delta-star SSSP each pass warmup, post-init
native memory refusal, and three successful queries on the same two live workers
and session. The 32 MiB native quota sits inside a 48 MiB Sail pool: retaining a
full old reservation would prevent admission of the next. Every initialized
owner closes, native tasks terminate without replay, prior Parquet outputs remain
readable, owned views disappear, and final session/process/staging cleanup passes.
Independent result and stage/task checks accompany the causal native receipts.

The native wheel is built from `5ce69ac89d70eb6e3358add5d346c0a62d2c6fe6`;
the Sail host is `d9c6381a0effe07ab89ddb87739a0b2003b9664b`. These are ARM
process-cluster functional checks, not physical two-host, Linux, RSS or timing
measurements. Earlier peer-cancellation-only and pre-init refusal diagnostics are
retained as diagnostics, not promoted to successful post-init qualifications.
See the [reproduction tutorial](../../../../examples/extensions/argentea/GRAPH_RESOURCES.md).


## Extended BFS phase budget

[BFS128 evidence](bfs128-functional-evidence.tar.gz) contains six passing ARM
process-cluster cases and six retained pre-execution failures. The
[bundle](bfs128-functional-bundle.json), [manifest](bfs128-functional-manifest.json),
and [byte scan](bfs128-functional-scan.json) pin all 838 files.

Reference, frontier and direction BFS each pass a 62-vertex chain requiring
62 expansions and the standard graph converging after four. Every case executes
a 128-stage native plan with five owners and two workers; exact answers,
owner identity, task placement, no replay, phase views, process and storage
cleanup are audited. The client source is
`b1173b1d61fa621286f4861ffccd259e8d735a1e`; native wheel source is recorded in the
bundle's build receipt. The host remains the focused failed-task cleanup build.

The earlier attempts failed because the client omitted the explicit view-composer
budget. A public-client regression test reproduces that failure, then passes with
the forwarding fix. Defaults stay at14 levels/32 stages; callers opt into the
larger budget. Linux and physical two-host execution at128 stages remain pending.
See [BFS reproduction commands](../../../../examples/extensions/argentea/BFS.md#extended-128-stage-qualification).

## Remote-harness local regressions

The [regression bundle](remote-harness-local-regressions.tar.gz) preserves 71
original files plus their hash manifest. Its [index](remote-harness-local-regressions.json)
pins the source, artifacts, commands, host, and archive digest. The three runs
passed on local macOS process workers:

- SSSP delta-star: warmup, post-init native memory refusal, and three reuse jobs
  in the same session, with retained earlier Parquet outputs.
- WCC star: loss of native owner 1 after both owners initialized.
- SSSP delta-star: cancellation after both owners initialized.

Complete final logs were re-audited. Each run's teardown receipt reports all
supervised processes absent and no owned staging files. The qualifier snapshot
`95f36ff6306529d7443053862df78e45866739bc` has the same source tree as published
commit `8c6c9423b4f774f945f8c6423320014d442d5375`. Runtime and native build commits
are recorded separately in the index. These runs check the local execution path
after adding the remote adapters; they do **not** qualify physical two-host
execution or Linux, and their durations are not benchmark results.

## Linux ARM64 native core gate

The [Linux core bundle](linux-arm64-core-8154357.tar.gz) and
[index](linux-arm64-core-8154357.json) retain 92 passing release-mode native core
tests and strict all-target Clippy on frozen source
`8154357553af2147e16fdea650e013379b0af2cf`. The image digest, three-CPU/6 GiB
container limits, commands, and logs are recorded. The first Clippy attempt
failed because the base image lacked the component; that attempt is retained
alongside the successful run after installing it.

This is functional Linux ARM64 evidence on Capitola's Docker Desktop VM. It
covers the native core, not the full Sail process matrix or physical two-host
execution. Durations in the logs are not published benchmark measurements.

## Linux ARM64 process-cluster qualification

The [functional archive](linux-arm64-functional-8154357.tar.gz) and
[index](linux-arm64-functional-8154357.json) preserve 41 passing cases:

- Six 128-phase BFS cases: reference, frontier, and direction-optimizing BFS on
  the 62-vertex chain and the graph fixture.
- Eight WCC cases: reference and seeded star contraction, with graph, signed-ID
  extremes, isolates, and expected cap refusal.
- Ten SSSP cases: reference and delta-star, with graph, signed-ID extremes,
  isolates, owner skew, and expected cap refusal.
- Thirteen fault cases: PageRank-delta cancellation, then cancellation and loss
  of either native owner for each WCC/SSSP variant.
- Four resource cases: warmup, post-initialization native memory refusal, and
  three same-session reuse operations for each WCC/SSSP variant.

All 41 complete logs were re-audited. All 41 teardown receipts report absent
supervised processes and empty owned staging. The archive includes 3,153 files
plus the manifest, build and runner scripts, exact commands, image and binary
hashes, and original receipts. The native wheel built and installed successfully;
Pecan's initial installation then failed because setuptools tried to write into
the read-only checkout. That log is retained, along with the successful install
from a byte-verified writable copy.

The host and native extension were built from
`8154357553af2147e16fdea650e013379b0af2cf` in the development profile with debug
information disabled. Runs used Linux ARM64 under Docker Desktop on Capitola,
three container CPUs and a 10 GiB container memory limit without swap. These are
functional results, not release-profile performance measurements. They do not
replace physical Capitola/Morrobay qualification.

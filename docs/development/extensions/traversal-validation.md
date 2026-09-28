# BFS/SSSP implementation and qualification

The traversal branch adds reference and advanced BFS/SSSP to Pecan, Nutmeg Banda
and Nutmeg Grenada. The existing PageRank/WCC campaign remains frozen under its
original source identities. This document records functional qualification;
Linux functional and CPU-capped stress gates also pass with the native lint
exception disclosed below. Isolated time/memory comparisons and large capacity
runs remain pending.

Build and run every method with the
[traversal tutorial](../../../examples/extensions/benchmarks/TRAVERSAL-TUTORIAL.md).
The [input guide](../../../examples/extensions/benchmarks/GRAPH500.md) builds the
pinned official Graph500 generator and streams its output to partitioned Parquet.
The [external-control guide](../../../examples/extensions/benchmarks/traversal-controls/README.md)
provides fixed-source GAPBS and Parallel-SSSP checks on exact integer fixtures.

## Implemented paths

Pecan and Grenada share relational full-set, frontier, and advanced methods.
BFS's advanced method changes push/pull join orientation; it does not claim
native adjacency early exit. SSSP's advanced method is all-edge delta-star
stepping, explicitly distinguished from classical light/heavy delta-stepping.
Both return distance, hop count and rooted parents, including zero-weight cycles.

Banda retains its existing BFS, Bellman–Ford and Dijkstra kernels. New native
`bfsDirection` builds forward/reverse adjacency and switches between sparse push
and early-exit pull. `ssspDeltaStar` uses parallel all-edge relaxation and an
indexed bucket queue. Its output is distances only. Native graph computation
remains driver-local, including when the surrounding Sail server has workers.
[Argentea](argentea-plan.md) is a separate distributed-native workstream.

Query-owned adjacency and scratch use the existing native resource accounting.
Weighted snapshots validate matching graph revision and entry identity, including
drop/recreate races. Retained output arrays hold their reservations. Input and
work quotas, cancellation, invalid weights, overflow and iteration exhaustion
have explicit failure behavior.

## Findings incorporated before qualification

The original delta-star queue read mutable atomic distances directly as heap
keys. A parallel wave could change several queued priorities before sequential
repairs and invalidate the heap order. That violated lowest-bucket selection;
it was not evidence of a demonstrated wrong final distance. The fixed queue
owns admitted priority keys and applies decreases sequentially after each wave.
The regression fails against the original production queue and passes against
the replacement; the negative-control log is retained below.

Native pull BFS originally checked work/cancellation only while visiting edges.
Long scans over isolated or already visited vertices could postpone cancellation.
It now charges fixed destination blocks as well as examined edges. A deterministic
work-limit test exercises the empty-adjacency scan. Parallel fixtures cross the
actual execution threshold and verify requested widths 1, 2 and 8.

Portable SSSP now rejects overflowing explored relaxations even when another
shorter path dominates that candidate, matching the new native kernel contract.
Cancellation tests cover every portable traversal method and verify owned-stage
removal. Partition-count checks include an isolated source.

## Recorded functional evidence

The core qualification candidate is
`b2e7e0faf86b64b8c7a54fb14210db7485c9fe99`, tested in detached worktrees. The
Mac functional server uses the unchanged Sail runtime source
`70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`; each receipt pins its binary and
installed native package files separately. Rust tests are native arm64 on
Capitola. Server/Python integration uses matching x86_64 artifacts through
Rosetta. These Mac checks do not establish Linux compatibility or performance;
the separately pinned Linux qualification follows below.

| Check | Observed result |
| --- | --- |
| Native unit and admission tests | 85 unit tests and one admission integration test passed |
| Native graph-core Clippy | All targets passed with warnings denied; this gate covers `nutmeg-graph`, not the wheel adapter |
| Native release stress | Four idle and 80 targeted runs with ten CPU-saturating processes passed; every load process was cleaned up |
| Portable traversal, existing reference algorithms and certificate tests | 51 passed locally and 51 with process workers |
| Full entry-path matrix | 36 passed: eighteen methods in each Sail mode |
| Graph500 input plus certificate integration | Six passed: BFS and SSSP across all three paths, with process workers |
| Frozen harness tests | 126 passed; nine live-server certificate tests skipped here and run in the preceding integration gate |
| Pinned generator and external controls | 58 passed in their separate frozen source gate |

The six Graph500 cases use 64 vertices, 256 original edge tuples, and 48 vertices
reachable from the selected source. The checks therefore exercise traversal,
including unreachable vertices, rather than succeeding on an isolated root.

[Functional evidence](traversal-validation/functional-evidence.json) records all
42 full-call cases, output hashes, source identities, correctness results and
cleanup. Their raw receipts remain at the recorded local paths. Output hashes
and zero remaining owned staging files were independently rechecked while
writing this evidence. The artifact deliberately omits unisolated timing and
memory observations from comparison tables.

## Linux qualification

Source `038c9b9597d3fcf7e0b8c30c1253d7d77563f012` passes the traversal
functional suite on Morrobay in Linux/Colima. Its Sail executable is a development
build; the repaired native wheel and stress executables are release builds.
These runs establish functional behavior, not comparative performance.

- All 36 local/process method combinations and six Graph500 certificate cases
  pass. A separate audit recomputes all 42 small result vectors with independent
  queue BFS and heap Dijkstra; maximum absolute distance error is zero.
- Live traversal/reference/certificate tests pass 51 cases in each Sail mode.
  Host library gates pass 258 tests; native graph gates pass 85 unit tests, one
  admission test and five extension FFI tests.
- Host and native graph-core Clippy pass with warnings denied. Native extension
  strict Clippy fails on the unchanged `mutation.rs` large-enum finding. The
  controlled rerun allows only `clippy::large_enum_variant`; the original failure
  and source-parity evidence remain included. It is not a strict native pass.
- Five idle and 90 loaded release runs pass with two CPUs, 8 GiB and no swap.
  Throttling counters confirm the CPU limit applied; every owned load process
  was reaped. These elapsed times are not benchmark results.
- Harness tests pass 61 cases plus three later real-generator integrations.
  The remaining 27 optional tests require external GAP/Parallel-SSSP binaries;
  their earlier qualification does not become a Linux verdict.

The [summary](traversal-validation/linux-038c-summary.json) pins runtime and
wheel hashes. The [archive](traversal-validation/linux-038c-evidence.tar.gz)
contains 321 files, including exact commands, failures, receipts, small Parquet
inputs/results and an independently runnable vector auditor. Its SHA256 is
`cd033060b69ec26a93e2ddd46da2befb8322d236394ff5c7a9adbfccff1c5734`;
the [archive receipt](traversal-validation/linux-038c-archive.json) and
[decompressed scan](traversal-validation/linux-038c-scan.json) record byte
verification and the credential-pattern check.

Verify all delivered files without extracting them:

```sh
python3 - <<'PY'
import hashlib, json, tarfile
from pathlib import Path
root = Path('docs/development/extensions/traversal-validation')
receipt = json.loads((root / 'linux-038c-archive.json').read_text())
archive = root / receipt['archive']
assert hashlib.sha256(archive.read_bytes()).hexdigest() == receipt['sha256']
with tarfile.open(archive, 'r:gz') as bundle:
    manifest = json.load(bundle.extractfile('evidence/manifest.json'))
    names = bundle.getnames()
    assert len(names) == len(set(names)) == receipt['files']
    assert set(names) == {'evidence/' + f['path'] for f in manifest['files']} | {'evidence/manifest.json'}
    for item in manifest['files']:
        data = bundle.extractfile('evidence/' + item['path']).read()
        assert len(data) == item['bytes']
        assert hashlib.sha256(data).hexdigest() == item['sha256']
print('Linux traversal evidence: all bytes verified')
PY
```

## Large-result validation

The default gate compares every distance with independently generated reference
vectors. Large inputs may instead explicitly request a distributed certificate:
all-edge triangle/reachability inequalities plus source-rooted reachability
through tight edges. The latter condition rejects disconnected zero-weight
cycles with fabricated finite distances, which local inequalities alone miss.

BFS uses exact integer distances. Floating-point SSSP records local slack and a
conservative accumulated absolute-distance error bound; it does not claim the
same relative-error guarantee as a full-vector oracle. Parent/hop columns are
checked when a method exposes them. Certificate work runs after the measured
algorithm call, has its own round cap, and cannot turn incomplete validation
into success. Its joins and materializations remain server-side.

## Remaining qualification

Isolated measurements and large Graph500/real-graph capacity runs remain
separate requirements. External
integer controls do not yet cover the official generator's fractional weights;
those inputs must not be silently quantized for comparison. Native SSSP lacks
parent output, so these checks do not establish official Graph500 compliance.

Argentea has separate [PageRank integration evidence](argentea-validation/README.md).
Those results do not qualify distributed native BFS/SSSP. Their worker state,
routing, cancellation and two-host behavior still require actual execution;
source-level scheduler probes and partition-core tests are insufficient.

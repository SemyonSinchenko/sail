# Pecan, Banda and Grenada on large Graph Kernels inputs

The completed Morrobay campaign contains **180 trials: 179 passes and one
timeout**. It measures full Sail PageRank/WCC calls on four graphs with
2,097,152 or 4,194,304 vertices. An independent audit checked all 179 successful
output vectors, replayed the summaries and reconstructed 117,958 memory samples.
The timeout remains in the evidence and in the affected method's sample count.

The [reproduction guide](../../../examples/extensions/benchmarks/LARGE-GRAPHS.md)
describes installation, exact inputs and matrix execution. The accompanying
[evidence README](pecan-nutmeg-large-benchmark/README.md) explains the artifacts,
hash verification and independent raw audit. The earlier
[sparse-graph campaign](pecan-nutmeg-benchmark.md) remains a separate experiment.

## Execution paths and methods

| Path | Where the work runs |
| --- | --- |
| Pecan | Python controls bounded iterations; Sail/DataFusion workers run relational queries and materialize state. |
| Banda | Nutmeg stages the graph and CSR inside Sail's driver; native kernels run in that process. |
| Grenada | Nutmeg graph tables enter the same Pecan relational controller and Sail/DataFusion execution. |

All trials use a driver and two worker processes on one physical host. Banda
remains driver-local; this campaign does not measure distributed native graph
execution. Grenada is a separate graph-table entry path, sharing Pecan's
algorithm implementation. No extra remote graph service is involved.

| Algorithm | Selector | Pecan / Grenada | Banda |
| --- | --- | --- | --- |
| PageRank | `reference` | Power iteration | Grust 0.23.0 power iteration |
| PageRank | `optimized` | Delta/frontier | Native delta/frontier |
| WCC | `reference` | Minimum-label propagation | Grust 0.23.0 union-find |
| WCC | `optimized` | Randomized contraction | Native randomized contraction |
| WCC | `fused` | Fused randomized contraction | Native contraction with deferred initial deduplication |

The `optimized` selector names an implementation, not a performance guarantee.
All previous methods remain available. Reference WCC compares different
algorithms; its columns should not be interpreted as equivalent kernel costs.
The [algorithm contracts](../../../examples/extensions/vendor/nutmeg-graph/OPTIMIZED_ALGORITHMS.md)
and [earlier report](pecan-nutmeg-benchmark.md) explain frontier semantics,
contraction, Sem's fusion and the native memory admission contract.

PageRank uses float64, damping 0.85, uniform initialization/restart and uniform
dangling redistribution. The tolerance is `1e-8`, with a 1,000-iteration cap.
Every successful result is checked for its complete vertex set, normalization,
finite nonnegative scores, full-vector error and true fixed-point L1 residual.
WCC treats edges as undirected and checks every vertex against independent
union-find after normalizing component labels. Randomized WCC uses seed 42.

## Inputs and resource envelope

These are the exact hash-pinned hub and uniform inputs used by adversari.al's
Graph Kernels campaign, imported without changing edge order or multiplicity.
WCC extends that campaign's large-input PageRank coverage. This experiment is
not a Graph500 submission.

| Input | Vertices | Edges |
| --- | ---: | ---: |
| hub-2097152 | 2,097,152 | 16,762,845 |
| uniform-2097152 | 2,097,152 | 16,777,173 |
| hub-4194304 | 4,194,304 | 33,525,729 |
| uniform-4194304 | 4,194,304 | 33,554,395 |

Each imported graph has one weak component, no isolates and no dangling
vertices. These properties limit generalization to long chains, many small
components, graphs with dangling mass, or application-specific degree patterns.
The [input pins](../../../examples/extensions/benchmarks/large-fixtures.json)
and original dataset manifests in the proof archive give SHA256 values.

Morrobay is an Intel iMac Pro with a Xeon W-2191B, 36 logical CPUs and 128 GiB
RAM. Linux ran in the x86_64 Colima/QEMU `sail-gate` VM. The host was allocated
to this campaign; no other task build or benchmark ran during timed trials.
Ordinary macOS background activity and the idle second Colima VM remained.
[Before/after host inventories](pecan-nutmeg-large-benchmark/inventory/pre-large-full-host.json)
retain the machine and container observations. Whole-VM steal was **0.0% in all
180 recorded observations**; this does not measure host scheduling outside
the VM.

| Setting | Same setting for all three paths |
| --- | --- |
| Container | Fresh container for every trial; 16 CPUs, cpuset `0-15`, 56 GiB, no swap |
| Processes | Driver and two Sail workers on Morrobay |
| Parallelism | 16 requested threads / relational partitions |
| Scheduling | 64 task slots per worker |
| Sail memory pool | 48 GiB per process |
| Driver-native admission | 32 GiB, prepaid within the driver's Sail pool |
| Timeout | 1,800 seconds per call; 2,400-second outer supervisor cap |
| Repetitions | Three per graph / path / method; 180 planned trials |

Pools and native admission are accounting limits, not allocated RSS. Their
sum does not override the aggregate container limit. Requested native
parallelism is not evidence that every kernel phase uses that width; the audit
retains the advanced-kernel width observations, while native reference actual
widths are not exported.

The timer starts with lazy input handles and ends after caller-owned output
Parquet is written. It includes reads, validation, graph staging/CSR where used,
iterations, intermediate materialization, convergence checks and output writing.
Startup, independent verification and cleanup are outside this interval. Input
checksum reads occur before timing and warm caches. These measurements therefore
cannot be divided by historical kernel-only timings and described as speedups.

Memory tables use sampled peak PSS over the participating process tree. PSS
apportions shared mappings; RSS can count them repeatedly. The sampler waits
50 ms between scans, plus scan time, so short peaks can be missed. The CSV also
retains RSS, cgroup peaks, native accounting and VM steal. Cgroup peaks include
cache and lifetime activity and are not interchangeable with PSS. Missing
measurements remain unavailable rather than zero.

## Outcomes, qualification and the retained timeout

| Run | Envelope | Retained outcomes | Coverage |
| --- | --- | --- | --- |
| `large-pilots-aa4b5fa6` | 8 GiB native / 16 GiB Sail pool / 32 GiB container | 1 error, 29 not run | Capacity attempt; original logs and admission refusal retained |
| `large-qualified-pilots-aa4b5fa6` | 32 GiB native / 48 GiB Sail pool / 56 GiB container | 30 passes | One trial per path/method on each largest input; harness validation |
| `large-qualified-full-aa4b5fa6` | Same qualified envelope | 179 passes, 1 timeout | All 179 successful vectors independently audited |
| `large-timeout-control-aa4b5fa6` | Same source and settings as the full run | 1 pass, 32.089 seconds | Separate post-audit diagnostic; harness validation |

The initial capacity attempt refused native staging of the 33,554,395-edge
uniform graph: sorting requested 23,511,075,308 bytes of workspace under the
8 GiB native allowance. The record is an admission error, not a successful
duration or a cgroup OOM kill. The revised envelope was qualified separately;
its results do not erase the earlier refusal or the 29 unrun trials.

Full-run cell **133**, `large-wcc-r2-uniform-2097152-pecan-wcc-reference`,
timed out after **1,800.083 seconds**. Its two successful repetitions took
32.529 and 33.109 seconds. The successful median is 32.819 seconds, with **n=2**
for time and PSS; every other main-campaign time/PSS group has n=3. Failed-trial
memory remains in the CSV but is excluded from successful-trial aggregates.
Successful-only medians cannot establish an ordering when completion counts
differ.

A fresh diagnostic reran that exact cell only after the full audit ended. It
passed in 32.089 seconds; only run ID and output roots changed. The
[configuration sidecar](pecan-nutmeg-large-benchmark/supplement/timeout-control/large-timeout-control-metadata.json)
and separate original logs are retained. This observation does not establish
the timeout's cause, repair its result or supply a third main-run repetition.
The cause remains unexplained. Pilots and this control did not receive the
main campaign's independent raw-vector audit and are not pooled into its tables.

## Reading the measurements

Banda's reference PageRank has lower median full-call time than the relational
paths on all four inputs, with higher sampled PSS. Delta/frontier takes longer
than reference PageRank for every path on these inputs; an active frontier
does not guarantee less total work.

For relational WCC, fusion reduces median full-call time relative to the
matched unfused contraction method on every input: 15.0–24.6% for Pecan and
10.8–20.2% for Grenada. Their sampled PSS medians rise 6.1–22.1% and 7.9–24.0%,
respectively. Native fusion does not show the same uniform time reduction.
These are observations for the stated graphs and envelope, not universal
algorithm-selection rules or a claim of a fastest implementation.

The following tables include every method and path. Values are successful-trial
median [minimum, maximum], with a separate sample count for each metric.
[Complete CSV](pecan-nutmeg-large-benchmark/summary/cells.csv),
[RSS/PSS/cgroup tables](pecan-nutmeg-large-benchmark/summary/tables.md) and
[machine-readable summary](pecan-nutmeg-large-benchmark/summary/summary.json)
retain the unrounded observations and outcomes.

### hub-2097152

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 40.992 [39.803, 43.044] (n=3) | 2.413 [2.344, 2.417] (n=3) |
| pagerank | Power | Banda | 3 passed | 23.664 [20.520, 24.580] (n=3) | 2.863 [2.836, 2.878] (n=3) |
| pagerank | Power | Grenada | 3 passed | 40.651 [38.525, 41.141] (n=3) | 2.436 [2.417, 2.663] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 48.046 [47.579, 49.840] (n=3) | 2.523 [2.465, 2.539] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 27.639 [27.250, 28.744] (n=3) | 3.234 [3.234, 3.267] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 48.537 [48.195, 48.595] (n=3) | 2.502 [2.489, 2.548] (n=3) |
| wcc | Reference | Pecan | 3 passed | 26.755 [25.977, 27.634] (n=3) | 3.701 [3.683, 4.080] (n=3) |
| wcc | Reference | Banda | 3 passed | 20.123 [19.251, 25.499] (n=3) | 2.838 [2.690, 2.953] (n=3) |
| wcc | Reference | Grenada | 3 passed | 27.049 [26.462, 27.445] (n=3) | 3.953 [3.697, 4.000] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 24.667 [24.042, 25.863] (n=3) | 2.961 [2.910, 2.976] (n=3) |
| wcc | Randomized | Banda | 3 passed | 26.332 [25.798, 30.463] (n=3) | 3.026 [3.025, 3.069] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 24.710 [24.175, 25.179] (n=3) | 2.872 [2.857, 2.881] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 20.965 [20.645, 24.935] (n=3) | 3.145 [3.119, 3.370] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 30.305 [26.396, 32.975] (n=3) | 3.042 [3.036, 3.058] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 22.052 [21.635, 23.047] (n=3) | 3.515 [3.094, 3.586] (n=3) |

![Time and memory for hub-2097152](pecan-nutmeg-large-benchmark/figures/hub-2097152.png)

### uniform-2097152

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 41.318 [39.825, 42.804] (n=3) | 2.454 [2.427, 2.497] (n=3) |
| pagerank | Power | Banda | 3 passed | 21.080 [19.638, 23.136] (n=3) | 3.022 [2.835, 3.053] (n=3) |
| pagerank | Power | Grenada | 3 passed | 39.370 [38.399, 40.002] (n=3) | 2.449 [2.444, 2.509] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 46.326 [44.550, 46.737] (n=3) | 2.639 [2.592, 2.645] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 28.067 [27.482, 31.685] (n=3) | 3.240 [3.216, 3.284] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 46.104 [45.009, 46.201] (n=3) | 2.617 [2.592, 2.619] (n=3) |
| wcc | Reference | Pecan | 2 passed, 1 timeout | 32.819 [32.529, 33.109] (n=2) | 3.863 [3.831, 3.894] (n=2) |
| wcc | Reference | Banda | 3 passed | 19.883 [18.886, 20.236] (n=3) | 3.017 [2.983, 3.034] (n=3) |
| wcc | Reference | Grenada | 3 passed | 33.154 [31.766, 39.526] (n=3) | 4.115 [3.760, 4.357] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 29.164 [25.265, 29.778] (n=3) | 2.929 [2.811, 2.934] (n=3) |
| wcc | Randomized | Banda | 3 passed | 26.496 [25.099, 31.394] (n=3) | 3.060 [3.040, 3.075] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 25.673 [25.032, 25.999] (n=3) | 2.856 [2.712, 2.996] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 21.990 [21.753, 24.597] (n=3) | 3.168 [3.124, 3.174] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 24.623 [24.301, 26.161] (n=3) | 3.030 [3.028, 3.055] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 22.127 [20.954, 26.367] (n=3) | 3.218 [3.126, 3.268] (n=3) |

![Time and memory for uniform-2097152](pecan-nutmeg-large-benchmark/figures/uniform-2097152.png)

### hub-4194304

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 83.353 [80.350, 83.480] (n=3) | 3.348 [3.345, 3.420] (n=3) |
| pagerank | Power | Banda | 3 passed | 44.827 [43.296, 54.059] (n=3) | 4.698 [4.688, 4.728] (n=3) |
| pagerank | Power | Grenada | 3 passed | 83.524 [81.019, 87.333] (n=3) | 3.353 [3.189, 3.397] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 94.931 [93.679, 95.014] (n=3) | 3.248 [3.243, 3.281] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 67.523 [66.526, 69.884] (n=3) | 5.283 [5.251, 5.306] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 92.023 [90.866, 93.662] (n=3) | 3.362 [3.311, 3.419] (n=3) |
| wcc | Reference | Pecan | 3 passed | 50.953 [49.844, 63.095] (n=3) | 5.838 [5.282, 6.619] (n=3) |
| wcc | Reference | Banda | 3 passed | 50.563 [43.848, 50.747] (n=3) | 4.442 [4.399, 4.446] (n=3) |
| wcc | Reference | Grenada | 3 passed | 58.900 [51.155, 89.989] (n=3) | 6.187 [5.711, 6.338] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 49.131 [49.091, 50.367] (n=3) | 4.019 [3.979, 4.143] (n=3) |
| wcc | Randomized | Banda | 3 passed | 58.738 [58.100, 60.728] (n=3) | 5.170 [5.108, 5.188] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 48.338 [47.842, 50.861] (n=3) | 4.307 [4.084, 4.338] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 40.822 [40.205, 40.896] (n=3) | 4.908 [4.860, 4.933] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 64.087 [56.957, 64.102] (n=3) | 5.106 [5.060, 5.131] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 41.722 [39.759, 43.107] (n=3) | 5.340 [4.937, 5.592] (n=3) |

![Time and memory for hub-4194304](pecan-nutmeg-large-benchmark/figures/hub-4194304.png)

### uniform-4194304

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 81.479 [81.103, 82.278] (n=3) | 3.354 [3.281, 3.569] (n=3) |
| pagerank | Power | Banda | 3 passed | 43.429 [43.153, 44.907] (n=3) | 4.669 [4.636, 4.686] (n=3) |
| pagerank | Power | Grenada | 3 passed | 80.245 [79.519, 83.024] (n=3) | 3.575 [3.296, 3.659] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 85.876 [83.772, 87.933] (n=3) | 3.338 [3.333, 3.454] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 65.893 [65.211, 66.700] (n=3) | 5.283 [5.263, 5.287] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 87.783 [87.648, 88.415] (n=3) | 3.366 [3.324, 3.370] (n=3) |
| wcc | Reference | Pecan | 3 passed | 62.018 [61.278, 71.842] (n=3) | 5.921 [5.892, 6.218] (n=3) |
| wcc | Reference | Banda | 3 passed | 50.225 [41.569, 54.476] (n=3) | 4.397 [4.390, 4.398] (n=3) |
| wcc | Reference | Grenada | 3 passed | 62.760 [62.013, 62.841] (n=3) | 6.216 [5.741, 6.487] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 52.485 [50.824, 52.983] (n=3) | 4.337 [4.304, 4.337] (n=3) |
| wcc | Randomized | Banda | 3 passed | 58.400 [58.185, 58.564] (n=3) | 5.129 [5.093, 5.158] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 51.952 [48.216, 58.897] (n=3) | 4.247 [3.926, 4.409] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 40.623 [40.119, 42.203] (n=3) | 4.601 [4.573, 4.836] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 62.804 [54.237, 63.235] (n=3) | 5.133 [5.045, 5.143] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 41.477 [40.429, 41.748] (n=3) | 4.582 [4.558, 4.767] (n=3) |

![Time and memory for uniform-4194304](pecan-nutmeg-large-benchmark/figures/uniform-4194304.png)

## Audit and source provenance

The [independent audit](pecan-nutmeg-large-benchmark/audit/audit.md), recorded
`2026-09-28T03:05:22.343670+00:00`, passed with no issues. It performed 3,286
inventory checks over 1,830 unique files, independently checked all 179
successful vectors and reconstructed 117,958 raw memory samples. The timeout
has receipt-only coverage because it produced no completed answer. Cross-path
WCC coefficients and contraction traces agree after allowing the first fused
raw-edge count to differ. No post-stop staging files remained in 180 cell
directories.

| Component | Measured source commit |
| --- | --- |
| Harness / Pecan | `aa4b5fa6bd1a8e0ac833ec9db0862386777575c2` |
| Sail executable | `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822` |
| Native wheel | `9d7155aa2302d9dda4e93e48ee3fd81aac1ee637` |

The Docker image, executable hash, installed native-file identity and auditor
source hash are pinned in the audit JSON and accompanying configuration.
Reporting changes do not alter these measured source identities.

The [main proof archive](pecan-nutmeg-large-benchmark/proof/evidence.tar.gz)
contains the original exported diagnostics, including the failed trial and
memory samples. Its SHA256 is
`5cf0f2b14e9792c25c7fb1957324f5edca9170049c56cd98f8eb6e469814002f`.
It contains 1,109 diagnostic/summary/audit files plus its internal manifest.
728 Parquet files are omitted from public delivery with original size/hash
entries retained; the raw audit read the originals on Morrobay. A separate
[supplement archive](pecan-nutmeg-large-benchmark/supplement/evidence.tar.gz)
preserves the capacity pilots and fresh timeout control.

Each archive was reopened, hash-checked and scanned over its decompressed
files. Receipts disclose the actual credential patterns and available exact
values checked; this is not exhaustive secret detection. Figure PNG/SVG bytes
and their render-time receipt are preserved. The receipt's historical
“audit pending” wording describes when the figures were prepared; the separate
final audit supplies the publication verdict.

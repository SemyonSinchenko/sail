# Fixed-source traversal controls

These adapters run the established GAPBS and Parallel-SSSP kernels against the
same small weighted fixture and explicit source used by Sail. They qualify the
input and result protocol before a performance campaign. They do not replace
the upstream algorithm code and do not claim a universally fastest algorithm.

| Control | Pinned source | Methods |
| --- | --- | --- |
| [GAPBS](https://github.com/sbeamer/gapbs/tree/2972aeb2703165bafd921222f4ed7196f542d3a8) | `2972aeb2703165bafd921222f4ed7196f542d3a8` | Direction-optimizing BFS; upstream `DeltaStep` SSSP with bucket fusion |
| [Parallel-SSSP](https://github.com/ucrparlay/Parallel-SSSP/tree/a160e5eaf5bed40e2c2626d6d46e526129b2f274) | `a160e5eaf5bed40e2c2626d6d46e526129b2f274` | Rho-stepping, delta-stepping, Bellman–Ford |

Parallel-SSSP's Parlay submodule is pinned to
`9e6078709438072b8e46e2922939af0c0555be83`. Its stock driver chooses ten hashed
sources and runs warmups/repetitions. The thin driver here takes one explicit
source and makes one solver call, so source selection and repetition policy do
not silently differ from the Sail cell.

## Build

Run from the Sail checkout on a Linux host with a C++17 compiler and OpenMP.
The Python environment needs NumPy, PyArrow and pytest.

```bash
export CONTROL_WORK="$HOME/sail-traversal-controls"
mkdir -p "$CONTROL_WORK"
git clone https://github.com/sbeamer/gapbs.git "$CONTROL_WORK/gapbs"
git -C "$CONTROL_WORK/gapbs" checkout --detach 2972aeb2703165bafd921222f4ed7196f542d3a8
git clone https://github.com/ucrparlay/Parallel-SSSP.git "$CONTROL_WORK/parallel-sssp"
git -C "$CONTROL_WORK/parallel-sssp" checkout --detach a160e5eaf5bed40e2c2626d6d46e526129b2f274
git -C "$CONTROL_WORK/parallel-sssp" submodule update --init --recursive

python3 -m venv "$CONTROL_WORK/venv"
"$CONTROL_WORK/venv/bin/python" -m pip install numpy pyarrow pytest
CXX=g++ python3 examples/extensions/benchmarks/traversal-controls/build_controls.py \
  gap "$CONTROL_WORK/gapbs" "$CONTROL_WORK/gap-control"
CXX=g++ python3 examples/extensions/benchmarks/traversal-controls/build_controls.py \
  parallel "$CONTROL_WORK/parallel-sssp" "$CONTROL_WORK/parallel-control"
```

The builder checks the pinned commits and rejects tracked modifications. It
records compiler arguments, source/submodule pins, driver and executable hashes
in `<binary>.build.json`. Preserve that receipt with the executable. GAP's driver
includes the two original kernel translation units with their `main` functions
renamed; Parallel-SSSP's driver includes its original kernel header. Upstream
licenses apply to those kernels.

On Apple Silicon with Homebrew LLVM and libomp, the tested GAP compiler settings
were:

```bash
CXX="$(brew --prefix llvm)/bin/clang++" \
CONTROL_CXXFLAGS="-O3 -fopenmp -I$(brew --prefix libomp)/include -L$(brew --prefix libomp)/lib -Wl,-rpath,$(brew --prefix libomp)/lib" \
  python3 examples/extensions/benchmarks/traversal-controls/build_controls.py \
  gap "$CONTROL_WORK/gapbs" "$CONTROL_WORK/gap-control"
CXX="$(brew --prefix llvm)/bin/clang++" \
  python3 examples/extensions/benchmarks/traversal-controls/build_controls.py \
  parallel "$CONTROL_WORK/parallel-sssp" "$CONTROL_WORK/parallel-control"
```

## Prepare exactly representable integer weights

```bash
"$CONTROL_WORK/venv/bin/python" examples/extensions/benchmarks/traversal_fixture.py \
  --output "$CONTROL_WORK/input" --vertices 256 --degree 8 --seed 42 \
  --source 0 --directed
"$CONTROL_WORK/venv/bin/python" examples/extensions/benchmarks/traversal-controls/controls.py prepare \
  --dataset "$CONTROL_WORK/input" --output "$CONTROL_WORK/csr"
```

The first command is the same bounded fixture preparer used by the Sail
traversal harness. The second produces `.sg` and `.wsg` CSR inputs, with every
vertex, original edge multiplicity and self-loop preserved. It bypasses GAP's
edge-list builder, which otherwise changes those properties. For an undirected
fixture, it adds one reverse arc for each tuple, including loops, and stores the
result as explicit directed CSR. Parallel-SSSP's `symmetrized` optimization flag
stays false for that representation; the logical graph still matches the
declared undirected traversal.

The exporter is limited to 100,000 vertices and 1,000,000 original edge tuples.
It requires dense IDs `0..N-1`, reference distances and exact nonnegative integer
weights. It rejects fractional weights instead of rounding them. A conservative
`N * maximum_weight` bound must remain below GAP's int32 distance sentinel.
Consequently, ordinary Graph500 float32-weight inputs cannot silently be used
as an equivalent SSSP workload through these integer controls. Large control
input preparation and float-weight compatibility remain separate work.

## Run and validate every output vertex

```bash
"$CONTROL_WORK/venv/bin/python" examples/extensions/benchmarks/traversal-controls/controls.py run \
  --dataset "$CONTROL_WORK/csr" --binary "$CONTROL_WORK/gap-control" \
  --output "$CONTROL_WORK/gap-bfs" --method bfs --threads 4 --parameter 15 --beta 18
"$CONTROL_WORK/venv/bin/python" examples/extensions/benchmarks/traversal-controls/controls.py run \
  --dataset "$CONTROL_WORK/csr" --binary "$CONTROL_WORK/gap-control" \
  --output "$CONTROL_WORK/gap-sssp" --method sssp --threads 4 --parameter 4
"$CONTROL_WORK/venv/bin/python" examples/extensions/benchmarks/traversal-controls/controls.py run \
  --dataset "$CONTROL_WORK/csr" --binary "$CONTROL_WORK/parallel-control" \
  --output "$CONTROL_WORK/rho-sssp" --method rho --threads 4 --parameter 8
```

For Parallel-SSSP's other methods, use `--method delta --parameter 4` or
`--method bellman-ford`; select a fresh output directory for each call. The
parameter means alpha for GAP BFS, delta for either stepping implementation, or
rho for rho-stepping. Bellman–Ford ignores it. Parameters here are correctness
examples, not tuned performance recommendations. Train and freeze any tuning
before a measured campaign.

Every call records its explicit source, build and input identities, command,
thread environment, logs, complete `result.tsv`, output hash and validation
receipt. It compares every distance with independent Python deque-BFS or
heap-Dijkstra references. GAP BFS also exports parents; their edge membership,
depths and rooted acyclic structure are checked. SSSP controls export distances
only, so no SSSP parent-tree claim is made. The isolated-source test verifies
that the driver honors the specified root instead of choosing a non-isolate.

Parallel-SSSP's sparse/dense scale is the driver's upstream `M/N` choice clamped
to `[1,N]`. This keeps its `N/scale` threshold positive for low-density and dense
multigraph fixtures. The effective scale is recorded; kernel sources remain
unchanged. This driver parameter is part of the disclosed experiment.

Outcomes remain separate: `passed`, `mismatch`, `error` and `timeout`. Failed
runs retain logs and any output. Existing output directories are never reused.

## Measurement boundary and gates

The reported kernel diagnostic begins after CSR loading and includes algorithm
state allocation, initialization, execution and result return. It excludes input
conversion, CSR loading, output writing and verification. BFS depths are derived
from the returned parent tree outside this timer. The Parallel-SSSP driver
constructs a fresh solver inside the timer. This boundary differs from a full
Sail call, and this runner has no process isolation or peak-memory sampler:
its receipts explicitly say **functional qualification only**. Publishable
time/memory comparisons still require the shared benchmark resource envelope,
memory measurement, repetitions, host qualification and separate boundary labels.

```bash
GAP_CONTROL_BINARY="$CONTROL_WORK/gap-control" \
PARALLEL_CONTROL_BINARY="$CONTROL_WORK/parallel-control" \
  "$CONTROL_WORK/venv/bin/python" -m pytest -q \
  examples/extensions/benchmarks/test_traversal_controls.py
```

The real-driver tests cover all five methods, directed and undirected inputs,
one and four threads, loops, duplicates, zero-weight cycles, ties, disconnected
vertices, an explicitly isolated root and dense multigraph tuning. Without the
binary environment variables, those integration cases are skipped; the export
unit tests alone do not qualify the controls.

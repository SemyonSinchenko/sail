# Reproduce and inspect the large-graph campaign

The [report](../pecan-nutmeg-large-benchmark.md) covers the frozen 180-cell
campaign. All original outcomes remain: 179 passes, one timeout. Reporting code
and documentation commits are distinct from the measured implementation pins.

## Read the delivered evidence

| Location | Contents |
| --- | --- |
| `configuration.json` | Exact main-run paths, source pins, limits and matrix |
| `summary/` | All 180 CSV rows, 60 method groups, time/PSS/RSS/cgroup tables |
| `audit/` | Original independent audit JSON and Markdown |
| `proof/` | Original diagnostics archive, included/omitted manifest, archive hash, scan receipt |
| `figures/` | Four PNG/SVG pairs, generated tables and original render receipt |
| `inventory/` | Export/raw inventories and original host observations |
| `supplement/` | Separate capacity/qualification summaries and timeout-control metadata, plus original diagnostic archive |
| `reproduce/` | Archive tools and public-artifact verifier |

The main archive has 1,109 evidence files plus an internal manifest. It retains
all 1,102 original exported diagnostics and 728 omitted-Parquet inventory
entries. The supplement has 234 files plus an internal manifest and 136 omitted
Parquet entries across the two pilots and timeout control. No raw Parquet bytes
are included. They remain on Morrobay; their omission prevents a recipient from
repeating the raw correctness audit using only these archives.

The original auditor is
[`audit_matrix.py`](../pecan-nutmeg-benchmark/reproduce/audit_matrix.py), SHA256
`85070d03778ad1dd2e578835032fda7f000c0b4fcde8f9d1f74d2d3a5791935a`.
Its source predates this report and is left unchanged. The audit records all
179 successful full-vector checks, not just first repetitions. The pilot and
control diagnostics have harness correctness receipts but no new independent
raw audit; they are not pooled with the 180 trials.

From the repository root, with Python 3.12 or later:

```sh
python3 docs/development/extensions/pecan-nutmeg-large-benchmark/reproduce/verify_publication.py
```

This verifies every delivered byte against the publication manifest, checks
the complete CSV and summary, reopens both archives and verifies every member,
and performs the documented credential-pattern scan. It checks the original
audit's identity and coverage without claiming a new raw-vector audit.
The separate scan receipts describe exact coverage and are not a guarantee of
exhaustive secret detection.

## Build and run the same implementation

1. Clone the reporting branch. Keep its evidence tools available while creating
   separate clean source trees for the measured harness, runtime and native wheel:

   ```sh
   git clone --branch work/extensions-large-campaign-report https://github.com/querygraph/sail.git sail-large-report
   cd sail-large-report
   git worktree add ../sail-large-harness aa4b5fa6bd1a8e0ac833ec9db0862386777575c2
   git worktree add ../sail-large-runtime 70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822
   git worktree add ../sail-large-native 9d7155aa2302d9dda4e93e48ee3fd81aac1ee637
   ```

2. Follow the [build tutorial](../../../../examples/extensions/benchmarks/TUTORIAL.md)
   for the pinned toolchain, Sail binary and extension wheels. Build the runtime
   from `sail-large-runtime`, and the Nutmeg wheel from `sail-large-native`.
   Install the Pecan client and run the benchmark harness from the `aa4b5fa6`
   checkout. Keep these identities separate in the operator configuration.
   A new build can have a different binary hash; record it instead of reusing
   the historical binary identity.

3. Generate and import the exact inputs using the
   [large-input guide](../../../../examples/extensions/benchmarks/LARGE-GRAPHS.md).
   The upstream generator is pinned to
   `fe50ea8f112a5970214c619ed244eb38917250f1`; the importer checks every input
   SHA256. Keep generation outside timed trials.

4. Copy `configuration.json` to an operator path outside the checkout. Replace
   its host/container paths, Docker context/image and run/output roots. Keep
   source pins and the tested resource envelope if reproducing this experiment.
   The configuration contains Morrobay paths, not portable installation defaults.
   The container volume must contain the three clean source trees, installed
   packages, built binary and generated inputs at the configured paths.

5. Check the matrix, prepare inputs, run separate qualification pilots, then
   execute the full matrix without other builds or measurements on the host:

   ```sh
   cd ../sail-large-harness
   python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --dry-run
   python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --prepare-only
   python3 examples/extensions/benchmarks/run_matrix.py --config /path/large.json --skip-prepare
   python3 examples/extensions/benchmarks/summarize.py --evidence /path/new-evidence --output /path/new-summary
   ```

   For a pilot, use a distinct run/output root, the two largest datasets and one
   repetition for all methods. This gives 30 trials. Preserve failed pilots;
   changing capacity is a new qualification run. The main matrix has four
   graphs, five methods, three paths and three repetitions. `--resume` retains
   completed failures; a diagnostic rerun needs a separate root.

6. Export diagnostics with omitted Parquet hashes, retain the raw originals,
   and run the independent auditor in the reporting checkout, using the
   **measured harness checkout** as `--repo`:

   ```sh
   cd ../sail-large-report
   python3 docs/development/extensions/pecan-nutmeg-large-benchmark/reproduce/export_evidence.py \
     /path/original-run /path/exported-diagnostics
   python3 docs/development/extensions/pecan-nutmeg-benchmark/reproduce/audit_matrix.py \
     --kind large --repo /path/measured-harness \
     --evidence /path/exported-diagnostics --summary /path/new-summary \
     --raw /path/original-run --require-raw --output /path/new-independent-audit
   ```

   The auditor requires NumPy and PyArrow; the benchmark environment already
   provides them. `--require-raw` must pass before declaring raw qualification.
   Do not copy this campaign's audit to a new run.

7. Render figures from the new summary/configuration with Matplotlib 3.11.2:

   ```sh
   python3 examples/extensions/benchmarks/render_large.py \
     --summary /path/new-summary/summary.json --config /path/new-evidence/configuration.json \
     --host 'actual host and VM description' --output /path/new-figures
   ```

## Recreate the original proof formats

`reproduce/package_graphframes_large.py` is the exact code that created the
main proof and scanned every decompressed member. Its SHA256 matches the
original bundle and scan receipts. It expects the original complete export,
summary and passed independent audit:

```sh
python3 docs/development/extensions/pecan-nutmeg-large-benchmark/reproduce/package_graphframes_large.py \
  --export /path/export --summary /path/summary --audit /path/audit-without-suffix \
  --output /path/new-proof
```

The deterministic tar/gzip metadata fixes ordering, ownership and timestamps;
the receipt records Python/zlib versions. The supplement uses
`reproduce/package_supplement.py --export NAME=DIRECTORY` for each original
pilot/control export, followed by `--output NEW_DIRECTORY`. Both tools verify
included-file hashes, keep omitted-file entries and scan decompressed bytes.
The existing archives remain unchanged when new diagnostic runs are added.

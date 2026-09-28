# WCC and shortest-path memory quota reuse

`python/qualify_graph_resources.py` checks release of native quota after a
post-initialization memory refusal. It uses one Spark session and the same two
worker processes for five operations: a successful warmup, the expected memory
refusal, and three successful queries. Earlier materialized results remain
readable throughout.

Build Sail and the Nutmeg wheel following [the extension tutorial](../README.md).
Use a wheel containing the native memory-cause receipts and a Sail binary with
failed-job task cleanup. Run from the repository root in the wheel's Python
environment:

```sh
python examples/extensions/argentea/python/qualify_graph_resources.py \
  --algorithm sssp_delta_star \
  --output /tmp/argentea-sssp-memory \
  --sail-binary /absolute/path/to/sail \
  --runtime-source-sha FULL_SAIL_COMMIT \
  --native-source-sha FULL_NUTMEG_COMMIT
```

Repeat with distinct output directories for `sssp_reference`, `wcc_reference`,
and `wcc_star`. The source checkout must be clean. `--allow-working-tree`
explicitly marks a development run rather than a frozen qualification.

The default worker pool is 48 MiB and each native quota is 32 MiB. The pool can
admit one such reservation, but cannot admit a second while the first remains
held. The failure fixture contains isolated vertices: 524,288 for shortest paths
or 786,432 for WCC. These sizes exercise native state allocation rather than a
large edge shuffle. If allocation behavior changes, a run that no longer fails
at the intended boundary fails qualification; it is not silently accepted.

The auditor requires initialization and close on every owner, a typed native
memory refusal after initialization, a failed job, terminal tasks without retries,
and complete native stage inventory. A peer-cancellation RPC alone is insufficient
to establish the original cause. Subsequent successful jobs must preserve session,
worker IDs, and process IDs. Results are checked independently, owned phase views
must disappear, and session teardown must empty staging and terminate processes.

Receipts retain source hashes, binary and package identities, plans, task and
stage inventories, original errors, and native logs. Retained outputs are Parquet
files, not live native Arrow buffers. This test establishes quota reuse for the
specified operations; it does not measure RSS or establish universal leak freedom.

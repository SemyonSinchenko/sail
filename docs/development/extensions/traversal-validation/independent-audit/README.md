# Independent traversal output audit

These read-only tools check retained BFS/SSSP Parquet outputs without running
Sail or invoking Pecan, Banda, or Grenada. Install NumPy and PyArrow in an isolated
Python environment, then run (without Python's `-O` flag):

```sh
python test_positive_certificate.py
python test_parquet_audit.py
python test_retained_outputs.py
python audit_parquet.py /path/to/receipt.json /path/to/dataset /path/to/result
```

The dataset directory contains `vertices.parquet` and `edges.parquet` files or
partition directories. The result directory contains the files named in the
receipt. The receipt supplies dataset and result SHA-256 hashes, counts, source
vertex, directedness, and algorithm. A successful audit verifies every input and
output file against those pins, exact vertex coverage, all edge inequalities,
and a source-rooted witness for every finite distance. Extra dataset files such
as reference vectors are hash-checked but are not used to compute the answer.

## Proof and limits

The graph must use vertex IDs `0..N-1` and integer weights in `1..16`. BFS replaces
weights with one. Distances must be nonnegative integers below `2**52`; null is
the unreachable marker. These bounds make every checked addition exact in
binary64. Each reached non-source vertex must have a tight incoming edge.
Following such edges backwards strictly decreases distance. A chain cannot
cycle and must terminate at the unique zero-distance source. Together with all
edge inequalities, this proves shortest distances exactly. Directed and
undirected graphs are supported.

Do not apply this proof to zero-weight or arbitrary floating-point graphs.
The audit does not validate parent columns, performance measurements, process
placement, artifact provenance, or cleanup; those require their separate
receipts and checks. File pins establish agreement with a receipt, not the
independent authenticity of that receipt. Preserve and verify the campaign's
source identities and frozen configuration separately.

## Validation evidence

`retained-linux-output-controls.json` records successful independent audits of
12 retained Linux release qualification outputs: 65,536 vertices per output,
786,432 total vertex results across Pecan, Banda, and Grenada BFS/SSSP controls.
These outputs are retained in the sibling `linux-release-538b-evidence.tar.gz`.
The checker also passes 100 seeded comparisons with heap Dijkstra and rejects
invalid distance, reachability, weight, cardinality, and hash controls.

This evidence validates the checker on those controls. It is not a completed
audit of the ongoing 216-run large traversal campaign.

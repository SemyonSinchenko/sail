#!/usr/bin/env python3
"""Import a SNAP-style edge list (cit-Patents and its kin) into the fixture layout.

SNAP files carry comment lines starting with `#` and one `src<tab>dst` pair per
line with arbitrary non-negative integer IDs that are neither dense nor
contiguous. Every distinct ID becomes a dense vertex 0..V-1 in ascending
order of the original ID, kept in `vertices.parquet` as `original_id`.
Duplicate arcs, self loops and the input order are preserved; weights follow
the traversal importer's disclosed policies (`unit` or `splitmix64-1-16`), so
the fixture serves BFS, SSSP, PageRank and WCC. The whole file is read into
memory; cit-Patents (16.5 M arcs) needs about 1 GiB.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from graph500_fixture import MAX_CHUNK, RECORD, sha256, utc, write_json
from traversal_source import MAX_DEGREE, parse_source
from imported_traversal_fixture import WEIGHT_POLICIES, weights

FAMILY = 'snap-edge-list'


def parse(data):
    """Endpoint pairs from SNAP text: comments dropped, whitespace-separated integers."""
    lines = [line for line in data.split(b'\n') if line and not line.startswith(b'#')]
    if any(not re.fullmatch(rb'\s*[0-9]{1,19}\s+[0-9]{1,19}\s*', line) for line in lines):
        raise ValueError('expected two nonnegative decimal integers per non-comment line')
    values = np.fromstring(b'\n'.join(lines), dtype=np.int64, sep=' ') if lines else np.empty(0, dtype=np.int64)
    if values.size != 2 * len(lines):
        raise ValueError('malformed edge rows')
    return values[::2].copy(), values[1::2].copy()


def prepare(output, edge_file, *, vertices, edge_sha256, weight_policy='unit', weight_seed=42,
            source=0, directed=True, chunk_edges=262144, chunk_vertices=262144):
    if type(vertices) is not int or not 1 <= vertices < 1 << 63:
        raise ValueError('vertices must be the positive number of distinct IDs')
    if weight_policy not in WEIGHT_POLICIES:
        raise ValueError('weight policy must explicitly be unit or splitmix64-1-16')
    if type(weight_seed) is not int or not 0 <= weight_seed < 1 << 64:
        raise ValueError('weight seed must be an unsigned 64-bit integer')
    if any(type(n) is not int or not 1 <= n <= MAX_CHUNK for n in (chunk_edges, chunk_vertices)):
        raise ValueError(f'chunk sizes must be integers in 1..{MAX_CHUNK}')
    if not re.fullmatch(r'[a-f0-9]{64}', edge_sha256):
        raise ValueError('edge_sha256 must pin the original input bytes')
    edge_file = Path(edge_file).resolve()
    data = edge_file.read_bytes()
    if hashlib.sha256(data).hexdigest() != edge_sha256:
        raise ValueError('input SHA256 differs from the pinned file')
    src_original, dst_original = parse(data)
    del data
    originals, inverse = np.unique(np.concatenate([src_original, dst_original]), return_inverse=True)
    if originals.size != vertices:
        raise ValueError(f'input has {originals.size} distinct IDs, not the declared {vertices}')
    if source != MAX_DEGREE and (type(source) is not int or not 0 <= source < vertices):
        raise ValueError(f'source outside graph (a dense vertex id or {MAX_DEGREE!r})')
    src = inverse[:src_original.size].astype(np.int64)
    dst = inverse[src_original.size:].astype(np.int64)
    edges = int(src.size)
    # Out-degree when directed, both endpoints otherwise; picks `max-degree` and records the source's degree.
    degree = np.bincount(src, minlength=vertices) if directed else np.bincount(np.concatenate([src, dst]), minlength=vertices)
    requested = source
    if source == MAX_DEGREE:
        source = int(np.argmax(degree))  # the lowest dense id among ties
    source_degree, isolated = int(degree[source]), int(np.count_nonzero(degree == 0))
    del degree
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    started, start = utc(), time.monotonic()
    args = dict(vertices=vertices, edge_file=str(edge_file), edge_sha256=edge_sha256, weight_policy=weight_policy,
                weight_seed=weight_seed, source=source, directed=directed, chunk_edges=chunk_edges, chunk_vertices=chunk_vertices)
    write_json(output / 'preparation.json', dict(started_utc=started, arguments=args))
    try:
        (output / 'edges.parquet').mkdir()
        (output / 'vertices.parquet').mkdir()
        canonical = hashlib.sha256()
        loops = int(np.count_nonzero(src == dst))
        for part, begin in enumerate(range(0, max(edges, 1), chunk_edges)):
            count = min(chunk_edges, edges - begin)
            rows = np.empty(count, dtype=RECORD)
            rows['src'], rows['dst'] = src[begin:begin + count], dst[begin:begin + count]
            rows['weight'] = weights(begin, count, weight_policy, weight_seed)
            canonical.update(memoryview(rows))
            table = pa.table({key: pa.array(rows[key]) for key in RECORD.names})
            pq.write_table(table, output / 'edges.parquet' / f'part-{part:08d}.parquet', compression='zstd')
        vertex_digest = hashlib.sha256()
        for part, begin in enumerate(range(0, vertices, chunk_vertices)):
            ids = np.arange(begin, min(begin + chunk_vertices, vertices), dtype=np.int64)
            vertex_digest.update(memoryview(ids))
            pq.write_table(pa.table({'id': pa.array(ids), 'original_id': pa.array(originals[begin:begin + ids.size])}),
                           output / 'vertices.parquet' / f'part-{part:08d}.parquet', compression='zstd')
        manifest = dict(schema_version=1, family=FAMILY, parameters=args,
            counts=dict(vertices=vertices, edges=edges, self_loops=loops),
            input=dict(path=str(edge_file), sha256=edge_sha256, bytes=edge_file.stat().st_size,
                       format='SNAP text: # comments, one src dst pair per line', original_id_range=[int(originals[0]), int(originals[-1])]),
            traversal=dict(source=source, directed=directed, weight_policy=weight_policy, weight_seed=weight_seed,
                           source_policy=('highest-degree dense vertex, lowest id among ties' if requested == MAX_DEGREE
                                          else 'explicit fixed dense vertex'),
                           source_original_id=int(originals[source]), source_degree=source_degree,
                           degree_counted='out-edges (src)' if directed else 'both endpoints of every tuple',
                           zero_degree_vertices=isolated),
            canonical=dict(edges=dict(sha256=canonical.hexdigest(), bytes=edges * RECORD.itemsize,
                                      format='little-endian int64 src, int64 dst, float64 weight; 24 bytes per record',
                                      self_loops=loops, ordering='original input row order', duplicate_policy='preserved'),
                           vertices=dict(sha256=vertex_digest.hexdigest(), bytes=vertices * 8,
                                         format='little-endian int64 dense id; original_id ascending beside it')),
            remap='dense id = rank of the original ID in ascending order over all endpoints',
            preparation=dict(started_utc=started, finished_utc=utc(), seconds=time.monotonic() - start,
                             converter_sha256=sha256(__file__), pyarrow_version=pa.__version__, numpy_version=np.__version__),
            validation=dict(status='reference-not-generated', reference=None,
                            input='pinned original bytes; dense remap; deterministic weights; output hashes'),
            purpose='real-graph capacity input; reference vectors are not generated at this size',
            files={p.relative_to(output).as_posix(): dict(sha256=sha256(p), bytes=p.stat().st_size)
                   for p in sorted(output.rglob('*.parquet')) if p.is_file()})
        write_json(output / 'manifest.json', manifest)
        return manifest
    except BaseException as error:
        write_json(output / 'failure.json', dict(failed_utc=utc(), error=type(error).__name__, message=str(error)))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--edge-file', type=Path, required=True)
    parser.add_argument('--vertices', type=int, required=True)
    parser.add_argument('--edge-sha256', required=True)
    parser.add_argument('--weight-policy', choices=WEIGHT_POLICIES, default='unit')
    parser.add_argument('--weight-seed', type=int, default=42)
    parser.add_argument('--source', type=parse_source, default=0,
                        help=f'traversal source: a dense vertex id, or {MAX_DEGREE} for the highest out-degree vertex')
    parser.add_argument('--directed', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--chunk-edges', type=int, default=262144)
    parser.add_argument('--chunk-vertices', type=int, default=262144)
    manifest = prepare(**vars(parser.parse_args()))
    print(json.dumps(dict(counts=manifest['counts'], canonical=manifest['canonical']), sort_keys=True))


if __name__ == '__main__':
    main()

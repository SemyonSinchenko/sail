#!/usr/bin/env python3
"""Stream pinned Graph Kernels topology into a weighted traversal fixture.

Original inputs contain a V E header followed by E unweighted src dst rows.
Unit weights preserve that workload. splitmix64-1-16 is explicitly a derived
weighted workload: edge i receives 1 + low4(SplitMix64(i + weight_seed)).
The input order, duplicate arcs, self loops and isolated vertices are retained.
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

from graph500_fixture import (MAX_CHUNK, ORACLE_EDGES, ORACLE_VERTICES, RECORD,
                              sha256, utc, write_json, write_reference, write_vertices)

PAIR = re.compile(rb'[0-9]{1,19}[ \t]+[0-9]{1,19}[ \t]*(?:\r?\n)?')
WEIGHT_POLICIES = ('unit', 'splitmix64-1-16')


def checked_pair(stream, digest):
    # Bound malformed rows too; a malicious single line cannot consume RAM.
    raw = stream.readline(128)
    if not raw or (len(raw) == 128 and not raw.endswith(b'\n')) or not PAIR.fullmatch(raw):
        raise ValueError('expected exactly two nonnegative decimal integers per row')
    digest.update(raw)
    return raw


def weights(start, count, policy, seed):
    if policy == 'unit':
        return np.ones(count, dtype='<f8')
    # Unsigned wraparound is part of the formula, independent of chunk size.
    z = np.arange(start, start + count, dtype=np.uint64)
    z += np.uint64(seed)
    z += np.uint64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> 30)) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> 27)) * np.uint64(0x94D049BB133111EB)
    z ^= z >> 31
    return ((z & 15) + 1).astype('<f8')


def convert(stream, output, *, vertices, edges, chunk_edges, weight_policy, weight_seed, input_digest):
    canonical = hashlib.sha256()
    loops = 0
    minimum = maximum = None
    for part, start in enumerate(range(0, edges, chunk_edges)):
        count = min(chunk_edges, edges - start)
        raw = b' '.join(checked_pair(stream, input_digest) for _ in range(count))
        endpoints = np.fromstring(raw, dtype=np.int64, sep=' ')
        if endpoints.size != count * 2 or np.any(endpoints < 0) or np.any(endpoints >= vertices):
            raise ValueError('edge endpoint outside the declared vertex range')
        rows = np.empty(count, dtype=RECORD)
        rows['src'], rows['dst'] = endpoints[::2], endpoints[1::2]
        rows['weight'] = weights(start, count, weight_policy, weight_seed)
        canonical.update(memoryview(rows))
        loops += int(np.count_nonzero(rows['src'] == rows['dst']))
        lo, hi = float(rows['weight'].min()), float(rows['weight'].max())
        minimum = lo if minimum is None else min(minimum, lo)
        maximum = hi if maximum is None else max(maximum, hi)
        table = pa.table({key: pa.array(rows[key]) for key in RECORD.names})
        pq.write_table(table, output / f'part-{part:08d}.parquet', compression='zstd')
    if stream.read(1):
        raise ValueError('input contains more rows than the declared edge count')
    # A zero-edge input still needs an explicit Parquet schema.
    if edges == 0:
        pq.write_table(pa.table({key: pa.array([], type=pa.float64() if key == 'weight' else pa.int64())
                                 for key in RECORD.names}), output / 'part-00000000.parquet')
    return dict(sha256=canonical.hexdigest(), bytes=edges * RECORD.itemsize,
                format='little-endian int64 src, int64 dst, float64 weight; 24 bytes per record',
                self_loops=loops, minimum_weight=minimum, maximum_weight=maximum,
                ordering='original input row order; increasing partition filename then row order',
                duplicate_policy='preserved; not counted or deduplicated')


def prepare(output, edge_file, *, vertices, edge_sha256, weight_policy,
            weight_seed=42, source=0, directed=True, chunk_edges=262144,
            chunk_vertices=262144, reference=False, expected_edge_sha256=None):
    if type(vertices) is not int or not 1 <= vertices < 1 << 63:
        raise ValueError('vertices must be a positive int64 count')
    if type(source) is not int or not 0 <= source < vertices:
        raise ValueError('source outside graph')
    if type(directed) is not bool:
        raise ValueError('directed must be boolean')
    if weight_policy not in WEIGHT_POLICIES:
        raise ValueError('weight policy must explicitly be unit or splitmix64-1-16')
    if type(weight_seed) is not int or not 0 <= weight_seed < 1 << 64:
        raise ValueError('weight seed must be an unsigned 64-bit integer')
    if any(type(n) is not int or not 1 <= n <= MAX_CHUNK for n in (chunk_edges, chunk_vertices)):
        raise ValueError(f'chunk sizes must be integers in 1..{MAX_CHUNK}')
    if not re.fullmatch(r'[a-f0-9]{64}', edge_sha256):
        raise ValueError('edge_sha256 must pin the original input bytes')
    if expected_edge_sha256 is not None and not re.fullmatch(r'[a-f0-9]{64}', expected_edge_sha256):
        raise ValueError('expected_edge_sha256 must pin canonical weighted edge bytes')
    edge_file = Path(edge_file).resolve()
    if sha256(edge_file) != edge_sha256:
        raise ValueError('input SHA256 differs from the pinned topology')
    digest = hashlib.sha256()
    with edge_file.open('rb') as stream:
        header = checked_pair(stream, digest)
        declared_vertices, edges = map(int, header.split())
        if declared_vertices != vertices or edges >= 1 << 63:
            raise ValueError('header counts differ from declared vertices or exceed int64')
        if reference and (vertices > ORACLE_VERTICES or edges > ORACLE_EDGES):
            raise ValueError('full Python oracle is limited to 100000 vertices and 1000000 edges')
        output = Path(output).resolve()
        output.mkdir(parents=True, exist_ok=False)
        started, start = utc(), time.monotonic()
        source_sha = sha256(__file__)
        args = dict(vertices=vertices, edge_file=str(edge_file), edge_sha256=edge_sha256,
                    weight_policy=weight_policy, weight_seed=weight_seed, source=source,
                    directed=directed, chunk_edges=chunk_edges, chunk_vertices=chunk_vertices,
                    reference=reference, expected_edge_sha256=expected_edge_sha256)
        write_json(output / 'preparation.json', dict(started_utc=started, arguments=args))
        try:
            for name in ('vertices.parquet', 'edges.parquet'):
                (output / name).mkdir()
            edge_identity = convert(stream, output / 'edges.parquet', vertices=vertices, edges=edges,
                chunk_edges=chunk_edges, weight_policy=weight_policy, weight_seed=weight_seed,
                input_digest=digest)
            if digest.hexdigest() != edge_sha256 or sha256(edge_file) != edge_sha256:
                raise ValueError('input changed during conversion')
            if expected_edge_sha256 is not None and edge_identity['sha256'] != expected_edge_sha256:
                raise ValueError('canonical edge SHA256 differs from the pinned weighted input')
            vertex_identity = write_vertices(output / 'vertices.parquet', vertices, chunk_vertices)
            vertex_identity['format'] = 'little-endian int64 id; ascending 0..vertices-1'
            if reference:
                write_reference(output, vertices, source, directed)
            if sha256(__file__) != source_sha:
                raise ValueError('converter changed during preparation')
            manifest = dict(schema_version=1, family='edge-list-traversal', parameters=args,
                counts=dict(vertices=vertices, edges=edges),
                input=dict(path=str(edge_file), sha256=edge_sha256, bytes=edge_file.stat().st_size),
                traversal=dict(source=source, directed=directed, weight_policy=weight_policy,
                               weight_seed=weight_seed, source_policy='explicit fixed vertex'),
                canonical=dict(edges=edge_identity, vertices=vertex_identity),
                preparation=dict(started_utc=started, finished_utc=utc(), seconds=time.monotonic()-start,
                    converter_sha256=source_sha, pyarrow_version=pa.__version__, numpy_version=np.__version__),
                validation=dict(status='independent-reference-generated' if reference else 'reference-not-generated',
                    input='pinned original bytes; exact row count; int64 endpoints; deterministic weights; output hashes',
                    reference='Python deque BFS and heap Dijkstra' if reference else None),
                purpose='Graph Kernels topology; SSSP weights are a disclosed derived workload',
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
    parser.add_argument('--weight-policy', choices=WEIGHT_POLICIES, required=True)
    parser.add_argument('--weight-seed', type=int, default=42)
    parser.add_argument('--source', type=int, default=0)
    parser.add_argument('--directed', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--chunk-edges', type=int, default=262144)
    parser.add_argument('--chunk-vertices', type=int, default=262144)
    parser.add_argument('--reference', action='store_true')
    parser.add_argument('--expected-edge-sha256')
    manifest = prepare(**vars(parser.parse_args()))
    print(json.dumps(dict(counts=manifest['counts'], canonical=manifest['canonical']), sort_keys=True))


if __name__ == '__main__':
    main()

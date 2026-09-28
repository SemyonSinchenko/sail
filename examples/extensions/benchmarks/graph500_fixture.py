#!/usr/bin/env python3
"""Stream pinned Graph500 edges to Parquet without retaining the graph in RAM.

The canonical edge digest hashes little-endian (i64, i64, f64) records in
generator order. Parquet partition sizes may change without changing this
identity. Full Python oracles are opt-in and restricted to small fixtures.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

GENERATOR_COMMIT = 'f89d643ce4aaae9a823d310c6ab2dd10e3d2982c'
RECORD = np.dtype([('src', '<i8'), ('dst', '<i8'), ('weight', '<f8')])
MAX_CHUNK = 16_777_216
ORACLE_VERTICES = 100_000
ORACLE_EDGES = 1_000_000


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n')


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def checked_build(generator):
    """Reject stale binaries and adapters before launching the generator."""
    receipt = json.loads(Path(str(generator) + '.build.json').read_text())
    if receipt.get('generator_commit') != GENERATOR_COMMIT:
        raise ValueError('generator build uses a different source commit')
    if receipt.get('binary_sha256') != sha256(generator):
        raise ValueError('generator binary differs from its build receipt')
    adapter = Path(__file__).parent / 'traversal-controls' / 'graph500_stream.c'
    if receipt.get('adapter_sha256') != sha256(adapter):
        raise ValueError('generator adapter differs from this checkout; rebuild it')
    return receipt


def read_exact(stream, size):
    """Pipes can return short reads without EOF; allocation stays <= size."""
    data = bytearray(size)
    view = memoryview(data)
    offset = 0
    while offset < size:
        count = stream.readinto(view[offset:])
        if not count:
            raise ValueError(f'truncated generator stream: expected {size}, received {offset} bytes in chunk')
        offset += count
    return data


def validate_records(rows, vertices):
    if any(np.any((rows[name] < 0) | (rows[name] >= vertices)) for name in ('src', 'dst')):
        raise ValueError('generator endpoint outside declared vertex range')
    weights = rows['weight']
    # Upstream f32 arithmetic may round to exactly 1. Preserve, do not clamp.
    if not np.all(np.isfinite(weights) & (weights >= 0) & (weights <= 1)):
        raise ValueError('generator weights must be finite and in [0, 1]')
    if not np.array_equal(weights, weights.astype(np.float32).astype(np.float64)):
        raise ValueError('generator weight is not an exactly widened float32')


def convert_edges(stream, output, *, vertices, edges, chunk_edges):
    digest = hashlib.sha256()
    minimum = math.inf
    maximum = -math.inf
    loops = 0
    for partition, start in enumerate(range(0, edges, chunk_edges)):
        count = min(chunk_edges, edges - start)
        raw = read_exact(stream, count * RECORD.itemsize)
        rows = np.frombuffer(raw, dtype=RECORD)
        validate_records(rows, vertices)
        digest.update(raw)
        loops += int(np.count_nonzero(rows['src'] == rows['dst']))
        minimum = min(minimum, float(rows['weight'].min()))
        maximum = max(maximum, float(rows['weight'].max()))
        table = pa.table({name: pa.array(rows[name]) for name in RECORD.names})
        pq.write_table(table, output / f'part-{partition:08d}.parquet', compression='zstd')
    if stream.read(1):
        raise ValueError('generator emitted more records than the declared edge count')
    return dict(sha256=digest.hexdigest(), bytes=edges * RECORD.itemsize,
                format='little-endian int64 src, int64 dst, float64 weight; 24 bytes per record',
                self_loops=loops, minimum_weight=minimum, maximum_weight=maximum,
                ordering='generator edge index; preserved in increasing partition filename and row order',
                duplicate_policy='preserved; not counted or deduplicated')


def write_vertices(output, vertices, chunk_vertices):
    digest = hashlib.sha256()
    for partition, start in enumerate(range(0, vertices, chunk_vertices)):
        ids = np.arange(start, min(start + chunk_vertices, vertices), dtype='<i8')
        digest.update(memoryview(ids))
        pq.write_table(pa.table({'id': pa.array(ids)}), output / f'part-{partition:08d}.parquet', compression='zstd')
    return dict(sha256=digest.hexdigest(), bytes=8 * vertices, format='little-endian int64 id; ascending 0..2^scale-1')


def write_reference(output, vertices, source, directed):
    from traversal_reference import distances

    # The caller enforces both limits before any subprocess or output creation.
    ids = list(range(vertices))
    table = pq.read_table(output / 'edges.parquet').to_pydict()
    edges = list(zip(table['src'], table['dst'], table['weight']))
    columns = {'id': pa.array(ids, type=pa.int64())}
    for algorithm in ('bfs', 'sssp'):
        values = distances(ids, edges, source, weighted=algorithm == 'sssp', directed=directed)
        columns[algorithm] = pa.array([values[node] for node in ids], type=pa.float64())
    pq.write_table(pa.table(columns), output / 'reference.parquet', compression='zstd')


def prepare(output, generator, *, scale, edge_factor=16, seed1=42, seed2=54,
            chunk_edges=262_144, chunk_vertices=262_144, source=0, directed=False,
            reference=False, expected_edge_sha256=None):
    if not isinstance(scale, int) or not 1 <= scale <= 40:
        raise ValueError('scale must be an integer in 1..40')
    vertices = 1 << scale
    if not isinstance(edge_factor, int) or not 1 <= edge_factor <= ((1 << 63) - 1) // vertices:
        raise ValueError('edge factor must be positive and the edge count must fit int64')
    edges = vertices * edge_factor
    if not all(isinstance(n, int) and 1 <= n <= MAX_CHUNK for n in (chunk_edges, chunk_vertices)):
        raise ValueError(f'chunk sizes must be integers in 1..{MAX_CHUNK}')
    if not all(isinstance(n, int) and 0 <= n < 1 << 64 for n in (seed1, seed2)):
        raise ValueError('seeds must be unsigned 64-bit integers')
    if not isinstance(source, int) or not 0 <= source < vertices:
        raise ValueError('source outside graph')
    if reference and (vertices > ORACLE_VERTICES or edges > ORACLE_EDGES):
        raise ValueError('full Python oracle is limited to 100000 vertices and 1000000 edges')
    generator = Path(generator).resolve()
    build = checked_build(generator)
    converter_sha256 = sha256(__file__)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    arguments = dict(scale=scale, edge_factor=edge_factor, seed1=seed1, seed2=seed2,
                     chunk_edges=chunk_edges, chunk_vertices=chunk_vertices,
                     source=source, directed=directed, reference=reference,
                     expected_edge_sha256=expected_edge_sha256)
    started = utc()
    start = time.monotonic()
    write_json(output / 'preparation.json', dict(started_utc=started, arguments=arguments, build=build))
    process = None
    try:
        for name in ('vertices.parquet', 'edges.parquet'):
            (output / name).mkdir()
        command = [str(generator), *map(str, (scale, edge_factor, seed1, seed2, chunk_edges))]
        with (output / 'generator.stderr').open('wb') as stderr:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=stderr)
            try:
                edge_identity = convert_edges(process.stdout, output / 'edges.parquet', vertices=vertices,
                                              edges=edges, chunk_edges=chunk_edges)
            finally:
                process.stdout.close()
            if process.wait() != 0:
                raise ValueError(f'generator exited with status {process.returncode}; see generator.stderr')
        if expected_edge_sha256 is not None and edge_identity['sha256'] != expected_edge_sha256:
            raise ValueError('canonical edge SHA256 differs from the pinned input')
        vertex_identity = write_vertices(output / 'vertices.parquet', vertices, chunk_vertices)
        if reference:
            write_reference(output, vertices, source, directed)
        if sha256(generator) != build['binary_sha256']:
            raise ValueError('generator binary changed during preparation')
        if sha256(__file__) != converter_sha256:
            raise ValueError('converter source changed during preparation')
        files = {p.relative_to(output).as_posix(): dict(sha256=sha256(p), bytes=p.stat().st_size)
                 for p in sorted(output.rglob('*.parquet')) if p.is_file()}
        manifest = dict(
            schema_version=1, family='graph500', seed=None, parameters=arguments,
            counts=dict(vertices=vertices, edges=edges),
            traversal=dict(source=source, directed=directed,
                           source_policy='explicit fixed vertex; not Graph500 root sampling',
                           weight_policy='upstream SSSP float32 weights widened exactly to float64'),
            generator=dict(build=build, argv=command,
                           environment={key: os.environ.get(key) for key in ('OMP_NUM_THREADS', 'OMP_DYNAMIC', 'OMP_PROC_BIND', 'OMP_PLACES')}),
            preparation=dict(started_utc=started, finished_utc=utc(), seconds=time.monotonic() - start,
                             pyarrow_version=pa.__version__, numpy_version=np.__version__,
                             converter_sha256=converter_sha256, compression='zstd'),
            canonical=dict(edges=edge_identity, vertices=vertex_identity),
            validation=dict(status='independent-reference-generated' if reference else 'reference-not-generated',
                            input='exact record count, endpoints, finite [0,1] float32 weights, output hashes',
                            reference='Python deque BFS and heap Dijkstra' if reference else None),
            purpose='Graph500-generated input; not an official Graph500 benchmark result', files=files)
        write_json(output / 'manifest.json', manifest)
        return manifest
    except BaseException as error:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        write_json(output / 'failure.json', dict(failed_utc=utc(), error=type(error).__name__, message=str(error)))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--generator', type=Path, required=True)
    parser.add_argument('--scale', type=int, required=True)
    parser.add_argument('--edge-factor', type=int, default=16)
    parser.add_argument('--seed1', type=int, default=42)
    parser.add_argument('--seed2', type=int, default=54)
    parser.add_argument('--chunk-edges', type=int, default=262_144)
    parser.add_argument('--chunk-vertices', type=int, default=262_144)
    parser.add_argument('--source', type=int, default=0)
    parser.add_argument('--directed', action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument('--reference', action='store_true')
    parser.add_argument('--expected-edge-sha256')
    args = vars(parser.parse_args())
    print(json.dumps(prepare(**args), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()

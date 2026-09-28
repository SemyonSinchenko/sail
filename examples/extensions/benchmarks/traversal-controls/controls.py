#!/usr/bin/env python3
"""Bounded integer-fixture export and full-vector external-control qualification.

This is a correctness/protocol adapter. Its unisolated local diagnostics are
not qualified performance measurements and cannot be pooled with Sail calls.
"""
import argparse
import csv
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from traversal_reference import validate_rows
from build_controls import PINS

MAX_VERTICES = 100_000
MAX_EDGES = 1_000_000
GAP_INFINITY = ((1 << 31) - 1) // 2


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


def check_files(directory, manifest):
    files = manifest['files']
    actual = {path.relative_to(directory).as_posix() for path in directory.rglob('*.parquet') if path.is_file()}
    declared_parquet = {name for name in files if name.endswith('.parquet')}
    if actual != declared_parquet:
        raise ValueError('Parquet file membership differs from the manifest')
    for name, details in files.items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory.resolve()) or sha256(path) != details['sha256']:
            raise ValueError(f'file differs from manifest: {name}')


def write_csr(path, vertices, src, dst, weights=None):
    """Write direct GAP CSR: no builder deduplication, loop removal or ID inference."""
    if sys.byteorder != 'little':
        raise ValueError('control serialization requires a little-endian host')
    with Path(path).open('wb') as stream:
        stream.write(struct.pack('<?qq', True, len(src), vertices))
        for a, b in ((src, dst), (dst, src)):
            order = np.argsort(a, kind='stable')
            offsets = np.zeros(vertices + 1, dtype='<i8')
            np.cumsum(np.bincount(a, minlength=vertices), out=offsets[1:])
            stream.write(offsets.tobytes())
            if weights is None:
                stream.write(b[order].astype('<i4').tobytes())
            else:
                records = np.empty(len(order), dtype=[('v', '<i4'), ('w', '<i4')])
                records['v'] = b[order]
                records['w'] = weights[order]
                stream.write(records.tobytes())


def prepare(dataset, output):
    dataset, output = Path(dataset).resolve(), Path(output).resolve()
    manifest_path = dataset / 'manifest.json'
    original = json.loads(manifest_path.read_text())
    vertices, count = original['counts']['vertices'], original['counts']['edges']
    if not 1 <= vertices <= MAX_VERTICES or not 1 <= count <= MAX_EDGES:
        raise ValueError('control correctness adapter requires <=100000 vertices and 1..1000000 edges')
    if not (dataset / 'reference.parquet').is_file():
        raise ValueError('control qualification requires independent reference distances')
    check_files(dataset, original)
    vertex_table = pq.read_table(dataset / 'vertices.parquet')
    if vertex_table.schema.field('id').type != pa.int64():
        raise ValueError('control vertex IDs must be int64')
    ids = sorted(vertex_table['id'].to_pylist())
    if ids != list(range(vertices)):
        raise ValueError('control IDs must be the dense range 0..N-1, including isolates')
    edges = pq.read_table(dataset / 'edges.parquet').select(['src', 'dst', 'weight'])
    if [field.type for field in edges.schema] != [pa.int64(), pa.int64(), pa.float64()]:
        raise ValueError('control input requires int64 endpoints and float64 weights')
    if edges.num_rows != count:
        raise ValueError('edge count differs from manifest')
    src = edges['src'].to_numpy()
    dst = edges['dst'].to_numpy()
    weights = edges['weight'].to_numpy()
    if any(np.any((array < 0) | (array >= vertices)) for array in (src, dst)):
        raise ValueError('unknown edge endpoint')
    if not np.all(np.isfinite(weights) & (weights >= 0) & (weights == np.floor(weights))):
        raise ValueError('controls require exact nonnegative integer weights; no rounding is performed')
    maximum = int(weights.max())
    if vertices * maximum >= GAP_INFINITY:
        raise ValueError('conservative path bound reaches the GAP int32 distance sentinel')
    directed = original['traversal']['directed']
    source = original['traversal']['source']
    if not isinstance(source, int) or not 0 <= source < vertices:
        raise ValueError('source outside graph')
    if not directed:
        src, dst, weights = np.concatenate((src, dst)), np.concatenate((dst, src)), np.concatenate((weights, weights))
    output.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    write_csr(output / 'graph.sg', vertices, src, dst)
    write_csr(output / 'graph.wsg', vertices, src, dst, weights)
    pq.write_table(edges, output / 'edges.parquet')
    pq.write_table(pq.read_table(dataset / 'reference.parquet'), output / 'reference.parquet')
    check_files(dataset, original)
    result = dict(schema_version=1, purpose='bounded external-control protocol qualification',
                  counts=dict(vertices=vertices, original_edge_tuples=count, csr_arcs=len(src)),
                  source=source, directed=directed, input_manifest_sha256=sha256(manifest_path),
                  input_manifest=original, weight_policy='exact integer weights; no quantization',
                  csr_policy='directed CSR; reverse every input tuple for undirected interpretation; loops and duplicates preserved',
                  path_bound=dict(vertices_times_max_weight=vertices * maximum, sentinel=GAP_INFINITY),
                  prepared_utc=utc(), export_seconds=time.monotonic() - start,
                  converter_sha256=sha256(__file__),
                  files={p.name: dict(sha256=sha256(p), bytes=p.stat().st_size) for p in output.iterdir() if p.is_file()})
    write_json(output / 'manifest.json', result)
    return result


def validate(output, dataset, manifest, bfs):
    with Path(output).open() as stream:
        rows = [dict(id=int(row['id']), distance=float(row['distance']) if row['distance'] else None,
                     parent=int(row['parent']) if row['parent'] else None,
                     hops=int(row['distance']) if row['distance'] and bfs else None)
                for row in csv.DictReader(stream, delimiter='\t')]
    reference = pq.read_table(dataset / 'reference.parquet').to_pydict()
    expected = dict(zip(reference['id'], reference['bfs' if bfs else 'sssp']))
    table = pq.read_table(dataset / 'edges.parquet').to_pydict()
    edges = list(zip(table['src'], table['dst'], table['weight']))
    return validate_rows(rows, expected, edges, manifest['source'], weighted=not bfs,
                         directed=manifest['directed'], parents=bfs)


def run(dataset, binary, output, *, method, threads=1, parameter=None, beta=18, timeout=300):
    dataset, binary, output = Path(dataset).resolve(), Path(binary).resolve(), Path(output).resolve()
    build = json.loads(Path(str(binary) + '.build.json').read_text())
    kind = build['control']
    if kind not in PINS or build['source_commit'] != PINS[kind] or build['binary_sha256'] != sha256(binary):
        raise ValueError('control binary identity differs from its build receipt')
    if build['adapter_sha256'] != sha256(Path(__file__).with_name(f'{kind}_control.cc')):
        raise ValueError('control adapter differs from this checkout; rebuild it')
    if method not in ({'bfs', 'sssp'} if kind == 'gap' else {'rho', 'delta', 'bellman-ford'}):
        raise ValueError('method does not belong to this control')
    if threads < 1 or beta < 1 or timeout <= 0:
        raise ValueError('threads, beta and timeout must be positive')
    parameter = parameter if parameter is not None else {'bfs': 15, 'rho': 1 << 20}.get(method, 4)
    if not isinstance(parameter, int) or parameter < 1:
        raise ValueError('algorithm parameter must be a positive integer')
    manifest = json.loads((dataset / 'manifest.json').read_text())
    check_files(dataset, manifest)
    output.mkdir(parents=True, exist_ok=False)
    bfs = method == 'bfs'
    argv = [str(binary), method, str(dataset / ('graph.sg' if bfs else 'graph.wsg')),
            str(manifest['source']), str(output / 'result.tsv'), str(parameter)]
    if bfs:
        argv.append(str(beta))
    environment = dict(os.environ, OMP_NUM_THREADS=str(threads), OMP_DYNAMIC='FALSE', PARLAY_NUM_THREADS=str(threads))
    receipt = dict(started_utc=utc(), command=argv, build=build, source=manifest['source'],
                   dataset_manifest_sha256=sha256(dataset / 'manifest.json'),
                   environment={key: environment[key] for key in ('OMP_NUM_THREADS', 'OMP_DYNAMIC', 'PARLAY_NUM_THREADS')},
                   qualification='functional only; unisolated local diagnostic, not a published timing',
                   kernel_boundary='CSR already loaded; algorithm state allocation, initialization, kernel and result return; excludes verification/output',
                   parameter=parameter, beta=beta if bfs else None, outcome='running')
    write_json(output / 'receipt.json', receipt)
    start = time.monotonic()
    try:
        with (output / 'stdout.log').open('w') as stdout, (output / 'stderr.log').open('w') as stderr:
            completed = subprocess.run(argv, stdout=stdout, stderr=stderr, env=environment, timeout=timeout)
        receipt['returncode'] = completed.returncode
        if completed.returncode != 0:
            raise RuntimeError(f'control exited {completed.returncode}')
        result_lines = [line.split(' ', 1)[1] for line in (output / 'stdout.log').read_text().splitlines()
                        if line.startswith('SAIL_CONTROL_RESULT ')]
        if len(result_lines) != 1:
            raise ValueError('control did not emit exactly one timing record')
        receipt['kernel'] = json.loads(result_lines[0])
        if not math.isfinite(receipt['kernel']['kernel_seconds']) or receipt['kernel']['kernel_seconds'] < 0:
            raise ValueError('invalid kernel diagnostic')
        if receipt['kernel']['thread_capacity'] != threads:
            raise ValueError('runtime thread capacity differs from requested threads')
        receipt['validation'] = validate(output / 'result.tsv', dataset, manifest, bfs)
        receipt['result_sha256'] = sha256(output / 'result.tsv')
        check_files(dataset, manifest)
        if sha256(binary) != build['binary_sha256']:
            raise ValueError('control binary changed during run')
        receipt['outcome'] = 'passed'
    except subprocess.TimeoutExpired as error:
        receipt.update(outcome='timeout', error=str(error))
    except AssertionError as error:
        receipt.update(outcome='mismatch', error=str(error))
    except Exception as error:
        receipt.update(outcome='error', error=f'{type(error).__name__}: {error}')
    if (output / 'result.tsv').is_file():
        receipt['result_sha256'] = sha256(output / 'result.tsv')
    receipt.update(finished_utc=utc(), process_and_validation_seconds=time.monotonic() - start)
    write_json(output / 'receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    export = actions.add_parser('prepare')
    export.add_argument('--dataset', type=Path, required=True)
    export.add_argument('--output', type=Path, required=True)
    execute = actions.add_parser('run')
    execute.add_argument('--dataset', type=Path, required=True)
    execute.add_argument('--binary', type=Path, required=True)
    execute.add_argument('--output', type=Path, required=True)
    execute.add_argument('--method', choices=['bfs', 'sssp', 'rho', 'delta', 'bellman-ford'], required=True)
    execute.add_argument('--threads', type=int, default=1)
    execute.add_argument('--parameter', type=int)
    execute.add_argument('--beta', type=int, default=18)
    execute.add_argument('--timeout', type=float, default=300)
    args = vars(parser.parse_args())
    action = args.pop('action')
    result = prepare(**args) if action == 'prepare' else run(**args)
    print(json.dumps(result, sort_keys=True, indent=2))
    if action == 'run' and result['outcome'] != 'passed':
        sys.exit(1)


if __name__ == '__main__':
    main()

"""Bounded input conversion and pinned generator integration gates.

Set GRAPH500_SOURCE to the clean pinned upstream checkout to run the two real
generator tests as well as the converter unit tests. No Sail server is needed.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pyarrow.parquet as pq
import pytest

import graph500_fixture as fixture


def record_bytes(rows):
    return np.array(rows, dtype=fixture.RECORD).tobytes()


class ShortReads(io.BytesIO):
    def readinto(self, buffer):
        return super().readinto(buffer[:7])


def test_converter_preserves_order_duplicates_loops_and_float32_weights(tmp_path):
    rows = [(2, 1, 0.5), (1, 1, 0.0), (2, 1, 0.5), (0, 2, 1.0)]
    raw = record_bytes(rows)
    observed = fixture.convert_edges(ShortReads(raw), tmp_path, vertices=5, edges=4, chunk_edges=3)
    assert pq.read_table(tmp_path).to_pydict() == {
        'src': [2, 1, 2, 0], 'dst': [1, 1, 1, 2], 'weight': [0.5, 0, 0.5, 1]}
    assert observed['sha256'] == hashlib.sha256(raw).hexdigest()
    assert observed['bytes'] == 4 * 24
    assert observed['self_loops'] == 1
    assert observed['minimum_weight'] == 0 and observed['maximum_weight'] == 1


@pytest.mark.parametrize('row,match', [
    ((-1, 0, 0.5), 'endpoint'), ((0, 8, 0.5), 'endpoint'),
    ((0, 1, -1), 'weights'), ((0, 1, float('nan')), 'weights'),
    ((0, 1, float('inf')), 'weights'), ((0, 1, 1.5), 'weights'),
    ((0, 1, 0.1), 'float32'),
])
def test_converter_rejects_invalid_records(tmp_path, row, match):
    with pytest.raises(ValueError, match=match):
        fixture.convert_edges(io.BytesIO(record_bytes([row])), tmp_path, vertices=8, edges=1, chunk_edges=3)


@pytest.mark.parametrize('raw,match', [(b'', 'truncated'), (b'x' * 23, 'truncated'),
                                      (record_bytes([(0, 1, 0.5)]) + b'x', 'more records')])
def test_converter_rejects_incomplete_or_extra_stream(tmp_path, raw, match):
    with pytest.raises(ValueError, match=match):
        fixture.convert_edges(io.BytesIO(raw), tmp_path, vertices=8, edges=1, chunk_edges=3)


def test_vertices_include_isolates_and_last_partial_chunk(tmp_path):
    result = fixture.write_vertices(tmp_path, vertices=9, chunk_vertices=4)
    assert pq.read_table(tmp_path).to_pydict() == {'id': list(range(9))}
    assert len(list(tmp_path.glob('*.parquet'))) == 3
    assert result['sha256'] == hashlib.sha256(np.arange(9, dtype='<i8').tobytes()).hexdigest()


@pytest.mark.parametrize('arguments,match', [
    ({'scale': 41}, 'scale'), ({'scale': 0}, 'scale'),
    ({'edge_factor': 0}, 'edge factor'), ({'seed1': -1}, 'seeds'),
    ({'seed2': 1 << 64}, 'seeds'), ({'source': 8}, 'source'),
    ({'chunk_edges': 0}, 'chunk sizes'), ({'chunk_vertices': fixture.MAX_CHUNK + 1}, 'chunk sizes'),
    ({'scale': 17, 'reference': True}, 'oracle'),
    ({'scale': 16, 'edge_factor': 16, 'reference': True}, 'oracle'),
])
def test_rejects_invalid_or_unbounded_requests_before_starting(tmp_path, arguments, match):
    options = dict(scale=3)
    options.update(arguments)
    with pytest.raises(ValueError, match=match):
        fixture.prepare(tmp_path / 'out', tmp_path / 'not-a-generator', **options)
    assert not (tmp_path / 'out').exists()


def test_nonzero_generator_exit_cannot_publish_success(tmp_path, monkeypatch):
    binary = tmp_path / 'failed-generator'
    binary.write_text('#!/usr/bin/env python3\nimport struct, sys\n'
                      'sys.stdout.buffer.write(struct.pack("<qqd", 0, 1, 0.5) * 8)\n'
                      'print("generator failed after output", file=sys.stderr)\n'
                      'sys.exit(7)\n')
    binary.chmod(0o755)
    monkeypatch.setattr(fixture, 'checked_build', lambda path: dict(binary_sha256=fixture.sha256(path)))
    with pytest.raises(ValueError, match='status 7'):
        fixture.prepare(tmp_path / 'out', binary, scale=3, edge_factor=1)
    assert not (tmp_path / 'out/manifest.json').exists()
    assert 'generator failed' in (tmp_path / 'out/generator.stderr').read_text()
    assert json.loads((tmp_path / 'out/failure.json').read_text())['error'] == 'ValueError'


@pytest.fixture(scope='session')
def generator(tmp_path_factory):
    source = os.environ.get('GRAPH500_SOURCE')
    if not source:
        pytest.skip('set GRAPH500_SOURCE for the pinned real-generator integration gate')
    binary = tmp_path_factory.mktemp('graph500-build') / 'graph500_stream'
    script = Path(__file__).parent / 'traversal-controls' / 'build_graph500.sh'
    subprocess.run(['bash', str(script), source, str(binary)], check=True)
    return binary


def test_real_generator_chunk_invariance_manifest_and_small_oracle(tmp_path, generator):
    a = fixture.prepare(tmp_path / 'a', generator, scale=8, edge_factor=4, chunk_edges=37,
                        chunk_vertices=39, reference=True)
    b = fixture.prepare(tmp_path / 'b', generator, scale=8, edge_factor=4, chunk_edges=256,
                        chunk_vertices=128, expected_edge_sha256=a['canonical']['edges']['sha256'])
    assert a['canonical'] == b['canonical']
    assert a['counts'] == {'vertices': 256, 'edges': 1024}
    assert a['family'] == 'graph500'
    assert a['generator']['build']['generator_commit'] == fixture.GENERATOR_COMMIT
    assert a['generator']['build']['binary_sha256'] == fixture.sha256(generator)
    assert a['validation']['status'] == 'independent-reference-generated'
    assert b['validation']['status'] == 'reference-not-generated'
    assert not (tmp_path / 'b/reference.parquet').exists()
    assert pq.read_table(tmp_path / 'a/edges.parquet').equals(pq.read_table(tmp_path / 'b/edges.parquet'))
    reference = pq.read_table(tmp_path / 'a/reference.parquet').to_pydict()
    assert reference['id'] == list(range(256))
    assert reference['bfs'][0] == reference['sssp'][0] == 0
    for name, details in a['files'].items():
        assert details['sha256'] == fixture.sha256(tmp_path / 'a' / name)
        assert details['bytes'] == (tmp_path / 'a' / name).stat().st_size
    assert json.loads((tmp_path / 'a/manifest.json').read_text()) == a


def test_real_generator_rejects_stale_binary_and_retains_failed_preparation(tmp_path, generator):
    changed = tmp_path / 'changed'
    shutil.copy2(generator, changed)
    shutil.copy2(str(generator) + '.build.json', str(changed) + '.build.json')
    with changed.open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='binary differs'):
        fixture.prepare(tmp_path / 'stale', changed, scale=3)
    assert not (tmp_path / 'stale').exists()
    with pytest.raises(ValueError, match='SHA256 differs'):
        fixture.prepare(tmp_path / 'bad-hash', generator, scale=3, expected_edge_sha256='0' * 64)
    assert not (tmp_path / 'bad-hash/manifest.json').exists()
    assert (tmp_path / 'bad-hash/failure.json').exists()
    assert (tmp_path / 'bad-hash/preparation.json').exists()
    assert (tmp_path / 'bad-hash/generator.stderr').exists()
    with pytest.raises(FileExistsError):
        fixture.prepare(tmp_path / 'bad-hash', generator, scale=3)

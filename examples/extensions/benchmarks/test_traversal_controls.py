"""Set GAP_CONTROL_BINARY and PARALLEL_CONTROL_BINARY for real-driver gates."""
import json
import os
from pathlib import Path
import struct
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from traversal_fixture import prepare as prepare_traversal

CONTROL_DIR = Path(__file__).parent / 'traversal-controls'
sys.path.insert(0, str(CONTROL_DIR))
import controls


def test_export_preserves_all_vertex_ids_arcs_weights_and_multiplicity(tmp_path):
    prepare_traversal(tmp_path / 'input', vertices=16, degree=2, directed=False)
    result = controls.prepare(tmp_path / 'input', tmp_path / 'control')
    original = pq.read_table(tmp_path / 'input/edges.parquet').to_pydict()
    original_arcs = list(zip(original['src'], original['dst'], original['weight']))
    raw = (tmp_path / 'control/graph.wsg').read_bytes()
    directed, m, n = struct.unpack_from('<?qq', raw)
    assert directed and n == 16 and m == 2 * len(original_arcs)
    offset = 17
    for expected in (original_arcs + [(b, a, w) for a, b, w in original_arcs],
                     [(b, a, w) for a, b, w in original_arcs] + original_arcs):
        offsets = np.frombuffer(raw, dtype='<i8', count=n + 1, offset=offset)
        offset += (n + 1) * 8
        edges = np.frombuffer(raw, dtype=[('v', '<i4'), ('w', '<i4')], count=m, offset=offset)
        offset += m * 8
        observed = [(a, int(edges[i]['v']), int(edges[i]['w'])) for a in range(n)
                    for i in range(offsets[a], offsets[a + 1])]
        assert observed == sorted(expected, key=lambda row: row[0])
        assert offsets[-1] == m
        assert offsets[-2] == m  # Highest vertex is the original fixture's isolate.
    assert offset == len(raw)
    assert result['source'] == 0
    assert result['counts']['csr_arcs'] == m
    controls.check_files(tmp_path / 'control', result)


@pytest.mark.parametrize('weight,match', [(0.5, 'integer weights'), (-1, 'integer weights'),
                                          (float('nan'), 'integer weights'), (1 << 30, 'sentinel')])
def test_export_rejects_weight_conversion_or_integer_overflow(tmp_path, weight, match):
    manifest = prepare_traversal(tmp_path / 'input', vertices=16, degree=2)
    path = tmp_path / 'input/edges.parquet'
    table = pq.read_table(path).to_pydict()
    table['weight'][0] = weight
    pq.write_table(pa.table(table), path)
    manifest['files'][path.name]['sha256'] = controls.sha256(path)
    (tmp_path / 'input/manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match=match):
        controls.prepare(tmp_path / 'input', tmp_path / 'out')


def test_export_rejects_unlisted_partition(tmp_path):
    prepare_traversal(tmp_path / 'input', vertices=16, degree=2)
    pq.write_table(pa.table({'id': [1000]}), tmp_path / 'input/unlisted.parquet')
    with pytest.raises(ValueError, match='membership'):
        controls.prepare(tmp_path / 'input', tmp_path / 'out')


@pytest.mark.parametrize('directed', [False, True])
@pytest.mark.parametrize('threads', [1, 4])
@pytest.mark.parametrize('kind,method,parameter', [('gap', 'bfs', 15), ('gap', 'sssp', 4),
                                                   ('parallel', 'rho', 8), ('parallel', 'delta', 4),
                                                   ('parallel', 'bellman-ford', 1)])
def test_real_controls_match_every_reference_distance(tmp_path, directed, threads, kind, method, parameter):
    binary = os.environ.get('GAP_CONTROL_BINARY' if kind == 'gap' else 'PARALLEL_CONTROL_BINARY')
    if not binary:
        pytest.skip('set control binary environment variables for real-driver qualification')
    prepare_traversal(tmp_path / 'input', vertices=256, degree=8, directed=directed)
    controls.prepare(tmp_path / 'input', tmp_path / 'control')
    result = controls.run(tmp_path / 'control', binary, tmp_path / 'run', method=method,
                          threads=threads, parameter=parameter, timeout=30)
    assert result['outcome'] == 'passed', result
    assert result['source'] == 0
    assert result['validation']['vertices'] == 256
    assert result['validation']['parent_tree_checked'] == (method == 'bfs')
    assert result['kernel']['thread_capacity'] == threads


@pytest.mark.parametrize('kind,method', [('gap', 'bfs'), ('gap', 'sssp'), ('parallel', 'rho'),
                                        ('parallel', 'delta'), ('parallel', 'bellman-ford')])
def test_real_controls_preserve_explicit_isolated_source(tmp_path, kind, method):
    binary = os.environ.get('GAP_CONTROL_BINARY' if kind == 'gap' else 'PARALLEL_CONTROL_BINARY')
    if not binary:
        pytest.skip('set control binary environment variables')
    prepare_traversal(tmp_path / 'input', vertices=32, degree=2, source=31)
    controls.prepare(tmp_path / 'input', tmp_path / 'control')
    result = controls.run(tmp_path / 'control', binary, tmp_path / 'run', method=method, threads=4)
    assert result['outcome'] == 'passed', result
    assert result['source'] == 31 and result['validation']['reached'] == 1


def test_parallel_dense_multigraph_tuning_keeps_positive_threshold(tmp_path):
    binary = os.environ.get('PARALLEL_CONTROL_BINARY')
    if not binary:
        pytest.skip('set PARALLEL_CONTROL_BINARY')
    prepare_traversal(tmp_path / 'input', vertices=8, degree=64)
    controls.prepare(tmp_path / 'input', tmp_path / 'control')
    result = controls.run(tmp_path / 'control', binary, tmp_path / 'run', method='rho', threads=4, timeout=15)
    assert result['outcome'] == 'passed', result
    assert result['kernel']['sparse_dense_scale'] == 8


def test_control_mismatch_retains_full_output_and_receipt(tmp_path):
    binary = os.environ.get('PARALLEL_CONTROL_BINARY')
    if not binary:
        pytest.skip('set PARALLEL_CONTROL_BINARY')
    prepare_traversal(tmp_path / 'input', vertices=32, degree=2)
    manifest = controls.prepare(tmp_path / 'input', tmp_path / 'control')
    reference_path = tmp_path / 'control/reference.parquet'
    table = pq.read_table(reference_path).to_pydict()
    table['sssp'][0] = 1.  # Deliberately false reference must not produce a pass.
    pq.write_table(pa.table(table), reference_path)
    manifest['files']['reference.parquet']['sha256'] = controls.sha256(reference_path)
    (tmp_path / 'control/manifest.json').write_text(json.dumps(manifest))
    result = controls.run(tmp_path / 'control', binary, tmp_path / 'run', method='delta', threads=1)
    assert result['outcome'] == 'mismatch'
    assert result['result_sha256'] == controls.sha256(tmp_path / 'run/result.tsv')
    assert json.loads((tmp_path / 'run/receipt.json').read_text()) == result

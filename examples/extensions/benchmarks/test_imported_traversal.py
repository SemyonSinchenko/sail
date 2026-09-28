"""Input identity and bounded-memory conversion for Graph Kernels traversals."""
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

import numpy as np
import pyarrow.parquet as pq
import pytest

import imported_traversal_fixture as fixture
from graph_cell import validate_dataset_files, validate_dataset_identity
from run_matrix import cell_command, dataset_command, plan_cells


def source(tmp_path, content=b'6 6\n0 1\n0 1\n1 1\n1 2\n3 4\n4 3\n'):
    path = tmp_path / 'input.edges'
    path.write_bytes(content)
    return path, hashlib.sha256(content).hexdigest()


def prepare(tmp_path, **kwargs):
    path, digest = source(tmp_path)
    return fixture.prepare(tmp_path / 'data', path, vertices=6,
                           edge_sha256=digest, weight_policy='unit', **kwargs)


def test_exact_order_duplicates_loops_isolates_and_independent_reference(tmp_path):
    manifest = prepare(tmp_path, chunk_edges=2, chunk_vertices=2, reference=True)
    assert manifest['counts'] == {'vertices': 6, 'edges': 6}
    assert manifest['canonical']['edges']['self_loops'] == 1
    rows = pq.read_table(tmp_path / 'data/edges.parquet').to_pydict()
    expected = [(0, 1, 1.), (0, 1, 1.), (1, 1, 1.), (1, 2, 1.), (3, 4, 1.), (4, 3, 1.)]
    assert list(zip(rows['src'], rows['dst'], rows['weight'])) == expected
    assert pq.read_table(tmp_path / 'data/vertices.parquet')['id'].to_pylist() == list(range(6))
    oracle = pq.read_table(tmp_path / 'data/reference.parquet').to_pydict()
    assert oracle['bfs'] == oracle['sssp'] == [0., 1., 2., None, None, None]
    raw = b''.join(struct.pack('<qqd', *row) for row in expected)
    assert manifest['canonical']['edges']['sha256'] == hashlib.sha256(raw).hexdigest()
    for name, identity in manifest['files'].items():
        assert fixture.sha256(tmp_path / 'data' / name) == identity['sha256']
    validate_dataset_files(tmp_path / 'data', manifest)


def scalar_weight(index, seed):
    mask = (1 << 64) - 1
    z = (index + seed + 0x9E3779B97F4A7C15) & mask
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask
    return 1 + ((z ^ (z >> 31)) & 15)


@pytest.mark.parametrize('seed', [0, 42, (1 << 64) - 1])
def test_weight_formula_is_independent_of_array_chunking_and_overflow(seed):
    # Use enough fixed edge ordinals to exercise all 16 weight values for each
    # pinned seed; the first 100 ordinals at seed42 do not include weight14.
    count = 1024
    expected = [scalar_weight(i, seed) for i in range(count)]
    for width in (1, 7, 99, count):
        actual = np.concatenate([fixture.weights(i, min(width, count-i), 'splitmix64-1-16', seed)
                                 for i in range(0, count, width)])
        assert actual.tolist() == expected
        assert set(actual) == set(range(1, 17))


def test_parquet_partitioning_does_not_change_canonical_weighted_identity(tmp_path):
    path, digest = source(tmp_path)
    manifests = [fixture.prepare(tmp_path / str(size), path, vertices=6, edge_sha256=digest,
        weight_policy='splitmix64-1-16', chunk_edges=size, chunk_vertices=size) for size in (1, 4, 100)]
    assert all(m['canonical'] == manifests[0]['canonical'] for m in manifests)
    assert manifests[0]['traversal']['weight_seed'] == 42


@pytest.mark.parametrize('content,diagnostic', [
    (b'6 1\n0 6\n', 'endpoint'), (b'6 1\n-1 0\n', 'two nonnegative'),
    (b'6 1\n0.0 1\n', 'two nonnegative'), (b'6 1\n0 1 2\n', 'two nonnegative'),
    (b'6 1\n0 1 # comment\n', 'two nonnegative'),
    (b'6 1\n' + b'0'*200 + b' 1\n', 'two nonnegative'),
    # Two individually valid fragments must not become two records when they
    # are really one overlong four-token physical line.
    (b'6 2\n0 1' + b' '*125 + b'2 3\n', 'two nonnegative'),
    (b'6 1\n9999999999999999999 0\n', 'endpoint'),
    (b'6 2\n0 1\n', 'two nonnegative'), (b'6 1\n0 1\n1 2\n', 'more rows'),
])
def test_bad_row_retains_failure_and_never_final_manifest(tmp_path, content, diagnostic):
    path, digest = source(tmp_path, content)
    with pytest.raises(ValueError, match=diagnostic):
        fixture.prepare(tmp_path / 'bad', path, vertices=6, edge_sha256=digest, weight_policy='unit')
    assert (tmp_path / 'bad/failure.json').is_file()
    assert not (tmp_path / 'bad/manifest.json').exists()


@pytest.mark.parametrize('content', [b'6 0\n', b'6 1\n0 1', b'6 1\r\n0\t1\r\n'])
def test_zero_edges_and_valid_line_endings(tmp_path, content):
    path, digest = source(tmp_path, content)
    m = fixture.prepare(tmp_path / 'data', path, vertices=6, edge_sha256=digest, weight_policy='unit')
    table = pq.read_table(tmp_path / 'data/edges.parquet')
    assert table.num_rows == m['counts']['edges']
    assert table.schema.names == ['src', 'dst', 'weight']


@pytest.mark.parametrize('change,diagnostic', [
    ({'edge_sha256': 'a'*64}, 'input SHA256'),
    ({'vertices': 7}, 'header counts'),
    ({'source': 6}, 'source'),
    ({'weight_policy': 'random'}, 'weight policy'),
    ({'weight_seed': -1}, 'weight seed'),
    ({'weight_seed': True}, 'weight seed'),
    ({'chunk_edges': 0}, 'chunk sizes'),
    ({'directed': 'true'}, 'boolean'),
])
def test_invalid_contract_fails_before_output_creation(tmp_path, change, diagnostic):
    path, digest = source(tmp_path)
    args = dict(vertices=6, edge_sha256=digest, weight_policy='unit')
    args.update(change)
    with pytest.raises(ValueError, match=diagnostic):
        fixture.prepare(tmp_path / 'bad', path, **args)
    assert not (tmp_path / 'bad').exists()


def test_oversized_oracle_refused_before_any_parquet(tmp_path):
    path, digest = source(tmp_path, b'100001 0\n')
    with pytest.raises(ValueError, match='oracle is limited'):
        fixture.prepare(tmp_path / 'bad', path, vertices=100001, edge_sha256=digest,
                        weight_policy='unit', reference=True)
    assert not (tmp_path / 'bad').exists()


def test_input_changed_during_conversion_is_detected(tmp_path, monkeypatch):
    path, digest = source(tmp_path)
    original = fixture.convert
    def change_after_read(*args, **kwargs):
        result = original(*args, **kwargs)
        path.write_bytes(path.read_bytes().replace(b'0 1', b'0 2'))
        return result
    monkeypatch.setattr(fixture, 'convert', change_after_read)
    with pytest.raises(ValueError, match='input changed'):
        fixture.prepare(tmp_path / 'bad', path, vertices=6, edge_sha256=digest, weight_policy='unit')
    assert not (tmp_path / 'bad/manifest.json').exists()


def configuration(tmp_path):
    config = json.loads(Path(__file__).with_name('traversal-matrix.example.json').read_text())
    config['container_repo'] = str(Path(__file__).resolve().parents[3])
    config['container_root'] = str(tmp_path / 'output')
    path, digest = source(tmp_path)
    config['datasets'] = {'imported': dict(family='edge-list-traversal', vertices=6,
        edge_file=str(path), edge_sha256=digest, weight_policy='splitmix64-1-16',
        weight_seed=42, source=0, directed=False, validation='reference')}
    for suite in config['suites']:
        suite['datasets'] = ['imported']
    return config


def test_real_matrix_preparer_and_all_path_identity_arguments(tmp_path):
    config = configuration(tmp_path)
    cells = plan_cells(config)
    assert len(cells) == 36
    argv = dataset_command(config, 'imported')
    assert '--no-directed' in argv and '--reference' in argv
    assert '--validation' not in argv and '--tolerance' not in argv
    subprocess.run([sys.executable, *argv], check=True, capture_output=True, text=True)
    manifest = json.loads((tmp_path / 'output/datasets/imported/manifest.json').read_text())
    for cell in cells:
        command = cell_command(config, cell)
        def value(flag):
            return command[command.index(flag)+1]
        assert '--no-directed' in command
        assert value('--traversal-validation') == 'reference'
        assert value('--expected-input-sha256') == config['datasets']['imported']['edge_sha256']
        validate_dataset_identity(manifest, family=value('--expected-dataset-family'),
            vertices=int(value('--expected-vertices')), input_sha256=value('--expected-input-sha256'),
            weight_policy=value('--expected-weight-policy'), weight_seed=int(value('--expected-weight-seed')))
    for argument, value, diagnostic in [('input_sha256', 'a'*64, 'original topology'),
                                      ('edge_sha256', 'a'*64, 'canonical weighted'),
                                      ('weight_policy', 'unit', 'weight_policy'),
                                      ('weight_seed', 43, 'weight_seed')]:
        with pytest.raises(ValueError, match=diagnostic):
            validate_dataset_identity(manifest, **{argument: value})
    config['datasets']['imported']['expected_edge_sha256'] = 'a'*64
    for cell in cells:
        command = cell_command(config, cell)
        assert command[command.index('--expected-edge-sha256')+1] == 'a'*64
    config['container_root'] = str(tmp_path / 'wrong-pin')
    run = subprocess.run([sys.executable, *dataset_command(config, 'imported')], capture_output=True)
    assert run.returncode != 0
    assert (tmp_path / 'wrong-pin/datasets/imported/failure.json').exists()


@pytest.mark.parametrize('change,diagnostic', [
    ({'weight_policy': 'random'}, 'weight policy'), ({'weight_seed': -1}, 'weight seed'),
    ({'source': 6}, 'source'), ({'directed': 'true'}, 'boolean'),
    ({'validation': 'none'}, 'validation'), ({'vertices': 100001}, 'restricted'),
    ({'edge_file': 'relative'}, 'absolute'), ({'edge_sha256': 'a'}, 'imported bytes'),
    ({'expected_edge_sha256': 'a'}, 'canonical'), ({'certificate_max_rounds': 0}, 'round cap'),
])
def test_matrix_invalid_import_contract(tmp_path, change, diagnostic):
    config = configuration(tmp_path)
    config['datasets']['imported'].update(change)
    with pytest.raises(ValueError, match=diagnostic):
        plan_cells(config)


def test_published_large_matrix_plans_every_path_on_same_pinned_topology():
    root = Path(__file__).parent
    config = json.loads((root / 'graph-kernels-traversal-matrix.example.json').read_text())
    cells = plan_cells(config)
    assert len(cells) == 216
    pins = {item['name'].removesuffix('.edges'): item
            for item in json.loads((root / 'large-fixtures.json').read_text())['fixtures']}
    for name, dataset in config['datasets'].items():
        assert dataset['vertices'] == pins[name]['nodes']
        assert dataset['edge_sha256'] == pins[name]['sha256']
        assert dataset['weight_policy'] == 'splitmix64-1-16'
        assert dataset['validation'] == 'certificate'
        assert '--reference' not in dataset_command(config, name)
    for cell in cells:
        command = cell_command(config, cell)
        assert command[command.index('--traversal-validation')+1] == 'certificate'


@pytest.mark.parametrize('alteration', ['extra-parquet', 'extra-other-extension', 'missing', 'modified',
                                      'symlink-file', 'symlink-directory', 'escape-manifest'])
def test_unpinned_input_inventory_cannot_bypass_skip_prepare(tmp_path, alteration):
    manifest = prepare(tmp_path, chunk_edges=2)
    root = tmp_path / 'data'
    original = root / 'edges.parquet/part-00000000.parquet'
    if alteration.startswith('extra'):
        suffix = '.parquet' if alteration == 'extra-parquet' else '.data'
        # Both names are loadable partitions; pin checking must not filter by suffix.
        (original.parent / ('added' + suffix)).write_bytes(original.read_bytes())
    elif alteration == 'missing':
        original.unlink()
    elif alteration == 'modified':
        original.write_bytes(original.read_bytes() + b'changed')
    elif alteration == 'symlink-file':
        outside = tmp_path / 'outside.parquet'
        original.rename(outside)
        original.symlink_to(outside)
    elif alteration == 'symlink-directory':
        outside = tmp_path / 'outside-directory'
        original.parent.rename(outside)
        (root / 'edges.parquet').symlink_to(outside, target_is_directory=True)
    else:
        manifest['files']['../outside.parquet'] = dict(sha256='a'*64)
    with pytest.raises(ValueError, match='inventory|changed|symlinks|inside'):
        validate_dataset_files(root, manifest)


def test_inventory_guard_accepts_legacy_single_file_fixtures(tmp_path):
    from graph_fixtures import prepare as prepare_graph
    from traversal_fixture import prepare as prepare_traversal
    # Existing PR/WCC and traversal producers must pass the common preflight.
    first = prepare_graph(tmp_path / 'graph', vertices=8, family='chain')
    second = prepare_traversal(tmp_path / 'traversal', vertices=8)
    validate_dataset_files(tmp_path / 'graph', first)
    validate_dataset_files(tmp_path / 'traversal', second)

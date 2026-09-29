"""SNAP import: dense remap, retained order, pinned hashes, and ranking cells without a reference."""
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import imported_snap_fixture as fixture
from graph_cell import resolve_source, validate_dataset_files, validate_dataset_identity
from run_matrix import TRAVERSAL_FAMILIES, cell_command, dataset_command, plan_cells, validate_config

SNAP = b"# Directed graph (each unordered pair of nodes is saved once)\n# Nodes: 5 Edges: 6\n# FromNodeId\tToNodeId\n3858241\t956203\n3858241\t1324234\n956203\t956203\n1324234\t3858241\n6009\t3858241\n1324234\t3858241\n"


def source(tmp_path, content=SNAP):
    path = tmp_path / 'cit.txt'
    path.write_bytes(content)
    return path, hashlib.sha256(content).hexdigest()


def test_dense_remap_keeps_order_duplicates_and_loops(tmp_path):
    path, digest = source(tmp_path)
    manifest = fixture.prepare(tmp_path / 'data', path, vertices=4, edge_sha256=digest, chunk_edges=4, chunk_vertices=3)
    assert manifest['family'] == 'snap-edge-list'
    assert manifest['counts'] == {'vertices': 4, 'edges': 6, 'self_loops': 1}
    vertices = pq.read_table(tmp_path / 'data/vertices.parquet').to_pydict()
    assert vertices['id'] == [0, 1, 2, 3]
    assert vertices['original_id'] == [6009, 956203, 1324234, 3858241]
    edges = pq.read_table(tmp_path / 'data/edges.parquet').to_pydict()
    # 3858241->3, 956203->1, 1324234->2, 6009->0; order and duplicates preserved, unit weights.
    assert list(zip(edges['src'], edges['dst'], edges['weight'])) == [(3, 1, 1.0), (3, 2, 1.0), (1, 1, 1.0), (2, 3, 1.0), (0, 3, 1.0), (2, 3, 1.0)]
    assert len(list((tmp_path / 'data/edges.parquet').glob('*.parquet'))) == 2
    assert manifest['traversal']['source_original_id'] == 6009
    assert manifest['validation']['status'] == 'reference-not-generated'
    validate_dataset_identity(manifest, family='snap-edge-list', vertices=4, input_sha256=digest,
                              edge_sha256=manifest['canonical']['edges']['sha256'], weight_policy='unit', weight_seed=42)
    validate_dataset_files(tmp_path / 'data', manifest)


@pytest.mark.parametrize('content, message', [
    (b"# c\n1\t2\n3\n", 'two nonnegative'),
    (b"1\t2\n2\t3\n", 'distinct IDs'),
])
def test_malformed_or_miscounted_input_is_refused(tmp_path, content, message):
    path, digest = source(tmp_path, content)
    with pytest.raises(ValueError, match=message):
        fixture.prepare(tmp_path / 'data', path, vertices=4, edge_sha256=digest)
    assert not (tmp_path / 'data' / 'manifest.json').exists()


def test_wrong_pin_is_refused_before_reading(tmp_path):
    path, _ = source(tmp_path)
    with pytest.raises(ValueError, match='SHA256 differs'):
        fixture.prepare(tmp_path / 'data', path, vertices=4, edge_sha256='0' * 64)


def config(validation):
    return {
        'run_id': 'snap-test', 'docker_context': 'x', 'image': 'sha256:' + 'a' * 64, 'target_volume': 'v',
        'container_python': '/targets/py', 'container_repo': '/targets/repo', 'container_sail_binary': '/targets/sail',
        'runtime_source_sha': 'b' * 40, 'native_source_sha': 'b' * 40, 'harness_source_sha': 'b' * 40,
        'container_root': '/targets/root', 'host_output': '/tmp/out', 'seed': 1,
        'limits': {'cpus': 2, 'cpuset_cpus': '0-1', 'memory_gib': 4, 'outer_timeout_seconds': 100},
        'defaults': {'partitions': 2, 'threads': 2, 'worker_task_slots': 4, 'sail_pool_bytes': 2 << 30, 'native_quota': 1 << 30,
                     'tolerance': 1e-8, 'damping': 0.85, 'max_iterations': 10, 'timeout': 60, 'seed': 42, 'delta': 1.0},
        'datasets': {'cit': {'family': 'snap-edge-list', 'vertices': 4, 'edge_file': '/targets/inputs/cit.txt',
                             'edge_sha256': 'c' * 64, 'validation': validation, 'source': 0, 'directed': True, 'weight_policy': 'unit'}},
        'suites': [{'name': 's', 'mode': 'local', 'repetitions': 1, 'datasets': ['cit'],
                    'algorithms': ['pagerank', 'wcc', 'bfs'], 'variants': ['reference'], 'max_iterations': 10}],
    }


def test_ranking_on_a_traversal_fixture_needs_the_certificate_policy():
    assert 'snap-edge-list' in TRAVERSAL_FAMILIES
    with pytest.raises(ValueError, match='certificate'):
        validate_config(config('reference'))
    cells = plan_cells(config('certificate'))
    assert {c['algorithm'] for c in cells} == {'pagerank', 'wcc', 'bfs'}
    commands = {c['algorithm']: cell_command(config('certificate'), c) for c in cells}
    assert commands['pagerank'][-2:] == ['--ranking-validation', 'certificate']
    assert '--ranking-validation' not in commands['bfs']
    assert '--traversal-validation' in commands['bfs'] and '--expected-input-sha256' in commands['bfs']
    prepare = dataset_command(config('certificate'), 'cit')
    assert prepare[0].endswith('imported_snap_fixture.py')
    assert '--validation' not in prepare and '--edge-file' in prepare and '--directed' in prepare


def test_max_degree_source_uses_out_degree_and_the_cell_resolves_it(tmp_path):
    path, digest = source(tmp_path)
    # Dense out-degrees: 3 -> 2 (to 1 and 2), 1 -> 1, 2 -> 2 (3 twice), 0 -> 1; the lowest id among the tie wins.
    manifest = fixture.prepare(tmp_path / 'data', path, vertices=4, edge_sha256=digest, source='max-degree')
    traversal = manifest['traversal']
    assert traversal['source'] == 2 and traversal['source_original_id'] == 1324234 and traversal['source_degree'] == 2
    assert traversal['zero_degree_vertices'] == 0 and traversal['degree_counted'] == 'out-edges (src)'
    assert resolve_source('max-degree', traversal) == 2
    assert resolve_source(2, traversal) == 2
    with pytest.raises(ValueError, match='prepared for source 2'):
        resolve_source(0, traversal)
    explicit = fixture.prepare(tmp_path / 'explicit', path, vertices=4, edge_sha256=digest, source=0)
    assert explicit['traversal']['source_degree'] == 1
    with pytest.raises(ValueError, match='max-degree'):
        resolve_source('max-degree', explicit['traversal'])
    with pytest.raises(ValueError, match='source outside graph'):
        fixture.prepare(tmp_path / 'bad', path, vertices=4, edge_sha256=digest, source='hub')

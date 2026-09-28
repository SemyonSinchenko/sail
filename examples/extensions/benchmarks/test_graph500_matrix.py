"""Graph500 matrix contracts, including a real generated input when supplied."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from run_matrix import cell_command, dataset_command, plan_cells
from graph_cell import validate_dataset_identity


def configuration(scale=6, validation='reference'):
    config = json.loads(Path(__file__).with_name('traversal-matrix.example.json').read_text())
    config['container_repo'] = str(Path(__file__).resolve().parents[3])
    config['datasets'] = {'graph500': dict(family='graph500', vertices=2 ** scale,
        scale=scale, edge_factor=4, seed1=42, seed2=54, source=0,
        generator='/absolute/graph500-stream', validation=validation)}
    for suite in config['suites']:
        suite['datasets'] = ['graph500']
    return config


def test_all_paths_receive_same_explicit_graph500_contract():
    config = configuration(scale=24, validation='certificate')
    config['datasets']['graph500']['certificate_max_rounds'] = 200
    cells = plan_cells(config)
    assert len(cells) == 36
    for cell in cells:
        argv = cell_command(config, cell)
        assert '--no-directed' in argv
        assert argv[argv.index('--source') + 1] == '0'
        assert argv[argv.index('--traversal-validation') + 1] == 'certificate'
        assert argv[argv.index('--certificate-max-rounds') + 1] == '200'
    argv = dataset_command(config, 'graph500')
    assert argv[0].endswith('/graph500_fixture.py')
    assert '--vertices' not in argv and '--validation' not in argv
    assert '--certificate-max-rounds' not in argv and '--reference' not in argv
    assert argv[argv.index('--scale') + 1] == '24'


@pytest.mark.parametrize('change, diagnostic', [
    ({'vertices': 65}, r'2\^scale'),
    ({'validation': 'none'}, 'validation'),
    ({'source': 64}, 'source'),
    ({'generator': 'relative/binary'}, 'absolute'),
    ({'generator': '/tmp/../binary'}, 'absolute'),
    ({'expected_edge_sha256': 'not-a-digest'}, 'canonical'),
    ({'certificate_max_rounds': 0}, 'round cap'),
    ({'edge_factor': 0}, 'edge factor'),
])
def test_bad_graph500_contract_refused_before_preparation(change, diagnostic):
    config = configuration()
    config['datasets']['graph500'].update(change)
    with pytest.raises(ValueError, match=diagnostic):
        plan_cells(config)


def test_large_graph_cannot_request_bounded_oracle_or_pagerank_reference():
    config = configuration(scale=24)
    with pytest.raises(ValueError, match='restricted'):
        plan_cells(config)
    config = configuration()
    config['suites'][0]['algorithms'] = ['pagerank']
    with pytest.raises(ValueError, match='reference family'):
        plan_cells(config)


def test_directed_and_input_pin_propagate_to_preparer_and_cells():
    config = configuration()
    config['datasets']['graph500'].update(directed=True, expected_edge_sha256='a' * 64)
    argv = dataset_command(config, 'graph500')
    assert '--directed' in argv and '--reference' in argv
    assert argv[argv.index('--expected-edge-sha256') + 1] == 'a' * 64
    for cell in plan_cells(config):
        command = cell_command(config, cell)
        assert '--directed' in command
        assert command[command.index('--expected-graph500-sha256') + 1] == 'a' * 64


def test_prepared_input_cannot_bypass_matrix_identity_checks():
    manifest = dict(family='graph500', counts=dict(vertices=64),
                    canonical=dict(edges=dict(sha256='a' * 64)))
    validate_dataset_identity(manifest, family='graph500', vertices=64, graph500_sha256='a' * 64)
    for arguments, diagnostic in [
        ({'family': 'traversal'}, 'family'),
        ({'vertices': 128}, 'vertex count'),
        ({'graph500_sha256': 'b' * 64}, 'canonical edge'),
    ]:
        with pytest.raises(ValueError, match=diagnostic):
            validate_dataset_identity(manifest, **arguments)


def test_preparation_command_generates_real_graph500_fixture(tmp_path):
    generator = os.environ.get('GRAPH500_MATRIX_GENERATOR')
    if not generator:
        pytest.skip('set GRAPH500_MATRIX_GENERATOR to a pinned built upstream generator')
    config = configuration()
    config['container_root'] = str(tmp_path)
    config['datasets']['graph500']['generator'] = generator
    plan_cells(config)
    subprocess.run([sys.executable, *dataset_command(config, 'graph500')],
                   check=True, capture_output=True, text=True)
    manifest = json.loads((tmp_path / 'datasets/graph500/manifest.json').read_text())
    assert manifest['counts'] == {'vertices': 64, 'edges': 256}
    assert manifest['validation']['status'] == 'independent-reference-generated'
    assert manifest['traversal']['directed'] is False
    assert (tmp_path / 'datasets/graph500/reference.parquet').is_file()
    # An incompatible requested input identity must fail, retaining the failure.
    changed = copy.deepcopy(config)
    changed['container_root'] = str(tmp_path / 'wrong-pin')
    changed['datasets']['graph500']['expected_edge_sha256'] = 'a' * 64
    run = subprocess.run([sys.executable, *dataset_command(changed, 'graph500')],
                         capture_output=True, text=True)
    assert run.returncode != 0
    assert (tmp_path / 'wrong-pin/datasets/graph500/failure.json').is_file()
    assert not (tmp_path / 'wrong-pin/datasets/graph500/manifest.json').exists()

"""Preserve the full comparison matrix and distinguish failure outcomes."""
import json
from pathlib import Path
from types import SimpleNamespace

import run_matrix
from run_matrix import classify, plan_cells


def test_example_retains_every_algorithm_and_entry_path():
    config = json.loads(Path(__file__).with_name('matrix.example.json').read_text())
    cells = plan_cells(config)
    assert len(cells) == 150
    assert cells == plan_cells(config)
    tuples = {(c['engine'], c['algorithm'], c['variant']) for c in cells}
    assert len(tuples) == 12
    capped = [c for c in cells if c['expected_outcome'] == 'nonconverged']
    assert len(capped) == 2
    assert all(c['algorithm'] == 'wcc' and c['variant'] == 'reference' and
               c['max_iterations'] == 100 and c['dataset'] == 'chain-512' for c in capped)


def record(**kwargs):
    return dict(transport_errors=[], attach_returncode=0,
                inspect={'state': {'OOMKilled': False}}, **kwargs)


def test_worker_oom_overrides_surviving_controller_receipt():
    receipt = dict(harness_source_sha='a' * 40, outcome='passed',
                   cgroup_after={'memory.events': 'oom 1\noom_kill 1\n'})
    assert classify(record(), receipt, 'a' * 40) == 'oom'


def test_timeout_missing_receipt_and_source_mismatch_stay_distinct():
    assert classify(record(outer_timeout=True), None, 'a' * 40) == 'outer_timeout'
    assert classify(record(), None, 'a' * 40) == 'missing_receipt'
    assert classify(record(receipt_read_error='invalid JSON'), None, 'a' * 40) == 'invalid_receipt'
    assert classify(record(), dict(harness_source_sha='b' * 40, outcome='passed'), 'a' * 40) == 'source_mismatch'


def test_nonzero_exit_cannot_be_overridden_by_success_receipt():
    observation = record()
    observation['attach_returncode'] = 7
    receipt = dict(harness_source_sha='a' * 40, outcome='passed')
    assert classify(observation, receipt, 'a' * 40) == 'exit_receipt_mismatch'


def test_failed_required_copy_cannot_publish_apparent_pass(tmp_path, monkeypatch):
    config = json.loads(Path(__file__).with_name('matrix.example.json').read_text())
    monkeypatch.setattr(run_matrix, 'capture', lambda argv, **kwargs:
                        dict(returncode=1 if 'cp' in argv else 0, stdout='', stderr='copy failure'))
    monkeypatch.setattr(run_matrix, 'inspect_state', lambda *args:
                        {'state': {'OOMKilled': False, 'Running': False}})
    monkeypatch.setattr(run_matrix.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0))
    observation = run_matrix.run_container(config, 'test', ['cell'], tmp_path / 'cell',
                                          'image', 1, {'artifacts': '/output'})
    assert observation['copied']['artifacts']['returncode'] == 1
    assert observation['transport_errors'] == ['required artifact copy failed: artifacts']
    assert json.loads((tmp_path / 'cell/orchestration.json').read_text()) == json.loads(json.dumps(observation))
    receipt = dict(harness_source_sha='a' * 40, outcome='passed')
    assert classify(observation, receipt, 'a' * 40) == 'orchestration_error'
    observation['inspect']['state']['OOMKilled'] = True
    assert classify(observation, receipt, 'a' * 40) == 'oom'
    observation['inspect']['state']['OOMKilled'] = False
    observation['outer_timeout'] = True
    assert classify(observation, receipt, 'a' * 40) == 'outer_timeout'

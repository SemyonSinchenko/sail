"""Preserve the full comparison matrix and distinguish failure outcomes."""
import json
from pathlib import Path

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

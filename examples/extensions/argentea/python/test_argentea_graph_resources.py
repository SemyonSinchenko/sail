"""Adversarial checks of post-init memory-refusal evidence."""
from copy import deepcopy

import pytest
import argentea_graph_resource_evidence as evidence


@pytest.fixture(params=['sssp_delta_star','sssp_reference','wcc_star','wcc_reference'])
def case(monkeypatch, request):
    algorithm = request.param
    version = 5 if algorithm.startswith('sssp_') else 4
    phases = 28 if algorithm == 'wcc_star' else 10
    request = dict(algorithm=algorithm, version=version, max_rounds=3,
                   operation_id='op', snapshot_id='snapshot', generation=1, partitions=2)
    records = []
    for event in ('init', 'failure', 'close'):
        for partition in range(2):
            record = dict(request, event=event, protocol=version, partition=partition,
                          worker_id=partition+1, pid=100+partition, adjacency_id=1,
                          session_id='session', job_id=7)
            if event == 'failure':
                record.update(code='native_memory_budget', outcome='resource_refused',
                              initialized=True, memory_limit=100, live_bytes=40, peak_bytes=80)
            records.append(record)
    stages = [dict(session_id='session', job_id=7, stage=s, slot_group='worker-extension:op',
                   partitions=2, placement='Worker', mode='Pipelined') for s in range(phases)]
    tasks = [dict(session_id='session', job_id=7, stage=s, partition=p, attempt=0,
                  status='CANCELED', worker_id=p+1) for s in range(phases) for p in range(2)]
    check = dict(request=request, query_failed=True, cleanup_deferred=True, native_quota=100,
                 stages=stages, stored_tasks=tasks,
                 jobs=[dict(session_id='session', job_id=7, status='FAILED')])
    monkeypatch.setattr(evidence, 'parse_worker_tasks', lambda _: deepcopy(tasks))
    return records, check


def test_complete_refusal(case):
    records, check = case
    result = evidence.validate_memory_refusal('', records, check)
    assert result['initialized_owners'] == result['closed_owners'] == 2
    assert result['native_phases'] == (28 if check['request']['algorithm'] == 'wcc_star' else 10)
    assert len(result['memory_refusals']) == 2


@pytest.mark.parametrize('mutation', [
    'no_cause', 'pre_init', 'wrong_quota', 'changed_pid', 'missing_close',
    'duplicate_init', 'result', 'missing_stage', 'missing_task', 'retry',
    'active_task', 'successful_job', 'wrong_protocol', 'wrong_snapshot', 'returned_result',
])
def test_reject_incomplete_or_contradictory_evidence(case, mutation):
    records, check = case
    if mutation == 'no_cause':
        records[:] = [r for r in records if r['event'] != 'failure']
    elif mutation == 'pre_init':
        records[2]['initialized'] = False
    elif mutation == 'wrong_quota':
        check['native_quota'] += 1
    elif mutation == 'changed_pid':
        records[-1]['pid'] += 10
    elif mutation == 'missing_close':
        records.pop()
    elif mutation == 'duplicate_init':
        records.append(deepcopy(records[0]))
    elif mutation == 'result':
        records.append(dict(records[0], event='result'))
    elif mutation == 'missing_stage':
        check['stages'].pop()
    elif mutation == 'missing_task':
        check['stored_tasks'].pop()
    elif mutation == 'retry':
        check['stored_tasks'][0]['attempt'] = 1
    elif mutation == 'active_task':
        check['stored_tasks'][0]['status'] = 'RUNNING'
    elif mutation == 'successful_job':
        check['jobs'][0]['status'] = 'SUCCEEDED'
    elif mutation == 'wrong_protocol':
        records[-1]['protocol'] = 99
    elif mutation == 'wrong_snapshot':
        records[-1]['snapshot_id'] = 'other'
    elif mutation == 'returned_result':
        check['returned_result'] = {}
    with pytest.raises(AssertionError):
        evidence.validate_memory_refusal('', records, check)

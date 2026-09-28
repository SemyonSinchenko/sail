import copy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from argentea_evidence import parse_log, parse_worker_tasks, pagerank_reference, validate_audit, validate_rows


def evidence():
    request = dict(operation_id='op', snapshot_id='snapshot', generation=1, partitions=3, damping=.85)
    ids, edges = [0, 2], [(0, 2)]
    rows = [dict(id=node, pagerank=rank, owner=node % 3, worker_id=(node % 3) % 2 + 10,
                 pid=(node % 3) % 2 + 100, adjacency_id=node % 3 + 1000, round=1)
            for node, rank in pagerank_reference(ids, edges, 2, .85).items()]
    receipts = []
    for owner in range(3):
        for event, round_number in [('init',0),('emit',0),('consume',0),('emit',1),('consume',1),('result',1),('close',1)]:
            receipts.append(dict(**{key: request[key] for key in ('operation_id','snapshot_id','generation')},
                event=event, round=round_number, partition=owner, worker_id=owner % 2+10,
                pid=owner % 2+100, adjacency_id=owner+1000, job_id=7, session_id='s'))
    endpoints = [dict(worker_id=worker+10, host=f'host-{name}') for worker, name in enumerate(('a', 'b'))]
    supervisors = [dict(event='sail_remote_started', worker_id=str(worker+10), pid=worker+100,
                        hostname=f'host-{name}') for worker, name in enumerate(('a', 'b'))]
    stages = [dict(session_id='s', job_id=7, stage=i, partitions=3, placement='Worker',
                   slot_group='worker-extension:2', mode='Pipelined') for i in (2, 3, 4)]
    # Ordinary input scans and sinks are allowed different widths and groups.
    stages += [dict(session_id='s', job_id=7, stage=i, partitions=width, placement=placement,
                    slot_group='default', mode='Pipelined')
               for i, width, placement in [(0, 1, 'Worker'), (1, 1, 'Worker'), (5, 3, 'Worker'), (6, 1, 'Driver')]]
    tasks = [dict(job_id=7, stage=stage, partition=owner, worker_id=owner % 2+10, attempt=0, status=status)
             for stage in (2, 3, 4) for owner in range(3) for status in ('RUNNING', 'SUCCEEDED')]
    # Different ordinary-task assignments cannot prove or disprove native affinity.
    tasks += [dict(job_id=7, stage=5, partition=0, worker_id=11, attempt=0, status='SUCCEEDED')]
    return request, ids, edges, rows, receipts, endpoints, supervisors, stages, tasks


def test_full_vector_normalization_and_empty_partition_audit():
    request, ids, edges, rows, receipts, endpoints, supervisors, stages, tasks = evidence()
    assert validate_rows(rows, ids, edges, request, 2)['converged'] is None
    result = validate_audit(receipts, rows, request, 2, worker_endpoints=endpoints,
                            supervisors=supervisors, required_hosts=['host-a', 'host-b'], stages=stages, task_statuses=tasks)
    assert result['owners'][1]['adjacency_id'] == 1001  # Empty owner still participated.
    assert result['hosts'] == ['host-a', 'host-b']
    assert len(result['stages']) == 3 and result['native_phases'] == 3
    assert [stage['partitions'] for stage in result['ordinary_stages']] == [1, 1, 3, 1]
    assert len(result['native_task_statuses']) == 18


@pytest.mark.parametrize('fault', ['missing_empty', 'rebuild', 'new_job', 'replay', 'missing_close', 'wrong_host',
                                  'wrong_width', 'missing_stage', 'extra_stage', 'different_group', 'missing_group',
                                  'wrong_placement', 'wrong_mode', 'wrong_session', 'duplicate_stage'])
def test_receipts_reject_missing_or_misplaced_execution(fault):
    request, ids, edges, rows, receipts, endpoints, supervisors, stages, tasks = evidence()
    if fault == 'missing_empty': receipts = [r for r in receipts if r['partition'] != 1]
    if fault == 'rebuild': receipts[-1]['adjacency_id'] = 9999
    if fault == 'new_job': receipts[-1]['job_id'] = 8
    if fault == 'replay': receipts.append(copy.deepcopy(receipts[1]))
    if fault == 'missing_close': receipts.pop()
    if fault == 'wrong_host': supervisors[-1]['hostname'] = 'host-a'
    if fault == 'wrong_width': stages[1]['partitions'] = 1
    if fault == 'missing_stage': stages.pop(1)
    if fault == 'extra_stage': stages.append(dict(stages[0], stage=8))
    if fault == 'different_group': stages[1]['slot_group'] = 'worker-extension:3'
    if fault == 'missing_group': del stages[1]['slot_group']
    if fault == 'wrong_placement': stages[1]['placement'] = 'Driver'
    if fault == 'wrong_mode': stages[1]['mode'] = 'Blocking'
    if fault == 'wrong_session': stages[1]['session_id'] = 'another-session'
    if fault == 'duplicate_stage': stages.append(copy.deepcopy(stages[0]))
    with pytest.raises(AssertionError):
        validate_audit(receipts, rows, request, 2, worker_endpoints=endpoints,
                       supervisors=supervisors, required_hosts=['host-a', 'host-b'], stages=stages, task_statuses=tasks)


@pytest.mark.parametrize('fault', ['missing_empty_owner', 'missing_phase', 'wrong_worker', 'wrong_partition',
                                  'retry', 'failed_attempt_then_success', 'missing_completion', 'duplicate_completion',
                                  'different_job', 'wrong_stage'])
def test_each_native_phase_and_owner_needs_exact_attempt_zero_success(fault):
    request, _, _, rows, receipts, endpoints, supervisors, stages, tasks = evidence()
    if fault == 'missing_empty_owner': tasks = [task for task in tasks if task['partition'] != 1]
    if fault == 'missing_phase': tasks = [task for task in tasks if task['stage'] != 3]
    if fault == 'wrong_worker': tasks[1]['worker_id'] = 11  # Still a worker in the correct worker set.
    if fault == 'wrong_partition': tasks[1]['partition'] = 3
    if fault == 'retry': tasks[0]['attempt'] = 1  # A hidden attempted retry fails despite attempt-0 success.
    if fault == 'failed_attempt_then_success': tasks.insert(1, dict(tasks[0], status='FAILED'))
    if fault == 'missing_completion': tasks.pop(1)
    if fault == 'duplicate_completion': tasks.append(copy.deepcopy(tasks[1]))
    if fault == 'different_job': tasks[1]['job_id'] = 8
    if fault == 'wrong_stage': tasks[1]['stage'] = 5
    with pytest.raises(AssertionError):
        validate_audit(receipts, rows, request, 2, worker_endpoints=endpoints,
                       supervisors=supervisors, stages=stages, task_statuses=tasks)


def test_partial_or_wrong_rank_is_never_a_pass():
    request, ids, edges, rows, *_ = evidence()
    with pytest.raises(AssertionError): validate_rows(rows[:-1], ids, edges, request, 2)
    rows[0]['pagerank'] += .01
    with pytest.raises(AssertionError): validate_rows(rows, ids, edges, request, 2)


def test_log_parser_retains_json_and_rejects_truncated_native_receipt():
    assert parse_log('prefix ARGENTEA_RECEIPT {"event":"init"}\n')[0] == [dict(event='init')]
    with pytest.raises(ValueError): parse_log('ARGENTEA_RECEIPT {broken\n')


def test_task_parser_preserves_failures_and_retries_for_the_gate():
    log = '\n'.join(f'prefix worker_task_status worker_id=10 job_id=7 stage=2 partition=0 attempt={attempt} status={status}'
                    for attempt, status in [(0, 'RUNNING'), (0, 'FAILED'), (1, 'SUCCEEDED')])
    assert [(task['attempt'], task['status']) for task in parse_worker_tasks(log)] == [(0, 'RUNNING'), (0, 'FAILED'), (1, 'SUCCEEDED')]
    with pytest.raises(AssertionError, match='malformed'):
        parse_worker_tasks('worker_task_status worker_id=10 job_id=broken')

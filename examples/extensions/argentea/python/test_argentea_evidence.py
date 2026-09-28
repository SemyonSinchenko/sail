import copy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from argentea_evidence import parse_log, pagerank_reference, validate_audit, validate_rows


def evidence():
    request = dict(operation_id='op', snapshot_id='snapshot', generation=1, partitions=3, damping=.85)
    ids, edges = [0, 2], [(0, 2)]
    rows = [dict(id=node, pagerank=rank, owner=node % 3, worker_id=node % 3 + 10,
                 pid=node % 3 + 100, adjacency_id=node % 3 + 1000, round=1)
            for node, rank in pagerank_reference(ids, edges, 2, .85).items()]
    receipts = []
    for owner in range(3):
        for event, round_number in [('init',0),('emit',0),('consume',0),('emit',1),('consume',1),('result',1),('close',1)]:
            receipts.append(dict(**{key: request[key] for key in ('operation_id','snapshot_id','generation')},
                event=event, round=round_number, partition=owner, worker_id=owner+10,
                pid=owner+100, adjacency_id=owner+1000, job_id=7, session_id='s'))
    endpoints = [dict(worker_id=owner+10, host='host-a' if owner < 2 else 'host-b') for owner in range(3)]
    supervisors = [dict(event='sail_remote_started', worker_id=str(owner+10), pid=owner+100,
                        hostname='host-a' if owner < 2 else 'host-b') for owner in range(3)]
    stages = [dict(job_id=7, stage=i, partitions=3, placement='Worker') for i in range(4)]
    return request, ids, edges, rows, receipts, endpoints, supervisors, stages


def test_full_vector_normalization_and_empty_partition_audit():
    request, ids, edges, rows, receipts, endpoints, supervisors, stages = evidence()
    assert validate_rows(rows, ids, edges, request, 2)['converged'] is None
    result = validate_audit(receipts, rows, request, 2, worker_endpoints=endpoints,
                            supervisors=supervisors, required_hosts=['host-a', 'host-b'], stages=stages)
    assert result['owners'][1]['adjacency_id'] == 1001  # Empty owner still participated.
    assert result['hosts'] == ['host-a', 'host-b']


@pytest.mark.parametrize('fault', ['missing_empty', 'rebuild', 'new_job', 'replay', 'missing_close', 'wrong_host', 'scalar_stage'])
def test_receipts_reject_missing_or_misplaced_execution(fault):
    request, ids, edges, rows, receipts, endpoints, supervisors, stages = evidence()
    if fault == 'missing_empty': receipts = [r for r in receipts if r['partition'] != 1]
    if fault == 'rebuild': receipts[-1]['adjacency_id'] = 9999
    if fault == 'new_job': receipts[-1]['job_id'] = 8
    if fault == 'replay': receipts.append(copy.deepcopy(receipts[1]))
    if fault == 'missing_close': receipts.pop()
    if fault == 'wrong_host': supervisors[-1]['hostname'] = 'host-a'
    if fault == 'scalar_stage': stages[1]['partitions'] = 1
    with pytest.raises(AssertionError):
        validate_audit(receipts, rows, request, 2, worker_endpoints=endpoints,
                       supervisors=supervisors, required_hosts=['host-a', 'host-b'], stages=stages)


def test_partial_or_wrong_rank_is_never_a_pass():
    request, ids, edges, rows, *_ = evidence()
    with pytest.raises(AssertionError): validate_rows(rows[:-1], ids, edges, request, 2)
    rows[0]['pagerank'] += .01
    with pytest.raises(AssertionError): validate_rows(rows, ids, edges, request, 2)


def test_log_parser_retains_json_and_rejects_truncated_native_receipt():
    assert parse_log('prefix ARGENTEA_RECEIPT {"event":"init"}\n')[0] == [dict(event='init')]
    with pytest.raises(ValueError): parse_log('ARGENTEA_RECEIPT {broken\n')

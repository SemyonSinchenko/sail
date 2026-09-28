"""Fail-closed audit of a deliberately failed native job, not a result audit."""
from collections import Counter

from argentea_evidence import parse_worker_tasks


def validate_fault(log, records, check):
    request, case = check['request'], check['case']
    selected = [r for r in records if r.get('operation_id') == request['operation_id']]
    assert check['query_failed'] is True and 'rows' not in check, 'fault returned a result'
    assert check['cleanup_deferred'] is True, 'failed write lost session cleanup ownership'
    assert not any(r['event'] in ('result', 'failure') for r in selected), 'fault raced a completed result or another typed failure'
    native = [s for s in check['stages'] if s['slot_group'].startswith('worker-extension:')]
    assert len({(s['session_id'], s['job_id']) for s in native}) == 1, 'fault spanned or retried native jobs'
    session, job = native[0]['session_id'], native[0]['job_id']
    assert len(native) == len({s['stage'] for s in native}) == 4 * request['max_pushes'] + 4
    assert len({s['slot_group'] for s in native}) == 1
    assert all(s['partitions'] == 2 and s['placement'] == 'Worker' and s['mode'] == 'Pipelined' for s in native)
    ids = {s['stage'] for s in native}
    tasks = [t for t in parse_worker_tasks(log) if t['job_id'] == job and t['stage'] in ids]
    stored = [t for t in check['stored_tasks'] if t['job_id'] == job and t['stage'] in ids]
    assert tasks and stored and all(t['attempt'] == 0 for t in [*tasks, *stored]), 'native tasks missing or retried'
    assert all(t['status'] in ('SUCCEEDED', 'FAILED', 'CANCELED') for t in stored), 'native tasks remain active'
    assert any(t['status'] in ('FAILED', 'CANCELED') for t in stored), 'failed query has no failed/canceled native task'
    assert {(t['stage'], t['partition']) for t in stored} == {(s, p) for s in ids for p in range(2)}
    assert len(stored) == 2 * len(ids), 'duplicate native task attempt'
    jobs = [j for j in check['jobs'] if j['session_id'] == session and j['job_id'] == job]
    assert len(jobs) == 1 and jobs[0]['status'] in ('FAILED', 'CANCELED'), 'whole native job did not fail/cancel'
    if case == 'quota':
        assert not selected, 'bind-admission refusal unexpectedly initialized native state'
        assert 'procedure memory budget exceeded (limit 1)' in check['error'], 'quota cause was lost behind peer cancellation'
        assert check['native_quota'] == 1
        owners = {}
    else:
        assert check['injection']['window']['both_workers_stopped'] is True
        assert 'controller_error' not in check['injection']
        assert Counter(r['partition'] for r in selected if r['event'] == 'init') == Counter(range(2))
        owners = {r['partition']: r for r in selected if r['event'] == 'init'}
        assert len({r['pid'] for r in owners.values()}) == 2
        for record in selected:
            owner = owners[record['partition']]
            assert all(record[k] == owner[k] for k in ('worker_id', 'pid', 'adjacency_id', 'session_id', 'job_id'))
            assert record['session_id'] == session and record['job_id'] == job
            assert record['snapshot_id'] == request['snapshot_id'] and record['generation'] == request['generation']
        assert all(t['partition'] in owners and t['worker_id'] == owners[t['partition']]['worker_id'] for t in tasks)
        killed = check['injection'].get('killed_worker', {}).get('pid')
        expected_closed = Counter(p for p, r in owners.items() if r['pid'] != killed)
        assert Counter(r['partition'] for r in selected if r['event'] == 'close') == expected_closed, 'surviving owners did not close exactly once'
        if case == 'cancel':
            assert check['error_type'] == 'GraphCancelledError'
            assert check['interrupt_operation_ids'], 'cancel RPC did not target a registered operation'
            assert killed is None
        else:
            assert killed in {r['pid'] for r in owners.values()}
            assert any(s['pid'] == killed and s['signal'] == 'SIGKILL' for s in check['injection']['signals'])
            assert 'automatic retry disabled' in check['error'], 'worker loss lacks whole-job no-retry refusal'
    return dict(session_id=session, job_id=job, native_phases=len(native), native_task_count=len(stored),
                first_call_failed=True, whole_job_failed=True, no_result=True, no_native_retry=True,
                native_receipts=selected, native_tasks=stored, native_task_statuses=tasks,
                initialized_owners=len(owners), closed_surviving_owners=sum(r['event'] == 'close' for r in selected),
                boundary='pre-init bind admission refusal' if case == 'quota' else 'post-init native job fault with supervised POSIX barrier')

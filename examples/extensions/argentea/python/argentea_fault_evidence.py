"""Fail-closed audit of a deliberately failed native job, not a result audit."""
from collections import Counter

from argentea_evidence import parse_worker_tasks
from argentea_fault_control import select_victim, validate_window


def native_phase_count(request):
    algorithm=request['algorithm']
    if algorithm=='pagerank_delta':
        assert request['version']==2
        pushes=request['max_pushes']
        assert type(pushes) is int and 0<=pushes<=7
        return 4*pushes+4
    if algorithm in ('wcc_reference','wcc_star'):
        assert request['version']==4
        from argentea_wcc_client import phase_count
        return phase_count(request['max_rounds'],algorithm.removeprefix('wcc_'))
    if algorithm in ('sssp_reference','sssp_delta_star'):
        assert request['version']==5
        from argentea_sssp_client import phase_count
        return phase_count(request['max_rounds'],algorithm.removeprefix('sssp_'))
    raise AssertionError('unsupported fault protocol')


def output_task_placement(log, stages, workers, session, job):
    # JobGraph::try_new appends its final output stage after all input stages.
    # Read placement independently of the selected owner; do not assume that
    # choosing an owner determines the consumer error's arrival order.
    selected = [s for s in stages if s['session_id'] == session and s['job_id'] == job]
    final = max(selected, key=lambda s: s['stage'])
    assert final['placement'] == 'Worker' and final['partitions'] == 1 and not final['slot_group']
    tasks = [t for t in parse_worker_tasks(log) if t['job_id'] == job and t['stage'] == final['stage']]
    assert tasks and all(t['attempt'] == 0 and t['partition'] == 0 for t in tasks)
    ids = {t['worker_id'] for t in tasks}
    assert len(ids) == 1, 'final output task changed worker'
    matches = [w for w in workers if w['worker_id'] in ids and w['session_id'] == session]
    assert len(matches) == 1, 'final output task was not supervised'
    return dict(session_id=session, job_id=job, stage=final['stage'], partition=0,
                attempt=0, worker_id=matches[0]['worker_id'], pid=matches[0]['pid'])


def worker_loss_path(check, owners, session, job):
    """Classify the first RPC only after independent worker/job evidence holds."""
    assert [j for j in check['jobs'] if j['session_id'] == session and j['job_id'] == job] == [
        dict(session_id=session, job_id=job, status='FAILED')], 'worker loss did not fail the job'
    victim = check['injection']['killed_worker']
    assert victim in check['supervised_workers'], 'killed worker is not supervised'
    own = [r for r in owners.values() if r['pid'] == victim['pid']]
    assert len(own) == 1 and all(own[0][k] == victim[k] for k in ('worker_id', 'pid', 'session_id')), \
        'killed worker differs from native owner'
    window = check['injection']['window']
    validate_window(window['native_receipts'], check['request'], check['supervised_workers'],
                    {int(k): v for k, v in window['process_states'].items()}, victim['driver_pid'])
    initial = [r for r in window['native_receipts'] if r['event'] == 'init']
    assert sorted(initial, key=lambda r: r['partition']) == sorted(owners.values(), key=lambda r: r['partition']), \
        'held window differs from initialized owners'
    assert check['requested_victim_owner'] == check['injection']['requested_native_owner'], 'requested victim changed'
    expected_victim, owner = select_victim(window, check['supervised_workers'], check['injection']['requested_native_owner'])
    assert victim == expected_victim and owner == check['injection']['selected_native_owner'], 'victim selection changed'
    job_tasks = [t for t in check['stored_tasks'] if t['session_id'] == session and t['job_id'] == job]
    assert job_tasks and all(t['attempt'] == 0 and t['status'] in ('SUCCEEDED', 'FAILED', 'CANCELED')
                             for t in job_tasks), 'job tasks remain active or retried'
    assert check['connect_max_retries'] == 0, 'client retry policy is not disabled'
    # forward_job_output forwards an observed transport error unchanged. Only
    # refresh_job's FailJobOutput cause adds the retry-disabled context; either
    # error can reach the caller first. The task/owner audit proves no replay.
    if check['error_type'] == 'AnalysisException' and check['error'].startswith(
            'worker extension job failed; automatic retry disabled:'):
        return 'scheduler_failure'
    if (check['error_type'] == 'SparkRuntimeException' and
            check['error'] == 'h2 protocol error: error reading a body from connection'):
        return 'bare_h2_transport'
    raise AssertionError('worker loss has an unrecognized first RPC error')


def validate_fault(log, records, check):
    first_error_path = None
    output_placement = None
    request, case = check['request'], check['case']
    selected = [r for r in records if r.get('operation_id') == request['operation_id']]
    assert all(r['protocol']==request['version'] and r['algorithm']==request['algorithm'] for r in selected), 'fault protocol changed'
    assert check['query_failed'] is True and 'rows' not in check, 'fault returned a result'
    assert check['cleanup_deferred'] is True, 'failed write lost session cleanup ownership'
    assert not any(r['event'] in ('result', 'failure') for r in selected), 'fault raced a completed result or another typed failure'
    native = [s for s in check['stages'] if s['slot_group'].startswith('worker-extension:')]
    assert len({(s['session_id'], s['job_id']) for s in native}) == 1, 'fault spanned or retried native jobs'
    session, job = native[0]['session_id'], native[0]['job_id']
    assert len(native) == len({s['stage'] for s in native}) == native_phase_count(request)
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
            first_error_path = worker_loss_path(check, owners, session, job)
            output_placement = output_task_placement(log, check['stages'], check['supervised_workers'], session, job)
    return dict(session_id=session, job_id=job, native_phases=len(native), native_task_count=len(stored),
                first_call_failed=True, whole_job_failed=True, no_result=True, no_native_retry=True,
                native_receipts=selected, native_tasks=stored, native_task_statuses=tasks,
                initialized_owners=len(owners), closed_surviving_owners=sum(r['event'] == 'close' for r in selected),
                first_error_path=first_error_path,
                output_task_placement=output_placement,
                boundary='pre-init bind admission refusal' if case == 'quota' else 'post-init native job fault with supervised POSIX barrier')

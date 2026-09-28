"""Fail-closed evidence checks for same-session Argentea quota reuse."""
from collections import Counter
import os
import time

from argentea_evidence import parse_log, parse_worker_tasks


def operation_records(log, request):
    records, _ = parse_log(log)
    return [r for r in records if r.get('operation_id') == request['operation_id']]


def read_complete_log(log_path):
    # Polling may catch the final line mid-write. Preserve original bytes and
    # defer only that unfinished line; a completed malformed receipt still fails.
    text = log_path.read_text()
    return text if not text or text.endswith('\n') else text.rpartition('\n')[0]


def wait_closed(log_path, request, *, timeout=30.0):
    """Wait for published init owners to close; never infer close from RPC return."""
    deadline = time.monotonic()+timeout
    while True:
        records = operation_records(read_complete_log(log_path), request)
        initialized = Counter(r['partition'] for r in records if r['event']=='init')
        closed = Counter(r['partition'] for r in records if r['event']=='close')
        expected = Counter(range(request['partitions']))
        if initialized == closed == expected:
            return records
        if any(n > 1 for n in initialized.values()) or any(n > 1 for n in closed.values()):
            raise AssertionError('operation initialized or closed an owner more than once')
        if time.monotonic() >= deadline:
            raise AssertionError(f'operation did not initialize/close every owner within {timeout}s: '
                                 f'init={dict(initialized)}, close={dict(closed)}')
        time.sleep(0.05)


def live_pids(pids):
    result = []
    for pid in sorted(set(pids)):
        try:
            os.kill(pid,0)
        except ProcessLookupError:
            alive = False
        except PermissionError:
            alive = True  # Permission is not proof of absence.
        else:
            alive = True
        result.append(dict(pid=pid,alive=alive))
    return result


def validate_failed_operation(log, request, stages, *, expected_pids, expected_session, iterations):
    """The expected runtime failure must be post-init and have no native retry."""
    records = operation_records(log,request)
    assert records, 'expected runtime failure has no native receipts'
    jobs = {(r['session_id'],r['job_id']) for r in records}
    assert len(jobs)==1, 'failed operation spanned/retried multiple native jobs'
    session,job = next(iter(jobs))
    assert session==expected_session, 'failure ran in a different session'
    assert {r['pid'] for r in records}==set(expected_pids), 'failed operation replaced a worker process'
    assert all(r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in records)
    for partition in range(request['partitions']):
        owned = [r for r in records if r['partition']==partition]
        assert Counter(r['event'] for r in owned)['init']==1, 'failure did not follow CSR initialization on every owner'
        assert Counter(r['event'] for r in owned)['close']==1, 'failed owner did not close once'
        assert not any(r['event']=='result' for r in owned), 'failed operation emitted a certified result receipt'
        assert len({(r['worker_id'],r['pid'],r['adjacency_id']) for r in owned})==1, 'failed owner changed identity'
    assert {r['partition'] for r in records}==set(range(request['partitions']))
    native = [s for s in stages if s['session_id']==session and s['job_id']==job
              and s['slot_group'].startswith('worker-extension:')]
    assert len(native)==iterations+1, 'failed operation native stage inventory is incomplete'
    assert len({s['slot_group'] for s in native})==1
    assert all(s['partitions']==request['partitions'] and s['placement']=='Worker' and s['mode']=='Pipelined' for s in native)
    native_ids = {s['stage'] for s in native}
    assert len(native_ids)==iterations+1, 'duplicate native stage inventory'
    tasks = [t for t in parse_worker_tasks(log) if t['job_id']==job and t['stage'] in native_ids]
    assert tasks and all(t['attempt']==0 for t in tasks), 'failed native task was retried or has no task evidence'
    assert any(t['status']=='FAILED' for t in tasks), 'expected error lacks a failed native task receipt'
    assert all(0<=t['partition']<request['partitions'] for t in tasks)
    owners = {r['partition']:r for r in records if r['event']=='init'}
    assert all(t['worker_id']==owners[t['partition']]['worker_id'] for t in tasks)
    return dict(session_id=session,job_id=job,native_pids=sorted(expected_pids),
                native_workers=sorted({r['worker_id'] for r in records}),
                initialized_owners=len(owners),closed_owners=len(owners),native_receipts=records,
                stages=native,task_statuses=tasks,no_native_retry=True,post_csr_runtime_failure=True)

"""Audit post-initialization WCC/SSSP memory refusal independently of RPC text."""
from collections import Counter

from argentea_evidence import parse_worker_tasks
from argentea_fault_evidence import native_phase_count


def validate_memory_refusal(log, records, check):
    request = check['request']
    assert request['algorithm'] in ('wcc_reference', 'wcc_star', 'sssp_reference', 'sssp_delta_star')
    assert check['query_failed'] is True and 'returned_result' not in check and 'rows' not in check
    assert check['cleanup_deferred'] is True
    selected = [r for r in records if r.get('operation_id') == request['operation_id']]
    expected = Counter(range(request['partitions']))
    assert Counter(r['partition'] for r in selected if r['event'] == 'init') == expected
    assert Counter(r['partition'] for r in selected if r['event'] == 'close') == expected
    owners = {r['partition']: r for r in selected if r['event'] == 'init'}
    assert len({r['worker_id'] for r in owners.values()}) == 2
    identities = {(r['session_id'], r['job_id']) for r in selected}
    assert len(identities) == 1
    session, job = next(iter(identities))
    for r in selected:
        assert r['partition'] in owners
        owner = owners[r['partition']]
        assert owner['adjacency_id'] > 0
        assert all(r[k] == owner[k] for k in ('worker_id', 'pid', 'adjacency_id', 'session_id', 'job_id'))
        assert all(r[k] == request[k] for k in ('algorithm', 'snapshot_id', 'generation'))
        assert r['protocol'] == request['version']
        assert r['event'] != 'result'
    failures = [r for r in selected if r['event'] == 'failure']
    assert failures, 'RPC cancellation alone does not establish a memory refusal'
    for r in failures:
        assert r['code'] == 'native_memory_budget' and r['outcome'] == 'resource_refused'
        assert r['initialized'] is True and r['memory_limit'] == check['native_quota']
        assert 0 <= r['live_bytes'] <= r['peak_bytes'] <= r['memory_limit']
        assert selected.index(owners[r['partition']]) < selected.index(r)
    native = [s for s in check['stages'] if s['session_id'] == session and s['job_id'] == job
              and s['slot_group'].startswith('worker-extension:')]
    phases = native_phase_count(request)
    ids = {s['stage'] for s in native}
    assert len(native) == len(ids) == phases
    assert len({s['slot_group'] for s in native}) == 1
    assert all(s['partitions'] == request['partitions'] and s['placement'] == 'Worker'
               and s['mode'] == 'Pipelined' for s in native)
    stored = [t for t in check['stored_tasks'] if t['session_id'] == session and t['job_id'] == job]
    assert stored and all(t['attempt'] == 0 and t['status'] in ('SUCCEEDED', 'FAILED', 'CANCELED') for t in stored)
    native_tasks = [t for t in stored if t['stage'] in ids]
    assert len(native_tasks) == phases * request['partitions']
    assert {(t['stage'], t['partition']) for t in native_tasks} == {(s, p) for s in ids for p in owners}
    assert any(t['status'] in ('FAILED', 'CANCELED') for t in native_tasks)
    assert [j for j in check['jobs'] if j['session_id'] == session and j['job_id'] == job] == [
        dict(session_id=session, job_id=job, status='FAILED')]
    tasks = [t for t in parse_worker_tasks(log) if t['job_id'] == job and t['stage'] in ids]
    assert tasks and all(t['attempt'] == 0 and t['partition'] in owners
                         and t['worker_id'] == owners[t['partition']]['worker_id'] for t in tasks)
    return dict(session_id=session, job_id=job, native_pids=sorted({r['pid'] for r in owners.values()}),
                native_workers=sorted({r['worker_id'] for r in owners.values()}),
                initialized_owners=len(owners), closed_owners=len(owners), native_phases=phases,
                memory_refusals=failures, no_native_retry=True,
                boundary='post-init native accounting refusal; no RSS or pool-reuse claim')

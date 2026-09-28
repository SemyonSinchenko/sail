"""Bind resource-reuse evidence to live supervised workers on both hosts."""
from argentea_remote_fault_control import control_worker


def validate_remote_owners(records, request, workers):
    selected = [r for r in records if r.get('operation_id') == request['operation_id']]
    bindings = {(w['worker_id'], w['pid']): w for w in workers}
    if len(bindings) != 2 or len({w['host'] for w in workers}) != 2:
        raise ValueError('resource qualification requires two physical supervised workers')
    initialized = [r for r in selected if r['event'] == 'init']
    if len(initialized) != request['partitions'] or {r['partition'] for r in initialized} != set(range(request['partitions'])):
        raise ValueError('incomplete resource owner initialization')
    if {(r['worker_id'], r['pid']) for r in initialized} != set(bindings):
        raise ValueError('resource owners do not cover the supervised workers')
    if len({(r['session_id'], r['job_id']) for r in selected}) != 1:
        raise ValueError('resource operation spans multiple jobs')
    for record in selected:
        worker = bindings.get((record['worker_id'], record['pid']))
        if worker is None or record['session_id'] != worker['session_id']:
            raise ValueError('resource event is not bound to the supervised session')
    return [dict(partition=r['partition'], host=bindings[(r['worker_id'], r['pid'])]['host'],
                 worker_id=r['worker_id'], pid=r['pid']) for r in initialized]


def live_remote_workers(workers):
    states = []
    for worker in workers:
        state = control_worker(worker, {'state': True})
        if not state.get('alive') or any(c in state.get('status', '') for c in 'ZT'):
            raise ValueError('resource worker exited or stopped before reuse')
        if not state.get('status') or state.get('pgid') != worker['pid']:
            raise ValueError('resource worker is not the supervised group leader')
        states.append(dict(state, host=worker['host'], worker_id=worker['worker_id']))
    return states

"""Independent small-vector and strict worker-protocol checks for Argentea v2."""
from collections import Counter, defaultdict
import math

from argentea_evidence import validate_native_stages
from argentea_host_evidence import validate_host_graph

ROUND_OFF = 1e-12  # Qualification fixtures are small; not an arbitrary-size guarantee.


def transition(ids, edges, ranks, damping):
    adjacency = {node:[] for node in ids}
    for source,target in edges:
        assert source in adjacency and target in adjacency
        adjacency[source].append(target)
    dangling = math.fsum(ranks[node] for node in ids if not adjacency[node])
    incoming = {node:[] for node in ids}
    for source,targets in adjacency.items():
        for target in targets:
            incoming[target].append(ranks[source]/len(targets))
    return {node:(1-damping)/len(ids)+damping*(math.fsum(incoming[node])+dangling/len(ids)) for node in ids}


def residual(ids, edges, ranks, damping):
    mapped = transition(ids,edges,ranks,damping)
    return math.fsum(abs(mapped[node]-ranks[node]) for node in ids)


def reference(ids, edges, damping):
    ranks = {node:1/len(ids) for node in ids}
    for _ in range(10_000):
        ranks = transition(ids,edges,ranks,damping)
        mass = math.fsum(ranks.values())
        ranks = {node:value/mass for node,value in ranks.items()}
        certificate = residual(ids,edges,ranks,damping)
        if certificate<=1e-15:
            return ranks,certificate
    raise AssertionError('independent power reference did not converge')


def validate_rows(rows, ids, edges, request):
    assert len(ids)==len(set(ids)) and ids
    actual = {}
    metadata = set()
    for row in rows:
        node = row['id']
        assert node not in actual, 'duplicate output vertex'
        assert math.isfinite(row['pagerank']) and row['pagerank']>=0, 'invalid rank'
        assert row['owner']==node%request['partitions'], 'wrong vertex owner'
        assert row['phase']==2*request['max_pushes']+1 and row['converged']==1
        metadata.add(tuple(row[k] for k in ('pushes','certificate_passes','residual_l1','stationary_error_bound')))
        actual[node] = row['pagerank']
    assert actual.keys()==set(ids), 'missing or unexpected output vertex'
    assert len(metadata)==1, 'inconsistent global result diagnostics'
    pushes,certificates,reported,bound = next(iter(metadata))
    assert 0<=pushes<=request['max_pushes'] and 1<=certificates<=pushes+1
    assert math.isfinite(reported) and 0<=reported<=request['tolerance']
    assert math.isfinite(bound) and math.isclose(bound,reported/(1-request['damping']),rel_tol=1e-12,abs_tol=0)
    mass = math.fsum(actual.values())
    assert abs(mass-1)<=ROUND_OFF, 'output is not probability normalized'
    normalized = {node:value/mass for node,value in actual.items()}
    measured = residual(ids,edges,normalized,request['damping'])
    assert measured<=request['tolerance']+ROUND_OFF, 'independent residual exceeds tolerance'
    assert abs(measured-reported)<=ROUND_OFF, 'reported certificate differs from independently recomputed residual'
    target,reference_residual = reference(ids,edges,request['damping'])
    error = math.fsum(abs(normalized[node]-target[node]) for node in ids)
    error_bound = (measured+reference_residual)/(1-request['damping'])+ROUND_OFF
    assert error<=error_bound, 'full-vector stationary error exceeds residual bound'
    return dict(vertices=len(ids),pushes=pushes,certificate_passes=certificates,converged=True,
                normalized_mass=mass,true_residual=measured,reported_residual=reported,
                stationary_error=error,stationary_error_bound=error_bound,
                independent_reference_residual=reference_residual,roundoff_slack=ROUND_OFF,
                reference='independent full-power normalized vector and full transition')


def scoped_records(records, request):
    selected = [r for r in records if r.get('operation_id')==request['operation_id']]
    assert selected, 'no v2 native receipts'
    assert all(r.get('protocol')==2 and r.get('algorithm')=='pagerank_delta' for r in selected), 'wrong protocol or algorithm'
    assert all(r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in selected)
    assert len({(r['session_id'],r['job_id']) for r in selected})==1, 'native v2 operation spanned jobs'
    grouped = defaultdict(list)
    for r in selected: grouped[r['partition']].append(r)
    assert set(grouped)==set(range(request['partitions'])), 'missing owner including an empty partition'
    owners = {}
    for owner,receipts in grouped.items():
        identities = {(r['worker_id'],r['pid'],r['adjacency_id']) for r in receipts}
        assert len(identities)==1, 'owner moved or rebuilt native adjacency'
        worker,pid,adjacency = next(iter(identities))
        owners[owner] = dict(worker_id=worker,pid=pid,adjacency_id=adjacency)
        events = Counter(r['event'] for r in receipts)
        assert events['init']==events['close']==1, 'owner did not initialize/close exactly once'
        assert next(r for r in receipts if r['event']=='init')['phase']==0
    return selected,grouped,owners


def validate_audit(records, rows, request, *, stages, task_statuses, worker_endpoints,
                   minimum_workers=2, supervisors=(), required_hosts=(), edges=()):
    selected,grouped,owners = scoped_records(records,request)
    slots = 2*request['max_pushes']+1
    phase_reports = defaultdict(list)
    for owner,receipts in grouped.items():
        assert {r['event'] for r in receipts}<={'init','statistics','emit','result','close'}, 'unexpected v2 event'
        stats = [r for r in receipts if r['event']=='statistics']
        emits = [r for r in receipts if r['event']=='emit']
        results = [r for r in receipts if r['event']=='result']
        assert sorted(r['output_phase'] for r in stats)==list(range(slots+1)), 'statistics phases incomplete or replayed'
        assert sorted(r['phase'] for r in emits)==list(range(slots)), 'emit phases incomplete or replayed'
        assert len(results)==1 and results[0]['phase']==slots and results[0]['converged'] is True
        close = next(r for r in receipts if r['event']=='close')
        assert close['phase']==slots
        size = sum(row['owner']==owner for row in rows)
        for stat in stats:
            output_phase = stat['output_phase']
            assert stat['phase']==max(0,output_phase-1), 'statistics input/output phase mismatch'
            assert stat['vertices']==size
            assert all(math.isfinite(stat[k]) and stat[k]>=0 for k in ('mass','residual_l1'))
            assert (stat['minimum_score'] is None if size==0 else
                    math.isfinite(stat['minimum_score']) and stat['minimum_score']>=0)
            phase_reports[output_phase].append(stat)
        for emit in emits:
            after = next(r for r in stats if r['output_phase']==emit['phase']+1)
            assert emit['mode']==after['mode'], 'emitted/applied work mode disagrees'
        result = results[0]
        last = next(r for r in stats if r['output_phase']==slots)
        assert all(close[k]==result[k]==last[k] for k in ('pushes','certificate_passes'))
        for row in rows:
            if row['owner']==owner:
                assert all(row[k]==owners[owner][k] for k in ('worker_id','pid','adjacency_id'))
                assert all(row[k]==result[k] for k in ('pushes','certificate_passes','residual_l1','stationary_error_bound'))
    pushes,certificates,done = 0,0,False
    trace = []
    for phase in range(slots+1):
        reports = phase_reports[phase]
        modes = {r['mode'] for r in reports}
        assert len(modes)==1, 'owners selected different global work modes'
        mode = next(iter(modes))
        if phase==0:
            assert mode==0
        else:
            assert mode in (1,2,3) and (not done or mode==3)
            pushes += int(mode==1)
            certificates += int(mode==2)
            done |= mode==3
            if phase==1: assert mode==2, 'first work slot must certify uniform ranks'
        assert sum(r['vertices'] for r in reports)==request['vertices']
        assert all(r['pushes']==pushes and r['certificate_passes']==certificates for r in reports)
        trace.append(dict(output_phase=phase,mode=mode,pushes=pushes,certificate_passes=certificates,
                          residual_l1=math.fsum(r['residual_l1'] for r in reports)))
    assert pushes<=request['max_pushes'] and 1<=certificates<=pushes+1
    global_results = [r for r in selected if r['event']=='result']
    certificate = {(r['pushes'],r['certificate_passes'],r['residual_l1'],r['stationary_error_bound']) for r in global_results}
    assert len(certificate)==1, 'empty/nonempty owners disagree on final certificate'
    assert abs(trace[-1]['residual_l1']-global_results[0]['residual_l1'])<=ROUND_OFF
    workers = {v['worker_id'] for v in owners.values()}
    pids = {v['pid'] for v in owners.values()}
    assert len(workers)>=minimum_workers and len(pids)>=minimum_workers
    assert workers<={r['worker_id'] for r in worker_endpoints}, 'native worker absent from inventory'
    session,job = selected[0]['session_id'],selected[0]['job_id']
    native,ordinary,tasks = validate_native_stages(stages,task_statuses,session=session,job=job,
        owners=owners,partitions=request['partitions'],expected_native_phases=4*request['max_pushes']+4)
    host_graph = None
    if required_hosts:
        host_graph = validate_host_graph(rows,edges,supervisors,required_hosts=required_hosts)
    return dict(session_id=session,job_id=job,owners=owners,native_workers=sorted(workers),native_pids=sorted(pids),
                native_phases=len(native),transport_work_slots=slots,actual_pushes=pushes,certificate_passes=certificates,
                statistics_trace=trace,native_stages=native,ordinary_stages=ordinary,native_task_statuses=tasks,
                empty_owners=[p for p in owners if not any(row['owner']==p for row in rows)],
                native_host_graph=host_graph,closed_all_owners=True,same_adjacency_across_phases=True)


def validate_cap(records,request,*,stages,task_statuses,minimum_workers=2):
    selected,grouped,owners = scoped_records(records,request)
    assert request['max_pushes']==0
    assert not any(r['event']=='result' for r in selected), 'capped operation emitted result receipt'
    session,job = selected[0]['session_id'],selected[0]['job_id']
    native = [s for s in stages if s['session_id']==session and s['job_id']==job and s['slot_group'].startswith('worker-extension:')]
    assert len(native)==len({s['stage'] for s in native})==4
    assert len({s['slot_group'] for s in native})==1
    assert all(s['partitions']==request['partitions'] and s['placement']=='Worker' and s['mode']=='Pipelined' for s in native)
    tasks = [t for t in task_statuses if t['job_id']==job and t['stage'] in {s['stage'] for s in native}]
    assert tasks and all(t['attempt']==0 for t in tasks), 'cap operation lacks tasks or was retried'
    assert any(t['status']=='FAILED' for t in tasks), 'cap error has no failed native task'
    assert all(t['partition'] in owners and t['worker_id']==owners[t['partition']]['worker_id'] for t in tasks)
    workers,pids = {o['worker_id'] for o in owners.values()},{o['pid'] for o in owners.values()}
    assert len(workers)>=minimum_workers and len(pids)>=minimum_workers
    return dict(session_id=session,job_id=job,native_pids=sorted(pids),native_workers=sorted(workers),
                native_phases=4,closed_all_owners=True,post_init_cap=True,no_result=True,no_native_retry=True,
                native_receipts=selected,native_task_statuses=tasks)

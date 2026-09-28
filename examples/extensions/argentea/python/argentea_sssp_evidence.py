"""Independent tiny-graph Dijkstra and complete weighted native phase audit."""
from collections import Counter, defaultdict
import heapq
import math
from argentea_sssp_client import phase_count


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def reference(ids, edges, source):
    assert len(ids) == len(set(ids)) and source in ids
    adjacency = {v: [] for v in ids}
    for s, t, weight in edges:
        assert s in adjacency and t in adjacency
        assert math.isfinite(weight) and weight >= 0
        adjacency[s].append((t, weight))
    labels = {v: None for v in ids}
    labels[source] = (0.0, 0, source)
    queue = [(0.0, 0, source, source)]
    while queue:
        distance, hops, parent, vertex = heapq.heappop(queue)
        if labels[vertex] != (distance, hops, parent):
            continue
        for target, weight in adjacency[vertex]:
            candidate = (distance + weight, hops + 1, vertex)
            assert math.isfinite(candidate[0]), 'oracle distance overflow'
            if labels[target] is None or candidate < labels[target]:
                labels[target] = candidate
                heapq.heappush(queue, (*candidate, target))
    return labels


def validate_rows(rows, ids, edges, request):
    expected = reference(ids, edges, request['source'])
    actual = {}
    for row in rows:
        for key in ('id', 'owner', 'phase', 'rounds', 'reached', 'converged',
                    'worker_id', 'pid', 'adjacency_id'):
            assert integer(row[key])
        vertex = row['id']
        assert vertex not in actual, 'duplicate SSSP vertex'
        distance, hops, parent = (row[k] for k in ('distance', 'hops', 'parent'))
        if distance is None:
            assert hops is parent is None
            actual[vertex] = None
        else:
            assert isinstance(distance, float) and math.isfinite(distance) and distance >= 0
            assert integer(hops) and hops >= 0 and integer(parent)
            actual[vertex] = (distance, hops, parent)
        assert row['owner'] == vertex % request['partitions']
        assert row['phase'] == request['max_rounds'] + 1 and row['converged'] == 1
        assert 1 <= row['rounds'] <= request['max_rounds']
        assert row['adjacency_id'] > 0 and row['pid'] > 0 and row['worker_id'] >= 0
        assert row['reached'] == sum(v is not None for v in expected.values())
    assert actual == expected, 'SSSP differs from independent Dijkstra'
    assert len({row['rounds'] for row in rows}) == 1
    # Check a concrete predecessor edge for every reported path witness.
    for vertex, label in actual.items():
        if label is not None and vertex != request['source']:
            distance, hops, parent = label
            assert actual[parent] is not None and actual[parent][1] + 1 == hops
            assert any(s == parent and t == vertex and actual[parent][0] + w == distance
                       for s, t, w in edges), 'invalid predecessor witness'
    return dict(vertices=len(ids), arcs=len(edges), reached=sum(v is not None for v in expected.values()),
                independent_dijkstra=True, exact_distance_hop_parent=True)


def _events(records, request, slots, *, capped, minimum_workers):
    selected = [r for r in records if r.get('operation_id') == request['operation_id']]
    assert selected and all(r['protocol'] == 5 and r['algorithm'] == request['algorithm'] and
        r['snapshot_id'] == request['snapshot_id'] and r['generation'] == request['generation'] for r in selected)
    identities = {(r['session_id'], r['job_id']) for r in selected}
    assert len(identities) == 1
    grouped = defaultdict(list)
    for r in selected:
        grouped[r['partition']].append(r)
    assert set(grouped) == set(range(request['partitions'])), 'missing SSSP owner'
    owners, initial, results, decisions, applications = {}, [], [], defaultdict(list), defaultdict(list)
    for owner, events in grouped.items():
        counts = Counter(r['event'] for r in events)
        assert counts['init'] == counts['close'] == 1
        assert counts['decide'] == counts['apply'] == slots
        assert counts['result'] == (0 if capped else 1)
        assert set(counts) <= {'init', 'close', 'decide', 'apply', 'failure' if capped else 'result'}
        assert counts['failure'] <= 1
        origin = {(r['worker_id'], r['pid'], r['adjacency_id']) for r in events}
        assert len(origin) == 1, 'SSSP owner moved or rebuilt adjacency'
        worker, pid, adjacency = next(iter(origin))
        assert adjacency > 0 and pid > 0 and worker >= 0
        owners[owner] = dict(worker_id=worker, pid=pid, adjacency_id=adjacency)
        start = next(r for r in events if r['event'] == 'init')
        assert start['phase'] == start['output_phase'] == start['rounds'] == 0
        assert start['mode'] == 'topology'
        initial.append(start)
        for name, destination in [('decide', decisions), ('apply', applications)]:
            part = [r for r in events if r['event'] == name]
            assert sorted(r['phase'] for r in part) == list(range(slots))
            for r in part:
                destination[r['phase']].append(r)
        close = next(r for r in events if r['event'] == 'close')
        assert close['phase'] == slots
        if not capped:
            result = next(r for r in events if r['event'] == 'result')
            assert result['phase'] == slots and result['converged'] is True
            assert result['rounds'] == close['rounds']
            results.append(result)
        else:
            assert close['rounds'] == request['max_rounds']
    assert sum(r['vertices'] for r in initial) == request['vertices']
    assert sum(r['source_count'] for r in initial) == 1
    assert sum(r['active'] for r in initial) == sum(r['reached'] for r in initial) == 1
    assert len({x['worker_id'] for x in owners.values()}) >= minimum_workers
    assert len({x['pid'] for x in owners.values()}) >= minimum_workers
    return selected, next(iter(identities)), owners, initial, results, decisions, applications


def _trace(request, initial, decisions, applications, slots):
    previous, rounds, trace = initial, 0, []
    method = request['algorithm'].removeprefix('sssp_')
    for phase in range(slots):
        active = sum(r['active'] for r in previous)
        mode = 'topology' if phase == 0 else 'done' if active == 0 else method
        bucket = min((r['bucket'] for r in previous if r['bucket'] is not None), default=None) if mode == 'delta_star' else None
        if mode in ('reference', 'delta_star'):
            assert rounds < request['max_rounds']
        decide, apply = decisions[phase], applications[phase]
        assert all(r['mode'] == mode and r['rounds'] == rounds and r['bucket'] == bucket for r in decide), 'wrong SSSP global decision'
        expected = (sum(r['arcs'] for r in previous) if mode == 'topology' else
                    sum(r['reachable_edges'] for r in previous) if mode == 'reference' else
                    sum(r['bucket_edges'] for r in previous if r['bucket'] == bucket) if mode == 'delta_star' else 0)
        if mode in ('reference', 'delta_star'):
            rounds += 1
        assert all(r['mode'] == mode and r['rounds'] == rounds and r['output_phase'] == phase + 1 for r in apply)
        work = {}
        for field in ('examined_edges', 'examined_vertices', 'emitted_messages', 'received_messages', 'active', 'reached'):
            assert all(integer(r[field]) and r[field] >= 0 for r in apply)
            work[field] = sum(r[field] for r in apply)
        assert work['emitted_messages'] == work['received_messages'] == work['examined_edges'] == expected, 'lost or unexpected SSSP relaxations'
        if mode == 'done':
            assert work['active'] == work['examined_vertices'] == 0, 'DONE performed graph work'
        assert work['reached'] >= sum(r['reached'] for r in previous)
        for r in apply:
            shape=next(v for v in initial if v['partition']==r['partition'])
            assert all(r[k]==shape[k] for k in ('vertices','arcs','source_count')), 'partition shape changed'
            assert 0<=r['bucket_edges']<=r['reachable_edges']<=r['arcs']
            assert 0 <= r['active'] <= r['reached'] <= r['vertices']
            if method == 'delta_star':
                assert (r['active'] > 0) == (r['bucket'] is not None)
                if r['bucket'] is not None:
                    assert math.isfinite(r['bucket']) and r['bucket'] >= 0 and r['bucket'].is_integer()
            else:
                assert r['bucket'] is None and r['bucket_edges'] == 0
        trace.append(dict(phase=phase, mode=mode, bucket=bucket, rounds=rounds, **work))
        previous = apply
    return trace, previous, rounds


def validate_events(records, rows, request, *, minimum_workers=2):
    method = request['algorithm'].removeprefix('sssp_')
    count = phase_count(request['max_rounds'], method)
    slots = (count - 2) // 2
    _, _, owners, initial, results, decisions, applications = _events(records, request, slots, capped=False, minimum_workers=minimum_workers)
    trace, last, rounds = _trace(request, initial, decisions, applications, slots)
    reached = sum(r['reached'] for r in last)
    assert sum(r['active'] for r in last) == 0
    assert all(r['rounds'] == rounds and r['reached'] == reached for r in results)
    for owner, identity in owners.items():
        owned = [r for r in rows if r['owner'] == owner]
        assert next(r for r in initial if r['partition'] == owner)['vertices'] == len(owned)
        assert all(all(r[k] == v for k, v in identity.items()) and r['rounds'] == rounds and r['reached'] == reached for r in owned)
    return dict(owners=owners, trace=trace, rounds=rounds, reached=reached, native_phase_count=count,
                closed_all_owners=True, same_adjacency_across_phases=True,
                boundary='native events only; scheduler stage/task audit is separately required')


def validate_cap(records,request,*,stages,task_statuses,stored_tasks,jobs,query_failed,minimum_workers=2):
    assert query_failed is True
    limit=request['max_rounds'];stop=limit+1
    selected,(session,job),owners,initial,_,decisions,applications=_events(
        records,request,stop,capped=True,minimum_workers=minimum_workers)
    assert [r for r in jobs if r['session_id']==session and r['job_id']==job]==[dict(session_id=session,job_id=job,status='FAILED')]
    trace,last,rounds=_trace(request,initial,decisions,applications,stop)
    active=sum(r['active'] for r in last);reached=sum(r['reached'] for r in last)
    assert rounds==limit and active>0
    causes=[r for r in selected if r['event']=='failure']
    assert causes and all(r['code']=='sssp_round_cap' and r['outcome']=='nonconverged' and
        r['rounds']==r['max_rounds']==limit and r['phase']==stop and
        r['active']==active and r['reached']==reached for r in causes)
    method=request['algorithm'].removeprefix('sssp_')
    native=[s for s in stages if s['session_id']==session and s['job_id']==job and s['slot_group'].startswith('worker-extension:')]
    assert len(native)==len({s['stage'] for s in native})==phase_count(limit,method)
    assert len({s['slot_group'] for s in native})==1
    assert all(s['partitions']==request['partitions'] and s['placement']=='Worker' and s['mode']=='Pipelined' for s in native)
    ids={s['stage'] for s in native}
    tasks=[t for t in task_statuses if t['job_id']==job and t['stage'] in ids]
    assert tasks and any(t['status']=='FAILED' for t in tasks)
    assert all(t['attempt']==0 and t['partition'] in owners and t['worker_id']==owners[t['partition']]['worker_id'] for t in tasks)
    stored=[t for t in stored_tasks if t['session_id']==session and t['job_id']==job]
    assert stored and all(t['attempt']==0 and t['status'] in ('SUCCEEDED','FAILED','CANCELED') for t in stored)
    native_stored=[t for t in stored if t['stage'] in ids]
    expected={(s,p) for s in ids for p in range(request['partitions'])}
    assert len(native_stored)==len(expected) and {(t['stage'],t['partition']) for t in native_stored}==expected
    assert len({v['worker_id'] for v in owners.values()})>=minimum_workers
    assert len({v['pid'] for v in owners.values()})>=minimum_workers
    return dict(session_id=session,job_id=job,owners=owners,native_pids=sorted({v['pid'] for v in owners.values()}),
                native_phase_count=len(native),cap_failures=causes,measured_active=active,reached=reached,
                native_task_statuses=tasks,stored_tasks=stored,no_result=True,no_native_retry=True,
                closed_all_owners=True,complete_pre_cap_barriers=stop,trace=trace)

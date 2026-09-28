"""Independent tiny-fixture BFS answers and all-edge correctness certificate."""
from collections import deque


def normalize(edges, *, directed):
    return list(edges) if directed else [*edges,*[(target,source) for source,target in edges]]


def reference(ids, edges, source):
    assert len(ids)==len(set(ids)) and source in ids
    adjacency={node:[] for node in ids}
    for start,end in edges:
        assert start in adjacency and end in adjacency
        adjacency[start].append(end)
    distances={node:None for node in ids};distances[source]=0
    queue=deque([source])
    while queue:
        start=queue.popleft()
        for end in adjacency[start]:
            if distances[end] is None:
                distances[end]=distances[start]+1
                queue.append(end)
    parents={node:None for node in ids};parents[source]=source
    for start,end in edges:
        if distances[start] is not None and distances[end]==distances[start]+1:
            parents[end]=start if parents[end] is None else min(parents[end],start)
    return {node:dict(distance=distances[node],hops=distances[node],parent=parents[node]) for node in ids}


def validate_rows(rows, ids, edges, request):
    expected=reference(ids,edges,request['source'])
    actual={};metadata=set()
    for row in rows:
        node=row['id']
        assert node not in actual, 'duplicate BFS vertex'
        actual[node]=row
        assert row['owner']==node%request['partitions'], 'wrong BFS owner'
        assert row['phase']==request['max_levels']+1 and row['converged']==1
        metadata.add((row['levels'],row['reached'],row['converged']))
        for name in ('distance','hops','parent'):
            assert row[name] is None or isinstance(row[name],int) and not isinstance(row[name],bool), 'BFS result is not nullable integer'
        if row['distance'] is None:
            assert row['hops'] is None and row['parent'] is None, 'unreachable BFS row has a hop or parent'
        else:
            assert row['distance']>=0 and row['hops']==row['distance'] and row['parent'] is not None
        assert row['incoming_adjacency_id']>0 if request['algorithm']=='bfs_direction' else row['incoming_adjacency_id']==0
    assert actual.keys()==expected.keys(), 'BFS output vertex set differs'
    assert len(metadata)==1, 'BFS global counters disagree'
    levels,reached,_=next(iter(metadata))
    assert 1<=levels<=request['max_levels']
    assert reached==sum(row['distance'] is not None for row in rows)
    assert levels==max(row['distance'] for row in rows if row['distance'] is not None)+1, 'missing emptiness expansion or wrong level count'
    for node,answer in expected.items():
        assert all(actual[node][name]==answer[name] for name in ('distance','hops','parent')), 'BFS differs from independent queue reference'
    # This is separately checked against every input arc, including unreachable
    # components. Parent witnesses prove realizable paths; edge inequalities
    # and reachability closure prove no shorter or omitted reachable path.
    source=request['source']
    assert actual[source]['distance']==0 and actual[source]['parent']==source
    for start,end in edges:
        left,right=actual[start]['distance'],actual[end]['distance']
        if left is not None:
            assert right is not None and right<=left+1, 'BFS edge certificate failed'
    parents={node:[] for node in ids}
    for start,end in edges:
        left,right=actual[start]['distance'],actual[end]['distance']
        if left is not None and right==left+1:
            parents[end].append(start)
    for node,row in actual.items():
        if node!=source and row['distance'] is not None:
            assert parents[node] and row['parent']==min(parents[node]), 'BFS parent lacks minimum-ID preceding-level witness'
    return dict(vertices=len(ids),edges=len(edges),reached=reached,levels=levels,
                unreachable=sorted(node for node,row in actual.items() if row['distance'] is None),
                exact_queue_reference=True,all_edge_certificate=True,minimum_numeric_parent=True)


def validate_audit(records, rows, request, *, stages, task_statuses, worker_endpoints,
                   edges, minimum_workers=2, supervisors=(), required_hosts=()):
    from collections import Counter,defaultdict
    from argentea_evidence import validate_native_stages
    from argentea_host_evidence import validate_host_graph
    selected=[r for r in records if r.get('operation_id')==request['operation_id']]
    assert selected and all(r['protocol']==3 and r['algorithm']==request['algorithm'] for r in selected)
    assert all(r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in selected)
    assert len({(r['session_id'],r['job_id']) for r in selected})==1, 'BFS native operation spanned jobs'
    grouped=defaultdict(list)
    for record in selected:grouped[record['partition']].append(record)
    assert set(grouped)==set(range(request['partitions'])), 'BFS omitted an owner'
    owners={};applications=defaultdict(list);decisions=defaultdict(list);initial=[];results=[]
    for owner,events in grouped.items():
        counts=Counter(r['event'] for r in events)
        assert set(counts)=={'init','decide','apply','result','close'}
        assert counts['init']==counts['result']==counts['close']==1
        assert counts['decide']==counts['apply']==request['max_levels']+1
        identities={(r['worker_id'],r['pid'],r['adjacency_id']) for r in events}
        assert len(identities)==1, 'BFS owner moved or rebuilt outgoing adjacency'
        worker,pid,adjacency=next(iter(identities));assert adjacency>0
        owners[owner]=dict(worker_id=worker,pid=pid,adjacency_id=adjacency)
        init=next(r for r in events if r['event']=='init');initial.append(init)
        assert init['phase']==init['output_phase']==init['levels']==0 and init['mode']=='topology'
        assert init['incoming_adjacency_id']==0
        assert sorted(r['phase'] for r in events if r['event']=='decide')==list(range(request['max_levels']+1))
        assert sorted(r['phase'] for r in events if r['event']=='apply')==list(range(request['max_levels']+1))
        for record in events:
            if record['event']=='apply':
                assert record['output_phase']==record['phase']+1
                applications[record['phase']].append(record)
            elif record['event']=='decide':decisions[record['phase']].append(record)
        result=next(r for r in events if r['event']=='result');results.append(result)
        close=next(r for r in events if r['event']=='close')
        assert result['phase']==close['phase']==request['max_levels']+1 and result['converged'] is True
        assert all(result[name]==close[name] for name in ('levels','local_reached','incoming_adjacency_id'))
        incoming=applications[0][-1]['incoming_adjacency_id']
        if request['algorithm']=='bfs_direction':assert incoming>0
        else:assert incoming==0
        for record in events:
            if record['event'] not in ('init',) and not(record['event']=='decide' and record['phase']==0):
                assert record['incoming_adjacency_id']==incoming, 'BFS incoming adjacency changed after topology'
        owned=[row for row in rows if row['owner']==owner]
        assert result['local_reached']==sum(row['distance'] is not None for row in owned)
        for row in owned:
            assert all(row[name]==result[name] for name in ('worker_id','pid','adjacency_id','incoming_adjacency_id','levels','reached'))
    assert len({(r['levels'],r['reached']) for r in results})==1
    assert sum(r['local_reached'] for r in results)==results[0]['reached']
    outdegrees=Counter(start for start,_ in edges)
    def check_statistics(reports,level):
        assert len(reports)==request['partitions'] and {r['partition'] for r in reports}==set(range(request['partitions']))
        for report in reports:
            owner=report['partition'];owned=[r for r in rows if r['owner']==owner]
            visited=[r for r in owned if r['distance'] is not None and r['distance']<=level]
            frontier=[r for r in owned if r['distance']==level]
            assert report['vertices']==len(owned) and report['levels']==level
            assert report['local_reached']==len(visited)
            assert report['frontier_vertices']==len(frontier)
            assert report['frontier_edges']==sum(outdegrees[r['id']] for r in frontier)
            assert report['remaining_edges']==sum(outdegrees[r['id']] for r in owned if r not in visited)
    level=0;previous_mode='topology';previous=initial;trace=[]
    check_statistics(initial,0)
    for phase in range(request['max_levels']+1):
        decided,applied=decisions[phase],applications[phase]
        frontier=sum(r['frontier_vertices'] for r in previous)
        frontier_edges=sum(r['frontier_edges'] for r in previous)
        remaining=sum(r['remaining_edges'] for r in previous)
        if phase==0:mode='topology'
        elif not frontier:mode='done'
        elif request['algorithm']=='bfs_reference':mode='reference'
        elif request['algorithm']=='bfs_frontier':mode='push'
        else:
            pull=frontier*request['beta']>=request['vertices'] if previous_mode=='pull' else frontier_edges*request['alpha']>remaining
            mode='pull' if pull else 'push'
        assert all(r['mode']==mode and r['levels']==level for r in decided), 'BFS global direction choice or level mismatch'
        for decision in decided:
            before=next(r for r in previous if r['partition']==decision['partition'])
            assert decision['local_reached']==before['local_reached'] and decision['frontier_vertices']==before['frontier_vertices']
        if mode in ('reference','push','pull'):level+=1
        assert all(r['mode']==mode for r in applied)
        check_statistics(applied,level)
        work={name:0 for name in ('examined_edges','examined_vertices','emitted_messages','received_messages')}
        for report in applied:
            for name in work:
                assert isinstance(report[name],int) and not isinstance(report[name],bool) and report[name]>=0
                work[name]+=report[name]
                if mode=='done':assert report[name]==0, 'BFS DONE relay performed graph work'
        trace.append(dict(phase=phase,mode=mode,levels=level,frontier_before=frontier,
                          frontier_after=sum(r['frontier_vertices'] for r in applied),**work))
        previous,previous_mode=applied,mode
    assert level==results[0]['levels'] and sum(r['frontier_vertices'] for r in previous)==0
    workers={v['worker_id'] for v in owners.values()};pids={v['pid'] for v in owners.values()}
    assert len(workers)>=minimum_workers and len(pids)>=minimum_workers
    assert workers<={r['worker_id'] for r in worker_endpoints}
    session,job=selected[0]['session_id'],selected[0]['job_id']
    native,ordinary,tasks=validate_native_stages(stages,task_statuses,session=session,job=job,owners=owners,
        partitions=request['partitions'],expected_native_phases=2*request['max_levels']+4)
    host_graph=validate_host_graph(rows,edges,supervisors,required_hosts=required_hosts) if required_hosts else None
    return dict(session_id=session,job_id=job,owners=owners,native_pids=sorted(pids),native_workers=sorted(workers),
                native_phases=len(native),native_stages=native,ordinary_stages=ordinary,native_task_statuses=tasks,
                levels=level,reached=results[0]['reached'],trace=trace,closed_all_owners=True,
                empty_owners=[owner for owner in owners if not any(row['owner']==owner for row in rows)],
                native_host_graph=host_graph,same_adjacency_across_phases=True)


def validate_cap(records,request,*,stages,task_statuses,query_failed,minimum_workers=2):
    from collections import Counter,defaultdict
    assert query_failed is True and request['max_levels']==0
    selected=[r for r in records if r.get('operation_id')==request['operation_id']]
    assert selected and all(r['protocol']==3 and r['algorithm']==request['algorithm'] for r in selected)
    assert all(r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in selected)
    assert len({(r['session_id'],r['job_id']) for r in selected})==1
    assert not any(r['event']=='result' for r in selected)
    grouped=defaultdict(list)
    for record in selected:grouped[record['partition']].append(record)
    assert set(grouped)==set(range(request['partitions']))
    causes=[r for r in selected if r['event']=='failure']
    assert causes and len({r['partition'] for r in causes})==len(causes), 'missing or replayed BFS native cap cause'
    assert all(r['code']=='bfs_level_cap' and r['outcome']=='nonconverged' for r in causes)
    assert all(r['phase']==1 and r['levels']==r['max_levels']==0 and r['frontier_vertices']>0 for r in causes)
    assert len({(r['levels'],r['frontier_vertices'],r['reached']) for r in causes})==1
    owners={};frontier=reached=vertices=0
    for owner,events in grouped.items():
        counts=Counter(r['event'] for r in events)
        assert counts['init']==counts['decide']==counts['apply']==counts['close']==1
        identities={(r['worker_id'],r['pid'],r['adjacency_id']) for r in events};assert len(identities)==1
        worker,pid,adjacency=next(iter(identities));assert adjacency>0
        owners[owner]=dict(worker_id=worker,pid=pid,adjacency_id=adjacency)
        init=next(r for r in events if r['event']=='init');assert init['phase']==0
        decide=next(r for r in events if r['event']=='decide');assert decide['phase']==0 and decide['mode']=='topology'
        apply=next(r for r in events if r['event']=='apply')
        assert apply['phase']==0 and apply['output_phase']==1 and apply['mode']=='topology' and apply['levels']==0
        close=next(r for r in events if r['event']=='close');assert close['phase']==1 and close['levels']==0
        assert close['local_reached']==apply['local_reached']
        frontier+=apply['frontier_vertices'];reached+=apply['local_reached'];vertices+=apply['vertices']
    assert vertices==request['vertices'] and frontier==causes[0]['frontier_vertices']==1
    assert reached==causes[0]['reached']==1
    workers={r['worker_id'] for r in owners.values()};pids={r['pid'] for r in owners.values()}
    assert len(workers)>=minimum_workers and len(pids)>=minimum_workers
    session,job=selected[0]['session_id'],selected[0]['job_id']
    native=[s for s in stages if s['session_id']==session and s['job_id']==job and s['slot_group'].startswith('worker-extension:')]
    assert len(native)==len({s['stage'] for s in native})==4 and len({s['slot_group'] for s in native})==1
    assert all(s['partitions']==request['partitions'] and s['placement']=='Worker' and s['mode']=='Pipelined' for s in native)
    tasks=[t for t in task_statuses if t['job_id']==job and t['stage'] in {s['stage'] for s in native}]
    assert tasks and all(t['attempt']==0 for t in tasks) and any(t['status']=='FAILED' for t in tasks)
    assert all(t['partition'] in owners and t['worker_id']==owners[t['partition']]['worker_id'] for t in tasks)
    return dict(session_id=session,job_id=job,native_pids=sorted(pids),native_workers=sorted(workers),native_phases=4,
                native_stages=native,native_task_statuses=tasks,closed_all_owners=True,no_result=True,no_native_retry=True,
                query_failed=True,native_cause='bfs_level_cap',cap_failures=causes,complete_topology=True)

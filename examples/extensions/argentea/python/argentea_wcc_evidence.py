"""Tiny-fixture union-find answers and WCC event/phase ownership audit."""
from collections import Counter,defaultdict
from argentea_wcc_client import phase_count


def reference(ids,edges):
    assert len(ids)==len(set(ids))
    parents={v:v for v in ids}
    def root(v):
        while parents[v]!=v:
            parents[v]=parents[parents[v]]
            v=parents[v]
        return v
    for source,target in edges:
        assert source in parents and target in parents
        left,right=root(source),root(target)
        parents[max(left,right)]=min(left,right)
    return {v:root(v) for v in ids}


def validate_rows(rows,ids,edges,request):
    expected=reference(ids,edges)
    method=request['algorithm'].removeprefix('wcc_')
    slots=(phase_count(request['max_rounds'],method)-2)//2
    actual={}
    for row in rows:
        for key in ('id','component','owner','phase','rounds','converged'):
            assert isinstance(row[key],int) and not isinstance(row[key],bool)
        assert row['id'] not in actual,'duplicate WCC vertex'
        actual[row['id']]=row['component']
        assert row['owner']==row['id']%request['partitions']
        assert row['phase']==slots and row['converged']==1
        assert 0<=row['rounds']<=request['max_rounds']
        assert row['adjacency_id']>0 and row['incoming_adjacency_id']>0
    assert actual==expected,'WCC differs from independent union-find'
    assert len({row['rounds'] for row in rows})==1
    assert all(actual[s]==actual[t] for s,t in edges)
    return dict(vertices=len(ids),arcs=len(edges),components=len(set(expected.values())),
                independent_union_find=True,minimum_id_labels=True)


def validate_events(records,rows,request,*,minimum_workers=2):
    selected=[r for r in records if r.get('operation_id')==request['operation_id']]
    assert selected
    assert all(r['protocol']==4 and r['algorithm']==request['algorithm'] and
               r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in selected)
    assert len({(r['session_id'],r['job_id']) for r in selected})==1
    method=request['algorithm'].removeprefix('wcc_')
    count=phase_count(request['max_rounds'],method);slots=(count-2)//2
    grouped=defaultdict(list)
    for record in selected:grouped[record['partition']].append(record)
    assert set(grouped)==set(range(request['partitions'])),'missing WCC owner'
    owners={};decisions=defaultdict(list);applications=defaultdict(list);results=[]
    for owner,events in grouped.items():
        assert Counter(r['event'] for r in events)==dict(init=1,decide=slots,apply=slots,result=1,close=1)
        identities={(r['worker_id'],r['pid'],r['adjacency_id']) for r in events}
        assert len(identities)==1,'WCC owner moved or rebuilt outgoing adjacency'
        worker,pid,adjacency=next(iter(identities));assert adjacency>0
        owners[owner]=dict(worker_id=worker,pid=pid,adjacency_id=adjacency)
        initial=next(r for r in events if r['event']=='init')
        assert initial['phase']==initial['output_phase']==initial['rounds']==0
        assert initial['mode']=='topology' and initial['incoming_adjacency_id']==0
        owned=[r for r in rows if r['owner']==owner]
        assert initial['vertices']==len(owned)
        for event in ('decide','apply'):
            subset=[r for r in events if r['event']==event]
            assert sorted(r['phase'] for r in subset)==list(range(slots))
            for record in subset:(decisions if event=='decide' else applications)[record['phase']].append(record)
        applied=next(r for r in events if r['event']=='apply' and r['phase']==0)
        incoming=applied['incoming_adjacency_id'];assert incoming>0
        for record in events:
            if record['event']!='init' and not(record['event']=='decide' and record['phase']==0):
                assert record['incoming_adjacency_id']==incoming,'incoming WCC adjacency rebuilt'
        result=next(r for r in events if r['event']=='result');results.append(result)
        close=next(r for r in events if r['event']=='close')
        assert result['phase']==close['phase']==slots and result['converged'] is True
        assert result['rounds']==close['rounds']
        for row in owned:
            assert all(row[key]==result[key] for key in ('worker_id','pid','adjacency_id','incoming_adjacency_id','rounds'))
    assert len({r['rounds'] for r in results})==1
    previous='topology';rounds=0;changed=crossing=0;trace=[]
    for phase in range(slots):
        if phase==0:mode='topology'
        elif method=='reference':
            mode='done' if previous=='done' or (previous=='reference' and changed==0) else 'reference'
            if mode=='done':assert crossing==0
        elif previous in ('topology','hook_return'):mode='neighbors'
        elif previous=='neighbors':mode='normalize_route' if crossing==0 else 'hook_route'
        else:mode={'hook_route':'hook_return','normalize_route':'normalize_return','normalize_return':'done','done':'done'}[previous]
        if mode in ('reference','hook_route'):assert rounds<request['max_rounds']
        decide,apply=decisions[phase],applications[phase]
        assert all(r['mode']==mode and r['rounds']==rounds for r in decide),'wrong WCC global decision'
        if mode in ('reference','hook_return'):rounds+=1
        assert all(r['mode']==mode and r['rounds']==rounds and r['output_phase']==phase+1 for r in apply)
        work={}
        for field in ('examined_edges','examined_vertices','emitted_messages','received_messages','changed','crossing','members'):
            assert all(isinstance(r[field],int) and not isinstance(r[field],bool) and r[field]>=0 for r in apply)
            work[field]=sum(r[field] for r in apply)
        assert work['emitted_messages']==work['received_messages'],'lost WCC messages'
        if mode=='done':assert all(v==0 for v in work.values()),'DONE performed graph work'
        changed,crossing=work['changed'],work['crossing']
        trace.append(dict(phase=phase,mode=mode,rounds=rounds,**work));previous=mode
    assert rounds==results[0]['rounds'] and previous in ('done','normalize_return','reference')
    if previous=='reference':assert changed==crossing==0
    assert len({x['worker_id'] for x in owners.values()})>=minimum_workers
    assert len({x['pid'] for x in owners.values()})>=minimum_workers
    return dict(owners=owners,trace=trace,rounds=rounds,native_phase_count=count,
                closed_all_owners=True,same_adjacency_across_phases=True,
                boundary='native events only; scheduler stage/task audit is separately required')


def validate_cap(records,request,*,stages,task_statuses,stored_tasks,jobs,query_failed,minimum_workers=2):
    assert query_failed is True
    selected=[r for r in records if r.get('operation_id')==request['operation_id']]
    assert selected and all(r['protocol']==4 and r['algorithm']==request['algorithm'] and
        r['snapshot_id']==request['snapshot_id'] and r['generation']==request['generation'] for r in selected)
    identity={(r['session_id'],r['job_id']) for r in selected};assert len(identity)==1
    session,job=next(iter(identity))
    assert [r for r in jobs if r['session_id']==session and r['job_id']==job]==[dict(session_id=session,job_id=job,status='FAILED')]
    assert not any(r['event']=='result' for r in selected),'capped WCC returned components'
    method=request['algorithm'].removeprefix('wcc_');limit=request['max_rounds']
    stop=limit+1 if method=='reference' else 3*limit+2
    causes=[r for r in selected if r['event']=='failure']
    assert causes and len({r['partition'] for r in causes})==len(causes)
    assert all(r['code']=='wcc_round_cap' and r['outcome']=='nonconverged' and
               r['rounds']==r['max_rounds']==limit and r['phase']==stop for r in causes)
    unresolved={r['unresolved'] for r in causes};assert len(unresolved)==1
    pending=method=='reference' and limit==0
    assert all(r['certificate_not_attempted'] is pending for r in causes)
    assert next(iter(unresolved))==0 if pending else next(iter(unresolved))>0
    grouped=defaultdict(list)
    for r in selected:grouped[r['partition']].append(r)
    assert set(grouped)==set(range(request['partitions']))
    owners={};last=[]
    for owner,events in grouped.items():
        counts=Counter(r['event'] for r in events)
        assert counts['init']==counts['close']==1 and counts['decide']==counts['apply']==stop
        assert set(counts)<={'init','close','decide','apply','failure'}
        origin={(r['worker_id'],r['pid'],r['adjacency_id']) for r in events};assert len(origin)==1
        worker,pid,adjacency=next(iter(origin));owners[owner]=dict(worker_id=worker,pid=pid,adjacency_id=adjacency)
        assert adjacency>0
        for event in ('decide','apply'):
            assert sorted(r['phase'] for r in events if r['event']==event)==list(range(stop))
        initial=next(r for r in events if r['event']=='init');assert initial['phase']==initial['rounds']==0
        applied=[r for r in events if r['event']=='apply'];incoming={r['incoming_adjacency_id'] for r in applied}
        assert len(incoming)==1 and next(iter(incoming))>0
        assert all(r['output_phase']==r['phase']+1 for r in applied)
        terminal=next(r for r in applied if r['phase']==stop-1);last.append(terminal)
        assert terminal['mode']==('topology' if pending else 'reference' if method=='reference' else 'neighbors')
        close=next(r for r in events if r['event']=='close')
        assert close['phase']==stop and close['rounds']==limit and close['incoming_adjacency_id'] in incoming
    measured=sum(r['changed'] if method=='reference' else r['crossing'] for r in last)
    assert measured==next(iter(unresolved)),'cap reports synthetic unresolved work'
    for phase in range(stop):
        applied=[r for r in selected if r['event']=='apply' and r['phase']==phase]
        assert sum(r['emitted_messages'] for r in applied)==sum(r['received_messages'] for r in applied)
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
                native_phase_count=len(native),cap_failures=causes,measured_unresolved=measured,
                native_task_statuses=tasks,stored_tasks=stored,no_result=True,no_native_retry=True,
                closed_all_owners=True,complete_pre_cap_barriers=stop)

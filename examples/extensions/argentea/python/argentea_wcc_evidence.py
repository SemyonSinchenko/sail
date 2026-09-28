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

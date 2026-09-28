"""Adversarial controls for the independent weighted result and runtime auditor."""
from copy import deepcopy
import pytest
from argentea_sssp_client import request
from argentea_sssp_evidence import reference, validate_rows, validate_events, validate_cap

IDS = [-5, 0, 1]
EDGES = [(-5, 0, 1.0), (0, 1, 2.0)]


def fixture(method='delta_star'):
    base = request(vertices_count=3, source=-5, method=method, max_rounds=3, partitions=2)
    records, rows = [], []
    # States before topology, after topology, and after each relaxation.
    # Tuple fields are reached, reachable edges, active, active bucket edges.
    states = [[(0,0,0,0),(1,1,1,1)], [(0,0,0,0),(1,1,1,1)],
              [(1,1,1,1),(1,1,0,0)], [(1,1,0,0),(2,1,1,0)],
              [(1,1,0,0),(2,1,0,0)]]
    expected = {-5:(0.0,0,-5), 0:(1.0,1,-5), 1:(3.0,2,0)}
    for owner in range(2):
        common = dict(protocol=5, algorithm=base['algorithm'], operation_id=base['operation_id'],
            snapshot_id=base['snapshot_id'], generation=1, session_id='session', job_id=7,
            partition=owner, worker_id=10+owner, pid=100+owner, adjacency_id=20+owner)
        def add(event, phase, rounds, **fields):
            records.append(dict(common,event=event,phase=phase,rounds=rounds,**fields))
        def stats(index):
            reached, edges, active, bucket_edges = states[index][owner]
            return dict(vertices=1+owner, arcs=1, source_count=owner, reached=reached,
                reachable_edges=edges, active=active,
                bucket=0.0 if method=='delta_star' and active else None,
                bucket_edges=bucket_edges if method=='delta_star' else 0)
        add('init',0,0,output_phase=0,mode='topology',**stats(0))
        for phase in range(4):
            mode='topology' if phase==0 else method
            add('decide',phase,max(0,phase-1),mode=mode,
                bucket=0.0 if mode=='delta_star' else None)
            emitted = 1 if phase==0 else states[phase][owner][1 if method=='reference' else 3]
            received = 1 if phase==0 else states[phase][1-owner][1 if method=='reference' else 3]
            add('apply',phase,phase,mode=mode,output_phase=phase+1,
                examined_edges=emitted,examined_vertices=1+owner if mode!='delta_star' else states[phase][owner][2],
                emitted_messages=emitted,received_messages=received,**stats(phase+1))
        add('result',4,3,converged=True,reached=3)
        add('close',4,3)
        for vertex,(distance,hops,parent) in expected.items():
            if vertex%2==owner:
                rows.append(dict(id=vertex,distance=distance,hops=hops,parent=parent,owner=owner,
                    worker_id=10+owner,pid=100+owner,adjacency_id=20+owner,phase=4,rounds=3,reached=3,converged=1))
    return base,records,rows


@pytest.mark.parametrize('method',['reference','delta_star'])
def test_weighted_vectors_and_complete_rounds(method):
    base,records,rows=fixture(method)
    assert validate_rows(rows,IDS,EDGES,base)['reached']==3
    result=validate_events(records,rows,base)
    assert result['rounds']==3 and result['native_phase_count']==10
    assert result['trace'][0]['emitted_messages']==2


def test_oracle_handles_zero_cycles_duplicates_extremes_ties_and_isolates():
    lo,hi=-(1<<63),(1<<63)-1
    assert reference([lo,hi,0,1,9],[(lo,0,1.0),(lo,1,1.0),(0,1,0.0),
        (1,0,0.0),(0,hi,0.0),(1,hi,0.0),(0,hi,0.0)],lo)=={
        lo:(0.0,0,lo),0:(1.0,1,lo),1:(1.0,1,lo),hi:(1.0,2,0),9:None}


@pytest.mark.parametrize('fault',['duplicate','owner','distance','hops','parent','phase','bool','nan','null'])
def test_result_corruption_is_rejected(fault):
    base,_,rows=fixture()
    if fault=='duplicate':rows.append(dict(rows[0]))
    elif fault=='nan':rows[0]['distance']=float('nan')
    elif fault=='null':rows[0]['distance']=None
    elif fault=='bool':rows[0]['hops']=True
    else:rows[0][fault]+=1
    with pytest.raises(AssertionError):validate_rows(rows,IDS,EDGES,base)


@pytest.mark.parametrize('fault',['missing','duplicate','origin','mode','messages','round','identity',
    'bucket','edge-count','shape','active','source-count','result-reached'])
def test_event_corruption_is_rejected(fault):
    base,records,rows=fixture();records=deepcopy(records)
    target=next(r for r in records if r['event']=='apply' and r['phase']==2)
    if fault=='missing':records.remove(target)
    elif fault=='duplicate':records.append(dict(target))
    elif fault=='origin':target['pid']+=1
    elif fault=='mode':target['mode']='done'
    elif fault=='messages':target['received_messages']+=1
    elif fault=='round':target['rounds']+=1
    elif fault=='identity':target['snapshot_id']='foreign'
    elif fault=='bucket':next(r for r in records if r['event']=='decide' and r['phase']==2)['bucket']=1.0
    elif fault=='edge-count':target['examined_edges']+=1
    elif fault=='shape':target['arcs']+=1
    elif fault=='active':target['active']=99
    elif fault=='source-count':next(r for r in records if r['event']=='init')['source_count']+=1
    else:next(r for r in records if r['event']=='result')['reached']=2
    with pytest.raises(AssertionError):validate_events(records,rows,base)


def cap_fixture():
    base,events,_=fixture();base['max_rounds']=0
    records=[]
    for r in events:
        r=dict(r)
        if r['event']=='result' or r['event'] in ('decide','apply') and r['phase']>0:continue
        if r['event']=='close':r.update(phase=1,rounds=0)
        records.append(r)
    cause=dict(next(r for r in records if r['event']=='close'),event='failure',code='sssp_round_cap',
        outcome='nonconverged',max_rounds=0,active=1,reached=1)
    records.append(cause)
    stages=[dict(session_id='session',job_id=7,stage=i,slot_group='worker-extension:one',partitions=2,
        placement='Worker',mode='Pipelined') for i in range(4)]
    tasks=[dict(job_id=7,stage=i,partition=p,worker_id=10+p,attempt=0,status='FAILED' if i==3 else 'SUCCEEDED')
        for i in range(4) for p in range(2)]
    stored=[dict(session_id='session',**{k:v for k,v in t.items() if k!='worker_id'}) for t in tasks]
    args=dict(stages=stages,task_statuses=tasks,stored_tasks=stored,jobs=[dict(session_id='session',job_id=7,status='FAILED')],query_failed=True)
    return base,records,args


def test_cap_retains_measured_active_work_and_terminal_tasks():
    base,records,args=cap_fixture()
    result=validate_cap(records,base,**args)
    assert result['measured_active']==result['reached']==result['complete_pre_cap_barriers']==1


@pytest.mark.parametrize('fault',['cause','synthetic','partial','active','retry','missing-task','owner','job','close','mode'])
def test_cap_evidence_rejects_missing_cause_or_incomplete_cleanup(fault):
    base,records,args=cap_fixture()
    cause=next(r for r in records if r['event']=='failure')
    if fault=='cause':records.remove(cause)
    elif fault=='synthetic':cause['active']=2
    elif fault=='partial':records.append(dict(cause,event='result'))
    elif fault=='active':args['stored_tasks'][0]['status']='RUNNING'
    elif fault=='retry':args['task_statuses'][0]['attempt']=1
    elif fault=='missing-task':args['stored_tasks'].pop()
    elif fault=='owner':args['task_statuses'][0]['worker_id']=999
    elif fault=='job':args['jobs'][0]['status']='CANCELED'
    elif fault=='mode':next(r for r in records if r['event']=='decide')['mode']='done'
    else:records.remove(next(r for r in records if r['event']=='close'))
    with pytest.raises(AssertionError):validate_cap(records,base,**args)

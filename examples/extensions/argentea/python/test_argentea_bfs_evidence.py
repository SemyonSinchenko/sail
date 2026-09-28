import copy

import pytest

from argentea_bfs_evidence import normalize,reference,validate_rows

IDS=[-5,0,1,6,10,11,20,21]
EDGES=[(-5,0),(-5,1),(-5,1),(0,6),(1,6),(6,10),(10,10),(20,21)]


def fixture(*,method='reference',source=-5,directed=True):
    edges=normalize(EDGES,directed=directed)
    req=dict(source=source,partitions=5,max_levels=14,algorithm='bfs_'+method)
    answer=reference(IDS,edges,source)
    reached=sum(v['distance'] is not None for v in answer.values())
    levels=max(v['distance'] for v in answer.values() if v['distance'] is not None)+1
    rows=[dict(id=node,**value,owner=node%5,worker_id=node%5+10,pid=node%5+100,
               adjacency_id=node%5+1000,incoming_adjacency_id=node%5+2000 if method=='direction' else 0,
               phase=15,levels=levels,reached=reached,converged=1) for node,value in answer.items()]
    return rows,edges,req


@pytest.mark.parametrize('method',['reference','frontier','direction'])
def test_negative_source_tied_parent_duplicate_loop_isolate_and_unreachable_edge(method):
    rows,edges,req=fixture(method=method)
    result=validate_rows(rows,IDS,edges,req)
    assert result['reached']==5 and result['levels']==4 and result['unreachable']==[11,20,21]
    assert next(r for r in rows if r['id']==6)['parent']==0
    assert result['all_edge_certificate'] and result['minimum_numeric_parent']


def test_undirected_normalization_changes_reverse_reachability():
    directed,edges,req=fixture(source=10)
    assert validate_rows(directed,IDS,edges,req)['reached']==1
    undirected,edges,req=fixture(source=10,directed=False)
    result=validate_rows(undirected,IDS,edges,req)
    assert result['reached']==5 and result['levels']==4
    assert len(edges)==2*len(EDGES)


@pytest.mark.parametrize('fault',['missing','duplicate','distance','parent_tie','unreachable_parent',
    'reachable_null','owner','phase','levels','counter_disagreement','reached','float_distance','source_parent',
    'unexpected_incoming','missing_incoming','nonconverged'])
def test_wrong_bfs_answers_or_metadata_cannot_pass(fault):
    rows,edges,req=fixture(method='direction' if fault=='missing_incoming' else 'reference')
    by_id={r['id']:r for r in rows}
    if fault=='missing':rows.pop()
    if fault=='duplicate':rows.append(copy.deepcopy(rows[0]))
    if fault=='distance':by_id[6]['distance']=by_id[6]['hops']=3
    if fault=='parent_tie':by_id[6]['parent']=1
    if fault=='unreachable_parent':by_id[11]['parent']=-5
    if fault=='reachable_null':
        for name in ('distance','hops','parent'):by_id[10][name]=None
    if fault=='owner':rows[0]['owner']=3
    if fault=='phase':rows[0]['phase']=0
    if fault=='levels':
        for row in rows:row['levels']=3
    if fault=='counter_disagreement':rows[0]['levels']=5
    if fault=='reached':
        for row in rows:row['reached']=6
    if fault=='float_distance':by_id[6]['distance']=2.0
    if fault=='source_parent':by_id[-5]['parent']=0
    if fault=='unexpected_incoming':rows[0]['incoming_adjacency_id']=123
    if fault=='missing_incoming':rows[0]['incoming_adjacency_id']=0
    if fault=='nonconverged':rows[0]['converged']=0
    with pytest.raises(AssertionError):validate_rows(rows,IDS,edges,req)


def test_extreme_bigint_ids_and_deterministic_parent_order():
    ids=[-(1<<63),-1,0,(1<<63)-1]
    edges=[(-(1<<63),0),(-(1<<63),-1),(0,(1<<63)-1),(-1,(1<<63)-1)]
    forward=reference(ids,edges,ids[0]);reverse=reference(list(reversed(ids)),list(reversed(edges)),ids[0])
    assert forward==reverse and forward[ids[-1]]==dict(distance=2,hops=2,parent=-1)


def audit_fixture(method='reference'):
    from collections import Counter
    rows,edges,req=fixture(method=method)
    req.update(operation_id='op',snapshot_id='snapshot',generation=1,vertices=len(IDS),alpha=14,beta=24)
    degrees=Counter(start for start,_ in edges);records=[]
    for owner in range(5):
        base=dict(protocol=3,algorithm=req['algorithm'],operation_id='op',snapshot_id='snapshot',generation=1,
                  partition=owner,worker_id=owner%2+10,pid=owner%2+100,adjacency_id=owner+1000,job_id=7,session_id='s')
        owned=[r for r in rows if r['owner']==owner]
        def stats(level):
            visited=[r for r in owned if r['distance'] is not None and r['distance']<=level]
            frontier=[r for r in owned if r['distance']==level]
            return dict(vertices=len(owned),levels=level,local_reached=len(visited),frontier_vertices=len(frontier),
                        frontier_edges=sum(degrees[r['id']] for r in frontier),remaining_edges=sum(degrees[r['id']] for r in owned if r not in visited))
        records.append(dict(base,event='init',phase=0,output_phase=0,mode='topology',incoming_adjacency_id=0,**stats(0)))
        level=0;incoming=0
        for phase in range(15):
            mode='topology' if phase==0 else 'done' if phase>4 else {'reference':'reference','frontier':'push','direction':'pull'}[method]
            before=stats(level)
            records.append(dict(base,event='decide',phase=phase,mode=mode,levels=level,
                local_reached=before['local_reached'],frontier_vertices=before['frontier_vertices'],incoming_adjacency_id=incoming))
            if phase==0 and method=='direction':incoming=owner+2000
            if mode in ('reference','push','pull'):level+=1
            records.append(dict(base,event='apply',phase=phase,output_phase=phase+1,mode=mode,
                incoming_adjacency_id=incoming,examined_edges=0 if mode=='done' else 1,examined_vertices=0,
                emitted_messages=0,received_messages=0,**stats(level)))
        records.append(dict(base,event='result',phase=15,converged=True,levels=4,reached=5,
                            local_reached=stats(4)['local_reached'],incoming_adjacency_id=incoming))
        records.append(dict(base,event='close',phase=15,levels=4,local_reached=stats(4)['local_reached'],incoming_adjacency_id=incoming))
    stages=[dict(session_id='s',job_id=7,stage=i,partitions=5,placement='Worker',slot_group='worker-extension:3',mode='Pipelined') for i in range(32)]
    tasks=[dict(job_id=7,stage=stage,partition=owner,worker_id=owner%2+10,attempt=0,status=status)
           for stage in range(32) for owner in range(5) for status in ('RUNNING','SUCCEEDED')]
    endpoints=[dict(worker_id=i+10,host='host-'+host) for i,host in enumerate(('a','b'))]
    supervisors=[dict(event='sail_remote_started',worker_id=str(i+10),pid=i+100,hostname='host-'+host) for i,host in enumerate(('a','b'))]
    return rows,edges,req,records,stages,tasks,endpoints,supervisors


def audit(values):
    from argentea_bfs_evidence import validate_audit
    rows,edges,req,records,stages,tasks,endpoints,supervisors=values
    return validate_audit(records,rows,req,stages=stages,task_statuses=tasks,worker_endpoints=endpoints,
                          edges=edges,supervisors=supervisors,required_hosts=['host-a','host-b'])


@pytest.mark.parametrize('method',['reference','frontier','direction'])
def test_bfs_phase_audit_proves_32_stages_four_levels_and_empty_owners(method):
    result=audit(audit_fixture(method))
    assert result['native_phases']==32 and result['levels']==4 and result['reached']==5
    assert result['empty_owners']==[2,3,4] and result['closed_all_owners']
    assert result['native_host_graph']['vertices_by_host']=={'host-a':4,'host-b':4}
    assert result['native_host_graph']['cross_host_edge_count']==5
    assert [r['mode'] for r in result['trace']][5:]==['done']*10


@pytest.mark.parametrize('fault',['protocol','missing_owner','identity','missing_apply','replayed_decide',
    'wrong_mode','wrong_level','wrong_remaining','wrong_reached','wrong_frontier','wrong_incoming','bad_work',
    'done_work','result','close','retry','stage_count','stage_width','stage_group','failed_task'])
def test_bfs_audit_rejects_stage_barrier_work_and_identity_defects(fault):
    values=audit_fixture('direction');rows,edges,req,records,stages,tasks,endpoints,supervisors=values
    applied=[r for r in records if r['event']=='apply'];decided=[r for r in records if r['event']=='decide']
    if fault=='protocol':records[0]['protocol']=2
    if fault=='missing_owner':records[:]=[r for r in records if r['partition']!=4]
    if fault=='identity':records[-1]['adjacency_id']=999
    if fault=='missing_apply':records.remove(applied[0])
    if fault=='replayed_decide':records.append(copy.deepcopy(decided[0]))
    if fault=='wrong_mode':decided[1]['mode']='push'
    if fault=='wrong_level':applied[1]['levels']=0
    if fault=='wrong_remaining':applied[1]['remaining_edges']+=1
    if fault=='wrong_reached':applied[1]['local_reached']+=1
    if fault=='wrong_frontier':applied[1]['frontier_vertices']+=1
    if fault=='wrong_incoming':applied[2]['incoming_adjacency_id']+=1
    if fault=='bad_work':applied[1]['examined_edges']=-1
    if fault=='done_work':applied[10]['examined_edges']=1
    if fault=='result':next(r for r in records if r['event']=='result')['reached']=6
    if fault=='close':records.pop()
    if fault=='retry':tasks[0]['attempt']=1
    if fault=='stage_count':stages.pop()
    if fault=='stage_width':stages[0]['partitions']=4
    if fault=='stage_group':stages[0]['slot_group']='worker-extension:other'
    if fault=='failed_task':tasks[1]['status']='FAILED'
    with pytest.raises(AssertionError):audit(values)


def cap_fixture():
    rows,edges,req,records,stages,tasks,endpoints,supervisors=audit_fixture()
    req['max_levels']=0
    records=[r for r in records if r['event']=='init' or r['event'] in ('decide','apply') and r['phase']==0]
    for init in [r for r in records if r['event']=='init']:
        records.append(dict(init,event='close',phase=1))
    first=records[0]
    records.append(dict(first,event='failure',phase=1,code='bfs_level_cap',outcome='nonconverged',max_levels=0,frontier_vertices=1,reached=1))
    stages=stages[:4];tasks=[t for t in tasks if t['stage']<4];tasks[-1]['status']='FAILED'
    return req,records,stages,tasks


def test_bfs_cap_requires_typed_actual_cause_after_complete_topology():
    from argentea_bfs_evidence import validate_cap
    req,records,stages,tasks=cap_fixture()
    result=validate_cap(records,req,stages=stages,task_statuses=tasks,query_failed=True)
    assert result['native_cause']=='bfs_level_cap' and result['complete_topology'] and result['no_result']


@pytest.mark.parametrize('fault',['no_cause','wrong_code','no_frontier','wrong_reached','wrong_phase',
                                  'missing_topology','missing_close','retry','query_success'])
def test_bfs_cancellation_without_complete_cap_evidence_fails(fault):
    from argentea_bfs_evidence import validate_cap
    req,records,stages,tasks=cap_fixture();cause=records[-1];failed=True
    if fault=='no_cause':records.pop()
    if fault=='wrong_code':cause['code']='cancelled'
    if fault=='no_frontier':cause['frontier_vertices']=0
    if fault=='wrong_reached':cause['reached']=2
    if fault=='wrong_phase':cause['phase']=0
    if fault=='missing_topology':records.remove(next(r for r in records if r['event']=='apply'))
    if fault=='missing_close':records.remove(next(r for r in records if r['event']=='close'))
    if fault=='retry':tasks[0]['attempt']=1
    if fault=='query_success':failed=False
    with pytest.raises(AssertionError):validate_cap(records,req,stages=stages,task_statuses=tasks,query_failed=failed)

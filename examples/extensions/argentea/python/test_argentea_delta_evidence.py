import copy
from pathlib import Path
import sys

import pytest

sys.path.insert(0,str(Path(__file__).parent))
from argentea_delta_evidence import reference, residual, transition, validate_audit, validate_cap, validate_rows


def request():
    return dict(version=2,algorithm='pagerank_delta',operation_id='op',snapshot_id='snapshot',generation=1,
                max_pushes=7,partitions=5,vertices=3,damping=.85,tolerance=1e-3)


def rows_for(ranks,edges,req,*,pushes=0,certificates=1):
    value=residual(list(ranks),edges,ranks,req['damping'])
    return [dict(id=node,pagerank=rank,owner=node%req['partitions'],worker_id=node%req['partitions']%2+10,
        pid=node%req['partitions']%2+100,adjacency_id=node%req['partitions']+1000,phase=2*req['max_pushes']+1,
        pushes=pushes,certificate_passes=certificates,residual_l1=value,
        stationary_error_bound=value/(1-req['damping']),converged=1) for node,rank in ranks.items()]


def test_independent_signed_fixture_vector_and_certificate():
    ids,edges,req=[0,1,2],[(0,1),(1,1)],request()
    ranks={node:1/3 for node in ids}
    initial=transition(ids,edges,ranks,.85)
    assert initial[0]-ranks[0]<0 and initial[2]-ranks[2]<0
    for _ in range(7): ranks=transition(ids,edges,ranks,.85)
    rows=rows_for(ranks,edges,req,pushes=7,certificates=2)
    result=validate_rows(rows,ids,edges,req)
    assert 0<result['true_residual']<1e-3
    assert result['stationary_error']<=result['stationary_error_bound']
    target,certificate=reference(ids,edges,.85)
    assert target[0]==pytest.approx(3/43) and target[1]==pytest.approx(37/43)
    assert certificate<=1e-15


@pytest.mark.parametrize('fault',['missing','duplicate','negative','nan','normalization','owner','false_certificate','inconsistent_certificate','not_converged'])
def test_rank_validation_rejects_wrong_or_uncertified_output(fault):
    req=request(); edges=[(0,1),(1,1)]
    ranks,_=reference([0,1,2],edges,.85)
    rows=rows_for(ranks,edges,req,pushes=2,certificates=2)
    if fault=='missing': rows.pop()
    if fault=='duplicate': rows.append(dict(rows[0]))
    if fault=='negative': rows[0]['pagerank']=-1
    if fault=='nan': rows[0]['pagerank']=float('nan')
    if fault=='normalization': rows[0]['pagerank']+=.1
    if fault=='owner': rows[0]['owner']=1
    if fault=='false_certificate':
        for row in rows: row['pagerank']=1/3
    if fault=='inconsistent_certificate': rows[-1]['residual_l1']=1e-4
    if fault=='not_converged': rows[-1]['converged']=0
    with pytest.raises(AssertionError): validate_rows(rows,[0,1,2],edges,req)


def fixture():
    req=request(); edges=[(0,1),(1,2),(2,0)]
    rows=rows_for({node:1/3 for node in range(3)},edges,req)
    records=[]; slots=15
    for owner in range(5):
        base=dict(protocol=2,algorithm='pagerank_delta',operation_id='op',snapshot_id='snapshot',generation=1,
                  partition=owner,worker_id=owner%2+10,pid=owner%2+100,adjacency_id=owner+1000,job_id=7,session_id='s')
        size=int(owner<3)
        records.append(dict(base,event='init',phase=0))
        for phase in range(slots+1):
            records.append(dict(base,event='statistics',phase=max(0,phase-1),output_phase=phase,
                vertices=size,mass=size/3,residual_l1=0.,minimum_score=1/3 if size else None,
                mode=0 if phase==0 else 2 if phase==1 else 3,pushes=0,certificate_passes=int(phase>0)))
        for phase in range(slots):
            records.append(dict(base,event='emit',phase=phase,mode=2 if phase==0 else 3))
        records.append(dict(base,event='result',phase=slots,converged=True,pushes=0,certificate_passes=1,
                            residual_l1=0.,stationary_error_bound=0.))
        records.append(dict(base,event='close',phase=slots,pushes=0,certificate_passes=1))
    stages=[dict(session_id='s',job_id=7,stage=i,partitions=5,placement='Worker',slot_group='worker-extension:2',mode='Pipelined') for i in range(32)]
    stages.append(dict(session_id='s',job_id=7,stage=32,partitions=1,placement='Driver',slot_group='default',mode='Blocking'))
    tasks=[dict(job_id=7,stage=stage,partition=owner,worker_id=owner%2+10,attempt=0,status=status)
           for stage in range(32) for owner in range(5) for status in ('RUNNING','SUCCEEDED')]
    endpoints=[dict(worker_id=i+10,host='host-'+host) for i,host in enumerate(('a','b'))]
    supervisors=[dict(event='sail_remote_started',worker_id=str(i+10),pid=i+100,hostname='host-'+host) for i,host in enumerate(('a','b'))]
    return req,rows,records,stages,tasks,endpoints,supervisors,edges


def audit_fixture(values):
    req,rows,records,stages,tasks,endpoints,supervisors,edges=values
    return validate_audit(records,rows,req,stages=stages,task_statuses=tasks,worker_endpoints=endpoints,
                          supervisors=supervisors,required_hosts=['host-a','host-b'],edges=edges)


def test_audit_distinguishes_zero_pushes_one_certificate_and_32_native_phases():
    result=audit_fixture(fixture())
    assert result['actual_pushes']==0 and result['certificate_passes']==1
    assert result['native_phases']==32 and result['transport_work_slots']==15
    assert result['empty_owners']==[3,4]
    assert len(result['native_task_statuses'])==320
    assert result['native_host_graph']['vertices_by_host']=={'host-a':2,'host-b':1}
    assert result['native_host_graph']['cross_host_edge_count']==2


@pytest.mark.parametrize('fault',['protocol','empty_owner','adjacency','missing_stat','duplicate_stat','phase',
    'empty_minimum','different_mode','counter','missing_emit','result_phase','missing_close','result_certificate',
    'another_job','width','group','retry','wrong_worker','failed_task','missing_success'])
def test_v2_phase_owner_and_task_defects_never_become_a_pass(fault):
    req,rows,records,stages,tasks,endpoints,supervisors,edges=fixture()
    stats=[r for r in records if r['event']=='statistics']
    if fault=='protocol': records[0]['protocol']=1
    if fault=='empty_owner': records=[r for r in records if r['partition']!=4]
    if fault=='adjacency': records[-1]['adjacency_id']=999
    if fault=='missing_stat': records.remove(stats[0])
    if fault=='duplicate_stat': records.append(copy.deepcopy(stats[0]))
    if fault=='phase': stats[2]['phase']=99
    if fault=='empty_minimum': stats[-1]['minimum_score']=float('inf')
    if fault=='different_mode': stats[2]['mode']=1
    if fault=='counter': stats[2]['pushes']=1
    if fault=='missing_emit': records.remove(next(r for r in records if r['event']=='emit'))
    if fault=='result_phase': next(r for r in records if r['event']=='result')['phase']=14
    if fault=='missing_close': records.pop()
    if fault=='result_certificate': next(r for r in records if r['event']=='result')['residual_l1']=.01
    if fault=='another_job': records[-1]['job_id']=8
    if fault=='width': stages[0]['partitions']=4
    if fault=='group': stages[0]['slot_group']='worker-extension:other'
    if fault=='retry': tasks[0]['attempt']=1
    if fault=='wrong_worker': tasks[0]['worker_id']=11
    if fault=='failed_task': tasks[1]['status']='FAILED'
    if fault=='missing_success': tasks.pop(1)
    with pytest.raises(AssertionError): audit_fixture((req,rows,records,stages,tasks,endpoints,supervisors,edges))


def test_cap_failure_needs_post_init_close_and_no_native_retry_or_result():
    req,_,records,stages,tasks,_,_,_=fixture()
    req['max_pushes']=0
    records=[r for r in records if r['event'] in ('init','close')]
    stages=stages[:4]
    tasks=[t for t in tasks if t['stage']<4]
    tasks[-1]['status']='FAILED'
    result=validate_cap(records,req,stages=stages,task_statuses=tasks)
    assert result['post_init_cap'] and result['no_result'] and result['native_phases']==4
    tasks[0]['attempt']=1
    with pytest.raises(AssertionError): validate_cap(records,req,stages=stages,task_statuses=tasks)

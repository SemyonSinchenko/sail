"""Negative controls for independent WCC answers and native event ownership."""
from copy import deepcopy
import pytest
from argentea_wcc_client import request
from argentea_wcc_evidence import reference,validate_rows,validate_events


def fixture():
    base=request(vertices_count=2,method='star',max_rounds=0,partitions=2)
    records=[];rows=[]
    modes=['topology','neighbors','normalize_route','normalize_return']
    for owner in range(2):
        common=dict(protocol=4,algorithm=base['algorithm'],operation_id=base['operation_id'],
                    snapshot_id=base['snapshot_id'],generation=1,session_id='session',job_id=7,
                    partition=owner,worker_id=10+owner,pid=100+owner,adjacency_id=20+owner)
        def add(event,phase,**fields):
            records.append(dict(common,event=event,phase=phase,rounds=0,
                                incoming_adjacency_id=0 if event=='init' or (event=='decide' and phase==0) else 30+owner,**fields))
        add('init',0,output_phase=0,mode='topology',vertices=1)
        for phase,mode in enumerate(modes):
            add('decide',phase,mode=mode)
            messages=int(mode in ('normalize_route','normalize_return'))
            add('apply',phase,mode=mode,output_phase=phase+1,vertices=1,
                examined_edges=0,examined_vertices=1,emitted_messages=messages,received_messages=messages,
                changed=0,crossing=0,members=int(mode=='normalize_route'))
        add('result',4,converged=True);add('close',4)
        rows.append(dict(id=owner,component=owner,owner=owner,worker_id=10+owner,pid=100+owner,
                         adjacency_id=20+owner,incoming_adjacency_id=30+owner,phase=4,rounds=0,converged=1))
    return base,records,rows


def test_union_find_signed_extremes_loops_duplicates_and_isolates():
    lo,hi=-(1<<63),(1<<63)-1
    assert reference([lo,hi,0,1],[(hi,lo),(hi,lo),(0,0)])=={lo:lo,hi:lo,0:0,1:1}


def test_empty_edge_graph_can_normalize_without_a_contraction():
    base,records,rows=fixture()
    assert validate_rows(rows,[0,1],[],base)['components']==2
    audit=validate_events(records,rows,base)
    assert audit['rounds']==0 and audit['native_phase_count']==10


@pytest.mark.parametrize('fault',['duplicate','owner','label','phase','bool'])
def test_result_corruption_is_rejected(fault):
    base,_,rows=fixture()
    if fault=='duplicate':rows.append(dict(rows[0]))
    elif fault=='owner':rows[0]['owner']=1
    elif fault=='label':rows[1]['component']=0
    elif fault=='phase':rows[0]['phase']=3
    else:rows[0]['component']=False
    with pytest.raises(AssertionError):validate_rows(rows,[0,1],[],base)


@pytest.mark.parametrize('fault',['missing','duplicate','origin','incoming','mode','messages','round','identity'])
def test_event_corruption_is_rejected(fault):
    base,records,rows=fixture();records=deepcopy(records)
    target=next(r for r in records if r['event']=='apply' and r['phase']==2)
    if fault=='missing':records.remove(target)
    elif fault=='duplicate':records.append(dict(target))
    elif fault=='origin':target['pid']+=1
    elif fault=='incoming':target['incoming_adjacency_id']+=1
    elif fault=='mode':target['mode']='hook_route'
    elif fault=='messages':target['received_messages']+=1
    elif fault=='round':target['rounds']+=1
    else:target['snapshot_id']='foreign'
    with pytest.raises(AssertionError):validate_events(records,rows,base)

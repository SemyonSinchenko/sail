from pathlib import Path
import sys

import pytest

sys.path.insert(0,str(Path(__file__).parent))
from argentea_exercise import IDS, EDGES
from argentea_host_evidence import validate_host_graph

HOSTS = {'host-a','host-b'}


def fixture(partitions):
    rows = [dict(id=node,owner=node%partitions,worker_id=(node%partitions)%2+10,
                 pid=(node%partitions)%2+100) for node in IDS]
    supervisors = [dict(event='sail_remote_started',worker_id=str(i+10),pid=i+100,hostname='host-'+host)
                   for i,host in enumerate(('a','b'))]
    return rows,supervisors


def test_five_owner_fixture_has_nonempty_vertices_on_both_hosts_and_crossing_arcs():
    rows,supervisors = fixture(5)
    assert 1 not in {row['owner'] for row in rows}  # Empty-owner protocol proof remains separate.
    result = validate_host_graph(rows,EDGES,supervisors,required_hosts=HOSTS)
    assert result['vertices']==6 and result['directed_edges']==6
    assert result['vertices_by_host']=={'host-a':4,'host-b':2}
    assert result['source_edges_by_host']=={'host-a':5,'host-b':1}
    assert result['cross_host_edge_count']==3
    assert [(r['src'],r['dst']) for r in result['cross_host_edges']]==[(0,3),(0,3),(3,2)]


def test_three_owner_fixture_only_proves_empty_owner_participation_on_second_host():
    rows,supervisors = fixture(3)
    assert {row['owner'] for row in rows}=={0,2}
    with pytest.raises(AssertionError,match='only empty owners'):
        validate_host_graph(rows,EDGES,supervisors,required_hosts=HOSTS)


def test_nonempty_vertices_alone_do_not_prove_a_cross_host_graph_edge():
    rows,supervisors = fixture(5)
    with pytest.raises(AssertionError,match='no graph edge crossing'):
        validate_host_graph(rows,[(node,node) for node in IDS],supervisors,required_hosts=HOSTS)


@pytest.mark.parametrize('fault',['missing_pid','conflicting_host','duplicate_vertex','unexpected_host','missing_endpoint'])
def test_ambiguous_or_incomplete_host_graph_evidence_fails(fault):
    rows,supervisors = fixture(5)
    edges=list(EDGES)
    if fault=='missing_pid': rows[0]['pid']=999
    if fault=='conflicting_host': supervisors.append(dict(supervisors[0],hostname='other'))
    if fault=='duplicate_vertex': rows.append(dict(rows[0]))
    if fault=='unexpected_host': supervisors[1]['hostname']='other'
    if fault=='missing_endpoint': edges.append((0,999))
    with pytest.raises(AssertionError):
        validate_host_graph(rows,edges,supervisors,required_hosts=HOSTS)


def test_qualifier_requires_graph_placement_only_for_physical_host_gate(monkeypatch):
    import qualify
    rows,supervisors=fixture(3)
    monkeypatch.setattr(qualify,'parse_log',lambda _: ([],supervisors))
    monkeypatch.setattr(qualify,'parse_worker_tasks',lambda _: [])
    monkeypatch.setattr(qualify,'completed_worker_tasks',lambda _: [])
    monkeypatch.setattr(qualify,'validate_audit',lambda *args,**kwargs:dict(generic_native_audit=True))
    check=dict(rows=rows,edges=EDGES,request={},iterations=2,worker_endpoints=[],stages=[])
    ordinary=dict(checks=check)
    qualify.audit(ordinary,'',minimum_workers=2)
    assert 'native_host_graph' not in ordinary
    with pytest.raises(AssertionError,match='only empty owners'):
        qualify.audit(dict(checks=check),'',minimum_workers=2,required_hosts=HOSTS)
    check['rows'],_=fixture(5)
    physical=dict(checks=check)
    qualify.audit(physical,'',minimum_workers=2,required_hosts=HOSTS)
    assert physical['native_host_graph']['cross_host_edge_count']==3

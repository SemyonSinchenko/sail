"""BFS v3 wire, scalar validation, and retained-result ownership controls."""
import json
from types import SimpleNamespace

import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.types import LongType,StructField,StructType

import argentea_bfs_client as client
from pyspark_pecan import CancellationToken
from test_argentea_delta_client import envelope,source,scalar_collect,wrapper


@pytest.mark.parametrize('levels',range(15))
@pytest.mark.parametrize('method',client.METHODS)
def test_static_bfs_schedule_and_wire_identity(envelope,levels,method):
    frame,request=client.build_plan(object(),source('v'),source('e'),vertices_count=7,
                                   source=-(1<<63),method=method,max_levels=levels,
                                   max_phase_budget=2*levels+4,partitions=5)
    observed=[];node=frame._plan.plan(None)
    while True:
        decoded=envelope.FromString(node.extension.value)
        assert decoded.payload_type_url==client.TYPE_URL and decoded.envelope_version==1
        payload=json.loads(decoded.payload)
        assert payload['version']==3 and payload['algorithm']=='bfs_'+method
        assert all(payload[key]==value for key,value in request.items())
        assert payload['source']==-(1<<63) and payload['alpha']==14 and payload['beta']==24
        observed.append((payload['verb'],payload['phase']))
        if payload['verb']=='init':
            assert len(decoded.inputs)==2
            break
        assert len(decoded.inputs)==1
        node=decoded.inputs[0].root
    expected=[('init',0)]
    for phase in range(levels+1):expected.extend([('decide',phase),('apply',phase)])
    expected.append(('result',levels+1))
    assert list(reversed(observed))==expected and len(observed)==2*levels+4<=32
    assert set(request)=={'version','algorithm','source','max_levels','alpha','beta','partitions','vertices',
        'max_phase_budget','batch_rows','generation','operation_id','snapshot_id'}


@pytest.mark.parametrize('changes',[
    {'source':True},{'source':1<<63},{'source':-(1<<63)-1},{'source':1.0},
    {'method':'sssp'},{'method':None},{'directed':1},
    {'max_levels':-1},{'max_levels':15},{'max_levels':True},{'max_levels':1.0},
    {'partitions':0},{'partitions':65},{'partitions':True},
    {'alpha':0},{'alpha':1<<64},{'alpha':False},{'beta':0},{'beta':float('inf')},
    {'max_phase_budget':3},{'max_phase_budget':129},{'max_phase_budget':True},
    {'max_levels':1,'max_phase_budget':4},{'batch_rows':0},{'batch_rows':65_537},
])
def test_options_fail_before_staging_or_session(changes):
    options=dict(source=0);options.update(changes)
    with pytest.raises(ValueError):client.ArgenteaBfs(object()).bfs(object(),object(),**options)


@pytest.mark.parametrize('changes',[{'vertices_count':0},{'vertices_count':1<<63},{'vertices_count':True},
    {'generation':0},{'generation':1<<64},{'operation_id':'bad'},{'snapshot_id':123}])
def test_request_identity_validation(changes):
    options=dict(vertices_count=3,source=0);options.update(changes)
    with pytest.raises(ValueError):client.request(**options)


def diagnostic_row(*,vertices=7,levels=4,reached=5,phase=15):
    values=dict(phase=phase,levels=levels,reached=reached,converged=1)
    row={name+suffix:(vertices if suffix=='_count' else value)
         for name,value in values.items() for suffix in ('_min','_max','_count')}
    row.update(distance_count=reached,hops_count=reached,parent_count=reached,invalid_rows=0,source_rows=1)
    return row


class Stored:
    columns=['id','distance','hops','parent','phase','levels','reached','converged']
    schema=StructType([StructField(name,LongType(),name in ('distance','hops','parent')) for name in columns])
    def __init__(self):self.aggregates=[];self.exports=[]
    def agg(self,*expressions):
        self.aggregates.append(expressions)
        return source('stored').agg(*expressions)
    def collect(self):raise AssertionError('BFS rows cannot collect in the production client')
    def select(self,*columns):
        assert columns==('id','distance','hops','parent')
        return SimpleNamespace(write=SimpleNamespace(mode=lambda mode:SimpleNamespace(
            parquet=lambda path:self.exports.append((path,mode)))))


def test_stored_diagnostics_collect_only_one_scalar_aggregate(scalar_collect):
    scalar_collect.row=diagnostic_row()
    stored=Stored();base=client.request(vertices_count=7,source=-5)
    result=client._diagnostics(stored,base,CancellationToken())
    assert result==dict(phase=15,levels=4,reached=5,converged=1)
    assert len(scalar_collect.calls)==len(stored.aggregates)==1
    assert len(scalar_collect.calls[0].aggregate.aggregate_expressions)==17


@pytest.mark.parametrize('changes',[
    {'levels_max':5},{'reached_count':6},{'converged_min':0,'converged_max':0},
    {'phase_min':14,'phase_max':14},{'levels_min':0,'levels_max':0},
    {'levels_min':15,'levels_max':15},{'reached_min':0,'reached_max':0},
    {'reached_min':8,'reached_max':8},{'distance_count':4},{'hops_count':4},
    {'parent_count':4},{'invalid_rows':1},{'source_rows':0},{'source_rows':2},
])
def test_bad_result_diagnostics_cannot_be_retained(scalar_collect,changes):
    scalar_collect.row=diagnostic_row();scalar_collect.row.update(changes)
    with pytest.raises(RuntimeError):
        client._diagnostics(Stored(),client.request(vertices_count=7,source=0),CancellationToken())


@pytest.fixture
def bfs_wrapper(wrapper,scalar_collect,monkeypatch):
    scalar_collect.row=diagnostic_row()
    wrapper.stored=Stored();wrapper.count_calls=[]
    wrapper.spark.session_id='fixture-session'
    def count(frame):
        pytest.fail('BFS wrapper must not issue input validation count queries')
    monkeypatch.setattr(DataFrame,'count',count)
    return wrapper


@pytest.mark.parametrize('method',client.METHODS)
@pytest.mark.parametrize('directed',[False,True])
def test_public_bfs_keeps_one_native_materialization_and_server_side_arc_normalization(bfs_wrapper,method,directed):
    observed=[];state=bfs_wrapper
    result=client.ArgenteaBfs(state.spark,observer=observed.append).bfs(
        source('v',state.spark),source('e',state.spark),source=-5,method=method,directed=directed,partitions=3)
    assert state.count_calls==[] and state.events.count('write-native-result')==1
    assert len(observed)==1 and observed[0]['native_phase_count']==32
    assert len(observed[0]['view_registrations'])==32 and not state.views
    # Inspect the serialized init edge child, before the owner projection.
    from google.protobuf import descriptor_pool,message_factory
    Envelope=message_factory.GetMessageClass(descriptor_pool.Default().FindMessageTypeByName('argentea.delta_test.Envelope'))
    from pyspark.sql.connect import proto
    init=proto.Plan.FromString(observed[0]['view_registrations'][0]['plan_bytes']).root
    edge=Envelope.FromString(init.extension.value).inputs[1].root
    assert edge.HasField('project')
    assert edge.project.input.HasField('set_op') is (not directed)
    if not directed:
        union=edge.project.input.set_op
        assert union.is_all and union.by_name
        reversed_columns=union.right_input.project.expressions
        assert [e.alias.name[0] for e in reversed_columns]==['src','dst']
        assert [e.alias.expr.unresolved_attribute.unparsed_identifier for e in reversed_columns]==['dst','src']
    assert result.levels==result.iterations==4 and result.reached==5 and result.converged is True
    assert result.algorithm=='argentea-bfs_'+method and result.native_phase_count==32
    assert result.native_frame is state.stored and not state.run.closed
    result.write_parquet('caller/bfs')
    assert state.stored.exports==[('caller/bfs','error')]
    result.close()
    with pytest.raises(RuntimeError,match='closed'):_=result.frame


def test_bfs_native_failure_defers_uncertain_write_but_drops_views(bfs_wrapper):
    failure=RuntimeError('BFS max_levels exhausted')
    bfs_wrapper.failure,bfs_wrapper.uncertain=failure,True
    with pytest.raises(RuntimeError) as caught:
        client.ArgenteaBfs(bfs_wrapper.spark).bfs(source('v'),source('e'),source=0,partitions=3)
    assert caught.value is failure and failure.cleanup_deferred
    assert failure.view_cleanup_deferred is False and not bfs_wrapper.views
    assert not bfs_wrapper.run.closed


@pytest.mark.parametrize('method',client.METHODS)
def test_explicit_extended_budget_and_boundary(method):
    value=client.request(vertices_count=62,source=0,method=method,max_levels=62,max_phase_budget=128)
    assert len(client.phases(value['max_levels']))==128
    with pytest.raises(ValueError):
        client.request(vertices_count=62,source=0,method=method,max_levels=63,max_phase_budget=128)
    with pytest.raises(ValueError):
        client.request(vertices_count=62,source=0,method=method,max_levels=62)


@pytest.mark.parametrize('method',client.METHODS)
def test_public_bfs_explicit_budget_reaches_view_composer(bfs_wrapper,scalar_collect,method):
    scalar_collect.row=diagnostic_row(phase=63)
    observed=[];state=bfs_wrapper
    state.expected_views=128
    with client.ArgenteaBfs(state.spark,observer=observed.append).bfs(
            source('v',state.spark),source('e',state.spark),source=-5,method=method,
            partitions=3,max_levels=62,max_phase_budget=128) as result:
        assert result.native_phase_count==128
        assert len(observed[0]['view_registrations'])==128
        assert state.events.count('write-native-result')==1 and not state.views

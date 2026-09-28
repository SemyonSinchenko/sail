"""Client protocol and ownership tests; these do not qualify native execution."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pyspark.sql.connect import proto
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import Read
from pyspark.sql.types import DoubleType, LongType, StructField, StructType
from sail_nutmeg.client import ENVELOPE_TYPE_URL, ExtensionRelation

sys.path.insert(0, str(Path(__file__).parent))
import argentea_delta_client as client
from argentea_client import TYPE_URL as V1_TYPE_URL, build_plan as build_reference
from pyspark_pecan import CancellationToken, GraphCancelledError
from pyspark_pecan.lifecycle import GraphResult


@pytest.fixture(scope='module')
def envelope():
    file = descriptor_pb2.FileDescriptorProto(name='argentea_delta_test.proto', package='argentea.delta_test', syntax='proto3')
    file.dependency.append(proto.Plan.DESCRIPTOR.file.name)
    message = file.message_type.add(name='Envelope')
    for name, number, kind in [('payload_type_url',1,9),('payload',2,12),('inputs',3,11),('envelope_version',5,13)]:
        field = message.field.add(name=name, number=number, type=kind, label=3 if name=='inputs' else 1)
        if name == 'inputs':
            field.type_name = '.spark.connect.Plan'
    pool = descriptor_pool.Default()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName('argentea.delta_test.Envelope'))


def source(label, spark=None):
    return DataFrame(ExtensionRelation(dict(version=1, verb='input-fixture', label=label)), object() if spark is None else spark)


@pytest.mark.parametrize('pushes', range(8))
def test_complete_static_dag_phase_count_arity_and_versions(envelope, pushes):
    # A plain object cannot issue RPCs; serialization is the entire test action.
    frame, request = client.build_plan(object(), source('vertices'), source('edges'),
                                      vertices_count=7, max_pushes=pushes, partitions=3,
                                      max_phase_budget=4*pushes+4, generation=2)
    node, observed = frame._plan.plan(None), []
    while True:
        assert node.extension.type_url == ENVELOPE_TYPE_URL
        decoded = envelope.FromString(node.extension.value)
        assert decoded.envelope_version == 1  # Envelope version differs from payload version.
        assert decoded.payload_type_url == client.TYPE_URL != V1_TYPE_URL
        payload = json.loads(decoded.payload)
        assert set(payload) == set(request) | {'verb','phase'}
        assert all(payload[k] == v for k,v in request.items())
        observed.append((payload['verb'],payload['phase']))
        if payload['verb']=='init':
            assert len(decoded.inputs)==2
            assert [json.loads(p.root.extension.value)['label'] for p in decoded.inputs]==['vertices','edges']
            break
        assert len(decoded.inputs)==1
        node = decoded.inputs[0].root
    expected = [('init',0)]
    for phase in range(2*pushes+1):
        expected.extend([('decide',phase),('apply',phase)])
    expected.append(('result',2*pushes+1))
    assert list(reversed(observed))==expected
    assert len(observed)==4*pushes+4<=request['max_phase_budget']<=32
    assert request['version']==2 and request['algorithm']=='pagerank_delta'
    assert request['damping']==.85 and request['tolerance']==1e-8 and request['generation']==2
    assert set(request)=={'version','algorithm','operation_id','snapshot_id','generation','partitions',
                         'vertices','damping','tolerance','max_pushes','max_phase_budget','batch_rows'}


@pytest.mark.parametrize('changed', [
    {'max_pushes':-1}, {'max_pushes':True}, {'max_pushes':8}, {'max_pushes':1.0},
    {'partitions':0}, {'partitions':65}, {'partitions':True},
    {'batch_rows':0}, {'batch_rows':65_537}, {'batch_rows':False},
    {'max_phase_budget':3}, {'max_phase_budget':33}, {'max_phase_budget':True},
    {'max_pushes':3,'max_phase_budget':15},
    {'reset_probability':0}, {'reset_probability':1.01}, {'reset_probability':float('nan')},
    {'reset_probability':float('inf')}, {'reset_probability':True}, {'reset_probability':1e-100},
    {'tolerance':None}, {'tolerance':True}, {'tolerance':0}, {'tolerance':-1},
    {'tolerance':float('nan')}, {'tolerance':float('inf')},
])
def test_invalid_options_fail_before_session_or_owned_staging(changed):
    with pytest.raises(ValueError):
        client.ArgenteaDelta(object()).pagerank(object(), object(), **changed)


@pytest.mark.parametrize('changed', [{'vertices_count':0}, {'vertices_count':True}, {'vertices_count':1<<63},
                                    {'generation':0}, {'generation':True}, {'generation':1<<64},
                                    {'operation_id':'not-an-operation'}, {'snapshot_id':123},
                                    {'snapshot_id':'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'}])
def test_graph_identity_limits_before_serialization(changed):
    kwargs = dict(vertices_count=3)
    kwargs.update(changed)
    with pytest.raises(ValueError):
        client.build_plan(object(), source('vertices'), source('edges'), **kwargs)


def test_full_reset_and_zero_pushes_remain_certified_not_fixed_round(envelope):
    frame, request = client.build_plan(object(), source('v'), source('e'), vertices_count=3,
                                      max_pushes=0, max_phase_budget=4, reset_probability=1)
    assert request['damping']==0 and request['max_pushes']==0
    assert json.loads(envelope.FromString(frame._plan.plan(None).extension.value).payload)['phase']==1
    old, old_request = build_reference(object(), source('v'), source('e'), vertices_count=3, iterations=2)
    decoded = envelope.FromString(old._plan.plan(None).extension.value)
    assert decoded.payload_type_url==V1_TYPE_URL
    assert old_request['version']==1 and 'algorithm' not in old_request
    assert json.loads(decoded.payload)['verb']=='result' and json.loads(decoded.payload)['round']==1


FLOATS = {'residual_l1','stationary_error_bound'}
DIAG_NAMES = ('phase','pushes','certificate_passes','residual_l1','stationary_error_bound','converged')


def diagnostic_row(*, pushes=0, certificates=1, residual=0.0, vertices=7, phase=15):
    values = dict(phase=phase, pushes=pushes, certificate_passes=certificates,
                  residual_l1=residual, stationary_error_bound=residual/(1-.85), converged=1)
    return {name+suffix:(vertices if suffix=='_count' else value)
            for name,value in values.items() for suffix in ('_min','_max','_count')}


class Stored:
    schema = StructType([StructField(name, DoubleType() if name in FLOATS else LongType(), True) for name in DIAG_NAMES])
    columns = list(DIAG_NAMES)

    def __init__(self, row):
        self.row = row
        self.aggregate_calls = []
        self.exports = []
        self.selections = []

    def agg(self, *expressions):
        self.aggregate_calls.append(expressions)
        return source('stored-result').agg(*expressions)

    def select(self, *columns):
        self.selections.append(columns)
        assert columns == ('id', 'pagerank')
        return SimpleNamespace(write=SimpleNamespace(mode=lambda mode:SimpleNamespace(
            parquet=lambda path:self.exports.append((path,mode)))))

    def collect(self):
        raise AssertionError('rank rows must never collect to the client')


@pytest.fixture
def scalar_collect(monkeypatch):
    state = SimpleNamespace(row=diagnostic_row(), calls=[])

    def collect(frame):
        plan = frame._plan.plan(None)
        assert plan.HasField('aggregate'), 'only a scalar aggregate may collect'
        assert not plan.aggregate.grouping_expressions
        state.calls.append(plan)
        return [state.row]
    monkeypatch.setattr(DataFrame,'collect',collect)
    return state


def request(**updates):
    result = dict(max_pushes=7, vertices=7, damping=.85, tolerance=1e-8)
    result.update(updates)
    return result


def test_diagnostics_collect_only_one_aggregate_after_staging(scalar_collect):
    scalar_collect.row = diagnostic_row(pushes=2, certificates=3, residual=1e-9)
    stored = Stored(scalar_collect.row)
    result = client._read_diagnostics(stored, request(), CancellationToken())
    assert result['pushes']==2 and result['certificate_passes']==3 and result['residual_l1']==1e-9
    assert len(scalar_collect.calls)==1 and len(stored.aggregate_calls)==1
    expressions = scalar_collect.calls[0].aggregate.aggregate_expressions
    assert len(expressions)==18
    for expression, name in zip(expressions, [n+s for n in DIAG_NAMES for s in ('_min','_max','_count')]):
        assert list(expression.alias.name)==[name]
        assert expression.alias.expr.unresolved_function.function_name==name.rsplit('_',1)[1]


@pytest.mark.parametrize('change,match', [
    ({'pushes_max':1}, 'inconsistent'), ({'converged_count':6}, 'missing'),
    ({'residual_l1_min':None,'residual_l1_max':None}, 'missing'),
    ({'phase_min':14,'phase_max':14}, 'terminal certificate'),
    ({'pushes_min':8,'pushes_max':8}, 'terminal certificate'),
    ({'pushes_min':-1,'pushes_max':-1}, 'terminal certificate'),
    ({'certificate_passes_min':0,'certificate_passes_max':0}, 'terminal certificate'),
    ({'certificate_passes_min':2,'certificate_passes_max':2}, 'terminal certificate'),
    ({'converged_min':0,'converged_max':0}, 'terminal certificate'),
    ({'residual_l1_min':1e-7,'residual_l1_max':1e-7}, 'residual'),
    ({'residual_l1_min':-1e-9,'residual_l1_max':-1e-9}, 'residual'),
    ({'residual_l1_min':float('inf'),'residual_l1_max':float('inf')}, 'residual'),
    ({'stationary_error_bound_min':1.0,'stationary_error_bound_max':1.0}, 'residual'),
])
def test_inconsistent_or_uncertified_diagnostics_cannot_be_retained(scalar_collect, change, match):
    scalar_collect.row.update(change)
    with pytest.raises(RuntimeError,match=match):
        client._read_diagnostics(Stored(scalar_collect.row), request(), CancellationToken())


def test_wrong_diagnostic_type_fails_without_collect(scalar_collect):
    stored = Stored(scalar_collect.row)
    stored.schema = StructType([StructField(name,DoubleType(),True) for name in DIAG_NAMES])
    with pytest.raises(RuntimeError,match='invalid diagnostic column phase'):
        client._read_diagnostics(stored, request(), CancellationToken())
    assert scalar_collect.calls==[]


@pytest.fixture
def wrapper(monkeypatch, scalar_collect):
    import pyspark_pecan.algorithms as algorithms
    events = []
    spark = SimpleNamespace(client=None, addTag=lambda tag:events.append('tag-add'),
                            removeTag=lambda tag:events.append('tag-remove'), interruptTag=lambda tag:None)
    state = SimpleNamespace(events=events, spark=spark, failure=None, uncertain=False,
                            stored=Stored(scalar_collect.row), views={}, registrations=[], drops=[],
                            create_failure=None, drop_failure=None, cancel_on_write=False)
    def create(frame, name):
        assert name not in state.views
        if state.create_failure is not None:
            raise state.create_failure
        state.views[name] = frame
        state.registrations.append(name)
        events.append('register-view')
    def drop(name):
        if state.drop_failure is not None:
            raise state.drop_failure
        del state.views[name]
        state.drops.append(name)
        events.append('drop-view')
        return True
    monkeypatch.setattr(DataFrame,'createTempView',create)
    spark.table = lambda name:DataFrame(Read(name),spark)
    spark.catalog = SimpleNamespace(dropTempView=drop)

    class Run:
        def __init__(self, session, utils, cancellation, partitions):
            assert session is spark and partitions==3
            self.cancellation, self.closed, self.write_uncertain = cancellation,False,False
            self.path, self.result_path = 'owned/run',None
            state.run = self
            events.append('allocate')

        def materialize(self, frame, *, expected_rows):
            assert isinstance(frame._plan,Read) and expected_rows==7
            assert len(state.views)==getattr(state,'expected_views',32)  # Exact schedule stays alive through materialization.
            events.append('write-native-result')
            self.write_uncertain = state.uncertain
            if state.cancel_on_write:
                self.cancellation.cancel()
            if state.failure:
                raise state.failure
            return self.path+'/result',state.stored

        def finish(self, path, frame, **kwargs):
            self.cancellation.check()
            self.result_path = path
            events.append(('finish',kwargs))
            return GraphResult(self,frame,**kwargs)

        def touch(self):
            if self.closed:
                raise RuntimeError('closed')
            events.append('touch')

        def close(self):
            self.closed = True
            events.append('close')

    def snapshot(run,nodes,edges,columns):
        events.append('snapshot-validate')
        return nodes,edges,7
    monkeypatch.setattr(algorithms,'GraphUtils',lambda session:object())
    monkeypatch.setattr(algorithms,'StagingRun',Run)
    monkeypatch.setattr(algorithms,'_check_input_schema',lambda *args:events.append('check-input-schema'))
    monkeypatch.setattr(algorithms,'_snapshot',snapshot)
    return state


def test_public_wrapper_uses_owned_lifecycle_and_actual_counters(wrapper, scalar_collect):
    scalar_collect.row.update(diagnostic_row(pushes=2, certificates=2,residual=1e-9))
    observed = []
    result = client.ArgenteaDelta(wrapper.spark,observer=observed.append).pagerank(
        source('v',wrapper.spark),source('e',wrapper.spark),partitions=3)
    assert wrapper.events[:4]==['check-input-schema','tag-add','allocate','snapshot-validate']
    assert len(observed)==1 and observed[0]['native_phase_count']==32
    assert len(observed[0]['view_registrations'])==32
    assert not wrapper.views and wrapper.drops==list(reversed(wrapper.registrations))
    assert observed[0]['request']['version']==2 and observed[0]['plan_bytes']
    assert result.pushes==result.iterations==2 and result.certificate_passes==2
    assert result.native_phase_count==32 and result.phase==15 and result.converged is True
    assert result.residual==1e-9 and result.error_bound==pytest.approx(1e-9/.15)
    assert result.algorithm=='argentea-pagerank-delta' and result.path=='owned/run/result'
    assert wrapper.events[-2]==('finish',dict(algorithm='argentea-pagerank-delta',iterations=2,converged=True))
    assert result.native_frame is wrapper.stored and not wrapper.run.closed
    with result as same:
        assert same is result
    assert wrapper.run.closed
    with pytest.raises(RuntimeError,match='closed'):
        _ = result.native_frame
    with pytest.raises(RuntimeError,match='closed'):
        _ = result.frame


def test_native_cap_failure_propagates_with_existing_uncertain_write_ownership(wrapper):
    cap = RuntimeError('native residual PageRank did not certify at push cap')
    wrapper.failure,wrapper.uncertain = cap,True
    with pytest.raises(RuntimeError,match='push cap') as caught:
        client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    assert caught.value is cap and cap.cleanup_deferred is True and cap.run_path=='owned/run'
    assert not wrapper.run.closed and 'close' not in wrapper.events
    assert not wrapper.views and wrapper.drops==list(reversed(wrapper.registrations))
    assert cap.view_cleanup_deferred is False
    assert not any(isinstance(e,tuple) and e[0]=='finish' for e in wrapper.events)


def test_bad_stored_certificate_closes_known_finished_write(wrapper, scalar_collect):
    scalar_collect.row['converged_min']=scalar_collect.row['converged_max']=0
    with pytest.raises(RuntimeError,match='terminal certificate') as caught:
        client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    assert caught.value.cleanup_deferred is False and wrapper.run.closed
    assert wrapper.events.index('close')>wrapper.events.index('write-native-result')


def test_observer_cancellation_prevents_terminal_write(wrapper):
    token = CancellationToken()
    with pytest.raises(GraphCancelledError):
        client.ArgenteaDelta(wrapper.spark,observer=lambda event:token.cancel()).pagerank(
            source('v'),source('e'),partitions=3,cancellation=token)
    assert wrapper.run.closed and 'write-native-result' not in wrapper.events
    assert not wrapper.views and len(wrapper.drops)==32


def test_rank_export_delegates_to_caller_path_without_transferring_owned_result(wrapper):
    result = client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    result.write_parquet('caller/export',mode='overwrite')
    assert wrapper.stored.exports==[('caller/export','overwrite')]
    assert wrapper.stored.selections==[('id','pagerank')]
    assert result.native_frame is wrapper.stored and result.path=='owned/run/result'
    result.close()
    assert wrapper.stored.exports==[('caller/export','overwrite')]
    with pytest.raises(RuntimeError,match='closed'):
        result.write_parquet('caller/late')
    assert len(wrapper.stored.exports)==1


def test_registration_failure_closes_staging_without_terminal_write(wrapper):
    failure = RuntimeError('registration failed')
    wrapper.create_failure = failure
    with pytest.raises(RuntimeError) as caught:
        client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    assert caught.value is failure and wrapper.run.closed
    assert 'write-native-result' not in wrapper.events
    assert failure.view_cleanup_deferred and len(failure.uncertain_view_names)==1
    assert failure.cleanup_deferred is False  # No uncertain Parquet write.


def test_cleanup_failure_prevents_retaining_a_successful_materialization(wrapper):
    wrapper.drop_failure = RuntimeError('drop transport failed')
    with pytest.raises(RuntimeError,match='cleanup is deferred') as caught:
        client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    assert len(caught.value.view_cleanup_errors)==32 and wrapper.run.closed
    assert not any(isinstance(e,tuple) and e[0]=='finish' for e in wrapper.events)


def test_cancellation_wrapper_preserves_view_and_write_cleanup_diagnostics(wrapper):
    failure = RuntimeError('interrupted native write')
    wrapper.failure,wrapper.uncertain,wrapper.cancel_on_write = failure,True,True
    wrapper.drop_failure = RuntimeError('drop transport failed')
    with pytest.raises(GraphCancelledError) as caught:
        client.ArgenteaDelta(wrapper.spark).pagerank(source('v'),source('e'),partitions=3)
    assert caught.value.__cause__ is failure
    assert caught.value.cleanup_deferred and caught.value.run_path=='owned/run'
    assert caught.value.view_cleanup_deferred and len(caught.value.view_cleanup_errors)==32
    assert not wrapper.run.closed

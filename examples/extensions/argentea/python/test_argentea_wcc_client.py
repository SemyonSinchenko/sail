"""WCC request, shallow-view identity and failure-cleanup contract."""
import json
import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import Read
from pyspark_pecan import CancellationToken
import argentea_wcc_client as client
from argentea_views import compose_views
from test_argentea_delta_client import envelope, source
from test_argentea_delta_views import session


@pytest.mark.parametrize('method,rounds', [('reference',0),('reference',14),('reference',62),
                                         ('star',0),('star',3),('star',14),('star',19)])
def test_shallow_views_pin_all_phases_and_identity(session,envelope,method,rounds):
    base=client.request(vertices_count=8,method=method,max_rounds=rounds,seed=(1<<64)-1)
    schedule=client.phases(rounds,method)
    with compose_views(session.spark,DataFrame(Read('v'),session.spark),DataFrame(Read('e'),session.spark),request=base,phases=schedule,
                       relation_type=client.ArgenteaWccRelation,cancellation=CancellationToken(),
                       max_phases=128) as composition:
        assert len(composition.registrations)==client.phase_count(rounds,method)
        from pyspark.sql.connect import proto
        for registration,(verb,phase) in zip(composition.registrations,schedule):
            plan=proto.Plan.FromString(registration['plan_bytes'])
            body=envelope.FromString(plan.root.extension.value)
            assert body.payload_type_url==client.TYPE_URL and body.envelope_version==1
            assert json.loads(body.payload)==dict(base,verb=verb,phase=phase)
            assert len(body.inputs)==(2 if verb=='init' else 1)
            assert all(frame.root.HasField('read') for frame in body.inputs)
            assert registration['plan_size']<4096
    assert list(session.views)==['unrelated']
    assert session.dropped==list(reversed(session.registered))


@pytest.mark.parametrize('changes',[{'method':'gf64'},{'method':None},{'max_rounds':True},
    {'max_rounds':-1},{'max_rounds':63},{'method':'star','max_rounds':20},
    {'seed':-1},{'seed':1<<64},{'seed':False},{'partitions':0},{'partitions':65},
    {'max_phase_budget':129},{'max_phase_budget':True},{'max_phase_budget':4},
    {'batch_rows':0},{'batch_rows':65_537}])
def test_invalid_options_fail_before_session_or_staging(changes):
    with pytest.raises(ValueError):
        client.ArgenteaWcc(object()).wcc(object(),object(),**changes)


@pytest.mark.parametrize('limit',[True,0,129,1.5])
def test_composer_rejects_invalid_bound_before_registration(session,limit):
    with pytest.raises(ValueError):
        with compose_views(session.spark,source('v'),source('e'),request={},phases=[('init',0)],
                           relation_type=client.ArgenteaWccRelation,cancellation=CancellationToken(),max_phases=limit):
            pass
    assert not session.attempts


def test_default_composer_limit_is_unchanged(session):
    base=client.request(vertices_count=8,method='star',max_rounds=14)
    with pytest.raises(ValueError):
        with compose_views(session.spark,source('v'),source('e'),request=base,
                           phases=client.phases(14,'star'),relation_type=client.ArgenteaWccRelation,
                           cancellation=CancellationToken()):
            pass
    assert not session.attempts


def test_late_registration_failure_cleans_only_confirmed_views(session):
    session.fail_after=70
    base=client.request(vertices_count=8,method='star',max_rounds=14)
    with pytest.raises(RuntimeError,match='reply lost') as caught:
        with compose_views(session.spark,source('v'),source('e'),request=base,phases=client.phases(14,'star'),
                           relation_type=client.ArgenteaWccRelation,cancellation=CancellationToken(),max_phases=128):
            pass
    assert len(session.dropped)==69
    assert list(session.views)==['unrelated',session.attempts[-1]]
    assert caught.value.uncertain_view_names==(session.attempts[-1],)
    assert caught.value.view_cleanup_deferred is True

from pyspark.sql.types import LongType,StructType,StructField
from test_argentea_delta_client import scalar_collect,wrapper

class Stored:
    columns=['id','component','phase','rounds','converged']
    schema=StructType([StructField(name,LongType(),False) for name in columns])
    def agg(self,*expressions):return source('stored').agg(*expressions)
    def collect(self):raise AssertionError('component vectors must stay on the server')
    def select(self,*columns):
        assert columns==('id','component')
        return columns


def diagnostic_row():
    values=dict(phase=46,rounds=5,converged=1)
    row={name+suffix:(7 if suffix=='_count' else value)
         for name,value in values.items() for suffix in ('_min','_max','_count')}
    row.update(component_count=7,invalid_rows=0)
    return row


def test_diagnostics_are_one_scalar_reduction(scalar_collect):
    scalar_collect.row=diagnostic_row()
    result=client._diagnostics(Stored(),client.request(vertices_count=7,method='star'),CancellationToken())
    assert result==dict(phase=46,rounds=5,converged=1)
    assert len(scalar_collect.calls)==1
    assert len(scalar_collect.calls[0].aggregate.aggregate_expressions)==11


@pytest.mark.parametrize('changes',[{'rounds_max':6},{'rounds_count':6},
    {'phase_min':45,'phase_max':45},{'converged_min':0,'converged_max':0},
    {'rounds_min':15,'rounds_max':15},{'rounds_min':-1,'rounds_max':-1},
    {'component_count':6},{'invalid_rows':1}])
def test_bad_diagnostics_never_retain_result(scalar_collect,changes):
    scalar_collect.row=diagnostic_row();scalar_collect.row.update(changes)
    with pytest.raises(RuntimeError):
        client._diagnostics(Stored(),client.request(vertices_count=7,method='star'),CancellationToken())


def test_public_wcc_owns_one_materialization_and_all_lazy_views(wrapper,scalar_collect):
    scalar_collect.row=diagnostic_row();wrapper.stored=Stored();wrapper.expected_views=94
    events=[]
    result=client.ArgenteaWcc(wrapper.spark,observer=events.append).wcc(
        source('v',wrapper.spark),source('e',wrapper.spark),method='star',partitions=3)
    assert wrapper.events.count('write-native-result')==1
    assert len(events)==1 and events[0]['native_phase_count']==94
    assert len(events[0]['view_registrations'])==94 and not wrapper.views
    assert result.rounds==5 and result.converged and result.native_phase_count==94
    assert result.frame==('id','component')
    assert result.close() is None


def test_wheel_manifest_advertises_only_explicit_worker_wcc_role():
    from sail_nutmeg.argentea_factory import Extension
    manifest=Extension().manifest()
    assert manifest['placement']=='worker'
    matches=[x for x in manifest['relation_types'] if x['type_url']==client.TYPE_URL]
    assert matches==[dict(type_url=client.TYPE_URL,accepts_bare=False,min_inputs=1,max_inputs=2)]

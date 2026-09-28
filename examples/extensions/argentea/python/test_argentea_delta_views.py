"""Lazy view wire shape and cleanup ownership, without a live server."""
from types import SimpleNamespace
import uuid

import pytest
from pyspark.sql.connect import proto
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import Read

import argentea_delta_views as views
from pyspark_pecan import CancellationToken, GraphCancelledError
from test_argentea_delta_client import envelope, source


@pytest.fixture
def session(monkeypatch):
    state = SimpleNamespace(views={'unrelated':object()},attempts=[],registered=[],dropped=[],
                            fail_before=None,fail_after=None,drop_failure=None,on_register=None)
    spark = SimpleNamespace(client=None)
    def create(frame, name):
        state.attempts.append(name)
        if name in state.views:
            raise RuntimeError('view already exists')
        if len(state.attempts)==state.fail_before:
            raise RuntimeError('registration refused')
        state.views[name] = frame
        state.registered.append(name)
        if len(state.attempts)==state.fail_after:
            raise RuntimeError('registration reply lost')
        if state.on_register is not None:
            state.on_register()
    def drop(name):
        state.dropped.append(name)
        if name==state.drop_failure:
            raise RuntimeError('drop failed')
        return state.views.pop(name,None) is not None
    def collect(frame):
        raise AssertionError('composition must not execute the native relation')
    monkeypatch.setattr(DataFrame,'createTempView',create)
    monkeypatch.setattr(DataFrame,'collect',collect)
    spark.catalog = SimpleNamespace(dropTempView=drop)
    spark.table = lambda name:DataFrame(Read(name),spark)
    state.spark=spark
    return state


def compose(session, **options):
    options = dict(vertices_count=3,max_pushes=7,partitions=5,**options)
    return views.compose_plan(session.spark,source('vertices'),source('edges'),
                              cancellation=CancellationToken(),**options)


@pytest.mark.parametrize('pushes',range(8))
def test_all_phases_have_shallow_wire_inputs_and_one_identity(session,envelope,pushes):
    import json
    with views.compose_plan(session.spark,source('vertices'),source('edges'),cancellation=CancellationToken(),
                            vertices_count=3,max_pushes=pushes,partitions=5) as plan:
        assert len(plan.registrations)==4*pushes+4
        assert len(session.views)==len(plan.registrations)+1
        aliases = [r['alias'] for r in plan.registrations]
        assert len(set(aliases))==len(aliases)
        prefix = aliases[0].split('_')[2]
        assert uuid.UUID(hex=prefix).hex==prefix
        expected = [('init',0)]
        for phase in range(2*pushes+1):expected.extend([('decide',phase),('apply',phase)])
        expected.append(('result',2*pushes+1))
        assert [(r['verb'],r['phase']) for r in plan.registrations]==expected
        for index, registration in enumerate(plan.registrations):
            node = proto.Plan.FromString(registration['plan_bytes']).root
            decoded = envelope.FromString(node.extension.value)
            payload = json.loads(decoded.payload)
            assert all(payload[key]==value for key,value in plan.request.items())
            assert decoded.envelope_version==1 and payload['version']==2
            if index==0:
                assert len(decoded.inputs)==2
            else:
                assert len(decoded.inputs)==1
                child = decoded.inputs[0].root
                assert child.HasField('read')
                assert child.read.named_table.unparsed_identifier==aliases[index-1]
        assert plan.frame._plan.plan(None).read.named_table.unparsed_identifier==aliases[-1]
    assert session.views.keys()=={'unrelated'}
    assert session.dropped==list(reversed(aliases))


def test_cleanup_happens_after_body_and_preserves_unrelated_view(session):
    with compose(session) as plan:
        assert session.dropped==[]
        assert set(r['alias'] for r in plan.registrations)<=session.views.keys()
    assert session.views.keys()=={'unrelated'}


@pytest.mark.parametrize('lost_reply',[False,True])
def test_partial_registration_preserves_uncertain_alias_and_cleans_confirmed(session,lost_reply):
    if lost_reply:session.fail_after=3
    else:session.fail_before=3
    with pytest.raises(RuntimeError,match='registration') as caught:
        with compose(session):pytest.fail('incomplete composition yielded a terminal relation')
    assert session.dropped==list(reversed(session.attempts[:2]))
    assert caught.value.uncertain_view_names==(session.attempts[2],)
    assert caught.value.view_cleanup_deferred is True
    assert session.views.keys()==({'unrelated',session.attempts[2]} if lost_reply else {'unrelated'})


def test_creation_collision_never_deletes_preexisting_alias(session,monkeypatch):
    fixed=uuid.UUID('11111111-2222-3333-4444-555555555555')
    monkeypatch.setattr(views.uuid,'uuid4',lambda:fixed)
    alias=f'argentea_delta_{fixed.hex}_00_init_0'
    original=object();session.views[alias]=original
    with pytest.raises(RuntimeError,match='already exists'):
        with compose(session):pytest.fail('colliding view was replaced')
    assert session.views[alias] is original and session.dropped==[]


def test_materialization_error_drops_every_confirmed_view_without_masking(session):
    failure=RuntimeError('native cap failure')
    with pytest.raises(RuntimeError) as caught:
        with compose(session):raise failure
    assert caught.value is failure and failure.view_cleanup_deferred is False
    assert session.dropped==list(reversed(session.registered)) and len(session.dropped)==32
    assert session.views.keys()=={'unrelated'}


@pytest.mark.parametrize('body_fails',[False,True])
def test_failed_drop_attempts_all_aliases_and_exposes_deferred_cleanup(session,body_fails):
    failure=RuntimeError('terminal write failed')
    with pytest.raises(RuntimeError) as caught:
        with compose(session):
            session.drop_failure=session.registered[-2]
            if body_fails:raise failure
    if body_fails:assert caught.value is failure
    else:assert 'cleanup is deferred' in str(caught.value)
    assert caught.value.view_cleanup_deferred is True
    assert [r['alias'] for r in caught.value.view_cleanup_errors]==[session.drop_failure]
    assert session.dropped==list(reversed(session.registered))
    assert session.views.keys()=={'unrelated',session.drop_failure}


def test_cancellation_after_registration_cleans_just_acknowledged_view(session):
    token=CancellationToken();session.on_register=token.cancel
    with pytest.raises(GraphCancelledError):
        with views.compose_plan(session.spark,source('v'),source('e'),cancellation=token,vertices_count=3):
            pytest.fail('cancelled composition yielded')
    assert len(session.registered)==len(session.dropped)==1 and session.views.keys()=={'unrelated'}


def test_invalid_budget_prevents_any_registration(session):
    with pytest.raises(ValueError,match='exceed'):
        with compose(session,max_phase_budget=4):pass
    assert session.attempts==session.dropped==[]

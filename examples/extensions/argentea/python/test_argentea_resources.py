"""Resource qualifier controls; synthetic records are not a live lease verdict."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).parent))
import argentea_resource_evidence as evidence
from test_argentea_evidence import evidence as successful_fixture


def failed_fixture():
    request,_,_,_,records,_,_,stages,tasks = successful_fixture()
    records = [r for r in records if r['event'] in ('init','emit','close') and r['round']==0 or r['event']=='close']
    tasks = [t for t in tasks if t['stage'] in (2,3,4)]
    tasks[-1]['status']='FAILED'
    return request,records,stages,tasks


def log_text(records,tasks=()):
    return '\n'.join(['ARGENTEA_RECEIPT '+json.dumps(r) for r in records]+[
        'worker_task_status worker_id={worker_id} job_id={job_id} stage={stage} partition={partition} attempt={attempt} status={status}'.format(**t)
        for t in tasks])+'\n'


def check(request,records,stages,tasks):
    return evidence.validate_failed_operation(log_text(records,tasks),request,stages,
        expected_pids=[100,101],expected_session='s',iterations=2)


def test_runtime_failure_requires_all_csr_owners_close_and_attempt_zero():
    request,records,stages,tasks = failed_fixture()
    result = check(request,records,stages,tasks)
    assert result['post_csr_runtime_failure'] and result['no_native_retry']
    assert result['initialized_owners']==result['closed_owners']==3


@pytest.mark.parametrize('fault',['no_init','no_close','replay','new_pid','new_session','result',
                                  'retry','no_failed_task','different_worker','missing_stage','duplicate_stage'])
def test_failure_audit_cannot_mask_lifecycle_or_retry_defects(fault):
    request,records,stages,tasks = failed_fixture()
    if fault=='no_init': records=[r for r in records if not(r['event']=='init' and r['partition']==1)]
    if fault=='no_close': records=[r for r in records if not(r['event']=='close' and r['partition']==1)]
    if fault=='replay': records.append(copy.deepcopy(records[0]))
    if fault=='new_pid': records[-1]['pid']=999
    if fault=='new_session':
        for r in records: r['session_id']='replacement'
    if fault=='result': records.append(dict(records[0],event='result'))
    if fault=='retry': tasks[-1]['attempt']=1
    if fault=='no_failed_task': tasks[-1]['status']='SUCCEEDED'
    if fault=='different_worker': tasks[0]['worker_id']=999
    if fault=='missing_stage': stages.pop(0)
    if fault=='duplicate_stage': stages[1]=copy.deepcopy(stages[0])
    with pytest.raises(AssertionError): check(request,records,stages,tasks)


def test_close_poll_waits_for_every_initialized_owner(tmp_path,monkeypatch):
    request,records,_,_ = failed_fixture()
    file=tmp_path/'server.log'
    file.write_text(log_text([r for r in records if r['event']!='close']))
    ticks=iter([0,0,1])
    monkeypatch.setattr(evidence.time,'monotonic',lambda:next(ticks))
    calls=[]
    def sleep(seconds):
        calls.append(seconds)
        file.write_text(log_text(records))
    monkeypatch.setattr(evidence.time,'sleep',sleep)
    assert evidence.wait_closed(file,request)==records
    assert calls==[.05]


def test_close_poll_has_bounded_failure_and_does_not_wait_for_session_teardown(tmp_path,monkeypatch):
    request,records,_,_=failed_fixture()
    file=tmp_path/'server.log'
    file.write_text(log_text([r for r in records if r['event']!='close']))
    ticks=iter([0,31])
    monkeypatch.setattr(evidence.time,'monotonic',lambda:next(ticks))
    monkeypatch.setattr(evidence.time,'sleep',lambda _:pytest.fail('deadline already passed'))
    with pytest.raises(AssertionError,match='within 30.0s'): evidence.wait_closed(file,request)


@pytest.fixture
def exercise_control(monkeypatch,tmp_path):
    import qualify_resources as runner
    import pyspark.sql.connect.session as connect
    calls=[]
    state=SimpleNamespace(calls=calls,results=[],fail_audit=False)
    class Result:
        def __init__(self,event):
            self.request=event['request']
            self.path='owned/'+self.request['operation_id']
            self.closed=False
            self.native_frame=SimpleNamespace(collect=lambda:[])
            self.frame=SimpleNamespace(count=self.count)
        def count(self):
            assert not self.closed
            return 6
        def close(self):
            self.closed=True
            calls.append('close-result')
    class Graph:
        def __init__(self,spark,observer): self.observer=observer
        def pagerank(self,*args,**kwargs):
            event=dict(request=dict(operation_id='op-'+str(len(state.results)),partitions=3),plan_bytes=b'plan')
            self.observer(event)
            result=Result(event)
            state.results.append(result)
            calls.append('valid')
            return result
    spark=SimpleNamespace(client=SimpleNamespace(set_retry_policies=lambda policies:None),
        createDataFrame=lambda *args:object(),stop=lambda:calls.append('stop-session'))
    monkeypatch.setattr(connect,'SparkSession',SimpleNamespace(builder=SimpleNamespace(remote=lambda endpoint:SimpleNamespace(create=lambda:spark))))
    monkeypatch.setattr(runner,'Argentea',Graph)
    def fail(*args):
        calls.append('injected-error')
        args[-1](dict(request=dict(operation_id='failed',partitions=3),plan_bytes=b'bad'))
        error=RuntimeError('incomplete or inconsistent global vertex count')
        error.cleanup_deferred=True
        error.run_path='owned/failed'
        raise error
    monkeypatch.setattr(runner,'invalid_cardinality',fail)
    monkeypatch.setattr(runner,'validate_rows',lambda *args:dict(valid=True))
    monkeypatch.setattr(runner,'inventory',lambda spark:([],[]))
    monkeypatch.setattr(runner,'parse_worker_tasks',lambda text:[])
    monkeypatch.setattr(runner,'wait_closed',lambda *args,**kwargs:calls.append('native-close') or [])
    monkeypatch.setattr(runner,'live_pids',lambda pids:[dict(pid=p,alive=True) for p in pids])
    jobs=iter(range(10))
    def audit(*args,**kwargs):
        if state.fail_audit: raise AssertionError('audit failure')
        return dict(native_pids=[100,101],native_workers=[10,11],session_id='same-session',job_id=next(jobs))
    monkeypatch.setattr(runner,'validate_audit',audit)
    monkeypatch.setattr(runner,'validate_failed_operation',audit)
    (tmp_path/'server.log').write_text('')
    state.runner=runner
    return state


def test_all_success_results_stay_open_across_error_and_three_reuses(exercise_control,tmp_path):
    state=exercise_control
    receipt={}
    state.runner.exercise('sc://fixture',tmp_path,receipt)
    assert [op['outcome'] for op in receipt['operations']]==['passed','expected-runtime-failure','passed','passed','passed']
    assert [op['retained_output_counts'] for op in receipt['operations']]==[[6],[6],[6,6],[6,6,6],[6,6,6,6]]
    assert state.calls[:10]==['valid','native-close','injected-error','native-close','valid','native-close','valid','native-close','valid','native-close']
    assert state.calls[10:]==['close-result']*4+['stop-session']
    assert all(r.closed for r in state.results) and receipt['retained_result_count_before_close']==4


def test_unexpected_audit_error_still_closes_retained_outputs_and_session(exercise_control,tmp_path):
    state=exercise_control
    state.fail_audit=True
    receipt={}
    with pytest.raises(AssertionError,match='audit failure'):
        state.runner.exercise('sc://fixture',tmp_path,receipt)
    assert state.calls[-2:]==['close-result','stop-session']
    assert (tmp_path/'exercise.json').exists()


def test_live_log_poll_defers_only_an_unfinished_last_line(tmp_path):
    file=tmp_path/'server.log'
    first='ARGENTEA_RECEIPT {"event":"init"}\n'
    file.write_text(first+'ARGENTEA_RECEIPT {broken')
    assert evidence.read_complete_log(file)==first.rstrip('\n')
    assert evidence.parse_log(evidence.read_complete_log(file))[0]==[{'event':'init'}]
    file.write_text(first+'ARGENTEA_RECEIPT {broken\n')
    with pytest.raises(ValueError): evidence.parse_log(evidence.read_complete_log(file))


@pytest.mark.parametrize('failure',['short','missing','different'])
def test_declared_source_must_be_an_available_exact_commit(monkeypatch,failure):
    import qualify_resources as runner
    import subprocess
    identity='a'*40
    def git(*args):
        if failure=='missing': raise subprocess.CalledProcessError(128,['git'])
        return 'b'*40 if failure=='different' else identity
    monkeypatch.setattr(runner,'git',git)
    with pytest.raises(ValueError):
        runner.validate_source_identities([identity[:12] if failure=='short' else identity])

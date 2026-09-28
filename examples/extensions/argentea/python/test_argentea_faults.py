import copy
from pathlib import Path
import sys

import pytest
sys.path.insert(0, str(Path(__file__).parent))
from argentea_fault_control import validate_window, workers_from_log
from argentea_fault_evidence import validate_fault


def fixture(case='cancel'):
    request = dict(operation_id='op', snapshot_id='snap', generation=1, partitions=2, max_pushes=1)
    workers = [dict(worker_id=p+1, pid=100+p, session_id='s', driver_pid=99) for p in range(2)]
    records = [dict(event='init', partition=p, worker_id=p+1, pid=100+p,
        adjacency_id=p+5, session_id='s', job_id=3, **{k:request[k] for k in ('operation_id','snapshot_id','generation')}) for p in range(2)]
    snapshot = {w['pid']:dict(status='T',pgid=99) for w in workers}
    window = copy.deepcopy(validate_window(records, request, workers, snapshot, 99))
    for record in list(records):
        if case != 'worker-loss' or record['pid'] != 100:
            records.append(dict(record,event='close'))
    stages = [dict(session_id='s',job_id=3,stage=i,slot_group='worker-extension:0',partitions=2,
                   placement='Worker',mode='Pipelined') for i in range(8)]
    tasks = [dict(session_id='s',job_id=3,stage=i,partition=p,attempt=0,status='CANCELED') for i in range(8) for p in range(2)]
    text = '\n'.join(f'worker_task_status worker_id={p+1} job_id=3 stage={i} partition={p} attempt=0 status=RUNNING' for i in range(8) for p in range(2))
    check = dict(case=case,request=request,query_failed=True,cleanup_deferred=True,stages=stages,stored_tasks=tasks,
        jobs=[dict(session_id='s',job_id=3,status='CANCELED')],injection=dict(window=window,signals=[]),
        error_type='GraphCancelledError',interrupt_operation_ids=['rpc1'],error='worker extension job failed; automatic retry disabled: h2 protocol error',
        supervised_workers=workers,connect_max_retries=0)
    if case=='worker-loss':
        check['injection'].update(killed_worker=workers[0],signals=[dict(pid=100,signal='SIGKILL')])
        check.update(error_type='AnalysisException',jobs=[dict(session_id='s',job_id=3,status='FAILED')])
    elif case=='quota':
        records=[]
        check.update(native_quota=1,error='procedure memory budget exceeded (limit 1)')
    return text,records,check,workers,snapshot


@pytest.mark.parametrize('case',['cancel','worker-loss','quota'])
def test_each_fault_audits_its_actual_boundary(case):
    text,records,check,_,_=fixture(case)
    result=validate_fault(text,records,check)
    assert result['first_call_failed'] and result['no_native_retry'] and result['native_task_count']==16
    assert result['closed_surviving_owners']=={'cancel':2,'worker-loss':1,'quota':0}[case]


@pytest.mark.parametrize('bad',['result','retry','wrong-worker','missing-close','missing-task','duplicate-task','running-task','wrong-group','wrong-width','successful-job','no-rpc','second-job','wrong-snapshot'])
def test_fault_audit_rejects_incomplete_or_misleading_evidence(bad):
    text,records,check,_,_=fixture()
    if bad=='result': records.append(dict(records[0],event='result'))
    elif bad=='retry': text=text.replace('attempt=0','attempt=1',1)
    elif bad=='wrong-worker': text=text.replace('worker_id=1','worker_id=2',1)
    elif bad=='missing-close': records.pop()
    elif bad=='missing-task': check['stored_tasks'].pop()
    elif bad=='duplicate-task': check['stored_tasks'].append(copy.deepcopy(check['stored_tasks'][0]))
    elif bad=='running-task': check['stored_tasks'][0]['status']='RUNNING'
    elif bad=='wrong-group': check['stages'][0]['slot_group']='worker-extension:other'
    elif bad=='wrong-width': check['stages'][0]['partitions']=1
    elif bad=='successful-job': check['jobs'][0]['status']='SUCCEEDED'
    elif bad=='no-rpc': check['interrupt_operation_ids']=[]
    elif bad=='second-job': records[-1]['job_id']=4
    elif bad=='wrong-snapshot': records[-1]['snapshot_id']='other'
    with pytest.raises(AssertionError):validate_fault(text,records,check)


def test_quota_settings_do_not_substitute_for_native_cause():
    text,records,check,_,_=fixture('quota')
    check['error']='operation canceled while reading input'
    with pytest.raises(AssertionError,match='quota cause'):validate_fault(text,records,check)


@pytest.mark.parametrize('path',['scheduler_failure','bare_h2_transport'])
def test_worker_loss_accepts_only_proven_first_error_paths(path):
    text,records,check,_,_=fixture('worker-loss')
    if path=='bare_h2_transport':
        check.update(error_type='SparkRuntimeException',error='h2 protocol error: error reading a body from connection')
    assert validate_fault(text,records,check)['first_error_path']==path


@pytest.mark.parametrize('path',['scheduler_failure','bare_h2_transport'])
@pytest.mark.parametrize('bad',['missing-kill','wrong-killed-worker','unsupervised-kill','unheld-window',
    'generic-error','wrong-error-type','active-task','retry','successful-job','canceled-job',
    'replayed-init','missing-close','result','client-retry','ordinary-task-active'])
def test_worker_loss_path_never_substitutes_for_fault_evidence(path,bad):
    text,records,check,_,_=fixture('worker-loss')
    if path=='bare_h2_transport':
        check.update(error_type='SparkRuntimeException',error='h2 protocol error: error reading a body from connection')
    if bad=='missing-kill':check['injection']['signals']=[]
    elif bad=='wrong-killed-worker':check['injection']['killed_worker']=dict(check['injection']['killed_worker'],worker_id=99)
    elif bad=='unsupervised-kill':check['supervised_workers']=check['supervised_workers'][1:]
    elif bad=='unheld-window':check['injection']['window']['process_states'][100]['status']='R'
    elif bad=='generic-error':check['error']='unrelated connection error'
    elif bad=='wrong-error-type':check['error_type']='ValueError'
    elif bad=='active-task':check['stored_tasks'][0]['status']='RUNNING'
    elif bad=='retry':check['stored_tasks'][0]['attempt']=1
    elif bad=='successful-job':check['jobs'][0]['status']='SUCCEEDED'
    elif bad=='canceled-job':check['jobs'][0]['status']='CANCELED'
    elif bad=='replayed-init':records.append(dict(records[0]))
    elif bad=='missing-close':records.pop()
    elif bad=='result':records.append(dict(records[0],event='result'))
    elif bad=='client-retry':check['connect_max_retries']=1
    elif bad=='ordinary-task-active':check['stored_tasks'].append(dict(session_id='s',job_id=3,stage=99,partition=0,attempt=0,status='RUNNING'))
    with pytest.raises(AssertionError):validate_fault(text,records,check)


@pytest.mark.parametrize('bad',['running','driver-group','terminal','one-init'])
def test_window_requires_both_native_initializations_and_stopped_processes(bad):
    _,records,check,workers,snapshot=fixture()
    records=[r for r in records if r['event']=='init']
    if bad=='running':snapshot[100]['status']='R'
    elif bad=='driver-group':snapshot[100]['pgid']=98
    elif bad=='terminal':records.append(dict(records[0],event='result'))
    elif bad=='one-init':records.pop()
    with pytest.raises(AssertionError):validate_window(records,check['request'],workers,snapshot,99)


def test_pid_authority_comes_from_supervised_driver_log():
    log='extension process worker 1: pid=Some(100), driver_pid=99, session=abc\n'
    log+='extension process worker 2: pid=Some(101), driver_pid=99, session=abc\n'
    assert [w['pid'] for w in workers_from_log(log,99)]==[100,101]
    with pytest.raises(AssertionError):workers_from_log(log,100)
    with pytest.raises(AssertionError):workers_from_log(log+log,99)


@pytest.mark.parametrize('victim',[None,{'status':'Z','pgid':99}])
def test_killed_child_may_await_reap_but_must_not_execute(victim):
    from argentea_fault_control import validate_after_close
    workers=[{'pid':100},{'pid':101}]
    snapshot={101:{'status':'S','pgid':99}}
    if victim is not None:snapshot[100]=victim
    validate_after_close(snapshot,workers,100)
    snapshot[100]={'status':'R','pgid':99}
    with pytest.raises(AssertionError,match='still executes'):validate_after_close(snapshot,workers,100)
    snapshot.pop(100)
    snapshot[101]={'status':'Z','pgid':99}
    with pytest.raises(AssertionError,match='surviving worker'):validate_after_close(snapshot,workers,100)

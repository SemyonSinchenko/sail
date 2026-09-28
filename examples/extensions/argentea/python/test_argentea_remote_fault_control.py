import copy
import pytest

from argentea_remote_fault_control import bind_supervised_workers


def fixture():
    targets = [dict(sail='/a/sail'), dict(sail='/b/sail')]
    inventories = [dict(host='a'), dict(host='b')]
    records = [dict(event='sail_remote_started', hostname=host, worker_id=str(i),
                    pid=123, argv=[targets[i]['sail'], 'worker'])
               for i, host in enumerate(('a', 'b'))]
    return records, targets, inventories


def test_equal_pids_on_distinct_hosts_are_distinct_workers():
    workers = bind_supervised_workers(*fixture())
    assert [(w['host'], w['pid']) for w in workers] == [('a', 123), ('b', 123)]


@pytest.mark.parametrize('defect', ['unknown-host', 'duplicate-worker', 'wrong-executable',
                                   'extra-start', 'missing-start', 'same-host', 'unsafe-pid'])
def test_rejects_ambiguous_or_unbound_supervision(defect):
    records, targets, inventories = copy.deepcopy(fixture())
    if defect == 'unknown-host': records[0]['hostname'] = 'other'
    elif defect == 'duplicate-worker': records[1]['worker_id'] = '0'
    elif defect == 'wrong-executable': records[0]['argv'] = ['/other', 'worker']
    elif defect == 'extra-start': records.append(dict(records[0]))
    elif defect == 'missing-start': records.pop()
    elif defect == 'same-host': inventories[1]['host'] = 'a'
    elif defect == 'unsafe-pid': records[0]['pid'] = 1
    with pytest.raises(ValueError):
        bind_supervised_workers(records, targets, inventories)


from argentea_remote_fault_control import validate_remote_window


def window_fixture():
    workers = bind_supervised_workers(*fixture())
    records = [dict(event='init', partition=i, worker_id=i, pid=123,
                    session_id='s', job_id=1, operation_id='op') for i in range(2)]
    snapshots = {(w['host'], w['pid']): dict(status='T', pgid=123) for w in workers}
    return records, dict(operation_id='op'), workers, snapshots


def test_remote_window_keeps_host_identity_with_equal_pids():
    assert validate_remote_window(*window_fixture())['owner_hosts'] == {0:'a', 1:'b'}


@pytest.mark.parametrize('defect', ['running', 'dead', 'wrong-group', 'missing-host',
                                   'terminal', 'wrong-job', 'wrong-operation', 'wrong-worker',
                                   'duplicate-owner'])
def test_remote_window_rejects_invalid_barrier(defect):
    records, request, workers, snapshots = window_fixture()
    if defect == 'running': snapshots['a',123]['status'] = 'R'
    elif defect == 'dead': snapshots['a',123]['status'] = 'ZT'
    elif defect == 'wrong-group': snapshots['a',123]['pgid'] = 456
    elif defect == 'missing-host': del snapshots['b',123]
    elif defect == 'terminal': records.append(dict(records[0], event='result'))
    elif defect == 'wrong-job': records[0]['job_id'] = 2
    elif defect == 'wrong-operation': records[0]['operation_id'] = 'other'
    elif defect == 'wrong-worker': records[0]['worker_id'] = 99
    elif defect == 'duplicate-owner': records[1]['partition'] = 0
    with pytest.raises(ValueError):
        validate_remote_window(records, request, workers, snapshots)


@pytest.mark.parametrize('fault', ['cancel', 'worker-loss'])
def test_controller_holds_both_hosts_before_injection(monkeypatch, fault):
    import argentea_remote_fault_control as module
    import argentea_resource_evidence as resources
    records, request, workers, _ = window_fixture()
    states = {w['host']:'R' for w in workers}
    calls = []
    def control(worker, payload):
        host = worker['host']
        if 'signal' in payload:
            name = payload['signal'];calls.append((host,name))
            if name=='SIGKILL':assert all(s=='T' for s in states.values())
            states[host]={'SIGSTOP':'T','SIGCONT':'R','SIGKILL':'dead'}[name]
            return dict(pid=123,delivered=True)
        return dict(pid=123,alive=states[host]!='dead',pgid=123,status=states[host])
    class Token:
        def cancel(self):
            assert all(s=='T' for s in states.values())
            calls.append(('query','cancel'))
    monkeypatch.setattr(module,'control_worker',control)
    monkeypatch.setattr(resources,'read_complete_log',lambda _: '')
    monkeypatch.setattr(resources,'operation_records',lambda *_: records)
    evidence={}
    controller=module.RemoteFaultController(fault,None,request,workers,Token(),evidence,victim_owner=1)
    controller.run()
    assert 'controller_error' not in evidence
    assert calls[:2]==[('a','SIGSTOP'),('b','SIGSTOP')]
    assert evidence['window']['owner_hosts']=={0:'a',1:'b'}
    assert states == ({'a':'R','b':'R'} if fault=='cancel' else {'a':'R','b':'dead'})


@pytest.mark.parametrize('reply', [dict(pid=999,delivered=True),
                                   dict(pid=123,error='ProcessLookupError')])
def test_control_transport_rejects_wrong_child_or_supervisor_error(monkeypatch, reply):
    import sys
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
    import two_host
    from argentea_remote_fault_control import control_worker
    worker=bind_supervised_workers(*fixture())[0]
    worker['fault_control']='/tmp/test/control.sock'
    monkeypatch.setattr(two_host,'target_python',lambda *args, **kwargs: reply)
    with pytest.raises(ValueError):control_worker(worker,{'signal':'SIGSTOP'})


def test_controller_rejects_result_before_pause_and_resumes(monkeypatch):
    import argentea_remote_fault_control as module
    import argentea_resource_evidence as resources
    records, request, workers, _ = window_fixture()
    records.append(dict(records[0],event='result'))
    states={w['host']:'R' for w in workers};signals=[];canceled=[]
    def control(worker,payload):
        host=worker['host']
        if 'signal' in payload:
            signals.append(payload['signal'])
            states[host]='T' if payload['signal']=='SIGSTOP' else 'R'
            return dict(pid=123,delivered=True)
        return dict(pid=123,alive=True,status=states[host],pgid=123)
    class Token:
        def cancel(self):canceled.append(True)
    monkeypatch.setattr(module,'control_worker',control)
    monkeypatch.setattr(resources,'read_complete_log',lambda _: '')
    monkeypatch.setattr(resources,'operation_records',lambda *_: records)
    evidence={}
    module.RemoteFaultController('worker-loss',None,request,workers,Token(),evidence).run()
    assert 'controller_error' in evidence and canceled
    assert 'injection_started_utc' not in evidence and 'SIGKILL' not in signals
    assert states=={'a':'R','b':'R'}


def test_terminal_inventory_keeps_same_pid_survivor_on_other_host(monkeypatch):
    import qualify_faults as module
    from types import SimpleNamespace

    class Row:
        def __init__(self, **values): self.values = values
        def asDict(self): return self.values

    spark = SimpleNamespace(sql=lambda _: SimpleNamespace(
        collect=lambda: [Row(status='FAILED')]))
    records = [dict(event='init', partition=i, worker_id=i, pid=123) for i in range(2)]
    records.append(dict(event='close', partition=1, worker_id=1, pid=123))
    monkeypatch.setattr(module, 'inventory', lambda _: ([], [dict(slot_group='worker-extension:test', job_id=7)]))
    monkeypatch.setattr(module, 'read_complete_log', lambda _: '')
    monkeypatch.setattr(module, 'operation_records', lambda *_: records)
    monkeypatch.setattr(module.time, 'sleep', lambda _: pytest.fail('completed owners were not recognized'))
    check = dict(request={}, case='worker-loss', injection=dict(killed_worker=dict(worker_id=0, pid=123)))
    module.terminal_inventory(spark, check, 'unused')


def test_control_transport_reaches_real_supervised_child():
    import subprocess
    import sys
    import threading
    from remote_fault_socket import FaultSocket
    from argentea_remote_fault_control import control_worker

    child = subprocess.Popen([sys.executable, '-I', '-c', 'import time;time.sleep(60)'],
                             start_new_session=True)
    server = FaultSocket(child)
    stop = threading.Event()
    def serve():
        while not stop.is_set():
            server.serve_pending()
            stop.wait(.005)
    thread = threading.Thread(target=serve)
    thread.start()
    worker = dict(pid=child.pid, target=dict(python=sys.executable), fault_control=server.path)
    try:
        state = control_worker(worker, {'state': True})
        assert state['alive'] and state['pgid'] == child.pid
        assert control_worker(worker, {'signal': 'SIGSTOP'})['delivered']
        assert 'T' in control_worker(worker, {'state': True})['status']
        assert control_worker(worker, {'signal': 'SIGCONT'})['delivered']
        assert control_worker(worker, {'signal': 'SIGKILL'})['delivered']
    finally:
        stop.set()
        thread.join(5)
        if child.poll() is None: child.kill()
        child.wait(timeout=5)
        server.close()
    assert not thread.is_alive()


def test_query_end_during_hold_prevents_late_fault_and_resumes(monkeypatch):
    import argentea_remote_fault_control as module
    import argentea_resource_evidence as resources
    records, request, workers, _ = window_fixture()
    calls = []
    state = {'a': 'R', 'b': 'R'}
    def control(worker, payload):
        host = worker['host']
        if 'signal' in payload:
            calls.append((host, payload['signal']))
            if payload['signal'] == 'SIGSTOP':
                state[host] = 'T'
                controller.stop.set()
            elif payload['signal'] == 'SIGCONT': state[host] = 'R'
            else: pytest.fail('late destructive signal')
            return dict(delivered=True, pid=123)
        return dict(alive=True, status=state[host], pgid=123, pid=123)
    from types import SimpleNamespace
    token = SimpleNamespace(cancel=lambda: calls.append(('query', 'cancel')))
    monkeypatch.setattr(module, 'control_worker', control)
    monkeypatch.setattr(resources, 'read_complete_log', lambda _: '')
    monkeypatch.setattr(resources, 'operation_records', lambda *_: records)
    evidence = {}
    controller = module.RemoteFaultController('worker-loss', None, request, workers, token, evidence)
    controller.run()
    assert 'controller_error' in evidence
    assert 'injection_started_utc' not in evidence
    assert state == {'a': 'R', 'b': 'R'}
    assert calls == [('a', 'SIGSTOP'), ('query', 'cancel'), ('a', 'SIGCONT')]


def test_remote_control_uses_bounded_transport_timeout(monkeypatch):
    import two_host
    from argentea_remote_fault_control import control_worker
    def transport(*args, **kwargs):
        assert kwargs == {'timeout': 5}
        return dict(pid=123, alive=True)
    monkeypatch.setattr(two_host, 'target_python', transport)
    assert control_worker(dict(pid=123, target={}, fault_control='/tmp/control'),
                          {'state': True})['alive']

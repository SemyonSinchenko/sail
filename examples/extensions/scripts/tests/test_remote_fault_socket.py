import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from remote_fault_socket import FaultSocket


def test_supervised_child_stop_continue_kill_and_cleanup():
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    control = FaultSocket(child)
    path = Path(control.path)
    try:
        def send(name):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(2)
                client.connect(str(path))
                client.sendall(json.dumps(dict(signal=name)).encode()+b'\n')
                control.serve_pending()
                return json.loads(client.recv(1024))
        assert send('SIGSTOP')['delivered']
        deadline = time.monotonic()+3
        while True:
            state = subprocess.check_output(['ps','-o','stat=','-p',str(child.pid)],text=True)
            if 'T' in state:break
            assert time.monotonic()<deadline
            time.sleep(.01)
        assert send('SIGCONT')['delivered']
        assert send('SIGKILL')['delivered']
        assert child.wait(timeout=3) == -signal.SIGKILL
        with pytest.raises(ProcessLookupError): control.apply(dict(signal='SIGSTOP'))
    finally:
        if child.poll() is None:child.kill()
        child.wait(timeout=3)
        control.close()
    assert not path.exists()


@pytest.mark.parametrize('payload',[{}, {'signal':'SIGTERM'}, {'signal':'SIGKILL','pid':1}, []])
def test_caller_cannot_select_process_or_arbitrary_signal(payload):
    class NoProcessAccess:
        def poll(self):raise AssertionError('invalid request accessed process')
    control = FaultSocket(NoProcessAccess())
    try:
        with pytest.raises(ValueError):control.apply(payload)
    finally:control.close()


def test_real_supervisor_lease_closure_cleans_stopped_child(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'two_host_remote.py'
    log = tmp_path/'supervisor.log'
    with log.open('w') as stream:
        supervisor = subprocess.Popen([sys.executable, str(script)], stdin=subprocess.PIPE,
                                      stdout=stream, stderr=subprocess.STDOUT)
        try:
            request = dict(version=1, argv=[sys.executable,'-c','import time; time.sleep(60)'],
                           environment={'SAIL_QUALIFICATION_FAULT_CONTROL':'1'}, lease_seconds=15)
            supervisor.stdin.write(json.dumps(request).encode()+b'\n');supervisor.stdin.flush()
            deadline=time.monotonic()+5
            while True:
                events=[json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
                started=next((e for e in events if e['event']=='sail_remote_started'),None)
                if started:break
                assert supervisor.poll() is None,log.read_text()
                assert time.monotonic()<deadline
                time.sleep(.01)
            def call(payload):
                with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as client:
                    client.settimeout(3);client.connect(started['fault_control'])
                    client.sendall(json.dumps(payload).encode()+b'\n')
                    return json.loads(client.recv(1024))
            assert call({'signal':'SIGSTOP'})['delivered']
            deadline=time.monotonic()+3
            while True:
                state=call({'state':True})
                if 'T' in state['status']:break
                assert time.monotonic()<deadline
            assert state['pgid']==started['pid']
            supervisor.stdin.close()
            supervisor.wait(timeout=12)
            events=[json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
            assert any(e['event']=='sail_remote_lease_closed' for e in events)
            assert any(e['event']=='sail_remote_stopped' for e in events)
            assert not Path(started['fault_control']).exists()
            with pytest.raises(ProcessLookupError):
                __import__('os').kill(started['pid'],0)
        finally:
            if supervisor.poll() is None:
                supervisor.terminate();supervisor.wait(timeout=12)

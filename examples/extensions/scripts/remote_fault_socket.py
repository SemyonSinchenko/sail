"""Opt-in, host-local fault controls for a qualification supervisor's child."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile


class FaultSocket:
    def __init__(self, process):
        self.process = process
        self.directory = tempfile.TemporaryDirectory(prefix='sail-fault-')
        self.path = str(Path(self.directory.name) / 'control.sock')
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(self.path)
        os.chmod(self.path, 0o600)
        self.socket.listen(4)
        self.socket.setblocking(False)

    def apply(self, request):
        # The caller cannot name a PID. Popen retains ownership of this child;
        # no other thread reaps it between poll and kill.
        if request == {'state': True}:
            if self.process.poll() is not None:
                return dict(pid=self.process.pid, alive=False)
            rows = subprocess.check_output(
                ['ps', '-o', 'pid=,pgid=,stat=', '-p', str(self.process.pid)],
                text=True, timeout=2).split()
            if len(rows) != 3 or int(rows[0]) != self.process.pid:
                raise ValueError('invalid child process state')
            return dict(pid=self.process.pid, alive=True, pgid=int(rows[1]), status=rows[2])
        if not isinstance(request, dict) or set(request) != {'signal'}:
            raise ValueError('expected one signal field')
        name = request['signal']
        if name not in ('SIGSTOP', 'SIGCONT', 'SIGKILL'):
            raise ValueError('unsupported qualification signal')
        if self.process.poll() is not None:
            raise ProcessLookupError('supervised child has exited')
        os.kill(self.process.pid, getattr(signal, name))
        return dict(pid=self.process.pid, signal=name, delivered=True)

    def serve_pending(self):
        try:
            connection, _ = self.socket.accept()
        except BlockingIOError:
            return
        with connection:
            connection.settimeout(1)
            try:
                data = bytearray()
                while not data.endswith(b'\n'):
                    chunk = connection.recv(1025-len(data))
                    if not chunk:
                        raise ValueError('incomplete control request')
                    data.extend(chunk)
                    if len(data) > 1024:
                        raise ValueError('control request too large')
                result = self.apply(json.loads(data))
            except Exception as error:
                result = dict(delivered=False, error=type(error).__name__)
            try:
                connection.sendall(json.dumps(result).encode()+b'\n')
            except OSError:
                pass

    def close(self):
        self.socket.close()
        self.directory.cleanup()

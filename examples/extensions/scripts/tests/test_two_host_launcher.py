"""Real-process cleanup controls for the trusted SSH worker launcher."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[2]


def wait_for(predicate, message):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


def stopped(pid):
    try:
        os.kill(pid, 0)
        return False
    except ProcessLookupError:
        return True


class LauncherLifecycle(unittest.TestCase):
    def run_cleanup(self, abrupt):
        with tempfile.TemporaryDirectory(prefix="sail-launcher-lifecycle-") as temporary:
            directory = Path(temporary)
            pidfile = directory / "worker.pid"
            worker = directory / "worker"
            worker.write_text(f"#!{sys.executable}\n" + """
import os
from pathlib import Path
import time
Path(os.environ["SAIL_CLUSTER__SESSION_ID"]).write_text(str(os.getpid()))
while True:
    time.sleep(1)
""")
            worker.chmod(0o755)
            target = dict(repo=str(REPO), python=sys.executable, sail=str(worker), advertise="127.0.0.1")
            env = dict(os.environ, SAIL_CLUSTER__WORKER_ID="1", SAIL_CLUSTER__SESSION_ID=str(pidfile))
            for key in ("PYTHONHOME", "PYTHONPATH", "DYLD_LIBRARY_PATH"):
                env.pop(key, None)
            with (directory / "log").open("w") as output:
                launcher = subprocess.Popen([sys.executable, str(SCRIPTS / "two_host_worker.py"),
                                             "--targets-json", json.dumps([target])],
                                            env=env, stdout=output, stderr=subprocess.STDOUT)
                worker_pid = None
                try:
                    wait_for(pidfile.exists, "worker never started")
                    worker_pid = int(pidfile.read_text())
                    launcher.send_signal(signal.SIGKILL if abrupt else signal.SIGTERM)
                    launcher.wait(timeout=15)
                    wait_for(lambda: stopped(worker_pid), "launcher exit orphaned its worker")
                    wait_for(lambda: "sail_remote_stopped" in (directory / "log").read_text(),
                             "remote supervisor did not acknowledge worker cleanup")
                finally:
                    if launcher.poll() is None:
                        launcher.kill()
                        launcher.wait()
                    if worker_pid is not None and not stopped(worker_pid):
                        os.kill(worker_pid, signal.SIGKILL)

    def test_normal_launcher_close_stops_worker(self):
        self.run_cleanup(False)

    def test_sigkill_closes_stdin_lease_and_stops_worker(self):
        self.run_cleanup(True)

    def test_invalid_remote_argv_never_starts_process(self):
        result = subprocess.run([sys.executable, str(SCRIPTS / "two_host_remote.py")],
                                input=json.dumps(dict(version=1, argv=[123])).encode() + b"\n",
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(b"sail_remote_started", result.stdout)

    def test_open_pipe_without_heartbeats_expires_and_stops_worker(self):
        with tempfile.TemporaryDirectory(prefix="sail-lease-expiry-") as temporary:
            directory = Path(temporary)
            pidfile = directory / "worker.pid"
            code = ("import os,time; from pathlib import Path; "
                    f"Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(300)")
            with (directory / "log").open("w") as output:
                supervisor = subprocess.Popen(
                    [sys.executable, str(SCRIPTS / "two_host_remote.py")],
                    stdin=subprocess.PIPE, stdout=output, stderr=subprocess.STDOUT)
                request = dict(version=1, argv=[sys.executable, "-c", code], lease_seconds=2)
                supervisor.stdin.write(json.dumps(request).encode() + b"\n")
                supervisor.stdin.flush()
                worker_pid = None
                try:
                    def ready():
                        # Hold the lease while bootstrapping a slow interpreter;
                        # the expiry window starts only after its ready marker.
                        supervisor.stdin.write(b"\n")
                        supervisor.stdin.flush()
                        return pidfile.exists()

                    wait_for(ready, "worker never started")
                    worker_pid = int(pidfile.read_text())
                    # Keep stdin open: expiry must be caused by missing heartbeats.
                    supervisor.wait(timeout=15)
                    self.assertTrue(stopped(worker_pid), "expired lease orphaned its worker")
                    self.assertIn("sail_remote_lease_expired", (directory / "log").read_text())
                    self.assertIn("sail_remote_stopped", (directory / "log").read_text())
                finally:
                    supervisor.stdin.close()
                    if supervisor.poll() is None:
                        supervisor.kill()
                        supervisor.wait()
                    if worker_pid is not None and not stopped(worker_pid):
                        os.kill(worker_pid, signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()

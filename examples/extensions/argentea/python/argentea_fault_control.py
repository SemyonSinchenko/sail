"""Qualification-only POSIX process barrier; never installed in the extension."""
from collections import Counter
import datetime
import os
import re
import signal
import subprocess
import threading
import time

from argentea_resource_evidence import operation_records, read_complete_log


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def workers_from_log(text, driver):
    pattern = r'extension process worker (\d+): pid=Some\((\d+)\), driver_pid=(\d+), session=([0-9a-f-]+)'
    workers = [dict(worker_id=int(w), pid=int(p), driver_pid=int(d), session_id=s)
               for w, p, d, s in re.findall(pattern, text)]
    assert len(workers) == 2 and len({r['pid'] for r in workers}) == 2
    assert all(r['driver_pid'] == driver and r['pid'] != driver for r in workers)
    assert len({r['session_id'] for r in workers}) == 1
    return sorted(workers, key=lambda r: r['worker_id'])


def states(pids):
    result = subprocess.run(['ps', '-o', 'pid=,pgid=,stat=', '-p', ','.join(map(str, pids))],
                            text=True, capture_output=True, check=False)
    assert result.returncode in (0, 1), result.stderr
    return {int(row[0]): dict(pgid=int(row[1]), status=row[2])
            for line in result.stdout.splitlines() if (row := line.split())}


def validate_window(records, request, workers, process_states, driver):
    """The observed stop state, not elapsed time, establishes a held window."""
    assert Counter(r['partition'] for r in records if r['event'] == 'init') == Counter(range(2))
    assert not any(r['event'] in ('result', 'close', 'failure') for r in records), 'terminal native event preceded fault'
    assert {r['pid'] for r in records} == {w['pid'] for w in workers}
    assert len({(r['session_id'], r['job_id']) for r in records}) == 1
    for w in workers:
        assert w['pid'] in process_states and 'T' in process_states[w['pid']]['status'], 'worker is not stopped'
        assert process_states[w['pid']]['pgid'] == driver, 'worker escaped supervised process group'
        own = [r for r in records if r['pid'] == w['pid']]
        assert all(r['worker_id'] == w['worker_id'] and r['session_id'] == w['session_id'] for r in own)
        assert all(r['operation_id'] == request['operation_id'] for r in own)
    return dict(recorded_utc=utc(), native_receipts=records, process_states=process_states,
                initialized_owners=2, terminal_events_before_fault=0, both_workers_stopped=True)


class FaultController:
    def __init__(self, case, log, request, workers, driver, token, evidence):
        self.case, self.log, self.request = case, log, request
        self.workers, self.driver, self.token, self.evidence = workers, driver, token, evidence
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name='argentea-fault-controller')
        self.evidence['signals'] = []

    def send(self, pid, sig):
        # Never signal a PID identified only by an untrusted native receipt.
        assert pid in {w['pid'] for w in self.workers} and pid != self.driver
        current = states([pid])
        assert pid in current and current[pid]['pgid'] == self.driver
        os.kill(pid, sig)
        self.evidence['signals'].append(dict(pid=pid, signal=sig.name, recorded_utc=utc()))

    def resume(self):
        for worker in self.workers:
            pid = worker['pid']
            current = states([pid])
            if pid in current and 'T' in current[pid]['status']:
                self.send(pid, signal.SIGCONT)

    def run(self):
        try:
            deadline = time.monotonic() + 90
            while True:
                if self.stop.is_set():
                    raise RuntimeError('query ended before the active-window barrier')
                records = operation_records(read_complete_log(self.log), self.request)
                if Counter(r['partition'] for r in records if r['event'] == 'init') == Counter(range(2)):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('both native owners did not initialize')
                self.stop.wait(.002)
            for worker in self.workers:
                self.send(worker['pid'], signal.SIGSTOP)
            deadline = time.monotonic() + 5
            while True:
                snapshot = states([w['pid'] for w in self.workers])
                if len(snapshot) == 2 and all('T' in p['status'] for p in snapshot.values()):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('workers did not reach POSIX stopped state')
                self.stop.wait(.002)
            self.evidence['window'] = validate_window(
                operation_records(read_complete_log(self.log), self.request), self.request,
                self.workers, snapshot, self.driver)
            self.evidence['injection_started_utc'] = utc()
            if self.case == 'cancel':
                self.token.cancel()
            else:
                victim = self.workers[0]
                self.send(victim['pid'], signal.SIGKILL)
                self.evidence['killed_worker'] = victim
            self.evidence['injection_finished_utc'] = utc()
        except BaseException as error:
            self.evidence['controller_error'] = repr(error)
            # This is cleanup, not accepted fault evidence.
            self.token.cancel()
        finally:
            self.resume()

    def finish(self):
        self.stop.set()
        self.thread.join(15)
        self.resume()
        assert not self.thread.is_alive(), 'fault controller did not terminate'
        assert 'controller_error' not in self.evidence, self.evidence.get('controller_error')


def validate_after_close(snapshot, workers, killed):
    for worker in workers:
        state = snapshot.get(worker['pid'])
        if worker['pid'] == killed:
            assert state is None or 'Z' in state['status'], 'SIGKILL victim still executes'
        else:
            assert state is not None and not any(c in state['status'] for c in 'ZT'), 'surviving worker is dead or stopped'

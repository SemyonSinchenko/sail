"""Host-qualified supervision identities for physical fault qualification.

Only launcher records paired with preflight inventory may identify a signal
recipient. Native receipts subsequently prove which graph owner ran there.
"""


def bind_supervised_workers(supervisors, targets, inventories):
    if len(targets) != 2 or len(inventories) != 2:
        raise ValueError('require exactly two worker targets and inventories')
    hosts = [item['host'] for item in inventories]
    if len(set(hosts)) != 2:
        raise ValueError('require distinct physical worker hosts')
    by_host = dict(zip(hosts, targets))
    workers = []
    for record in supervisors:
        if record.get('event') != 'sail_remote_started' or record.get('worker_id') is None:
            continue
        host = record['hostname']
        if host not in by_host:
            raise ValueError('worker supervisor host absent from preflight inventory')
        worker_id, pid = int(record['worker_id']), int(record['pid'])
        if worker_id < 0 or pid <= 1:
            raise ValueError('invalid supervised worker identity')
        target = by_host[host]
        if record.get('argv') != [target['sail'], 'worker']:
            raise ValueError('supervisor executable differs from configured worker')
        workers.append(dict(host=host, worker_id=worker_id, pid=pid, target=target,
                            fault_control=record.get('fault_control')))
    if len(workers) != 2 or {w['host'] for w in workers} != set(hosts):
        raise ValueError('require one supervised worker per physical host')
    if len({w['worker_id'] for w in workers}) != 2:
        raise ValueError('duplicate supervised worker ID')
    return sorted(workers, key=lambda w: w['worker_id'])


def validate_remote_window(records, request, workers, snapshots):
    """Prove both owners are initialized and held on their supervised hosts.

    Snapshots are keyed by (host, pid), never pid alone. Each remote Sail child
    leads its own process group; the controller's driver PID is irrelevant.
    """
    from collections import Counter
    initialized = [r for r in records if r['event'] == 'init']
    if Counter(r['partition'] for r in initialized) != Counter(range(2)):
        raise ValueError('both native owners must initialize exactly once')
    if any(r['event'] in ('result', 'close', 'failure') for r in records):
        raise ValueError('terminal native event preceded fault')
    if len({(r['session_id'], r['job_id']) for r in records}) != 1:
        raise ValueError('native events span multiple jobs')
    bindings = {(w['worker_id'], w['pid']): w for w in workers}
    if len(bindings) != 2:
        raise ValueError('ambiguous worker bindings')
    for record in records:
        worker = bindings.get((record['worker_id'], record['pid']))
        if worker is None or record['operation_id'] != request['operation_id']:
            raise ValueError('native event does not belong to supervised operation')
    if {(r['worker_id'], r['pid']) for r in initialized} != set(bindings):
        raise ValueError('native owners did not cover both supervised workers')
    for worker in workers:
        state = snapshots.get((worker['host'], worker['pid']))
        if state is None or 'T' not in state['status'] or 'Z' in state['status']:
            raise ValueError('remote worker is not held alive in stopped state')
        if state['pgid'] != worker['pid']:
            raise ValueError('remote worker is not its supervised group leader')
    return dict(initialized_owners=2, terminal_events_before_fault=0,
                both_workers_stopped=True,
                owner_hosts={r['partition']: bindings[(r['worker_id'], r['pid'])]['host']
                             for r in initialized})


REMOTE_CONTROL = r"""
import json, socket, sys
path, request = sys.argv[1], json.loads(sys.argv[2])
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
    connection.settimeout(4)
    connection.connect(path)
    connection.sendall(json.dumps(request).encode()+b'\n')
    data = bytearray()
    while not data.endswith(b'\n'):
        chunk = connection.recv(4097-len(data))
        if not chunk or len(data)+len(chunk)>4096:
            raise ValueError('invalid supervisor response')
        data.extend(chunk)
print(data.decode(), end='')
"""


def control_worker(worker, payload):
    import json
    from pathlib import Path
    from two_host import target_python
    endpoint = worker.get('fault_control')
    if not isinstance(endpoint, str) or not Path(endpoint).is_absolute():
        raise ValueError('worker lacks a supervised fault endpoint')
    response = target_python(worker['target'], REMOTE_CONTROL, endpoint, json.dumps(payload), timeout=5)
    if 'error' in response or response.get('pid') != worker['pid']:
        raise ValueError('supervisor rejected request or returned a different child')
    return response


class RemoteFaultController:
    """Hold both initialized owners before cancellation or one-owner loss."""
    def __init__(self, case, log, request, workers, token, evidence, victim_owner=None):
        import threading
        if case not in ('cancel', 'worker-loss'):
            raise ValueError('unsupported remote fault case')
        if victim_owner not in (None, 0, 1):
            raise ValueError('invalid victim owner')
        self.case, self.log, self.request = case, log, request
        self.workers, self.token, self.evidence = workers, token, evidence
        self.victim_owner = victim_owner
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name='argentea-remote-fault')
        self.evidence['signals'] = []
        self.killed = set()

    def send(self, worker, name):
        from argentea_fault_control import utc
        reply = control_worker(worker, {'signal': name})
        if reply.get('delivered') is not True:
            raise ValueError('supervisor did not confirm signal')
        self.evidence['signals'].append(dict(host=worker['host'], pid=worker['pid'],
                                             signal=name, recorded_utc=utc()))

    def snapshot(self):
        return {(w['host'], w['pid']): control_worker(w, {'state': True})
                for w in self.workers if (w['host'], w['pid']) not in self.killed}

    def resume(self):
        errors = []
        for worker in self.workers:
            if (worker['host'], worker['pid']) in self.killed:
                continue
            try:
                state = control_worker(worker, {'state': True})
                if state.get('alive') and 'T' in state['status']:
                    self.send(worker, 'SIGCONT')
            except Exception as error:
                errors.append(dict(host=worker['host'], pid=worker['pid'], error=repr(error)))
        if errors:
            self.evidence.setdefault('resume_errors', []).extend(errors)

    def run(self):
        from collections import Counter
        import time
        from argentea_fault_control import utc
        from argentea_resource_evidence import operation_records, read_complete_log
        def records(): return operation_records(read_complete_log(self.log), self.request)
        try:
            deadline = time.monotonic()+90
            while Counter(r['partition'] for r in records() if r['event']=='init') != Counter(range(2)):
                if self.stop.is_set(): raise RuntimeError('query ended before remote barrier')
                if time.monotonic() >= deadline: raise TimeoutError('remote owners did not initialize')
                self.stop.wait(.01)
            for worker in self.workers:
                if self.stop.is_set(): raise RuntimeError('query ended before remote hold')
                self.send(worker, 'SIGSTOP')
            deadline = time.monotonic()+10
            while True:
                if self.stop.is_set(): raise RuntimeError('query ended during remote hold')
                snapshot = self.snapshot()
                if all(s.get('alive') and 'T' in s.get('status','') for s in snapshot.values()): break
                if time.monotonic() >= deadline: raise TimeoutError('remote workers did not stop')
                self.stop.wait(.01)
            current = records()
            window = validate_remote_window(current, self.request, self.workers, snapshot)
            window.update(native_receipts=current, process_states=[dict(s, host=h, pid=p)
                          for (h,p),s in snapshot.items()])
            self.evidence['window'] = window
            if self.stop.is_set(): raise RuntimeError('query ended before remote injection')
            self.evidence['injection_started_utc'] = utc()
            if self.case == 'cancel': self.token.cancel()
            else:
                owner = 0 if self.victim_owner is None else self.victim_owner
                record = next(r for r in current if r['event']=='init' and r['partition']==owner)
                worker = next(w for w in self.workers if (w['worker_id'],w['pid']) == (record['worker_id'],record['pid']))
                self.send(worker,'SIGKILL')
                self.killed.add((worker['host'],worker['pid']))
                self.evidence.update(requested_native_owner=self.victim_owner,
                                     selected_native_owner=owner, killed_worker=worker)
            self.evidence['injection_finished_utc'] = utc()
        except BaseException as error:
            self.evidence['controller_error'] = repr(error)
            self.token.cancel()
        finally: self.resume()

    def finish(self):
        self.stop.set()
        self.thread.join(30)
        if self.thread.is_alive(): raise RuntimeError('remote fault controller did not terminate')
        self.resume()
        if 'controller_error' in self.evidence or self.evidence.get('resume_errors'):
            raise RuntimeError('remote fault control or resume failed; inspect receipt')

"""The gate must not confuse a listening driver with a registered worker pair."""
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import argentea_readiness as readiness


def worker(worker_id, port, status='RUNNING'):
    return dict(worker_id=worker_id, host='127.0.0.1', port=port, status=status)


class Session:
    def __init__(self, snapshots):
        self.snapshots = iter(snapshots)
        self.started = False
        self.queries = 0

    def range(self, start, stop, *, numPartitions):
        assert (start, stop, numPartitions) == (0, 1, 1)
        self.started = True
        return SimpleNamespace(count=lambda: 1)

    def sql(self, query):
        assert self.started
        assert query == readiness.WORKERS_SQL
        self.queries += 1
        snapshot = next(self.snapshots)
        if isinstance(snapshot, Exception):
            raise snapshot
        return SimpleNamespace(collect=lambda: [
            SimpleNamespace(asDict=lambda row=row: row) for row in snapshot])


@pytest.fixture
def clock(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(readiness.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(readiness.time, 'sleep', lambda delay: now.__setitem__(0, now[0] + delay))
    return now


def test_waits_for_registration_in_same_session(clock):
    early = [worker(2, 50461), worker(1, 0, 'STARTING')]
    ready = [worker(2, 50461), worker(1, 50462)]
    session = Session([early, early, ready])
    evidence = {}
    assert readiness.wait_for_workers(session, evidence=evidence) == list(reversed(ready))
    result = evidence['worker_readiness']
    assert result['outcome'] == 'ready' and session.queries == 3
    assert len(result['snapshots']) == 2
    assert result['elapsed_seconds'] == pytest.approx(.2)


@pytest.mark.parametrize('rows', [
    [worker(1, 1), worker(1, 2)],
    [worker(1, 1), worker(2, 1)],
    [worker(1, 1), worker(2, 0)],
    [worker(1, 1), worker(2, 2, 'STARTING')],
])
def test_duplicates_and_unready_worker_do_not_count(clock, rows):
    evidence = {}
    with pytest.raises(TimeoutError, match='distinct Sail workers'):
        readiness.wait_for_workers(Session([rows] * 4), evidence=evidence, timeout=.2)
    assert evidence['worker_readiness']['outcome'] == 'failed'
    assert len(evidence['worker_readiness']['snapshots']) == 1


def test_rpc_error_is_retained_and_not_retried(clock):
    error = RuntimeError('worker catalog unavailable')
    session = Session([error])
    evidence = {}
    with pytest.raises(RuntimeError) as raised:
        readiness.wait_for_workers(session, evidence=evidence)
    assert raised.value is error and session.queries == 1
    assert 'worker catalog unavailable' in evidence['worker_readiness']['error']


@pytest.mark.parametrize('options', [dict(minimum=0), dict(minimum=True),
    dict(timeout=0), dict(timeout=float('nan')), dict(timeout=float('inf'))])
def test_invalid_options_do_not_start_a_session(options):
    session = Session([])
    with pytest.raises(ValueError):
        readiness.wait_for_workers(session, evidence={}, **options)
    assert not session.started


def test_slow_catalog_cannot_report_ready_after_deadline(clock):
    session = Session([[worker(1, 1), worker(2, 2)]])
    query = session.sql
    def slow(query_text):
        clock[0] = 2
        return query(query_text)
    session.sql = slow
    with pytest.raises(TimeoutError):
        readiness.wait_for_workers(session, evidence={}, timeout=1)

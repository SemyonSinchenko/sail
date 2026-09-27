"""Regression checks for shutdown races observed in process-worker pilots."""
import signal
from types import SimpleNamespace

import runtime


def test_permission_denied_with_member_keeps_waiting(monkeypatch):
    def denied(*_):
        raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(runtime.os, 'killpg', denied)
    monkeypatch.setattr(runtime.subprocess, 'check_output', lambda *a, **k: ' 123 77\n')
    assert runtime.group_exists(77)


def test_permission_denied_requires_independent_absence(monkeypatch):
    def denied(*_):
        raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(runtime.os, 'killpg', denied)
    monkeypatch.setattr(runtime.subprocess, 'check_output', lambda *a, **k: ' 123 78\n')
    assert not runtime.group_exists(77)


def test_exited_driver_does_not_skip_descendant_shutdown(monkeypatch):
    calls = []
    monkeypatch.setattr(runtime.os, 'killpg', lambda *args: calls.append(args))
    clock = iter([0, 16, 16, 17])
    monkeypatch.setattr(runtime.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(runtime, 'group_exists', lambda _: False)
    process = SimpleNamespace(pid=77, poll=lambda: 0, wait=lambda **kwargs: 0)
    runtime.stop_group(process)
    assert calls == [(77, signal.SIGINT), (77, signal.SIGKILL)]

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

EXAMPLES = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(Path(__file__).parent), str(EXAMPLES / 'scripts'), str(EXAMPLES / 'benchmarks')]
from argentea_runtime import local_server, runtime_environment
from launch_worker import worker_environment


def arguments(tmp_path, mode='process-cluster'):
    return SimpleNamespace(mode=mode, worker_task_slots=32, sail_pool_bytes=2 << 30,
        native_quota=256 << 20, partitions=3, threads=4, output=tmp_path, sail_binary=Path('/tmp/sail'))


def test_local_environment_pins_admission_and_debug_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv('SAIL_EXPERIMENTAL_WORKER_COMMAND', '["stale"]')
    monkeypatch.setenv('SAIL_ARGENTEA_AUDIT_PATH', '/stale/audit')
    env = runtime_environment(arguments(tmp_path), tmp_path)
    assert 'SAIL_EXPERIMENTAL_WORKER_COMMAND' not in env
    assert 'SAIL_ARGENTEA_AUDIT_PATH' not in env
    assert env['SAIL_MODE'] == 'local-cluster'
    assert env['SAIL_CLUSTER__TASK_MAX_ATTEMPTS'] == '3'
    assert env['SAIL_ARGENTEA_MEMORY_BYTES'] == str(256 << 20)
    assert env['SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE'] == str(2 << 30)
    assert 'sail_execution::task_runner::actor::handler=debug' in env['RUST_LOG']
    assert env['PYTHONHOME'] == sys.base_prefix
    assert runtime_environment(arguments(tmp_path, 'local'), tmp_path)['SAIL_EXPERIMENTAL_PROCESS_WORKERS'] == '0'


def test_two_host_worker_keeps_resource_envelope_but_not_unrelated_environment():
    incoming = dict(SAIL_CLUSTER__WORKER_ID='2', SAIL_EXECUTION__DEFAULT_PARALLELISM='3',
        SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE='1024', SAIL_ARGENTEA_MEMORY_BYTES='512',
        RUST_LOG='info,handler=debug', TOKIO_WORKER_THREADS='4', PRIVATE_TOKEN='secret')
    env = worker_environment(dict(advertise='192.0.2.3', port=6000), incoming)
    assert 'PRIVATE_TOKEN' not in env
    assert env['SAIL_ARGENTEA_MEMORY_BYTES'] == '512'
    assert env['SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE'] == '1024'
    assert env['SAIL_EXECUTION__DEFAULT_PARALLELISM'] == '3'
    assert env['SAIL_CLUSTER__WORKER_EXTERNAL_HOST'] == '192.0.2.3'
    assert env['RUST_LOG'] == incoming['RUST_LOG']


def test_server_cleanup_runs_even_when_exercise_raises(tmp_path, monkeypatch):
    import argentea_runtime as runtime

    process = SimpleNamespace(pid=123)
    calls = []
    monkeypatch.setattr(runtime.subprocess, 'Popen', lambda *a, **kw: process)
    monkeypatch.setattr(runtime, 'wait_server', lambda *a: None)
    monkeypatch.setattr(runtime, 'stop_group', lambda p: calls.append(p.pid))
    with pytest.raises(RuntimeError, match='exercise failure'):
        with local_server(arguments(tmp_path), []):
            raise RuntimeError('exercise failure')
    assert calls == [123]


def test_cleanup_failure_is_retained_without_masking_original_error(tmp_path, monkeypatch):
    import argentea_runtime as runtime

    process = SimpleNamespace(pid=123)
    errors = []
    monkeypatch.setattr(runtime.subprocess, 'Popen', lambda *a, **kw: process)
    monkeypatch.setattr(runtime, 'wait_server', lambda *a: None)
    def fail(_):
        raise RuntimeError('cleanup failure')
    monkeypatch.setattr(runtime, 'stop_group', fail)
    with pytest.raises(RuntimeError, match='exercise failure'):
        with local_server(arguments(tmp_path), errors):
            raise RuntimeError('exercise failure')
    assert errors == [{'operation': 'stop_server_group', 'error': "RuntimeError('cleanup failure')"}]

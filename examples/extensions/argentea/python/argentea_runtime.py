"""Fresh local Sail processes with the existing process-group cleanup contract."""
import contextlib
import os
import socket
import subprocess
import sys
import sysconfig

from runtime import stop_group, validate_admission_settings
from two_host import wait_server


def runtime_environment(args, staging):
    validate_admission_settings(args.worker_task_slots, args.sail_pool_bytes, args.native_quota)
    env = dict(os.environ)
    for key in ('SAIL_INTERNAL__RUN_PYTHON', 'SAIL_EXPERIMENTAL_WORKER_COMMAND',
                'SAIL_EXPERIMENTAL_WORKER_PYTHONPATH', 'SAIL_ARGENTEA_AUDIT_PATH'):
        env.pop(key, None)
    env.update(
        PYTHONHOME=sys.base_prefix, PYTHONPATH=sysconfig.get_paths()['purelib'],
        LD_LIBRARY_PATH=sysconfig.get_config_var('LIBDIR') or '',
        DYLD_LIBRARY_PATH=sysconfig.get_config_var('LIBDIR') or '',
        SAIL_EXPERIMENTAL_EXTENSIONS='1',
        SAIL_MODE='local-cluster' if args.mode == 'process-cluster' else 'local',
        SAIL_EXPERIMENTAL_PROCESS_WORKERS='1' if args.mode == 'process-cluster' else '0',
        SAIL_CLUSTER__WORKER_INITIAL_COUNT='2', SAIL_CLUSTER__WORKER_MAX_COUNT='2',
        SAIL_CLUSTER__WORKER_TASK_SLOTS=str(args.worker_task_slots),
        SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS='600', SAIL_CLUSTER__TASK_MAX_ATTEMPTS='3',
        SAIL_EXECUTION__DEFAULT_PARALLELISM=str(args.partitions),
        SAIL_RUNTIME__MEMORY_POOL__TYPE='greedy',
        SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE=str(args.sail_pool_bytes),
        SAIL_ARGENTEA_MEMORY_BYTES=str(args.native_quota),
        SAIL_NUTMEG_MEMORY_BYTES=str(args.native_quota),
        SAIL_GRAPH_UTILS_ROOT=staging.as_uri(),
        TOKIO_WORKER_THREADS=str(args.threads), RAYON_NUM_THREADS=str(args.threads),
        RUST_LOG='info,sail_execution::task_runner::actor::handler=debug',
    )
    return env


@contextlib.contextmanager
def local_server(args, cleanup_errors):
    staging = args.output / 'staging'
    staging.mkdir()
    env = runtime_environment(args, staging)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    with (args.output / 'server.log').open('w') as log:
        process = subprocess.Popen(
            [str(args.sail_binary.resolve()), 'spark', 'server', '--ip', '127.0.0.1', '--port', str(port)],
            env=env, cwd=args.output, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            wait_server(process, '127.0.0.1', port)
            yield f'sc://127.0.0.1:{port}', process.pid
        finally:
            active_error = sys.exc_info()[1]
            try:
                stop_group(process)
            except BaseException as error:
                cleanup_errors.append({'operation': 'stop_server_group', 'error': repr(error)})
                if active_error is None:
                    raise

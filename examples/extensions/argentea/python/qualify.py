#!/usr/bin/env python3
"""Qualify bounded Argentea PageRank on local processes or two actual hosts."""
import argparse
import datetime
import json
from pathlib import Path
import platform
import re
import sys
import traceback

EXAMPLES = Path(__file__).resolve().parents[2]
REPO = EXAMPLES.parents[1]
sys.path[:0] = [str(EXAMPLES / 'graph-algorithms/src'), str(EXAMPLES / 'scripts'), str(EXAMPLES / 'benchmarks')]
from runtime import git, native_package_identity, package_versions, sha256, validate_admission_settings
from two_host import INVENTORY, completed_worker_tasks, process_cleanup, target_python, wait_server
from two_host_worker import launch, stop
from argentea_client import options
from argentea_evidence import parse_log, parse_worker_tasks, validate_audit
from argentea_exercise import exercise
from argentea_runtime import local_server


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def audit(receipt, log, *, minimum_workers, required_hosts=()):
    records, supervisors = parse_log(log)
    check = receipt['checks']
    receipt['native_receipts'] = records
    receipt['worker_task_statuses'] = tasks = parse_worker_tasks(log)
    receipt['completed_worker_tasks'] = completed_worker_tasks(log)
    receipt['native_execution'] = validate_audit(records, check['rows'], check['request'], check['iterations'],
        minimum_workers=minimum_workers, worker_endpoints=check['worker_endpoints'], supervisors=supervisors,
        required_hosts=required_hosts, stages=check['stages'], task_statuses=tasks)


def two_hosts(args, receipt):
    configuration = json.loads(args.two_host_config.read_text())
    driver, workers = configuration['driver'], configuration['workers']
    if len(workers) != 2:
        raise ValueError('the bounded two-host gate requires exactly two worker targets')
    receipt['configuration'] = configuration
    quota_env = dict(SAIL_ARGENTEA_MEMORY_BYTES=str(args.native_quota), SAIL_NUTMEG_MEMORY_BYTES=str(args.native_quota))
    # Compute manifest identities under the same quota configuration used by the
    # processes. Per-host Python environment and credentials remain host-local.
    inventory_program = 'import os, json, sys\nos.environ.update(json.loads(sys.argv.pop(1)))\n' + INVENTORY
    inventories = [target_python(target, inventory_program, json.dumps(quota_env), target['repo'], target['sail'])
                   for target in (driver, *workers)]
    receipt['inventories'] = inventories
    reference = inventories[0]
    assert all(item['source_commit'] == reference['source_commit'] and not item['source_dirty'] for item in inventories), 'two-host sources must be identical and clean'
    assert all(item['binary_sha256'] == reference['binary_sha256'] and item['packages'] == reference['packages'] for item in inventories), 'two-host binary/package identities differ'
    hostnames = {item['host'] for item in inventories[1:]}
    assert len(hostnames) == 2, 'workers must run on two distinct physical hostnames'
    launcher = [driver['python'], str(Path(driver['repo']) / 'examples/extensions/argentea/python/launch_worker.py'),
                '--targets-json', json.dumps(workers)]
    env = dict(quota_env, SAIL_EXPERIMENTAL_EXTENSIONS='1', SAIL_MODE='local-cluster',
        SAIL_EXPERIMENTAL_PROCESS_WORKERS='1', SAIL_EXPERIMENTAL_WORKER_COMMAND=json.dumps(launcher),
        SAIL_CLUSTER__DRIVER_LISTEN_HOST='0.0.0.0', SAIL_CLUSTER__DRIVER_LISTEN_PORT=str(driver['gateway_port']),
        SAIL_CLUSTER__DRIVER_EXTERNAL_HOST=driver['advertise'], SAIL_CLUSTER__DRIVER_EXTERNAL_PORT=str(driver['gateway_port']),
        SAIL_CLUSTER__WORKER_INITIAL_COUNT='2', SAIL_CLUSTER__WORKER_MAX_COUNT='2',
        SAIL_CLUSTER__WORKER_TASK_SLOTS=str(args.worker_task_slots), SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS='600',
        SAIL_CLUSTER__TASK_MAX_ATTEMPTS='3', SAIL_EXECUTION__DEFAULT_PARALLELISM=str(args.partitions),
        SAIL_RUNTIME__MEMORY_POOL__TYPE='greedy', SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE=str(args.sail_pool_bytes),
        TOKIO_WORKER_THREADS=str(args.threads), RAYON_NUM_THREADS=str(args.threads),
        RUST_LOG='info,sail_execution::task_runner::actor::handler=debug')
    log_path = args.output / 'server-and-workers.log'
    with log_path.open('w') as log:
        process = launch(driver, [driver['sail'], 'spark', 'server', '--ip', '0.0.0.0', '--port', str(driver['connect_port'])], env, stdout=log)
        try:
            wait_server(process, driver['advertise'], driver['connect_port'])
            receipt['checks'] = exercise(f"sc://{driver['advertise']}:{driver['connect_port']}", args.output,
                                         iterations=args.iterations, partitions=args.partitions)
        finally:
            stop(process)
            receipt['driver_supervisor_returncode'] = process.returncode
            log.flush()
            receipt['process_cleanup'] = process_cleanup([driver, *workers], inventories, log_path)
    audit(receipt, log_path.read_text(), minimum_workers=2, required_hosts=hostnames)
    assert not any(row['alive'] for rows in receipt['process_cleanup'].values() for row in rows), 'supervised Sail process remained alive'
    assert receipt['driver_supervisor_returncode'] == 0, 'driver shutdown failed'


def local_processes(args, receipt):
    if args.sail_binary is None:
        raise ValueError('--sail-binary is required for a local process gate')
    receipt['binary_sha256'] = sha256(args.sail_binary)
    receipt['native_package'] = native_package_identity()
    cleanup_errors = receipt['cleanup_errors'] = []
    with local_server(args, cleanup_errors) as (endpoint, pid):
        receipt['driver_pid'] = pid
        receipt['checks'] = exercise(endpoint, args.output, iterations=args.iterations,
            partitions=args.partitions, expect_local_rejection=args.mode == 'local')
    assert not cleanup_errors
    if args.mode == 'local':
        assert receipt['checks']['outcome'] == 'expected-local-rejection'
        receipt['execution_class'] = 'negative gate: worker relation rejected on local-only server'
    else:
        audit(receipt, (args.output / 'server.log').read_text(), minimum_workers=2)
        assert pid not in receipt['native_execution']['native_pids'], 'driver performed native worker work'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['local', 'process-cluster', 'two-host'], default='process-cluster')
    parser.add_argument('--sail-binary', type=Path)
    parser.add_argument('--two-host-config', type=Path)
    parser.add_argument('--runtime-source-sha', required=True)
    parser.add_argument('--native-source-sha', required=True)
    parser.add_argument('--allow-working-tree', action='store_true')
    parser.add_argument('--iterations', type=int, default=2)
    parser.add_argument('--partitions', type=int, default=3)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--worker-task-slots', type=int, default=32)
    parser.add_argument('--sail-pool-bytes', type=int, default=2 << 30)
    parser.add_argument('--native-quota', type=int, default=256 << 20)
    args = parser.parse_args()
    options(iterations=args.iterations, partitions=args.partitions, reset_probability=.15, batch_rows=4096)
    validate_admission_settings(args.worker_task_slots, args.sail_pool_bytes, args.native_quota)
    if args.partitions < 2 or args.threads < 1:
        parser.error('qualification requires at least two partitions and one thread')
    for identity in (args.runtime_source_sha, args.native_source_sha):
        if not re.fullmatch('[0-9a-f]{40}', identity):
            parser.error('source identities must be full lowercase git commits')
    if args.mode == 'two-host' and args.two_host_config is None:
        parser.error('--two-host-config is required for two-host mode')
    dirty = git(REPO, 'status', '--porcelain')
    if dirty and not args.allow_working_tree:
        parser.error('freeze a clean source checkout or explicitly select a development check')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    source = git(REPO, 'rev-parse', 'HEAD')
    hashes = {path.name: sha256(path) for path in Path(__file__).parent.glob('*.py')}
    receipt = dict(started_utc=utc(), outcome='running', source_commit=source, source_dirty=dirty,
        source_files_sha256=hashes, arguments={key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        controller_host=platform.node(), packages=package_versions(),
        boundary='functional qualification only; no publishable time or memory comparison',
        execution_class=args.mode)
    try:
        two_hosts(args, receipt) if args.mode == 'two-host' else local_processes(args, receipt)
        assert git(REPO, 'rev-parse', 'HEAD') == source, 'source HEAD moved'
        assert hashes == {path.name: sha256(path) for path in Path(__file__).parent.glob('*.py')}, 'client source changed'
        receipt['outcome'] = 'passed-development' if dirty else 'passed'
    except BaseException:
        receipt.update(outcome='failed', error=traceback.format_exc())
        raise
    finally:
        receipt['finished_utc'] = utc()
        (args.output / 'receipt.json').write_text(json.dumps(receipt, sort_keys=True, indent=2, default=str) + '\n')


if __name__ == '__main__':
    main()

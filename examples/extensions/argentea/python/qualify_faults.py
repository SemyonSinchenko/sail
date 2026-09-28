#!/usr/bin/env python3
"""Qualify a first-call Argentea failure on two POSIX worker processes; no timing claims."""
import argparse
import json
import os
from pathlib import Path
import platform
import sys
import time
import traceback

EXAMPLES = Path(__file__).resolve().parents[2]
REPO = EXAMPLES.parents[1]
sys.path[:0] = [str(EXAMPLES/'graph-algorithms/src'), str(EXAMPLES/'scripts'), str(EXAMPLES/'benchmarks')]
from runtime import git, group_exists, native_package_identity, package_versions, sha256
from argentea_delta_client import ArgenteaDelta
from argentea_evidence import parse_log
from argentea_fault_control import FaultController, states, utc, workers_from_log
from argentea_fault_evidence import validate_fault
from argentea_readiness import wait_for_workers
from argentea_resource_evidence import live_pids, operation_records, read_complete_log
from argentea_runtime import local_server
from qualify_resources import inventory, validate_source_identities, wait_empty


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2, default=str)+'\n')


def qualification_source_hashes():
    paths = [*Path(__file__).parent.glob('*.py'), *(EXAMPLES/'scripts').glob('*.py')]
    return {str(path.relative_to(REPO)): sha256(path) for path in sorted(paths)}


def terminal_inventory(spark, check, log):
    deadline = time.monotonic()+30
    while True:
        check['worker_endpoints'], check['stages'] = inventory(spark)
        native = [s for s in check['stages'] if s['slot_group'].startswith('worker-extension:')]
        assert len({s['job_id'] for s in native}) == 1
        job = native[0]['job_id']
        check['stored_tasks'] = [r.asDict() for r in spark.sql(f'''
            SELECT session_id, CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
                   CAST(partition AS BIGINT) AS partition, CAST(attempt AS BIGINT) AS attempt, status
            FROM system.execution.tasks WHERE job_id = {job}
        ''').collect()]
        check['jobs'] = [r.asDict() for r in spark.sql(f'''
            SELECT session_id, CAST(job_id AS BIGINT) AS job_id, status
            FROM system.execution.jobs WHERE job_id = {job}
        ''').collect()]
        records = operation_records(read_complete_log(log), check['request'])
        initialized = {r['partition'] for r in records if r['event'] == 'init'}
        victim = check.get('injection', {}).get('killed_worker', {})
        killed = (victim.get('worker_id'), victim.get('pid'))
        expected_closed = {r['partition'] for r in records if r['event'] == 'init'
                           and (r['worker_id'], r['pid']) != killed}
        closed = {r['partition'] for r in records if r['event'] == 'close'}
        stopped = check['stored_tasks'] and all(t['status'] in ('SUCCEEDED', 'FAILED', 'CANCELED') for t in check['stored_tasks'])
        if stopped and closed == expected_closed and (initialized == {0, 1} or check['case'] == 'quota'):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError('native tasks or surviving owners did not close')
        time.sleep(.05)


def exercise(endpoint, args, driver, check, *, remote_workers=None):
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark.sql.connect.session import SparkSession
    from pyspark_pecan import CancellationToken
    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([DefaultPolicy(max_retries=0, initial_backoff=100, max_backoff=100, jitter=0)])
    token, controller = CancellationToken(), None
    log = args.output/('server-and-workers.log' if remote_workers is not None else 'server.log')
    check.update(case=args.case, native_quota=args.native_quota, query_failed=False, cleanup_errors=[],
                 connect_max_retries=0, requested_victim_owner=args.victim_owner)
    try:
        wait_for_workers(spark, evidence=check)
        workers = remote_workers(spark) if callable(remote_workers) else (remote_workers if remote_workers is not None else workers_from_log(read_complete_log(log), driver))
        if remote_workers is not None:
            check['mode'] = 'two-host'
        check['supervised_workers'] = workers
        interrupt = spark.interruptTag
        def record_interrupt(tag):
            result = interrupt(tag)
            check['interrupt_operation_ids'] = result
            return result
        spark.interruptTag = record_interrupt
        def observe(event):
            nonlocal controller
            check['request'] = dict(event['request'])
            (args.output/'client-plan.pb').write_bytes(event['plan_bytes'])
            check['views'] = [v['alias'] for v in event['view_registrations']]
            assert not operation_records(read_complete_log(log), check['request'])
            if args.case != 'quota':
                if remote_workers is None:
                    controller = FaultController(args.case, log, check['request'], workers, driver, token,
                                                 check.setdefault('injection', {}), args.victim_owner)
                else:
                    from argentea_remote_fault_control import RemoteFaultController
                    controller = RemoteFaultController(args.case, log, check['request'], workers, token,
                                                       check.setdefault('injection', {}), args.victim_owner)
                controller.thread.start()
        # All 524288 arcs cross owners. PageRank's first certificate and
        # WCC/SSSP topology must emit them in one-row batches; this fixture actually enters native work before the
        # external pause. No fixed sleep or tolerance-based nontermination claim.
        vertices = 3 if args.case == 'quota' else 4096
        copies = 1 if args.case == 'quota' else 128
        nodes = spark.range(vertices).select('id')
        edges = spark.range(vertices*copies).selectExpr(f'id % {vertices} AS src', f'(id + 1) % {vertices} AS dst')
        check['fixture'] = dict(vertices=vertices, edges=vertices*copies, copies=copies, batch_rows=1)
        try:
            if args.algorithm=='pagerank_delta':
                result = ArgenteaDelta(spark, observer=observe).pagerank(nodes, edges, max_pushes=7,
                    partitions=2, tolerance=1e-12, batch_rows=1, cancellation=token)
            elif args.algorithm.startswith('wcc_'):
                from argentea_wcc_client import ArgenteaWcc
                result = ArgenteaWcc(spark,observer=observe).wcc(nodes,edges,
                    method=args.algorithm.removeprefix('wcc_'),max_rounds=3,
                    partitions=2,batch_rows=1,cancellation=token)
            else:
                from argentea_sssp_client import ArgenteaSssp
                from pyspark.sql.connect import functions as F
                result = ArgenteaSssp(spark,observer=observe).sssp(nodes,edges.withColumn('weight',F.lit(1.0)),
                    source=0,method=args.algorithm.removeprefix('sssp_'),max_rounds=3,
                    partitions=2,batch_rows=1,cancellation=token)

        except Exception as error:
            check.update(query_failed=True, error=str(error), error_type=type(error).__name__,
                error_traceback=traceback.format_exc(), cleanup_deferred=getattr(error, 'cleanup_deferred', None),
                run_path=getattr(error, 'run_path', None))
        else:
            result.close()
            raise AssertionError('faulted operation returned a successful result')
        finally:
            if controller is not None:
                controller.finish()
        if args.case == 'worker-loss':
            # A failed task RPC can precede the existing heartbeat detector.
            # Do not dispatch diagnostic SQL onto that still-advertised worker.
            victim = check['injection']['killed_worker']['worker_id']
            deadline = time.monotonic()+30
            marker = f'stopping worker {victim}'
            while marker not in read_complete_log(log):
                if time.monotonic() >= deadline:
                    raise TimeoutError('driver did not retire killed worker')
                time.sleep(.05)
            check['driver_retired_victim_utc'] = utc()
        terminal_inventory(spark, check, log)
        check['view_cleanup'] = [dict(alias=v, exists=spark.catalog.tableExists(v)) for v in check['views']]
        assert not any(v['exists'] for v in check['view_cleanup'])
        records, _ = parse_log(read_complete_log(log))
        check['live_audit'] = validate_fault(read_complete_log(log), records, check)
        if remote_workers is None:
            check['live_workers_after_close'] = live_pids([w['pid'] for w in workers])
            killed = check.get('injection', {}).get('killed_worker', {}).get('pid')
            check['worker_process_states_after_close'] = states([w['pid'] for w in workers])
            from argentea_fault_control import validate_after_close
            validate_after_close(check['worker_process_states_after_close'], workers, killed)
        else:
            from argentea_remote_fault_control import control_worker
            victim = check.get('injection', {}).get('killed_worker')
            surviving = [w for w in workers if w != victim]
            check['remote_survivors_after_close'] = []
            for worker in surviving:
                state = control_worker(worker, {'state': True})
                assert state['alive'] and not any(c in state['status'] for c in 'ZT')
                check['remote_survivors_after_close'].append(dict(state, host=worker['host']))
        # ProcessWorkerManager retains Child handles and calls wait() at session
        # stop. A SIGKILL victim may be a zombie until then; kill(pid,0) alone
        # does not distinguish it from a running process. The final gate below
        # still requires every PID to be absent, including that zombie.
    finally:
        if controller is not None:
            controller.stop.set()
            controller.resume()
            controller.thread.join(15)
        try:
            spark.stop()
        except BaseException as error:
            check['cleanup_errors'].append(repr(error))
        save(args.output/'exercise.json', check)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--sail-binary', type=Path)
    p.add_argument('--two-host-config', type=Path, help='physical-host launch configuration; otherwise use local processes')
    p.add_argument('--runtime-source-sha', required=True)
    p.add_argument('--native-source-sha', required=True)
    p.add_argument('--algorithm',choices=('pagerank_delta','wcc_reference','wcc_star','sssp_reference','sssp_delta_star'),default='pagerank_delta')
    p.add_argument('--case', choices=('cancel', 'worker-loss', 'quota'), required=True)
    p.add_argument('--victim-owner', type=int, choices=(0, 1),
                   help='worker-loss only: select this initialized native owner; default keeps lowest worker ID')
    p.add_argument('--allow-working-tree', action='store_true')
    args = p.parse_args()
    if args.victim_owner is not None and args.case != 'worker-loss':
        p.error('--victim-owner requires --case worker-loss')
    if args.two_host_config is None and args.sail_binary is None:
        p.error('--sail-binary is required for local process qualification')
    if args.two_host_config is not None and args.case == 'quota':
        p.error('two-host quota qualification uses the resource harness, not first-call fault injection')
    args.mode = 'two-host' if args.two_host_config else 'process-cluster'
    args.partitions, args.threads, args.worker_task_slots = 2, 4, 32
    args.sail_pool_bytes = 2 << 30
    # Bound fault detection through the existing worker-pool configuration;
    # these settings do not create a native execution delay or test hook.
    args.heartbeat_interval_seconds, args.heartbeat_timeout_seconds = 1, 10
    os.environ['SAIL_CLUSTER__WORKER_HEARTBEAT_INTERVAL_SECS'] = '1'
    os.environ['SAIL_CLUSTER__WORKER_HEARTBEAT_TIMEOUT_SECS'] = '10'
    args.native_quota = 1 if args.case == 'quota' else 256 << 20
    validate_source_identities((args.runtime_source_sha, args.native_source_sha))
    source, dirty = git(REPO, 'rev-parse', 'HEAD'), git(REPO, 'status', '--porcelain')
    if dirty and not args.allow_working_tree:
        p.error('freeze clean source before qualification, or label a development check')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    hashes = qualification_source_hashes()
    receipt = dict(started_utc=utc(), outcome='running', source_commit=source, source_dirty=dirty,
        source_files_sha256=hashes, arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        controller_host=platform.node(), binary_sha256=sha256(args.sail_binary) if args.sail_binary else None, native_package=native_package_identity(),
        packages=package_versions(), checks={}, cleanup_errors=[],
        boundary=f'{args.mode}; first-call fault and owner/task/process/storage cleanup; no timings, RSS-zero or retained Arrow-buffer claim')
    try:
        if args.two_host_config:
            from argentea_remote_fault_run import run_remote_fault
            run_remote_fault(args, receipt)
        else:
            try:
                with local_server(args, receipt['cleanup_errors']) as (endpoint, driver):
                    receipt['driver_pid'] = driver
                    exercise(endpoint, args, driver, receipt['checks'])
                    receipt['post_session_storage'] = wait_empty(args.output/'staging')
            finally:
                text = read_complete_log(args.output/'server.log')
                import re
                pids = [int(v) for v in re.findall(r'extension process worker \d+: pid=Some\((\d+)\)', text)]
                if 'driver_pid' in receipt:
                    receipt['process_cleanup'] = live_pids([receipt['driver_pid'], *pids])
                    receipt['driver_process_group_exists'] = group_exists(receipt['driver_pid'])
                receipt['final_storage_files'] = [str(f.relative_to(args.output/'staging'))
                    for f in (args.output/'staging').rglob('*') if f.is_file() or f.is_symlink()]
            assert not receipt['cleanup_errors'] and not receipt['checks']['cleanup_errors']
            assert not any(r['alive'] for r in receipt['process_cleanup']) and not receipt['driver_process_group_exists']
            assert not receipt['final_storage_files']
            text = (args.output/'server.log').read_text()
            records, _ = parse_log(text)
            receipt['native_execution'] = validate_fault(text, records, receipt['checks'])
        assert git(REPO, 'rev-parse', 'HEAD') == source
        assert hashes == qualification_source_hashes()
        receipt['outcome'] = 'passed-development' if dirty else 'passed'
    except BaseException:
        receipt.update(outcome='failed', error=traceback.format_exc())
        raise
    finally:
        receipt['finished_utc'] = utc()
        save(args.output/'receipt.json', receipt)


if __name__ == '__main__':
    main()

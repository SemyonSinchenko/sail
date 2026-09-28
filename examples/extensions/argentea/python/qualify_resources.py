#!/usr/bin/env python3
"""Qualify Argentea v1 quota reuse after success/error while workers stay alive.

Functional evidence only: 48 MiB worker pool, 32 MiB worker-native quota, one
SparkSession, P=3/K=2. Retained results are Parquet files, not retained native
Arrow buffers. No RSS-zero, universal leak-freedom or performance claim.
"""
import argparse
import datetime
import json
from pathlib import Path
import platform
import re
import sys
import subprocess
import time
import traceback

EXAMPLES = Path(__file__).resolve().parents[2]
REPO = EXAMPLES.parents[1]
sys.path[:0] = [str(EXAMPLES/'graph-algorithms/src'),str(EXAMPLES/'scripts'),str(EXAMPLES/'benchmarks')]
from runtime import git, group_exists, native_package_identity, package_versions, sha256, validate_admission_settings
from argentea_client import Argentea, build_plan
from argentea_evidence import parse_log, parse_worker_tasks, validate_audit, validate_rows
from argentea_exercise import IDS, EDGES
from argentea_resource_evidence import live_pids, read_complete_log, validate_failed_operation, wait_closed
from argentea_runtime import local_server


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value,sort_keys=True,indent=2,default=str)+'\n')


def inventory(spark):
    workers = [row.asDict() for row in spark.sql('''
        SELECT CAST(worker_id AS BIGINT) AS worker_id, host, CAST(port AS INT) AS port, status
        FROM system.cluster.workers
    ''').collect()]
    stages = [row.asDict() for row in spark.sql('''
        SELECT session_id, CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
               CAST(partitions AS BIGINT) AS partitions, placement, `group` AS slot_group, mode
        FROM system.execution.stages
    ''').collect()]
    return workers,stages


def invalid_cardinality(spark, nodes, edges, observe):
    """Bypass only the declared N after normal owned input validation."""
    from pyspark.sql.connect import functions as F
    from pyspark_pecan import GraphAlgorithms

    def body(run, vertices, links, size):
        native_nodes = vertices.select('id',F.pmod(F.col('id'),F.lit(3)).cast('long').alias('owner'))
        native_edges = links.select('src','dst',F.pmod(F.col('src'),F.lit(3)).cast('long').alias('owner'))
        frame,request = build_plan(spark,native_nodes,native_edges,vertices_count=size+1,iterations=2,partitions=3)
        observe(dict(request=request,plan_bytes=frame._plan.to_proto(spark.client).SerializeToString()))
        run.materialize(frame,expected_rows=size)
        raise AssertionError('wrong declared global vertex count unexpectedly completed')
    GraphAlgorithms(spark)._run(nodes,edges,3,None,body)


def exercise(endpoint, output, evidence):
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark.sql.connect.session import SparkSession

    spark = SparkSession.builder.remote(endpoint).create()
    # Zero transport retries: this gate must preserve the original failing call.
    spark.client.set_retry_policies([DefaultPolicy(max_retries=0,initial_backoff=100,max_backoff=100,jitter=0)])
    results,worker_pids,worker_ids,session,jobs = [],None,None,None,set()
    log_path = output/'server.log'
    evidence.update(started_utc=utc(),operations=[],retained_results=[],cleanup_errors=[])
    try:
        nodes = spark.createDataFrame([(node,) for node in IDS],'id long')
        edges = spark.createDataFrame(EDGES,'src long, dst long')
        for label in ('warmup','expected-cardinality-error','reuse-1','reuse-2','reuse-3'):
            operation = dict(label=label,started_utc=utc(),outcome='running')
            evidence['operations'].append(operation)
            operation_dir = output/label
            operation_dir.mkdir()

            def observe(event):
                operation['request'] = dict(event['request'])
                (operation_dir/'client-plan.pb').write_bytes(event['plan_bytes'])

            save(output/'exercise.json',evidence)
            if label=='expected-cardinality-error':
                try:
                    invalid_cardinality(spark,nodes,edges,observe)
                except Exception as error:
                    operation.update(error=str(error),error_type=type(error).__name__,
                        cleanup_deferred=getattr(error,'cleanup_deferred',None),run_path=getattr(error,'run_path',None))
                    assert 'incomplete or inconsistent global vertex count' in str(error), 'failure was not the injected post-init cardinality error'
                    assert operation['cleanup_deferred'] is True, 'failed write did not retain session ownership'
                else:
                    raise AssertionError('injected runtime failure returned successfully')
            else:
                result = Argentea(spark,observer=observe).pagerank(nodes,edges,iterations=2,partitions=3)
                results.append(result)  # Own it even if subsequent validation fails.
                rows = [row.asDict() for row in result.native_frame.collect()]
                operation.update(rows=rows,result_path=result.path,
                    validation=validate_rows(rows,IDS,EDGES,result.request,2))
                evidence['retained_results'].append(result.path)
            records = wait_closed(log_path,operation['request'],timeout=30)
            operation['native_closed_utc'] = utc()
            workers,stages = inventory(spark)
            text = read_complete_log(log_path)
            operation.update(worker_endpoints=workers,stages=stages)
            if label=='expected-cardinality-error':
                native = validate_failed_operation(text,operation['request'],stages,
                    expected_pids=worker_pids,expected_session=session,iterations=2)
                operation['outcome'] = 'expected-runtime-failure'
            else:
                native = validate_audit(records,operation['rows'],operation['request'],2,
                    minimum_workers=2,worker_endpoints=workers,stages=stages,task_statuses=parse_worker_tasks(text))
                operation['outcome'] = 'passed'
            operation['native_execution'] = native
            if worker_pids is None:
                worker_pids,worker_ids,session = native['native_pids'],native['native_workers'],native['session_id']
                assert len(worker_pids)==2, 'gate needs exactly two native worker processes'
            assert (native['native_pids']==worker_pids and native['native_workers']==worker_ids
                    and native['session_id']==session), 'worker/session identity changed between operations'
            assert native['job_id'] not in jobs, 'operations reused a native job identity'
            jobs.add(native['job_id'])
            assert len({op['request']['operation_id'] for op in evidence['operations']})==len(evidence['operations'])
            operation['live_workers_after_native_close'] = live_pids(worker_pids)
            assert all(p['alive'] for p in operation['live_workers_after_native_close']), 'worker exited before quota reuse'
            # Older materialized rank outputs remain usable while later worker
            # jobs acquire the same-size quota. They do not retain native leases.
            operation['retained_output_counts'] = [result.frame.count() for result in results]
            assert operation['retained_output_counts']==[len(IDS)]*len(results)
            operation['finished_utc'] = utc()
            save(operation_dir/'receipt.json',operation)
            save(output/'exercise.json',evidence)
        # Recheck the complete operation set while the session/workers remain
        # live, so a late retry or duplicate receipt cannot hide after an earlier
        # operation's close poll. Each saved inventory pins its actual job.
        final_text = read_complete_log(log_path)
        final_records,_ = parse_log(final_text)
        for operation in evidence['operations']:
            if operation['label']=='expected-cardinality-error':
                validate_failed_operation(final_text,operation['request'],operation['stages'],
                    expected_pids=worker_pids,expected_session=session,iterations=2)
            else:
                validate_audit(final_records,operation['rows'],operation['request'],2,
                    minimum_workers=2,worker_endpoints=operation['worker_endpoints'],
                    stages=operation['stages'],task_statuses=parse_worker_tasks(final_text))
        evidence['final_live_log_audit_utc'] = utc()
        evidence.update(session_id=session,native_pids=worker_pids,native_workers=worker_ids,native_jobs=sorted(jobs),
                        live_workers_before_session_stop=live_pids(worker_pids),
                        retained_result_count_before_close=len(results),outcome='passed-before-teardown')
        assert all(p['alive'] for p in evidence['live_workers_before_session_stop'])
    finally:
        for result in results:
            try:
                result.close()
            except BaseException as error:
                evidence['cleanup_errors'].append(dict(operation='close_result',path=result.path,error=repr(error)))
        evidence['results_closed_before_session_stop_utc'] = utc()
        try:
            spark.stop()
            evidence['session_stopped_utc'] = utc()
        except BaseException as error:
            evidence['cleanup_errors'].append(dict(operation='stop_session',error=repr(error)))
        save(output/'exercise.json',evidence)
    assert not evidence['cleanup_errors'], 'result/session cleanup failed'


def wait_empty(staging, *, timeout=30):
    deadline = time.monotonic()+timeout
    while True:
        files = [str(p.relative_to(staging)) for p in staging.rglob('*') if p.is_file() or p.is_symlink()]
        if not files:
            return dict(recorded_utc=utc(),remaining_files=[])
        if time.monotonic()>=deadline:
            raise AssertionError(f'staging remained nonempty after session stop: {files}')
        time.sleep(.1)


def validate_source_identities(identities):
    for identity in identities:
        if not re.fullmatch('[0-9a-f]{40}',identity):
            raise ValueError('source identities must be full lowercase git commits')
        try:
            resolved=git(REPO,'rev-parse','--verify',identity+'^{commit}')
        except subprocess.CalledProcessError as error:
            raise ValueError(f'declared source commit is unavailable: {identity}') from error
        if resolved!=identity:
            raise ValueError(f'declared source identity does not resolve exactly: {identity}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sail-binary',type=Path,required=True)
    parser.add_argument('--runtime-source-sha',required=True)
    parser.add_argument('--native-source-sha',required=True)
    parser.add_argument('--allow-working-tree',action='store_true')
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--worker-task-slots',type=int,default=32)
    parser.add_argument('--sail-pool-bytes',type=int,default=48<<20)
    parser.add_argument('--native-quota',type=int,default=32<<20)
    args = parser.parse_args()
    args.mode,args.partitions,args.iterations = 'process-cluster',3,2
    validate_admission_settings(args.worker_task_slots,args.sail_pool_bytes,args.native_quota)
    if args.threads<1 or not args.native_quota<args.sail_pool_bytes<2*args.native_quota:
        parser.error('require threads>=1 and quota < Sail pool < 2*quota')
    try:
        validate_source_identities((args.runtime_source_sha,args.native_source_sha))
    except ValueError as error:
        parser.error(str(error))
    dirty = git(REPO,'status','--porcelain')
    if dirty and not args.allow_working_tree:
        parser.error('freeze a clean source checkout or explicitly mark a development check')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    source = git(REPO,'rev-parse','HEAD')
    hashes = {p.name:sha256(p) for p in Path(__file__).parent.glob('*.py')}
    receipt = dict(started_utc=utc(),outcome='running',source_commit=source,source_dirty=dirty,
        source_files_sha256=hashes,arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        binary_sha256=sha256(args.sail_binary),native_package=native_package_identity(),packages=package_versions(),
        controller_host=platform.node(),checks={},cleanup_errors=[],
        boundary='same-session live quota reuse after completed and failed jobs; retained Parquet results are not retained native Arrow buffers; no RSS-zero or general leak-freedom claim')
    try:
        try:
            with local_server(args,receipt['cleanup_errors']) as (endpoint,pid):
                receipt['driver_pid'] = pid
                try:
                    exercise(endpoint,args.output,receipt['checks'])
                    receipt['post_session_storage'] = wait_empty(args.output/'staging')
                finally:
                    records,_ = parse_log(read_complete_log(args.output/'server.log'))
                    receipt['observed_native_pids'] = sorted({r['pid'] for r in records})
        finally:
            # Preserve cleanup observations on an unexpected runtime/audit error
            # too, after the context manager has attempted group shutdown.
            if 'driver_pid' in receipt:
                receipt['process_cleanup'] = live_pids([receipt['driver_pid'],*receipt.get('observed_native_pids',[])])
                receipt['driver_process_group_exists'] = group_exists(receipt['driver_pid'])
            receipt['final_storage_files'] = [str(p.relative_to(args.output/'staging'))
                for p in (args.output/'staging').rglob('*') if p.is_file() or p.is_symlink()]
        assert not any(p['alive'] for p in receipt['process_cleanup']) and not receipt['driver_process_group_exists']
        final_records,_ = parse_log((args.output/'server.log').read_text())
        receipt['final_native_receipt_count'] = len(final_records)
        assert not receipt['cleanup_errors'] and not receipt['final_storage_files']
        assert git(REPO,'rev-parse','HEAD')==source and hashes=={p.name:sha256(p) for p in Path(__file__).parent.glob('*.py')}
        receipt['outcome'] = 'passed-development' if dirty else 'passed'
    except BaseException:
        receipt.update(outcome='failed',error=traceback.format_exc())
        raise
    finally:
        receipt['finished_utc'] = utc()
        save(args.output/'receipt.json',receipt)


if __name__=='__main__':
    main()

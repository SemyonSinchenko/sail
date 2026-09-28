#!/usr/bin/env python3
"""Same-worker WCC/SSSP quota reuse after a genuine post-init native refusal.

One live session; quota < Sail pool < twice quota. Retained results are Parquet,
not native buffers. This is functional accounting evidence, not an RSS benchmark.
"""
import argparse
from pathlib import Path
import platform
import sys
import traceback
import time

EXAMPLES = Path(__file__).resolve().parents[2]
REPO = EXAMPLES.parents[1]
sys.path[:0] = [str(EXAMPLES/'graph-algorithms/src'),str(EXAMPLES/'scripts'),str(EXAMPLES/'benchmarks')]
from runtime import git, group_exists, native_package_identity, package_versions, sha256, validate_admission_settings
from qualify_resources import utc, save, inventory, validate_source_identities, wait_empty
from argentea_resource_evidence import live_pids, read_complete_log, wait_closed
from argentea_graph_resource_evidence import validate_memory_refusal
from argentea_evidence import parse_log
from argentea_runtime import local_server
from argentea_readiness import wait_for_workers


def qualification_source_hashes():
    paths = [*Path(__file__).parent.glob('*.py'), *(EXAMPLES/'scripts').glob('*.py')]
    return {str(path.relative_to(REPO)):sha256(path) for path in sorted(paths)}


def collect_terminal(spark, operation, records):
    selected = [r for r in records if r.get('operation_id') == operation['request']['operation_id']]
    identities = {(r['session_id'],r['job_id']) for r in selected}
    assert len(identities) == 1
    session, job = next(iter(identities))
    deadline = time.monotonic()+30
    while True:
        operation['worker_endpoints'],operation['stages'] = inventory(spark)
        operation['stored_tasks'] = [r.asDict() for r in spark.sql(f"SELECT session_id, CAST(job_id AS BIGINT) job_id, CAST(stage AS BIGINT) stage, CAST(partition AS BIGINT) partition, CAST(attempt AS BIGINT) attempt, status FROM system.execution.tasks WHERE job_id={job}").collect()]
        operation['jobs'] = [r.asDict() for r in spark.sql(f"SELECT session_id, CAST(job_id AS BIGINT) job_id, status FROM system.execution.jobs WHERE job_id={job}").collect()]
        if operation['stored_tasks'] and all(t['status'] in ('SUCCEEDED','FAILED','CANCELED') for t in operation['stored_tasks']):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError('native job tasks did not terminate')
        time.sleep(.05)


def audit_operation(log, operation, qualifier):
    if operation['label'] == 'memory-refusal':
        records,_ = parse_log(log)
        return validate_memory_refusal(log,records,operation)
    receipt = dict(checks=operation)
    qualifier.audit(receipt,log,minimum_workers=2)
    return receipt['native_execution']


def exercise(endpoint,args,evidence,*,remote_workers=None):
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from argentea_sssp_client import ArgenteaSssp
    from argentea_wcc_client import ArgenteaWcc
    import qualify_sssp
    import qualify_wcc

    sssp = args.algorithm.startswith('sssp_')
    qualifier = qualify_sssp if sssp else qualify_wcc
    method = args.algorithm.split('_',1)[1]
    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([DefaultPolicy(max_retries=0)])
    results, identities, jobs = [], None, set()
    evidence.update(operations=[],cleanup_errors=[])
    log_path = args.output/('server-and-workers.log' if remote_workers is not None else 'server.log')
    try:
        wait_for_workers(spark,evidence=evidence)
        workers = remote_workers(spark) if callable(remote_workers) else remote_workers
        if workers is not None:
            evidence['supervised_workers'] = workers
        for label in ('warmup','memory-refusal','reuse-1','reuse-2','reuse-3'):
            operation = dict(label=label,started_utc=utc(),native_quota=args.native_quota,outcome='running')
            evidence['operations'].append(operation)
            directory = args.output/label
            directory.mkdir()
            def observe(event):
                operation['request'] = dict(event['request'])
                operation['planned_native_phases'] = event['native_phase_count']
                (directory/'client-plan.pb').write_bytes(event['plan_bytes'])
                operation['views'] = [v['alias'] for v in event['view_registrations']]
                for i,v in enumerate(event['view_registrations']):
                    (directory/f'view-{i:03d}.pb').write_bytes(v['plan_bytes'])
            count = args.failure_vertices if label == 'memory-refusal' else 16
            nodes = spark.range(count).select('id')
            edges = spark.createDataFrame([], 'src long,dst long,weight double' if sssp else 'src long,dst long')
            try:
                common = dict(method=method,max_rounds=3,partitions=2,batch_rows=32)
                if sssp:
                    result = ArgenteaSssp(spark,observer=observe).sssp(nodes,edges,source=0,**common)
                else:
                    result = ArgenteaWcc(spark,observer=observe).wcc(nodes,edges,**common)
            except Exception as error:
                operation.update(query_failed=True,error=str(error),error_type=type(error).__name__,
                    cleanup_deferred=getattr(error,'cleanup_deferred',None),run_path=getattr(error,'run_path',None))
                if label != 'memory-refusal':
                    raise
            else:
                results.append(result)
                assert label != 'memory-refusal', 'intended memory refusal returned a result'
                rows = [r.asDict() for r in result.native_frame.collect()]
                operation.update(rows=rows,ids=list(range(count)),edges=[],result_path=result.path,
                    run_path=result._retained._run.path,
                    result_validation=qualifier.validate_rows(rows,list(range(count)),[],result.request),
                    outcome='answers-passed-awaiting-native-audit')
            records = wait_closed(log_path,operation['request'])
            collect_terminal(spark,operation,records)
            operation['view_cleanup'] = [dict(alias=v,exists=spark.catalog.tableExists(v)) for v in operation['views']]
            assert not any(v['exists'] for v in operation['view_cleanup'])
            native = audit_operation(read_complete_log(log_path),operation,qualifier)
            operation['native_execution'] = native
            identity = (native['session_id'],native['native_pids'],native['native_workers'])
            if identities is None:
                identities = identity
            assert identity == identities, 'session or worker processes changed during reuse'
            assert native['job_id'] not in jobs
            jobs.add(native['job_id'])
            if workers is None:
                operation['live_workers'] = live_pids(native['native_pids'])
                assert all(p['alive'] for p in operation['live_workers'])
            else:
                from argentea_remote_resource_control import validate_remote_owners, live_remote_workers
                operation['host_owners'] = validate_remote_owners(records,operation['request'],workers)
                operation['live_workers'] = live_remote_workers(workers)
            operation['retained_output_counts'] = [r.frame.count() for r in results]
            assert operation['retained_output_counts'] == [16]*len(results)
            operation.update(outcome='expected-memory-refusal' if label=='memory-refusal' else 'passed',finished_utc=utc())
            save(directory/'receipt.json',operation)
            save(args.output/'exercise.json',evidence)
        log = read_complete_log(log_path)
        for operation in evidence['operations']:
            audit_operation(log,operation,qualifier)
            if workers is not None:
                records,_ = parse_log(log)
                validate_remote_owners(records,operation['request'],workers)
        evidence.update(outcome='passed-before-teardown',native_jobs=sorted(jobs),
                        session_id=identities[0],native_pids=identities[1],native_workers=identities[2])
    finally:
        for result in results:
            try:
                result.close()
            except BaseException as error:
                evidence['cleanup_errors'].append(repr(error))
        try:
            spark.stop()
        finally:
            save(args.output/'exercise.json',evidence)
    assert not evidence['cleanup_errors']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sail-binary',type=Path)
    parser.add_argument('--two-host-config',type=Path)
    parser.add_argument('--runtime-source-sha',required=True)
    parser.add_argument('--native-source-sha',required=True)
    parser.add_argument('--allow-working-tree',action='store_true')
    parser.add_argument('--algorithm',choices=['wcc_reference','wcc_star','sssp_reference','sssp_delta_star'],required=True)
    parser.add_argument('--failure-vertices',type=int)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--worker-task-slots',type=int,default=32)
    parser.add_argument('--sail-pool-bytes',type=int,default=48<<20)
    parser.add_argument('--native-quota',type=int,default=32<<20)
    args = parser.parse_args()
    if args.two_host_config is None and args.sail_binary is None:
        parser.error('--sail-binary is required without --two-host-config')
    args.mode = 'two-host' if args.two_host_config else 'process-cluster'
    args.partitions,args.iterations = 2,2
    if args.failure_vertices is None:
        args.failure_vertices = 786432 if args.algorithm.startswith('wcc_') else 524288
    if args.failure_vertices < 2:
        parser.error('failure fixture requires at least two vertices')
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
    hashes = qualification_source_hashes()
    receipt = dict(started_utc=utc(),outcome='running',source_commit=source,source_dirty=dirty,
        source_files_sha256=hashes,arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        binary_sha256=sha256(args.sail_binary) if args.sail_binary else None,native_package=native_package_identity(),packages=package_versions(),
        controller_host=platform.node(),checks={},cleanup_errors=[],
        boundary='same-session live quota reuse after post-init native memory refusal; retained Parquet results are not retained native Arrow buffers; no RSS-zero or general leak-freedom claim')
    try:
        if args.two_host_config:
            from argentea_remote_resource_run import run_remote_resources
            run_remote_resources(args,receipt)
        else:
            try:
                with local_server(args,receipt['cleanup_errors']) as (endpoint,pid):
                    receipt['driver_pid'] = pid
                    try:
                        exercise(endpoint,args,receipt['checks'])
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
        assert git(REPO,'rev-parse','HEAD')==source and hashes==qualification_source_hashes()
        receipt['outcome'] = 'passed-development' if dirty else 'passed'
    except BaseException:
        receipt.update(outcome='failed',error=traceback.format_exc())
        raise
    finally:
        receipt['finished_utc'] = utc()
        save(args.output/'receipt.json',receipt)


if __name__=='__main__':
    main()

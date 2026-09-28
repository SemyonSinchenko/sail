#!/usr/bin/env python3
"""Functional Argentea v2 certificates and worker-phase qualification; no timings."""
import argparse
import datetime
import json
from pathlib import Path
import platform
import sys
import traceback

EXAMPLES = Path(__file__).resolve().parents[2]
REPO = EXAMPLES.parents[1]
sys.path[:0] = [str(EXAMPLES/'graph-algorithms/src'),str(EXAMPLES/'scripts'),str(EXAMPLES/'benchmarks')]
from runtime import git, group_exists, native_package_identity, package_versions, sha256, validate_admission_settings
from argentea_delta_client import ArgenteaDelta, options
from argentea_delta_evidence import validate_audit, validate_cap, validate_rows
from argentea_evidence import parse_log, parse_worker_tasks
from argentea_resource_evidence import live_pids
from argentea_runtime import local_server
from qualify import two_hosts
from qualify_resources import inventory, validate_source_identities, wait_empty

IDS = [0,1,2]
CASES = {
    'residual': dict(edges=[(0,1),(1,1)],max_pushes=7,tolerance=1e-3),
    'stationary': dict(edges=[(0,1),(1,2),(2,0)],max_pushes=7,tolerance=1e-12),
    'cap': dict(edges=[(0,1),(1,1)],max_pushes=0,tolerance=1e-12),
}


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(path,value):
    path.write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False,default=str)+'\n')


def exercise(endpoint,args):
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark.sql.connect.session import SparkSession

    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([DefaultPolicy(max_retries=0,initial_backoff=100,max_backoff=100,jitter=0)])
    case = CASES[args.case]
    evidence = dict(case=args.case,ids=IDS,edges=case['edges'],partitions=args.partitions,
                    max_pushes=case['max_pushes'],tolerance=case['tolerance'],outcome='running')
    try:
        def observe(event):
            evidence['request'] = dict(event['request'])
            evidence['planned_native_phases'] = event['native_phase_count']
            (args.output/'client-plan.pb').write_bytes(event['plan_bytes'])
        nodes = spark.createDataFrame([(node,) for node in IDS],'id long')
        edges = spark.createDataFrame(case['edges'],'src long, dst long')
        try:
            result = ArgenteaDelta(spark,observer=observe).pagerank(nodes,edges,
                max_pushes=case['max_pushes'],tolerance=case['tolerance'],partitions=args.partitions,
                max_phase_budget=32,batch_rows=args.batch_rows)
        except Exception as error:
            evidence.update(error=str(error),error_type=type(error).__name__,
                run_path=getattr(error,'run_path',None),cleanup_deferred=getattr(error,'cleanup_deferred',None))
            if args.mode=='local' and 'requires distributed Sail execution' in str(error):
                evidence['outcome'] = 'expected-local-rejection'
                return evidence
            if args.case=='cap' and args.mode!='local' and 'push cap' in str(error):
                evidence['outcome'] = 'expected-cap'
                evidence['worker_endpoints'],evidence['stages'] = inventory(spark)
                return evidence
            raise
        with result:
            assert args.mode!='local' and args.case!='cap', 'expected refusal unexpectedly returned ranks'
            rows = [row.asDict() for row in result.native_frame.collect()]
            evidence.update(rows=rows,result_validation=validate_rows(rows,IDS,case['edges'],result.request),
                result_path=result.path,actual_pushes=result.pushes,certificate_passes=result.certificate_passes,
                native_phase_count=result.native_phase_count,residual=result.residual,error_bound=result.error_bound)
            assert result.converged is True
            if args.case=='stationary':
                assert result.pushes==0 and result.certificate_passes==1, 'stationary fixture performed frontier pushes'
            else:
                assert result.pushes>0, 'nonstationary signed-residual fixture skipped pushes'
        try:
            _ = result.frame
        except RuntimeError:
            evidence['result_closed'] = True
        else:
            raise AssertionError('closed result still exposes ranks')
        evidence['worker_endpoints'],evidence['stages'] = inventory(spark)
        evidence['outcome'] = 'answers-passed-awaiting-native-audit'
        return evidence
    finally:
        try:
            spark.stop()
        finally:
            save(args.output/'exercise.json',evidence)


def audit(receipt,log,*,minimum_workers,required_hosts=()):
    records,supervisors = parse_log(log)
    tasks = parse_worker_tasks(log)
    check = receipt['checks']
    receipt.update(native_receipts=records,worker_task_statuses=tasks)
    if check['outcome']=='expected-cap':
        receipt['native_execution'] = validate_cap(records,check['request'],stages=check['stages'],
            task_statuses=tasks,minimum_workers=minimum_workers)
        # A failed operation has no rank rows with which to prove nonempty host
        # placement. Keep this separate from the positive physical-host proof.
        receipt['execution_scope'] = 'post-init cap refusal and owner cleanup; not a nonempty two-host graph proof'
    else:
        receipt['native_execution'] = validate_audit(records,check['rows'],check['request'],
            stages=check['stages'],task_statuses=tasks,worker_endpoints=check['worker_endpoints'],
            minimum_workers=minimum_workers,supervisors=supervisors,required_hosts=required_hosts,edges=check['edges'])


def local_processes(args,receipt):
    receipt.update(binary_sha256=sha256(args.sail_binary),native_package=native_package_identity(),cleanup_errors=[])
    try:
        with local_server(args,receipt['cleanup_errors']) as (endpoint,pid):
            receipt['driver_pid'] = pid
            receipt['checks'] = exercise(endpoint,args)
            receipt['post_session_storage'] = wait_empty(args.output/'staging')
    finally:
        if 'driver_pid' in receipt:
            log = (args.output/'server.log').read_text()
            records,_ = parse_log(log)
            receipt['process_cleanup'] = live_pids([receipt['driver_pid'],*[r['pid'] for r in records]])
            receipt['driver_process_group_exists'] = group_exists(receipt['driver_pid'])
    assert not receipt['cleanup_errors']
    assert not any(p['alive'] for p in receipt['process_cleanup']) and not receipt['driver_process_group_exists']
    if args.mode=='local':
        assert receipt['checks']['outcome']=='expected-local-rejection'
    else:
        audit(receipt,(args.output/'server.log').read_text(),minimum_workers=2)
        assert receipt['driver_pid'] not in receipt['native_execution']['native_pids'], 'driver performed native work'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['local','process-cluster','two-host'],default='process-cluster')
    parser.add_argument('--case',choices=CASES,default='residual')
    parser.add_argument('--sail-binary',type=Path)
    parser.add_argument('--two-host-config',type=Path)
    parser.add_argument('--runtime-source-sha',required=True)
    parser.add_argument('--native-source-sha',required=True)
    parser.add_argument('--allow-working-tree',action='store_true')
    parser.add_argument('--partitions',type=int,default=5)
    parser.add_argument('--batch-rows',type=int,default=2)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--worker-task-slots',type=int,default=32)
    parser.add_argument('--sail-pool-bytes',type=int,default=2<<30)
    parser.add_argument('--native-quota',type=int,default=256<<20)
    args = parser.parse_args()
    case = CASES[args.case]
    options(max_pushes=case['max_pushes'],partitions=args.partitions,reset_probability=.15,
            tolerance=case['tolerance'],max_phase_budget=32,batch_rows=args.batch_rows)
    validate_admission_settings(args.worker_task_slots,args.sail_pool_bytes,args.native_quota)
    try:
        validate_source_identities((args.runtime_source_sha,args.native_source_sha))
    except ValueError as error:
        parser.error(str(error))
    if args.partitions<2 or args.threads<1:
        parser.error('require at least two owners and one thread')
    if args.mode=='two-host' and args.two_host_config is None:
        parser.error('--two-host-config required')
    if args.mode!='two-host' and args.sail_binary is None:
        parser.error('--sail-binary required')
    dirty = git(REPO,'status','--porcelain')
    if dirty and not args.allow_working_tree:
        parser.error('freeze a clean checkout or explicitly label a development run')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    source = git(REPO,'rev-parse','HEAD')
    hashes = {p.name:sha256(p) for p in Path(__file__).parent.glob('*.py')}
    receipt = dict(started_utc=utc(),outcome='running',source_commit=source,source_dirty=dirty,
        source_files_sha256=hashes,arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        controller_host=platform.node(),packages=package_versions(),
        boundary='bounded v2 functional certificate/placement proof; no performance or arbitrary-graph convergence claim')
    try:
        if args.mode=='two-host':
            two_hosts(args,receipt,exercise_fn=exercise,audit_fn=audit)
        else:
            local_processes(args,receipt)
        assert git(REPO,'rev-parse','HEAD')==source
        assert hashes=={p.name:sha256(p) for p in Path(__file__).parent.glob('*.py')}, 'qualifier source changed'
        receipt['outcome'] = 'passed-development' if dirty else 'passed'
    except BaseException:
        receipt.update(outcome='failed',error=traceback.format_exc())
        raise
    finally:
        receipt['finished_utc'] = utc()
        save(args.output/'receipt.json',receipt)


if __name__=='__main__':
    main()

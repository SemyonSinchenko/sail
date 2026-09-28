#!/usr/bin/env python3
"""Functional Argentea v3 BFS answers, topology, and worker phases; no timings."""
import argparse
import contextlib
import io
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
from argentea_bfs_client import ArgenteaBfs, METHODS, options
from argentea_bfs_evidence import normalize, validate_audit, validate_cap, validate_rows
from argentea_evidence import parse_log, parse_worker_tasks
from argentea_resource_evidence import live_pids, read_complete_log
from argentea_runtime import local_server
from qualify import two_hosts
from qualify_resources import inventory, validate_source_identities, wait_empty

IDS = [-5,0,1,6,10,11,20,21]
EDGES = [(-5,0),(-5,1),(-5,1),(0,6),(1,6),(6,10),(10,10),(20,21)]
CASES = {
    'chain-62': dict(ids=list(range(62)),edges=[(i,i+1) for i in range(61)],source=0,directed=True,max_levels=62),
    'graph-62': dict(ids=IDS,edges=EDGES,source=-5,directed=True,max_levels=62),
    'graph': dict(ids=IDS,edges=EDGES,source=-5,directed=True,max_levels=14),
    'undirected': dict(ids=IDS,edges=EDGES,source=10,directed=False,max_levels=14),
    'source-only': dict(ids=[0],edges=[],source=0,directed=True,max_levels=14),
    'cap': dict(ids=IDS,edges=EDGES,source=-5,directed=True,max_levels=0),
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
    evidence = dict(case=args.case,method=args.method,ids=case['ids'],edges=case['edges'],
                    normalized_edges=normalize(case['edges'],directed=case['directed']),
                    directed=case['directed'],source=case['source'],partitions=args.partitions,
                    max_levels=case['max_levels'],outcome='running')
    try:
        if args.mode != 'local':
            from argentea_readiness import wait_for_workers
            wait_for_workers(spark, evidence=evidence)
        def observe(event):
            evidence['request'] = dict(event['request'])
            evidence['planned_native_phases'] = event['native_phase_count']
            (args.output/'client-plan.pb').write_bytes(event['plan_bytes'])
            registrations = []
            for index, registration in enumerate(event.get('view_registrations', ())):
                item = {k:v for k,v in registration.items() if k!='plan_bytes'}
                path = args.output/f'view-plan-{index:02d}.pb'
                path.write_bytes(registration['plan_bytes'])
                item['plan_file'] = path.name
                registrations.append(item)
            evidence['view_registrations'] = registrations
            capture = io.StringIO()
            with contextlib.redirect_stdout(capture):
                event['frame'].explain(mode='extended')
            (args.output/'terminal-explain.txt').write_text(capture.getvalue())
            log_path = args.output/('server-and-workers.log' if args.mode=='two-host' else 'server.log')
            before, _ = parse_log(read_complete_log(log_path))
            evidence['native_receipts_before_terminal'] = len(before)
            assert not before, 'native work executed during view registration or explain'
        def check_view_cleanup():
            observed = [dict(alias=item['alias'],exists=spark.catalog.tableExists(item['alias']))
                        for item in evidence.get('view_registrations', ())]
            evidence['view_cleanup'] = observed
            assert not any(item['exists'] for item in observed), 'owned phase view remains after operation'
        nodes = spark.createDataFrame([(node,) for node in case['ids']],'id long')
        edges = spark.createDataFrame(case['edges'],'src long, dst long')
        try:
            result = ArgenteaBfs(spark,observer=observe).bfs(nodes,edges,source=case['source'],
                method=args.method,directed=case['directed'],max_levels=case['max_levels'],
                partitions=args.partitions,alpha=args.alpha,beta=args.beta,
                max_phase_budget=max(32,2*case['max_levels']+4),batch_rows=args.batch_rows)
        except Exception as error:
            evidence.update(error=str(error),error_type=type(error).__name__,
                run_path=getattr(error,'run_path',None),cleanup_deferred=getattr(error,'cleanup_deferred',None),
                view_cleanup_deferred=getattr(error,'view_cleanup_deferred',None),
                uncertain_view_names=getattr(error,'uncertain_view_names',()))
            if args.mode=='local' and 'requires distributed Sail execution' in str(error):
                evidence['outcome'] = 'expected-local-rejection'
                return evidence
            if args.case=='cap' and args.mode!='local':
                # Peer cancellation can reach the RPC before the causal error.
                # Only the subsequent typed native-cause audit may accept it.
                evidence['outcome'] = 'failed-query-awaiting-cap-audit'
                evidence['query_failed'] = True
                check_view_cleanup()
                evidence['worker_endpoints'],evidence['stages'] = inventory(spark)
                return evidence
            raise
        with result:
            check_view_cleanup()
            assert args.mode!='local' and args.case!='cap', 'expected refusal unexpectedly returned ranks'
            rows = [row.asDict() for row in result.native_frame.collect()]
            evidence.update(rows=rows,result_validation=validate_rows(rows,case['ids'],evidence['normalized_edges'],result.request),
                result_path=result.path,levels=result.levels,reached=result.reached,
                native_phase_count=result.native_phase_count)
            assert result.converged is True
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
    if check['outcome']=='failed-query-awaiting-cap-audit':
        receipt['native_execution'] = validate_cap(records,check['request'],stages=check['stages'],
            task_statuses=tasks,query_failed=check['query_failed'],minimum_workers=minimum_workers)
        check['outcome'] = 'expected-cap'
        # A failed operation has no rank rows with which to prove nonempty host
        # placement. Keep this separate from the positive physical-host proof.
        receipt['execution_scope'] = 'post-init cap refusal and owner cleanup; not a nonempty two-host graph proof'
    else:
        receipt['native_execution'] = validate_audit(records,check['rows'],check['request'],
            stages=check['stages'],task_statuses=tasks,worker_endpoints=check['worker_endpoints'],
            minimum_workers=minimum_workers,supervisors=supervisors,required_hosts=required_hosts,edges=check['normalized_edges'])
        if check['method']=='direction' and check['case'] in ('graph','undirected'):
            assert any(step['mode']=='pull' for step in receipt['native_execution']['trace']), 'direction fixture did not execute pull'


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
    parser.add_argument('--case',choices=CASES,default='graph')
    parser.add_argument('--method',choices=METHODS,default='reference')
    parser.add_argument('--alpha',type=int,default=14)
    parser.add_argument('--beta',type=int,default=24)
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
    options(source=case['source'],method=args.method,directed=case['directed'],max_levels=case['max_levels'],
            partitions=args.partitions,alpha=args.alpha,beta=args.beta,max_phase_budget=max(32,2*case['max_levels']+4),batch_rows=args.batch_rows)
    validate_admission_settings(args.worker_task_slots,args.sail_pool_bytes,args.native_quota)
    try:
        validate_source_identities((args.runtime_source_sha,args.native_source_sha))
    except ValueError as error:
        parser.error(str(error))
    if args.partitions<2 or args.threads<1:
        parser.error('require at least two owners and one thread')
    if args.mode=='two-host' and args.case=='source-only':
        parser.error('source-only cannot prove nonempty vertices on two hosts')
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
        boundary='bounded v3 BFS functional answer/topology/placement proof; no performance or arbitrary-depth completion claim')
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

#!/usr/bin/env python3
"""Public missing-source and schema-time malformed-request controls, not post-init corruption."""
import argparse
from pathlib import Path
import re
import traceback

from qualify_bfs import IDS,EDGES,REPO,git,group_exists,native_package_identity,package_versions,sha256,utc,save
from argentea_bfs_client import ArgenteaBfs,ArgenteaBfsRelation,request,phases
from argentea_views import compose_views
from argentea_evidence import parse_log
from argentea_readiness import wait_for_workers
from argentea_resource_evidence import live_pids,read_complete_log
from argentea_runtime import local_server
from qualify_resources import inventory,wait_empty,validate_source_identities

INVALID_FIELD = 'unexpected_bfs_qualification_option'


def malformed_registration(spark,nodes,edges,evidence):
    from pyspark.sql.connect import functions as F
    from pyspark_pecan import GraphAlgorithms
    def body(run,vertices,links,count):
        base=request(vertices_count=count,source=-5,partitions=5)
        base[INVALID_FIELD]=True
        evidence['request']=base
        owned_nodes=vertices.select('id',F.pmod(F.col('id'),F.lit(5)).cast('long').alias('owner'))
        owned_edges=links.select('src','dst',F.pmod(F.col('src'),F.lit(5)).cast('long').alias('owner'))
        first=ArgenteaBfsRelation(dict(base,verb='init',phase=0),(owned_nodes,owned_edges))
        evidence['malformed_plan_bytes']=first.to_proto(spark.client).SerializeToString()
        with compose_views(spark,owned_nodes,owned_edges,request=base,phases=phases(base['max_levels']),
                           relation_type=ArgenteaBfsRelation,cancellation=run.cancellation):
            raise AssertionError('malformed request unexpectedly registered')
    GraphAlgorithms(spark)._run(nodes,edges,5,None,body)


def accept_error(case,error):
    if case=='missing-source':
        assert isinstance(error,ValueError) and str(error)=='BFS source must occur exactly once in the vertex snapshot'
    else:
        assert 'unknown field' in str(error) and INVALID_FIELD in str(error), 'unrelated error is not malformed-schema evidence'
    assert getattr(error,'cleanup_deferred',None) is False, 'pre-execution error left uncertain owned files'


def exercise(endpoint,args):
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.client.retries import DefaultPolicy
    spark=SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([DefaultPolicy(max_retries=0,initial_backoff=100,max_backoff=100,jitter=0)])
    evidence=dict(case=args.case,outcome='running',boundary='public pre-execution source validation' if args.case=='missing-source' else 'schema-time malformed request registration')
    try:
        wait_for_workers(spark,evidence=evidence)
        nodes=spark.createDataFrame([(node,) for node in IDS],'id long')
        edges=spark.createDataFrame(EDGES,'src long,dst long')
        try:
            if args.case=='missing-source':
                ArgenteaBfs(spark).bfs(nodes,edges,source=999,partitions=5)
            else:malformed_registration(spark,nodes,edges,evidence)
        except Exception as error:
            evidence.update(error=str(error),error_type=type(error).__name__,run_path=getattr(error,'run_path',None),
                cleanup_deferred=getattr(error,'cleanup_deferred',None),view_cleanup_deferred=getattr(error,'view_cleanup_deferred',None),
                uncertain_view_names=getattr(error,'uncertain_view_names',()))
            accept_error(args.case,error)
            observed=[dict(alias=name,exists=spark.catalog.tableExists(name)) for name in evidence['uncertain_view_names']]
            evidence['view_absence']=observed
            assert not any(v['exists'] for v in observed)
        else:raise AssertionError('invalid input unexpectedly completed')
        records,_=parse_log(read_complete_log(args.output/'server.log'))
        assert not records, 'input control unexpectedly performed Argentea worker-native execution'
        evidence['argentea_native_receipts']=0
        assert spark.range(0,3,numPartitions=1).count()==3
        evidence['session_usable_after_error']=True
        evidence['worker_endpoints'],evidence['stages']=inventory(spark)
        assert not any(stage['slot_group'].startswith('worker-extension:') for stage in evidence['stages'])
        evidence['outcome']='expected-pre-execution-rejection'
        return evidence
    finally:
        try:spark.stop()
        finally:
            data=evidence.pop('malformed_plan_bytes',None)
            if data is not None:(args.output/'malformed-plan.pb').write_bytes(data)
            save(args.output/'exercise.json',evidence)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',choices=['missing-source','unknown-field'],required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sail-binary',type=Path,required=True)
    parser.add_argument('--runtime-source-sha',required=True)
    parser.add_argument('--native-source-sha',required=True)
    args=parser.parse_args()
    validate_source_identities((args.runtime_source_sha,args.native_source_sha))
    source=git(REPO,'rev-parse','HEAD');assert not git(REPO,'status','--porcelain')
    args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    args.mode='process-cluster';args.partitions=5;args.threads=4;args.worker_task_slots=32
    args.sail_pool_bytes=2<<30;args.native_quota=256<<20
    receipt=dict(started_utc=utc(),source_commit=source,source_dirty='',outcome='running',
        arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        binary_sha256=sha256(args.sail_binary),native_package=native_package_identity(),packages=package_versions(),cleanup_errors=[])
    try:
        with local_server(args,receipt['cleanup_errors']) as(endpoint,pid):
            receipt['driver_pid']=pid
            receipt['checks']=exercise(endpoint,args)
            receipt['post_session_storage']=wait_empty(args.output/'staging')
        log=(args.output/'server.log').read_text();records,_=parse_log(log)
        workers=[int(x) for x in re.findall(r'extension process worker \d+: pid=Some\((\d+)\)',log)]
        assert len(set(workers))==2 and not records
        receipt['process_cleanup']=live_pids([receipt['driver_pid'],*workers])
        receipt['driver_process_group_exists']=group_exists(receipt['driver_pid'])
        assert not receipt['cleanup_errors'] and not any(p['alive'] for p in receipt['process_cleanup'])
        assert not receipt['driver_process_group_exists']
        assert git(REPO,'rev-parse','HEAD')==source and not git(REPO,'status','--porcelain')
        receipt['outcome']='passed'
    except BaseException:
        receipt.update(outcome='failed',error=traceback.format_exc());raise
    finally:
        if 'driver_pid' in receipt:
            log=(args.output/'server.log').read_text()
            workers=[int(x) for x in re.findall(r'extension process worker \d+: pid=Some\((\d+)\)',log)]
            receipt['process_cleanup']=live_pids([receipt['driver_pid'],*workers])
            receipt['driver_process_group_exists']=group_exists(receipt['driver_pid'])
            receipt['remaining_staging_files']=[str(p) for p in (args.output/'staging').rglob('*') if p.is_file() or p.is_symlink()]
        receipt['finished_utc']=utc();save(args.output/'receipt.json',receipt)


if __name__=='__main__':main()

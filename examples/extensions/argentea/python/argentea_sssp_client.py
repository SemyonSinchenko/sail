"""Bounded worker-native weighted SSSP: Bellman–Ford and all-edge delta-star."""
import math
import json
import uuid
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.types import LongType, DoubleType
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint
from argentea_client import ArgenteaResult
from argentea_bfs_client import _integer
from argentea_views import compose_views

TYPE_URL = 'type.googleapis.com/nutmeg.v5.ArgenteaSsspApi'
MAX_PHASE_BUDGET = 128
METHODS = ('reference', 'delta_star')


def phase_count(max_rounds, method):
    if method not in METHODS:
        raise ValueError('method must be reference or delta_star')
    _integer(max_rounds, 'max_rounds', 0, 62)
    return 2*max_rounds+4


def options(*, method, max_rounds, partitions, source, delta, max_phase_budget, batch_rows):
    count = phase_count(max_rounds, method)
    _integer(partitions, 'partitions', 1, 64)
    _integer(source, 'source', -(1<<63), (1<<63)-1)
    if isinstance(delta,bool) or not isinstance(delta,(int,float)) or not math.isfinite(delta) or delta<=0:
        raise ValueError('delta must be finite and positive')
    _integer(max_phase_budget, 'max_phase_budget', 4, MAX_PHASE_BUDGET)
    _integer(batch_rows, 'batch_rows', 1, 65_536)
    if count > max_phase_budget:
        raise ValueError('SSSP native stages exceed max_phase_budget')


def request(*, vertices_count, source, method='reference', max_rounds=14, partitions=2,
            delta=4.0, max_phase_budget=128, batch_rows=4096,
            operation_id=None, snapshot_id=None, generation=1):
    options(method=method,max_rounds=max_rounds,partitions=partitions,source=source,delta=delta,
            max_phase_budget=max_phase_budget,batch_rows=batch_rows)
    _integer(vertices_count,'vertices_count',1,(1<<63)-1)
    _integer(generation,'generation',1,(1<<64)-1)
    base = dict(version=5,algorithm='sssp_'+method,max_rounds=max_rounds,source=source,delta=float(delta),
                partitions=partitions,vertices=vertices_count,max_phase_budget=max_phase_budget,
                batch_rows=batch_rows,generation=generation,
                operation_id=str(uuid.uuid4()) if operation_id is None else operation_id,
                snapshot_id=str(uuid.uuid4()) if snapshot_id is None else snapshot_id)
    for name in ('operation_id','snapshot_id'):
        if not isinstance(base[name],str) or str(uuid.UUID(base[name]))!=base[name]:
            raise ValueError(f'{name} must be a canonical UUID string')
    return base


def phases(max_rounds, method):
    slots = (phase_count(max_rounds,method)-2)//2
    schedule = [('init',0)]
    for phase in range(slots):
        schedule.extend([('decide',phase),('apply',phase)])
    return [*schedule,('result',slots)]


class ArgenteaSsspRelation(LogicalPlan):
    def __init__(self,request,inputs):
        super().__init__(None)
        self.request,self.inputs = dict(request),tuple(inputs)

    def plan(self,session):
        relation = self._create_proto_relation()
        payload = json.dumps(self.request,separators=(',',':'),allow_nan=False).encode()
        envelope = _bytes_field(1,TYPE_URL.encode())+_bytes_field(2,payload)
        for frame in self.inputs:
            envelope += _bytes_field(3,frame._plan.to_proto(session).SerializeToString())
        envelope += _varint(5<<3)+_varint(1)
        relation.extension.type_url,relation.extension.value = ENVELOPE_TYPE_URL,envelope
        return relation


def build_plan(spark,vertices,edges,**options):
    """Raw serialization reference; deep plans can exceed the host wire guard."""
    base = request(**options)
    schedule = phases(base['max_rounds'],base['algorithm'].removeprefix('sssp_'))
    frame = DataFrame(ArgenteaSsspRelation(dict(base,verb='init',phase=0),(vertices,edges)),spark)
    for verb,phase in schedule[1:]:
        frame = DataFrame(ArgenteaSsspRelation(dict(base,verb=verb,phase=phase),(frame,)),spark)
    return frame,base


def _diagnostics(stored,base,cancellation):
    names = ('phase','rounds','reached','converged')
    for name in ('id','hops','parent',*names):
        if name not in stored.columns or not isinstance(stored.schema[name].dataType,LongType):
            raise RuntimeError(f'Argentea SSSP result has invalid column {name}')
    if 'distance' not in stored.columns or not isinstance(stored.schema['distance'].dataType,DoubleType):
        raise RuntimeError('Argentea SSSP result has invalid column distance')
    aggregates = []
    for name in names:
        aggregates.extend((F.min(name).alias(name+'_min'),F.max(name).alias(name+'_max'),
                           F.count(name).alias(name+'_count')))
    aggregates.extend(F.count(name).alias(name+'_count') for name in ('distance','hops','parent'))
    invalid = ((F.col('distance').isNull()!=F.col('hops').isNull()) |
               (F.col('distance').isNull()!=F.col('parent').isNull()) |
               F.isnan('distance') | (F.col('distance')<0) | (F.col('distance')==float('inf')) |
               (F.col('hops')<0) | ((F.col('hops')==0)&(F.col('id')!=F.lit(base['source']))))
    aggregates.append(F.sum(F.when(invalid,1).otherwise(0)).alias('invalid_rows'))
    source = (F.col('id')==F.lit(base['source'])) & (F.col('distance')==0) & (F.col('hops')==0) & (F.col('parent')==F.lit(base['source']))
    aggregates.append(F.sum(F.when(source,1).otherwise(0)).alias('source_rows'))
    cancellation.check()
    rows=stored.agg(*aggregates).collect()
    cancellation.check()
    if len(rows)!=1:
        raise RuntimeError('Argentea SSSP diagnostic reduction did not return one row')
    row,result=rows[0],{}
    for name in names:
        value=row[name+'_min']
        if row[name+'_count']!=base['vertices'] or value is None or value!=row[name+'_max']:
            raise RuntimeError(f'Argentea SSSP global diagnostic {name} is missing or inconsistent')
        result[name]=value
    if (result['phase']!=base['max_rounds']+1 or result['converged']!=1
            or not 1<=result['rounds']<=base['max_rounds']
            or not 1<=result['reached']<=base['vertices']):
        raise RuntimeError('Argentea SSSP result lacks a valid terminal certificate')
    if (any(row[name+'_count']!=result['reached'] for name in ('distance','hops','parent'))
            or row['invalid_rows']!=0 or row['source_rows']!=1):
        raise RuntimeError('Argentea SSSP result has inconsistent reachability or source rows')
    return result


class ArgenteaSsspResult(ArgenteaResult):
    def __init__(self,retained,base,plan_bytes,diagnostics):
        super().__init__(retained,base,plan_bytes)
        self.algorithm='argentea-'+base['algorithm']
        self.rounds=self.iterations=diagnostics['rounds']
        self.reached=diagnostics['reached']
        self.converged=True
        self.phase=diagnostics['phase']
        self.native_phase_count=phase_count(base['max_rounds'],base['algorithm'].removeprefix('sssp_'))

    @property
    def frame(self):
        return self._retained.frame.select('id','distance','hops','parent')


class ArgenteaSssp:
    def __init__(self,spark,*,observer=None):
        self.spark,self.observer=spark,observer

    def sssp(self,vertices,edges,*,source,directed=True,method='reference',max_rounds=14,partitions=2,
            delta=4.0,max_phase_budget=128,batch_rows=4096,cancellation=None):
        """Directed or symmetrized nonnegative weighted SSSP with certified results.

        The advanced method is all-edge delta-star, not classical light/heavy
        delta-stepping. Graph rows stay in Sail; the client collects scalar
        diagnostics only. Caps fail without returning partial distances.
        """
        options(method=method,max_rounds=max_rounds,partitions=partitions,source=source,delta=delta,
                max_phase_budget=max_phase_budget,batch_rows=batch_rows)
        if not isinstance(directed,bool):
            raise ValueError("directed must be boolean")
        if "weight" not in edges.columns or not isinstance(edges.schema["weight"].dataType,DoubleType):
            raise ValueError("SSSP requires a DOUBLE weight column")
        from pyspark_pecan import GraphAlgorithms,GraphCancelledError
        def body(run,nodes,links,count):
            # Source existence and every weight are checked by the native
            # topology/build path against this owned immutable snapshot.
            if not directed:
                links=links.unionByName(links.select(F.col('dst').alias('src'),F.col('src').alias('dst'),'weight'))
            native_nodes=nodes.select('id',F.pmod(F.col('id'),F.lit(partitions)).cast('long').alias('owner'))
            native_edges=links.select('src','dst','weight',F.pmod(F.col('src'),F.lit(partitions)).cast('long').alias('owner'))
            base=request(vertices_count=count,method=method,max_rounds=max_rounds,partitions=partitions,
                         source=source,delta=delta,max_phase_budget=max_phase_budget,batch_rows=batch_rows)
            schedule=phases(max_rounds,method)
            with compose_views(self.spark,native_nodes,native_edges,request=base,phases=schedule,
                               relation_type=ArgenteaSsspRelation,cancellation=run.cancellation,
                               max_phases=max_phase_budget) as composition:
                frame=composition.frame
                plan_bytes=frame._plan.to_proto(self.spark.client).SerializeToString()
                if self.observer is not None:
                    self.observer(dict(kind='native_plan',request=dict(base),native_phase_count=len(schedule),
                                       frame=frame,plan_bytes=plan_bytes,view_registrations=composition.registrations))
                run.cancellation.check()
                path,stored=run.materialize(frame)
                diagnostics=_diagnostics(stored,base,run.cancellation)
            retained=run.finish(path,stored,algorithm='argentea-'+base['algorithm'],
                                iterations=diagnostics['rounds'],converged=True)
            return ArgenteaSsspResult(retained,base,plan_bytes,diagnostics)
        try:
            return GraphAlgorithms(self.spark)._run(vertices,edges,partitions,cancellation,body,edge_columns=("src","dst","weight"))
        except GraphCancelledError as error:
            for name in ('view_cleanup_deferred','view_cleanup_errors','uncertain_view_names'):
                if not hasattr(error,name) and hasattr(error.__cause__,name):
                    setattr(error,name,getattr(error.__cause__,name))
            raise

"""Bounded reference, frontier, and direction-optimizing worker-native BFS."""
import json
import uuid

from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.types import LongType
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint

from argentea_client import ArgenteaResult
from argentea_views import compose_views

TYPE_URL = 'type.googleapis.com/nutmeg.v3.ArgenteaBfsApi'
METHODS = ('reference','frontier','direction')
MAX_LEVELS = 62
MAX_PHASE_BUDGET = 128


def _integer(value,name,minimum,maximum):
    if isinstance(value,bool) or not isinstance(value,int) or not minimum<=value<=maximum:
        raise ValueError(f'{name} must be an integer in {minimum}..{maximum}')


def options(*, source, method, directed, max_levels, partitions, alpha, beta,
            max_phase_budget, batch_rows):
    _integer(source,'source',-(1<<63),(1<<63)-1)
    _integer(max_levels,'max_levels',0,MAX_LEVELS)
    _integer(partitions,'partitions',1,64)
    _integer(alpha,'alpha',1,(1<<64)-1)
    _integer(beta,'beta',1,(1<<64)-1)
    _integer(max_phase_budget,'max_phase_budget',4,MAX_PHASE_BUDGET)
    _integer(batch_rows,'batch_rows',1,65_536)
    if method not in METHODS:
        raise ValueError('method must be reference, frontier, or direction')
    if not isinstance(directed,bool):
        raise ValueError('directed must be boolean')
    if 2*max_levels+4>max_phase_budget:
        raise ValueError('2*max_levels+4 native stages exceed max_phase_budget')


def request(*, vertices_count, source, method='reference', max_levels=14, partitions=2,
            alpha=14, beta=24, max_phase_budget=32, batch_rows=4096,
            operation_id=None, snapshot_id=None, generation=1):
    options(source=source,method=method,directed=True,max_levels=max_levels,partitions=partitions,
            alpha=alpha,beta=beta,max_phase_budget=max_phase_budget,batch_rows=batch_rows)
    _integer(vertices_count,'vertices_count',1,(1<<63)-1)
    _integer(generation,'generation',1,(1<<64)-1)
    base = dict(version=3,algorithm='bfs_'+method,source=source,max_levels=max_levels,
                alpha=alpha,beta=beta,partitions=partitions,vertices=vertices_count,
                max_phase_budget=max_phase_budget,batch_rows=batch_rows,generation=generation,
                operation_id=str(uuid.uuid4()) if operation_id is None else operation_id,
                snapshot_id=str(uuid.uuid4()) if snapshot_id is None else snapshot_id)
    for name in ('operation_id','snapshot_id'):
        if not isinstance(base[name],str) or str(uuid.UUID(base[name]))!=base[name]:
            raise ValueError(f'{name} must be a canonical UUID string')
    return base


def phases(max_levels):
    schedule = [('init',0)]
    for phase in range(max_levels+1):
        schedule.extend([('decide',phase),('apply',phase)])
    return [*schedule,('result',max_levels+1)]


class ArgenteaBfsRelation(LogicalPlan):
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
    schedule = phases(base['max_levels'])
    frame = DataFrame(ArgenteaBfsRelation(dict(base,verb='init',phase=0),(vertices,edges)),spark)
    for verb,phase in schedule[1:]:
        frame = DataFrame(ArgenteaBfsRelation(dict(base,verb=verb,phase=phase),(frame,)),spark)
    return frame,base


def _diagnostics(stored,base,cancellation):
    names = ('phase','levels','reached','converged')
    for name in ('id','distance','hops','parent',*names):
        if name not in stored.columns or not isinstance(stored.schema[name].dataType,LongType):
            raise RuntimeError(f'Argentea BFS result has invalid column {name}')
    aggregates = []
    for name in names:
        aggregates.extend((F.min(name).alias(name+'_min'),F.max(name).alias(name+'_max'),
                           F.count(name).alias(name+'_count')))
    aggregates.extend(F.count(name).alias(name+'_count') for name in ('distance','hops','parent'))
    invalid = ((F.col('distance').isNull()!=F.col('hops').isNull()) |
               (F.col('distance').isNull()!=F.col('parent').isNull()) |
               (F.col('distance')<0) | (F.col('distance')!=F.col('hops')))
    aggregates.append(F.sum(F.when(invalid,1).otherwise(0)).alias('invalid_rows'))
    source = (F.col('id')==F.lit(base['source'])) & (F.col('distance')==0) & (F.col('hops')==0) & (F.col('parent')==F.lit(base['source']))
    aggregates.append(F.sum(F.when(source,1).otherwise(0)).alias('source_rows'))
    cancellation.check()
    rows = stored.agg(*aggregates).collect()
    cancellation.check()
    if len(rows)!=1:
        raise RuntimeError('Argentea BFS diagnostic reduction did not return one row')
    row,result = rows[0],{}
    for name in names:
        value = row[name+'_min']
        if row[name+'_count']!=base['vertices'] or value is None or value!=row[name+'_max']:
            raise RuntimeError(f'Argentea BFS global diagnostic {name} is missing or inconsistent')
        result[name] = value
    if (result['phase']!=base['max_levels']+1 or result['converged']!=1
            or not 1<=result['levels']<=base['max_levels']
            or not 1<=result['reached']<=base['vertices']):
        raise RuntimeError('Argentea BFS result lacks a valid terminal certificate')
    if (any(row[name+'_count']!=result['reached'] for name in ('distance','hops','parent'))
            or row['invalid_rows']!=0 or row['source_rows']!=1):
        raise RuntimeError('Argentea BFS result has inconsistent reachability or source rows')
    return result


class ArgenteaBfsResult(ArgenteaResult):
    """Owned rows: unreachable distance/hops/parent are null; source parent=source."""
    def __init__(self,retained,base,plan_bytes,diagnostics):
        super().__init__(retained,base,plan_bytes)
        self.algorithm = 'argentea-'+base['algorithm']
        self.levels = self.iterations = diagnostics['levels']
        self.reached = diagnostics['reached']
        self.converged = True
        self.phase = diagnostics['phase']
        self.native_phase_count = 2*base['max_levels']+4

    @property
    def frame(self):
        return self._retained.frame.select('id','distance','hops','parent')


class ArgenteaBfs:
    def __init__(self,spark,*,observer=None):
        self.spark,self.observer = spark,observer

    def bfs(self,vertices,edges,*,source,method='reference',directed=True,max_levels=14,
            partitions=2,alpha=14,beta=24,max_phase_budget=32,batch_rows=4096,cancellation=None):
        """Unweighted BFS with exact minimum-numeric-ID preceding-level parent.

        Pecan owns separate BIGINT snapshots under the valid-graph contract.
        Undirected input is expanded
        to both arcs server-side; duplicates and loops do not change distances.
        No graph rows collect in this client. Inputs obey the valid-graph
        contract; stored-result diagnostics use ordinary jobs. All native phases execute
        in one job through bounded lazy views and retained worker adjacency.

        A reachable depth D needs D+1 expansions to prove an empty frontier;
        max_levels<=62 and 2K+4<=128 are bounded deployment limits.
        Defaults retain the original 14-level/32-stage envelope; larger calls
        must explicitly raise max_phase_budget. Cap failure
        never returns partial distances. An isolated source still needs one
        expansion. Direction mode may use an incoming CSR in the same quota.
        """
        options(source=source,method=method,directed=directed,max_levels=max_levels,partitions=partitions,
                alpha=alpha,beta=beta,max_phase_budget=max_phase_budget,batch_rows=batch_rows)
        from pyspark_pecan import GraphAlgorithms,GraphCancelledError
        def body(run,nodes,links,count):
            run.cancellation.check()
            if not directed:
                links = links.unionByName(links.select(F.col('dst').alias('src'),F.col('src').alias('dst')))
            native_nodes = nodes.select('id',F.pmod(F.col('id'),F.lit(partitions)).cast('long').alias('owner'))
            native_edges = links.select('src','dst',F.pmod(F.col('src'),F.lit(partitions)).cast('long').alias('owner'))
            base = request(vertices_count=count,source=source,method=method,max_levels=max_levels,partitions=partitions,
                           alpha=alpha,beta=beta,max_phase_budget=max_phase_budget,batch_rows=batch_rows)
            with compose_views(self.spark,native_nodes,native_edges,request=base,phases=phases(max_levels),
                               relation_type=ArgenteaBfsRelation,cancellation=run.cancellation,
                               max_phases=max_phase_budget) as composition:
                frame = composition.frame
                plan_bytes = frame._plan.to_proto(self.spark.client).SerializeToString()
                if self.observer is not None:
                    self.observer(dict(kind='native_plan',request=dict(base),native_phase_count=2*max_levels+4,
                                       frame=frame,plan_bytes=plan_bytes,view_registrations=composition.registrations))
                run.cancellation.check()
                path,stored = run.materialize(frame)
                diagnostics = _diagnostics(stored,base,run.cancellation)
            retained = run.finish(path,stored,algorithm='argentea-'+base['algorithm'],
                                  iterations=diagnostics['levels'],converged=True)
            return ArgenteaBfsResult(retained,base,plan_bytes,diagnostics)
        try:
            return GraphAlgorithms(self.spark)._run(vertices,edges,partitions,cancellation,body)
        except GraphCancelledError as error:
            for name in ('view_cleanup_deferred','view_cleanup_errors','uncertain_view_names'):
                if not hasattr(error,name) and hasattr(error.__cause__,name):
                    setattr(error,name,getattr(error.__cause__,name))
            raise

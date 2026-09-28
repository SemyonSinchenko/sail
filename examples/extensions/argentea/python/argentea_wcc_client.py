"""Bounded worker-native WCC: reference propagation and seeded star contraction."""
import json
import uuid
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.types import LongType
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint
from argentea_client import ArgenteaResult
from argentea_bfs_client import _integer
from argentea_views import compose_views

TYPE_URL = 'type.googleapis.com/nutmeg.v4.ArgenteaWccApi'
MAX_PHASE_BUDGET = 128
METHODS = ('reference', 'star')


def phase_count(max_rounds, method):
    if method not in METHODS:
        raise ValueError('method must be reference or star')
    _integer(max_rounds, 'max_rounds', 0, 62 if method == 'reference' else 19)
    return 2*max_rounds+4 if method == 'reference' else 6*max_rounds+10


def options(*, method, max_rounds, partitions, seed, max_phase_budget, batch_rows):
    count = phase_count(max_rounds, method)
    _integer(partitions, 'partitions', 1, 64)
    _integer(seed, 'seed', 0, (1<<64)-1)
    _integer(max_phase_budget, 'max_phase_budget', 4, MAX_PHASE_BUDGET)
    _integer(batch_rows, 'batch_rows', 1, 65_536)
    if count > max_phase_budget:
        raise ValueError('WCC native stages exceed max_phase_budget')


def request(*, vertices_count, method='reference', max_rounds=14, partitions=2,
            seed=42, max_phase_budget=128, batch_rows=4096,
            operation_id=None, snapshot_id=None, generation=1):
    options(method=method,max_rounds=max_rounds,partitions=partitions,seed=seed,
            max_phase_budget=max_phase_budget,batch_rows=batch_rows)
    _integer(vertices_count,'vertices_count',1,(1<<63)-1)
    _integer(generation,'generation',1,(1<<64)-1)
    base = dict(version=4,algorithm='wcc_'+method,max_rounds=max_rounds,seed=seed,
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


class ArgenteaWccRelation(LogicalPlan):
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
    schedule = phases(base['max_rounds'],base['algorithm'].removeprefix('wcc_'))
    frame = DataFrame(ArgenteaWccRelation(dict(base,verb='init',phase=0),(vertices,edges)),spark)
    for verb,phase in schedule[1:]:
        frame = DataFrame(ArgenteaWccRelation(dict(base,verb=verb,phase=phase),(frame,)),spark)
    return frame,base


def _diagnostics(stored,base,cancellation):
    names = ('phase','rounds','converged')
    for name in ('id','component',*names):
        if name not in stored.columns or not isinstance(stored.schema[name].dataType,LongType):
            raise RuntimeError(f'Argentea WCC result has invalid column {name}')
    aggregates = []
    for name in names:
        aggregates.extend((F.min(name).alias(name+'_min'),F.max(name).alias(name+'_max'),
                           F.count(name).alias(name+'_count')))
    aggregates.append(F.count('component').alias('component_count'))
    aggregates.append(F.sum(F.when(F.col('component')>F.col('id'),1).otherwise(0)).alias('invalid_rows'))
    cancellation.check()
    rows = stored.agg(*aggregates).collect()
    cancellation.check()
    if len(rows)!=1:
        raise RuntimeError('Argentea WCC diagnostic reduction did not return one row')
    row,result = rows[0],{}
    for name in names:
        value = row[name+'_min']
        if row[name+'_count']!=base['vertices'] or value is None or value!=row[name+'_max']:
            raise RuntimeError(f'Argentea WCC global diagnostic {name} is missing or inconsistent')
        result[name] = value
    method=base['algorithm'].removeprefix('wcc_')
    slots=(phase_count(base['max_rounds'],method)-2)//2
    if (result['phase']!=slots or result['converged']!=1
            or not 0<=result['rounds']<=base['max_rounds']
            or (method=='reference' and result['rounds']==0)
            or row['component_count']!=base['vertices'] or row['invalid_rows']!=0):
        raise RuntimeError('Argentea WCC result lacks a valid terminal certificate')
    return result


class ArgenteaWccResult(ArgenteaResult):
    def __init__(self,retained,base,plan_bytes,diagnostics):
        super().__init__(retained,base,plan_bytes)
        self.algorithm='argentea-'+base['algorithm']
        self.rounds=self.iterations=diagnostics['rounds']
        self.converged=True
        self.phase=diagnostics['phase']
        self.native_phase_count=phase_count(base['max_rounds'],base['algorithm'].removeprefix('wcc_'))

    @property
    def frame(self):
        return self._retained.frame.select('id','component')


class ArgenteaWcc:
    def __init__(self,spark,*,observer=None):
        self.spark,self.observer=spark,observer

    def wcc(self,vertices,edges,*,method='reference',max_rounds=14,partitions=2,
            seed=42,max_phase_budget=128,batch_rows=4096,cancellation=None):
        """Minimum-ID weak components, with a complete convergence certificate.

        Uses Pecan's validated snapshots and owned result lifecycle. Star is
        seeded head/tail contraction over retained original adjacency, distinct
        from Banda/Pecan GF64. Caps fail without partial labels. The128-stage
        bound needs separate live-host qualification; it is not a convergence
        guarantee. Only scalar diagnostics collect on the client.
        """
        options(method=method,max_rounds=max_rounds,partitions=partitions,seed=seed,
                max_phase_budget=max_phase_budget,batch_rows=batch_rows)
        from pyspark_pecan import GraphAlgorithms,GraphCancelledError
        def body(run,nodes,links,count):
            native_nodes=nodes.select('id',F.pmod(F.col('id'),F.lit(partitions)).cast('long').alias('owner'))
            native_edges=links.select('src','dst',F.pmod(F.col('src'),F.lit(partitions)).cast('long').alias('owner'))
            base=request(vertices_count=count,method=method,max_rounds=max_rounds,partitions=partitions,
                         seed=seed,max_phase_budget=max_phase_budget,batch_rows=batch_rows)
            schedule=phases(max_rounds,method)
            with compose_views(self.spark,native_nodes,native_edges,request=base,phases=schedule,
                               relation_type=ArgenteaWccRelation,cancellation=run.cancellation,
                               max_phases=max_phase_budget) as composition:
                frame=composition.frame
                plan_bytes=frame._plan.to_proto(self.spark.client).SerializeToString()
                if self.observer is not None:
                    self.observer(dict(kind='native_plan',request=dict(base),native_phase_count=len(schedule),
                                       frame=frame,plan_bytes=plan_bytes,view_registrations=composition.registrations))
                run.cancellation.check()
                path,stored=run.materialize(frame,expected_rows=count)
                diagnostics=_diagnostics(stored,base,run.cancellation)
            retained=run.finish(path,stored,algorithm='argentea-'+base['algorithm'],
                                iterations=diagnostics['rounds'],converged=True)
            return ArgenteaWccResult(retained,base,plan_bytes,diagnostics)
        try:
            return GraphAlgorithms(self.spark)._run(vertices,edges,partitions,cancellation,body)
        except GraphCancelledError as error:
            for name in ('view_cleanup_deferred','view_cleanup_errors','uncertain_view_names'):
                if not hasattr(error,name) and hasattr(error.__cause__,name):
                    setattr(error,name,getattr(error.__cause__,name))
            raise

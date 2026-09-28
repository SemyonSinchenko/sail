"""Bounded, certified residual PageRank in one Argentea worker-native DAG.

Version 2 is separate from fixed-round version 1. At most seven pushes fit the
initial 32-native-stage budget; ordinary strict-tolerance graphs may exceed it.
Native nonconvergence is an error, never an uncertified retained rank result.
"""
import json
import math
import uuid

from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.types import DoubleType, LongType
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint

from argentea_client import ArgenteaResult

TYPE_URL = 'type.googleapis.com/nutmeg.v2.ArgenteaDeltaApi'
MAX_PHASE_BUDGET = 32
MAX_PUSHES = 7
MAX_PARTITIONS = 64
_DIAGNOSTICS = ('phase', 'pushes', 'certificate_passes', 'residual_l1',
                'stationary_error_bound', 'converged')


def _integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f'{name} must be an integer in {minimum}..{maximum}')


def options(*, max_pushes, partitions, reset_probability, tolerance, max_phase_budget, batch_rows):
    """Reject an over-budget plan before any Spark action or owned allocation."""
    _integer(max_pushes, 'max_pushes', 0, MAX_PUSHES)
    _integer(partitions, 'partitions', 1, MAX_PARTITIONS)
    _integer(max_phase_budget, 'max_phase_budget', 4, MAX_PHASE_BUDGET)
    _integer(batch_rows, 'batch_rows', 1, 65_536)
    if 4 * max_pushes + 4 > max_phase_budget:
        raise ValueError('4*max_pushes+4 native stages exceed max_phase_budget')
    if (isinstance(reset_probability, bool) or not isinstance(reset_probability, (int, float))
            or not math.isfinite(reset_probability) or not 0 < reset_probability <= 1
            or 1.0-reset_probability == 1.0):
        raise ValueError('reset_probability must be finite and in (0,1], with representable damping below 1')
    if (isinstance(tolerance, bool) or not isinstance(tolerance, (int, float))
            or not math.isfinite(tolerance) or tolerance <= 0):
        raise ValueError('tolerance must be a positive finite number')


class ArgenteaDeltaRelation(LogicalPlan):
    def __init__(self, request, inputs):
        super().__init__(None)
        self.request = dict(request)
        self.inputs = tuple(inputs)

    def plan(self, session):
        relation = self._create_proto_relation()
        payload = json.dumps(self.request, separators=(',', ':'), allow_nan=False).encode()
        envelope = _bytes_field(1, TYPE_URL.encode()) + _bytes_field(2, payload)
        for frame in self.inputs:
            envelope += _bytes_field(3, frame._plan.to_proto(session).SerializeToString())
        envelope += _varint(5 << 3) + _varint(1)
        relation.extension.type_url = ENVELOPE_TYPE_URL
        relation.extension.value = envelope
        return relation


def _request(*, vertices_count, max_pushes=7, partitions=2,
             reset_probability=0.15, tolerance=1e-8, max_phase_budget=32,
             batch_rows=4096, operation_id=None, snapshot_id=None, generation=1):
    options(max_pushes=max_pushes, partitions=partitions, reset_probability=reset_probability,
            tolerance=tolerance, max_phase_budget=max_phase_budget, batch_rows=batch_rows)
    _integer(vertices_count, 'vertices_count (nonempty graph required)', 1, (1 << 63)-1)
    _integer(generation, 'generation', 1, (1 << 64)-1)
    base = dict(version=2, algorithm='pagerank_delta',
                operation_id=str(uuid.uuid4()) if operation_id is None else operation_id,
                snapshot_id=str(uuid.uuid4()) if snapshot_id is None else snapshot_id,
                generation=generation, partitions=partitions, vertices=vertices_count,
                damping=1.0-reset_probability, tolerance=tolerance,
                max_pushes=max_pushes, max_phase_budget=max_phase_budget, batch_rows=batch_rows)
    for name in ('operation_id', 'snapshot_id'):
        if not isinstance(base[name], str) or str(uuid.UUID(base[name])) != base[name]:
            raise ValueError(f'{name} must be a canonical UUID string')
    return base


def build_plan(spark, vertices, edges, *, vertices_count, max_pushes=7, partitions=2,
               reset_probability=0.15, tolerance=1e-8, max_phase_budget=32,
               batch_rows=4096, operation_id=None, snapshot_id=None, generation=1):
    """Describe the raw nested 4K+4-stage DAG without executing a Spark action.

    This serialization/reference helper can exceed the host protobuf nesting
    limit at larger K. The public client composes bounded session views instead;
    neither helper raises or bypasses the host wire guard.

    Inputs are already validated snapshots: vertices(id,owner), edges(src,dst,
    owner), all BIGINT. Init emits statistics 0; each j in 0..2K has decide(j)
    and apply(j); result consumes statistics 2K+1. Converged owners relay DONE
    through remaining slots, retaining their certified snapshot. Host scan,
    exchange and sink stages are additional to the native-stage budget.
    """
    base = _request(vertices_count=vertices_count, max_pushes=max_pushes, partitions=partitions,
                    reset_probability=reset_probability, tolerance=tolerance, max_phase_budget=max_phase_budget,
                    batch_rows=batch_rows, operation_id=operation_id, snapshot_id=snapshot_id, generation=generation)
    frame = DataFrame(ArgenteaDeltaRelation(dict(base, verb='init', phase=0), (vertices, edges)), spark)
    for phase in range(2 * max_pushes + 1):
        frame = DataFrame(ArgenteaDeltaRelation(dict(base, verb='decide', phase=phase), (frame,)), spark)
        frame = DataFrame(ArgenteaDeltaRelation(dict(base, verb='apply', phase=phase), (frame,)), spark)
    result = DataFrame(ArgenteaDeltaRelation(dict(base, verb='result', phase=2*max_pushes+1), (frame,)), spark)
    return result, base


def _read_diagnostics(stored, request, cancellation):
    """Reduce stored global diagnostics to one scalar row; never collect ranks."""
    aggregates = []
    for name in _DIAGNOSTICS:
        expected_type = DoubleType if name in ('residual_l1', 'stationary_error_bound') else LongType
        if name not in stored.columns or not isinstance(stored.schema[name].dataType, expected_type):
            raise RuntimeError(f'Argentea v2 result has invalid diagnostic column {name}')
        aggregates.extend((F.min(name).alias(name+'_min'), F.max(name).alias(name+'_max'),
                           F.count(name).alias(name+'_count')))
    cancellation.check()
    rows = stored.agg(*aggregates).collect()
    cancellation.check()
    if len(rows) != 1:
        raise RuntimeError('Argentea v2 diagnostic reduction did not return one row')
    row, result = rows[0], {}
    for name in _DIAGNOSTICS:
        value = row[name+'_min']
        if (row[name+'_count'] != request['vertices'] or value is None
                or value != row[name+'_max']):
            raise RuntimeError(f'Argentea v2 global diagnostic {name} is missing or inconsistent')
        result[name] = value
    if (result['phase'] != 2*request['max_pushes']+1
            or not 0 <= result['pushes'] <= request['max_pushes']
            or not 1 <= result['certificate_passes'] <= result['pushes']+1
            or result['converged'] != 1):
        raise RuntimeError('Argentea v2 result lacks a valid terminal certificate')
    residual, bound = result['residual_l1'], result['stationary_error_bound']
    if (not math.isfinite(residual) or not 0 <= residual <= request['tolerance']
            or not math.isfinite(bound) or bound < 0
            or not math.isclose(bound, residual/(1.0-request['damping']), rel_tol=1e-12, abs_tol=0.0)):
        raise RuntimeError('Argentea v2 result has an invalid certified residual/error bound')
    return result


class ArgenteaDeltaResult(ArgenteaResult):
    """Owned certified ranks, native provenance and actual work counters.

    iterations/pushes count residual pushes, not transport slots or certificates.
    residual is the full normalized fixed-point L1 certificate; error_bound is
    residual/(1-damping) in exact arithmetic. close() invalidates both frames;
    write_parquet() creates independently caller-owned output.
    """
    def __init__(self, retained, request, plan_bytes, diagnostics):
        super().__init__(retained, request, plan_bytes)
        self.algorithm = 'argentea-pagerank-delta'
        self.pushes = self.iterations = diagnostics['pushes']
        self.certificate_passes = diagnostics['certificate_passes']
        self.residual = diagnostics['residual_l1']
        self.error_bound = diagnostics['stationary_error_bound']
        self.converged = True
        self.phase = diagnostics['phase']
        self.native_phase_count = 4*request['max_pushes']+4


class ArgenteaDelta:
    def __init__(self, spark, *, observer=None):
        self.spark = spark
        self.observer = observer

    def pagerank(self, vertices, edges, *, max_pushes=7, partitions=2,
                 reset_probability=0.15, tolerance=1e-8, max_phase_budget=32,
                 batch_rows=4096, cancellation=None):
        """Run bounded signed-residual PageRank with strict global certification.

        Uniform initialization/restart and dangling redistribution retain parallel
        edges, self-loops and isolates. The tolerance is the freshly recomputed
        normalized fixed-point L1 residual, not successive-iterate change.
        Native cap failures propagate through Pecan's owned cleanup; they do not
        return capped rows. Seven pushes cannot certify arbitrary graphs at 1e-8.

        Requires worker-mode Sail and the v2 Nutmeg wheel on every participant.
        UUID-named session views register each native phase lazily; the terminal
        materialization runs all native phases in one job. Confirmed views are
        dropped before returning; uncertain registration/drop failures expose
        view_cleanup_deferred and remain session-owned. The raw build_plan
        helper is for reference and may exceed the host wire nesting limit.
        Pecan validates separate input snapshots;
        materialization checks and the scalar diagnostic read use ordinary jobs.
        This is no cross-job native handle or atomic two-table snapshot. Failed
        writes retain Pecan's uncertain-write/session-cleanup policy.
        """
        options(max_pushes=max_pushes, partitions=partitions, reset_probability=reset_probability,
                tolerance=tolerance, max_phase_budget=max_phase_budget, batch_rows=batch_rows)
        from pyspark_pecan import GraphAlgorithms, GraphCancelledError
        from argentea_delta_views import compose_plan

        def body(run, nodes, links, count):
            native_nodes = nodes.select('id', F.pmod(F.col('id'), F.lit(partitions)).cast('long').alias('owner'))
            native_edges = links.select('src', 'dst', F.pmod(F.col('src'), F.lit(partitions)).cast('long').alias('owner'))
            with compose_plan(self.spark, native_nodes, native_edges, cancellation=run.cancellation,
                vertices_count=count, max_pushes=max_pushes, partitions=partitions,
                reset_probability=reset_probability, tolerance=tolerance,
                max_phase_budget=max_phase_budget, batch_rows=batch_rows) as composition:
                frame, request = composition.frame, composition.request
                plan_bytes = frame._plan.to_proto(self.spark.client).SerializeToString()
                if self.observer is not None:
                    self.observer(dict(kind='native_plan', request=dict(request),
                                       native_phase_count=4*max_pushes+4, plan_bytes=plan_bytes,
                                       frame=frame, view_registrations=composition.registrations))
                run.cancellation.check()
                path, stored = run.materialize(frame, expected_rows=count)
                diagnostics = _read_diagnostics(stored, request, run.cancellation)
            retained = run.finish(path, stored, algorithm='argentea-pagerank-delta',
                                  iterations=diagnostics['pushes'], converged=True)
            return ArgenteaDeltaResult(retained, request, plan_bytes, diagnostics)

        try:
            return GraphAlgorithms(self.spark)._run(vertices, edges, partitions, cancellation, body)
        except GraphCancelledError as error:
            # Pecan may wrap a failed RPC after cancellation. Keep view cleanup
            # diagnostics directly available on that public cancellation error.
            for name in ('view_cleanup_deferred', 'view_cleanup_errors', 'uncertain_view_names'):
                if not hasattr(error, name) and hasattr(error.__cause__, name):
                    setattr(error, name, getattr(error.__cause__, name))
            raise

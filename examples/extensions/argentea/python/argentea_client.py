"""Bounded, fixed-round Argentea PageRank in one Sail worker job.

Argentea is Nutmeg's distributed native path. This example client uses Pecan's
owned snapshot/result lifecycle; it does not add a server extension package.
"""
import math
import uuid

from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint

TYPE_URL = 'type.googleapis.com/nutmeg.v1.ArgenteaApi'
MAX_FIXED_ROUNDS = 32
MAX_PARTITIONS = 64


def options(*, iterations, partitions, reset_probability, batch_rows):
    for name, value, maximum in (('iterations', iterations, MAX_FIXED_ROUNDS),
                                 ('partitions', partitions, MAX_PARTITIONS),
                                 ('batch_rows', batch_rows, 65_536)):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            raise ValueError(f'{name} must be an integer in 1..{maximum}')
    if (isinstance(reset_probability, bool) or not isinstance(reset_probability, (int, float))
            or not math.isfinite(reset_probability) or not 0 < reset_probability <= 1
            or 1.0-reset_probability == 1.0):
        raise ValueError('reset_probability must be finite and in (0,1], with representable damping below 1')


class ArgenteaRelation(LogicalPlan):
    def __init__(self, request, inputs):
        super().__init__(None)
        self.request = dict(request)
        self.inputs = tuple(inputs)

    def plan(self, session):
        import json

        relation = self._create_proto_relation()
        payload = json.dumps(self.request, separators=(',', ':'), allow_nan=False).encode()
        envelope = _bytes_field(1, TYPE_URL.encode()) + _bytes_field(2, payload)
        for frame in self.inputs:
            envelope += _bytes_field(3, frame._plan.to_proto(session).SerializeToString())
        envelope += _varint(5 << 3) + _varint(1)
        relation.extension.type_url = ENVELOPE_TYPE_URL
        relation.extension.value = envelope
        return relation


def build_plan(spark, vertices, edges, *, vertices_count, iterations=2, partitions=2,
               reset_probability=0.15, batch_rows=4096, operation_id=None, snapshot_id=None):
    """Describe one native DAG; this function performs no Spark action.

    Inputs must already be validated snapshots with explicit BIGINT owner
    columns. The public Argentea.pagerank() method prepares these inputs.
    """
    options(iterations=iterations, partitions=partitions,
            reset_probability=reset_probability, batch_rows=batch_rows)
    if isinstance(vertices_count, bool) or not isinstance(vertices_count, int) or not 1 <= vertices_count < 1 << 63:
        raise ValueError('the bounded Argentea prototype requires at least one vertex')
    base = dict(version=1, operation_id=str(uuid.uuid4()) if operation_id is None else operation_id,
                snapshot_id=str(uuid.uuid4()) if snapshot_id is None else snapshot_id, generation=1,
                partitions=partitions, vertices=vertices_count,
                damping=1.0-reset_probability, batch_rows=batch_rows)
    for name in ('operation_id', 'snapshot_id'):
        # Opaque labels are scoped by the host's actual job; they grant no
        # cross-query access to worker state. Canonical UUIDs aid evidence joins.
        if not isinstance(base[name], str) or str(uuid.UUID(base[name])) != base[name]:
            raise ValueError(f'{name} must be a canonical UUID string')
    frame = DataFrame(ArgenteaRelation(dict(base, verb='init', round=0), (vertices, edges)), spark)
    for round_number in range(1, iterations):
        frame = DataFrame(ArgenteaRelation(dict(base, verb='round', round=round_number), (frame,)), spark)
    result = DataFrame(ArgenteaRelation(dict(base, verb='result', round=iterations-1), (frame,)), spark)
    return result, base


class ArgenteaResult:
    """An owned materialized result plus complete native provenance columns."""
    def __init__(self, retained, request, plan_bytes):
        self._retained = retained
        self.request = dict(request)
        self.plan_bytes = plan_bytes
        self.algorithm = 'argentea-pagerank-power'
        self.iterations = retained.iterations
        self.converged = None

    @property
    def frame(self):
        return self._retained.frame.select('id', 'pagerank')

    @property
    def native_frame(self):
        return self._retained.frame

    @property
    def path(self):
        return self._retained.path

    def touch(self):
        self._retained.touch()

    def close(self):
        self._retained.close()

    def write_parquet(self, path, *, mode='error'):
        self.touch()
        self.frame.write.mode(mode).parquet(path)

    def __enter__(self):
        self.touch()
        return self

    def __exit__(self, *_):
        self.close()


class Argentea:
    def __init__(self, spark, *, observer=None):
        self.spark = spark
        self.observer = observer

    def pagerank(self, vertices, edges, *, iterations=2, partitions=2,
                 reset_probability=0.15, batch_rows=4096, cancellation=None):
        """Run exactly iterations full normalized PageRank updates.

        Initializes 1/N; redistributes dangling mass uniformly; retains parallel
        edges, loops and isolates. convergence is intentionally not asserted.
        Every native round is in one physical job. Snapshot validation and result
        reading are ordinary separate Sail jobs. This bounded spike requires
        worker mode; invoking it against a local-mode server fails explicitly.
        """
        options(iterations=iterations, partitions=partitions,
                reset_probability=reset_probability, batch_rows=batch_rows)
        from pyspark_pecan import GraphAlgorithms

        def body(run, nodes, links, count):
            native_nodes = nodes.select('id', F.pmod(F.col('id'), F.lit(partitions)).cast('long').alias('owner'))
            native_edges = links.select('src', 'dst', F.pmod(F.col('src'), F.lit(partitions)).cast('long').alias('owner'))
            frame, request = build_plan(self.spark, native_nodes, native_edges,
                vertices_count=count, iterations=iterations, partitions=partitions,
                reset_probability=reset_probability, batch_rows=batch_rows)
            plan_bytes = frame._plan.to_proto(self.spark.client).SerializeToString()
            if self.observer is not None:
                self.observer(dict(kind='native_plan', request=request, iterations=iterations,
                                   plan_bytes=plan_bytes, frame=frame))
            run.cancellation.check()
            # One terminal action drives init -> all rounds -> result. StagingRun
            # owns partial writes and applies the existing uncertain-write policy.
            path, stored = run.materialize(frame, expected_rows=count)
            retained = run.finish(path, stored, algorithm='argentea-pagerank-power',
                                  iterations=iterations, converged=None)
            return ArgenteaResult(retained, request, plan_bytes)

        graph = GraphAlgorithms(self.spark)
        return graph._run(vertices, edges, partitions, cancellation, body)

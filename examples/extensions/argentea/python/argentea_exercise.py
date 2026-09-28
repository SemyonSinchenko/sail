"""Tiny live-service checks; publication requires subsequent worker-log audit."""
import contextlib
import io
import json
from pathlib import Path

from argentea_client import Argentea
from argentea_evidence import validate_rows

IDS = [-6, 0, 2, 3, 5, 8]
EDGES = [(-6, 0), (0, 3), (0, 3), (3, 2), (2, -6), (5, 5)]


def exercise(endpoint, output, *, iterations=2, partitions=3, expect_local_rejection=False):
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark.sql.connect.session import SparkSession

    output = Path(output)
    evidence = dict(ids=IDS, edges=EDGES, iterations=iterations, partitions=partitions)
    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)])
    try:
        def observe(event):
            (output / 'client-plan.pb').write_bytes(event['plan_bytes'])
            evidence['request'] = event['request']
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                event['frame'].explain(mode='extended')
            explanation = stream.getvalue()
            (output / 'explain.txt').write_text(explanation)
            if not expect_local_rejection:
                assert 'WorkerExtensionExec' in explanation, 'analyzed physical plan lacks worker native execution'
            evidence['analyzed_plan_contains_worker_extension'] = 'WorkerExtensionExec' in explanation

        nodes = spark.createDataFrame([(node,) for node in IDS], 'id long')
        edges = spark.createDataFrame(EDGES, 'src long, dst long')
        graph = Argentea(spark, observer=observe)
        try:
            result = graph.pagerank(nodes, edges, iterations=iterations, partitions=partitions)
        except Exception as error:
            if expect_local_rejection and 'requires distributed Sail execution' in str(error):
                evidence.update(outcome='expected-local-rejection', error=str(error))
                return evidence
            raise
        if expect_local_rejection:
            result.close()
            raise AssertionError('worker-native relation unexpectedly executed in local mode')
        with result:
            assert result.converged is None and result.iterations == iterations
            rows = [row.asDict() for row in result.native_frame.collect()]
            evidence['request'] = result.request
            evidence['rows'] = rows
            evidence['result_validation'] = validate_rows(rows, IDS, EDGES, result.request, iterations)
            evidence['retained_result_path'] = result.path
        # The frame accessor must reject use after owned staging cleanup.
        try:
            _ = result.frame
        except RuntimeError:
            evidence['result_closed'] = True
        else:
            raise AssertionError('result still exposed after close')
        evidence['worker_endpoints'] = [row.asDict() for row in spark.sql('''
            SELECT CAST(worker_id AS BIGINT) AS worker_id, host, CAST(port AS INT) AS port, status
            FROM system.cluster.workers
        ''').collect()]
        evidence['stages'] = [row.asDict() for row in spark.sql('''
            SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
                   CAST(partitions AS BIGINT) AS partitions, placement
            FROM system.execution.stages
        ''').collect()]
        evidence['outcome'] = 'answers-passed-awaiting-native-audit'
        return evidence
    finally:
        spark.stop()
        (output / 'exercise.json').write_text(json.dumps(evidence, sort_keys=True, indent=2) + '\n')

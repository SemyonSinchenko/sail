"""Functional two-host exercise for client-controlled relational graph algorithms."""
import math
from pathlib import Path
import sys


def _reference_pagerank(ids, edges, steps, reset):
    ranks = {node: 1 / len(ids) for node in ids}
    outgoing = {node: [] for node in ids}
    for source, target in edges:
        outgoing[source].append(target)
    for _ in range(steps):
        dangling = sum(ranks[node] for node in ids if not outgoing[node])
        updated = {node: reset / len(ids) + (1 - reset) * dangling / len(ids) for node in ids}
        for source, targets in outgoing.items():
            for target in targets:
                updated[target] += (1 - reset) * ranks[source] / len(targets)
        ranks = updated
    return ranks


def exercise(endpoint, worker_hosts, evidence):
    # Test the client from this exact source distribution, even if another
    # version was previously installed in the controller's Python environment.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "graph-algorithms/src"))
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark_graph_algorithms import GraphAlgorithms, GraphUtils

    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([
        DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)
    ])
    try:
        evidence["iteration_windows"] = windows = []
        starts = {}

        def stages():
            return [r.asDict() for r in spark.sql("""
                SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
                       CAST(partitions AS BIGINT) AS partitions, placement
                FROM system.execution.stages
            """).collect()]

        def observe(event):
            key = event["algorithm"], event["iteration"]
            recorded = stages()
            if event["kind"] == "iteration_start":
                starts[key] = max((row["job_id"] for row in recorded), default=0)
            elif event["kind"] == "iteration_end":
                # Input snapshot and graph validation precede iteration_start.
                # System-table observation queries execute on the driver; only
                # distributed stages in this interval count as algorithm work.
                selected = [row for row in recorded if row["job_id"] > starts[key]
                            and row["placement"] == "Worker" and row["partitions"] >= 2]
                assert selected, f"no distributed algorithm stage in {key}"
                windows.append(dict(algorithm=key[0], iteration=key[1], stages=selected))

        utils = GraphUtils(spark)
        evidence["utils"] = dict(engine=utils.engine, capabilities=sorted(utils.capabilities),
                                 lease_seconds=utils.lease_seconds)
        ids = [0, 1, 2, 3, 4, 5, 6]
        links = [(0, 1), (0, 1), (1, 2), (2, 0), (2, 3), (4, 5), (5, 4)]
        vertices = spark.createDataFrame([(v,) for v in ids], "id long").repartition(4)
        edges = spark.createDataFrame(links, "src long,dst long").repartition(4)
        graph = GraphAlgorithms(spark, observer=observe)
        with graph.pagerank(vertices, edges, max_iterations=3, partitions=4) as result:
            ranks = {row.id: row.pagerank for row in result.frame.collect()}
            reference = _reference_pagerank(ids, links, 3, 0.15)
            assert ranks.keys() == reference.keys()
            assert all(math.isclose(ranks[k], reference[k], rel_tol=1e-10, abs_tol=1e-12)
                       for k in reference), (ranks, reference)
            evidence["pagerank"] = dict(ranks=ranks, iterations=result.iterations,
                                        converged=result.converged)
        with graph.wcc(vertices, edges, max_iterations=10, partitions=4) as result:
            labels = {row.id: row.component for row in result.frame.collect()}
            assert labels == {0: 0, 1: 0, 2: 0, 3: 0, 4: 4, 5: 4, 6: 6}
            evidence["wcc"] = dict(labels=labels, iterations=result.iterations,
                                   converged=result.converged, algorithm=result.algorithm)
        evidence["worker_endpoints"] = [r.asDict() for r in spark.sql("""
            SELECT CAST(worker_id AS BIGINT) AS worker_id, host, CAST(port AS INT) AS port,status
            FROM system.cluster.workers
        """).collect()]
        running = [r for r in evidence["worker_endpoints"] if r["status"] == "RUNNING"]
        assert worker_hosts <= {r["host"] for r in running}
        evidence["stage_placement"] = stages()
        return evidence
    finally:
        spark.stop()

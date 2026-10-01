"""Exact valid-input answers and necessary action counts on actual SQL."""
from typing import Any
import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.types import DoubleType, LongType

from pyspark_pecan import GraphAlgorithms
from pyspark_pecan.traversal_state import initial_state

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('algorithm,method', [
    ('bfs', 'reference'), ('bfs', 'frontier'), ('bfs', 'push_pull'),
    ('sssp', 'reference'), ('sssp', 'frontier'), ('sssp', 'delta_star')])
def test_valid_traversal_outputs_are_exact(spark: Any, algorithm: str, method: str) -> None:
    low, high = -(2**63), 2**63 - 1
    vertices = spark.createDataFrame([(node,) for node in (low, -7, 0, 4, high)], 'id BIGINT')
    edges = spark.createDataFrame([
        (low, 0, 2.), (low, -7, 1.), (low, -7, 1.),
        (-7, 0, 1.), (0, 4, 0.), (4, 0, 0.)],
        'src BIGINT, dst BIGINT, weight DOUBLE')
    d = 1. if algorithm == 'bfs' else 2.
    expected = [(low, 0., 0, low), (-7, 1., 1, low), (0, d, 1, low),
                (4, 2., 2, 0), (high, None, None, None)]
    graph = GraphAlgorithms(spark)
    with getattr(graph, algorithm)(vertices, edges, source=low, method=method,
                                  max_iterations=32, partitions=2) as result:
        rows = sorted((r.id, r.distance, r.hops, r.parent) for r in result.frame.collect())
        assert rows == expected
        assert result.converged is True



@pytest.mark.parametrize('source', [-(2**63), 2**63 - 1])
def test_singleton_seed_sql_preserves_extreme_bigint_and_column_types(spark: Any, source: int) -> None:
    frame = initial_state(spark, source)
    assert [(field.name, type(field.dataType)) for field in frame.schema] == [
        ('id', LongType), ('distance', DoubleType), ('hops', LongType), ('parent', LongType)]
    assert [tuple(row) for row in frame.collect()] == [(source, 0., 0, source)]


@pytest.mark.parametrize('method', ['reference', 'frontier'])
def test_bfs_issues_only_its_convergence_count_per_round(spark: Any, monkeypatch: Any, method: str) -> None:
    vertices = spark.createDataFrame([(0,), (1,), (2,), (9,)], 'id BIGINT')
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], 'src BIGINT, dst BIGINT')
    counts = []
    original = DataFrame.count

    def count(frame: Any) -> int:
        value = original(frame)
        counts.append(value)
        return value

    monkeypatch.setattr(DataFrame, 'count', count)
    with GraphAlgorithms(spark).bfs(vertices, edges, source=0, method=method,
                                    max_iterations=4, partitions=2) as result:
        assert [(r.id, r.distance) for r in result.frame.orderBy('id').collect()] == [
            (0, 0.), (1, 1.), (2, 2.), (9, None)]
        assert result.iterations == 3
        # The only count actions discover the next frontier and certify its
        # final emptiness. A separate infinity scan is redundant for unit hops:
        # any shortest simple path has <2**64 edges for distinct BIGINT IDs,
        # far below the finite DOUBLE range. Finite path sums are a valid-input precondition.
        assert counts == [1, 1, 0]

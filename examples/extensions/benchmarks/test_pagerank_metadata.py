"""Reject malformed PageRank metadata through both real SQL validators."""
import math
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from graph_cell import NonConvergedError, validate


@pytest.fixture(scope='module')
def spark():
    endpoint = os.environ.get('SAIL_GRAPH_TEST_REMOTE')
    if not endpoint:
        pytest.skip('set SAIL_GRAPH_TEST_REMOTE for actual PageRank validation checks')
    from pyspark.sql.connect.session import SparkSession
    session = SparkSession.builder.remote(endpoint).create()
    yield session
    session.stop()


def check(spark, tmp_path, policy, *, iterations=(1, 1), converged=(True, True),
          scores=(.5, .5), residual=(0., 0.), native=False, optimized=False):
    dataset, output = tmp_path/'dataset', tmp_path/'result'
    dataset.mkdir()
    output.mkdir()
    # Uniform scores are the exact fixed point on this cycle. A malformed
    # metadata test therefore cannot pass by failing the score proof first.
    pq.write_table(pa.table({'id': [0, 1]}), dataset/'vertices.parquet')
    pq.write_table(pa.table({'src': [0, 1], 'dst': [1, 0]}), dataset/'edges.parquet')
    pq.write_table(pa.table({'id': [0, 1], 'pagerank': [.5, .5]}), dataset/'reference.parquet')
    pq.write_table(pa.table({'id': [0, 1], 'score': pa.array(scores, type=pa.float64()),
                   'iterations': pa.array(iterations, type=pa.int64()),
                   'converged': pa.array(converged, type=pa.bool_()),
                   'residual': pa.array(residual, type=pa.float64())}), output/'part.parquet',
                   # These are validator controls. The separately retained
                   # Parquet statistics control demonstrates that an older
                   # reader can replace a NaN with the finite min=max value.
                   # Keep these malformed values in the scanned payload.
                   write_statistics=False)
    if any(value is not None and math.isnan(value) for value in scores):
        observed = spark.read.parquet(output.as_uri()).select('id', 'score').orderBy('id').collect()
        assert math.isnan(observed[0].score), 'NaN fixture did not reach the validator intact'
    return validate(spark, output, dataset, 'pagerank', 2, 1e-8, .85, 100,
                    native, optimized, policy=policy)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
@pytest.mark.parametrize('change', [
    {'converged': [True, None]}, {'iterations': [1, None]},
    {'iterations': [-1, -1]}, {'iterations': [101, 101]},
])
def test_invalid_metadata_is_checked_on_every_row(spark, tmp_path, policy, change):
    with pytest.raises(AssertionError, match='invalid PageRank metadata rows'):
        check(spark, tmp_path, policy, **change)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
def test_iterations_describe_one_completed_run(spark, tmp_path, policy):
    with pytest.raises(AssertionError):
        check(spark, tmp_path, policy, iterations=[1, 2])


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
@pytest.mark.parametrize('flags', [{'native': True}, {'optimized': True}])
@pytest.mark.parametrize('residual', [[0., None], [-1., -1.], [float('nan'), 0.], [float('inf'), 0.]])
def test_required_residuals_are_finite_and_present_on_every_row(spark, tmp_path, policy, flags, residual):
    with pytest.raises(AssertionError, match='invalid native residual rows'):
        check(spark, tmp_path, policy, residual=residual, **flags)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
@pytest.mark.parametrize('residual', [[0., 1e-10], [1e-4, 1e-4]])
def test_reported_residual_is_uniform_and_within_tolerance(spark, tmp_path, policy, residual):
    with pytest.raises(AssertionError):
        check(spark, tmp_path, policy, residual=residual, native=True)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
@pytest.mark.parametrize('scores', [[None, .5], [float('nan'), .5], [float('inf'), .5], [-.5, 1.5]])
def test_invalid_scores_remain_rejected(spark, tmp_path, policy, scores):
    with pytest.raises(AssertionError, match='invalid PageRank scores'):
        check(spark, tmp_path, policy, scores=scores)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
def test_nonconvergence_remains_distinct_from_mismatch(spark, tmp_path, policy):
    # A finite residual above tolerance is expected when the cap is exhausted.
    # Check this outcome before requiring the converged residual bound.
    with pytest.raises(NonConvergedError):
        check(spark, tmp_path, policy, converged=[False, False], residual=[1., 1.], native=True)


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
@pytest.mark.parametrize('change', [{}, {'native': True}, {'optimized': True, 'iterations': [0, 0]},
                                    {'residual': [None, None]}])
def test_valid_fixed_point_and_optional_baseline_residual(spark, tmp_path, policy, change):
    result = check(spark, tmp_path, policy, **change)
    assert result['rows'] == result['unique_ids'] == 2
    assert result['true_fixed_point_residual'] == 0


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
def test_baseline_iterations_start_at_one(spark, tmp_path, policy):
    with pytest.raises(AssertionError, match='invalid PageRank metadata rows'):
        check(spark, tmp_path, policy, iterations=[0, 0])


@pytest.mark.parametrize('policy', ['reference', 'certificate'])
def test_metadata_validation_does_not_replace_the_score_proof(spark, tmp_path, policy):
    with pytest.raises(AssertionError):
        check(spark, tmp_path, policy, scores=[.4, .6])

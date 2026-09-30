"""Row and metadata checks shared by PageRank's two verification policies."""
from pyspark.sql.connect import functions as F


def validate_pagerank_rows(actual, *, max_iterations, native, optimized):
    """Check every row before nullable aggregate reductions can hide bad metadata.

    Keep the baseline's optional residual convention. Native and optimized
    outputs must report a finite, nonnegative residual on every row.
    Scores, iteration bounds and convergence flags are checked for every path.
    One aggregate replaces the reference path's separate invalid-row scans.
    """
    invalid_score = (F.col('score').isNull() | F.isnan('score') |
                     (F.abs(F.col('score')) == F.lit(float('inf'))) | (F.col('score') < 0))
    invalid_meta = (F.col('iterations').isNull() | F.col('converged').isNull() |
                    (F.col('iterations') < (0 if optimized else 1)) |
                    (F.col('iterations') > max_iterations))
    fields = [
        F.sum(invalid_score.cast('long')).alias('invalid_scores'),
        F.sum(invalid_meta.cast('long')).alias('invalid_metadata'),
    ]
    if native or optimized:
        invalid_residual = (F.col('residual').isNull() | F.isnan('residual') |
                            (F.col('residual') < 0) |
                            (F.col('residual') == F.lit(float('inf'))))
        fields.append(F.sum(invalid_residual.cast('long')).alias('invalid_residuals'))
    checks = actual.agg(*fields).first().asDict()
    assert checks['invalid_scores'] == 0, f"{checks['invalid_scores']} invalid PageRank scores"
    assert checks['invalid_metadata'] == 0, f"{checks['invalid_metadata']} invalid PageRank metadata rows"
    if native or optimized:
        assert checks['invalid_residuals'] == 0, f"{checks['invalid_residuals']} invalid native residual rows"

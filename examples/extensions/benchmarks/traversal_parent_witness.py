"""Optional parent/hop proof of the existing tight-edge reachability condition.

This only selects a witness implementation. It never substitutes for the
caller's existing parent-output validation or the certificate's all-edge checks.
An unproved or over-cap parent path retains the original BFS fallback.
"""
from pyspark.sql.connect import functions as F
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType


def parent_witness_depth(actual, tight, *, source, vertices, max_rounds):
    if not {'parent', 'hops'} <= set(actual.columns):
        return None
    integral = (ByteType, ShortType, IntegerType, LongType)
    schema = actual.schema
    if (not isinstance(schema['hops'].dataType, integral)
            or not isinstance(schema['id'].dataType, integral)
            or schema['parent'].dataType != schema['id'].dataType):
        return None
    reached = F.col('distance').isNotNull()
    root = F.col('id') == source
    # Explicit null tests are necessary: SQL UNKNOWN is not an invalid row.
    invalid = (~reached & (F.col('parent').isNotNull() | F.col('hops').isNotNull())) | (
        reached & (F.col('parent').isNull() | F.col('hops').isNull()
                   | (F.col('hops') < 0) | (F.col('hops') >= vertices)
                   | (root & ((F.col('parent') != source) | (F.col('hops') != 0)))
                   | (~root & (F.col('hops') < 1))))
    bounds = actual.agg(F.count(F.when(invalid, 1)).alias('invalid'),
                        F.max('hops').alias('maximum')).first()
    if bounds.invalid or bounds.maximum is None or bounds.maximum > max_rounds:
        return None
    children = actual.where(reached & ~root).select('id', 'parent', 'hops')
    parents = actual.where(reached).select(F.col('id').alias('parent_id'),
                                           F.col('hops').alias('parent_hops'))
    linked = children.join(parents, children.parent == parents.parent_id, 'left')
    if linked.where(F.col('parent_id').isNull() | F.col('parent_hops').isNull()
                    | (F.col('hops') != F.col('parent_hops') + 1)).limit(1).count():
        return None
    # Use the certificate's exact candidate/allowed expression, already used
    # to build tight. Do not regroup floating arithmetic or use parent tolerance.
    if children.join(tight, (children.parent == tight.src) & (children.id == tight.dst),
                     'left_anti').limit(1).count():
        return None
    # Positive integral hops strictly decrease to the sole zero-hop root.
    # The supplied path can be deeper than BFS; only use it within the same cap.
    return int(bounds.maximum)

"""Fused closed-neighborhood representative planning for randomized WCC.

Forward/reverse edge projections feed one aggregation, following graphframes-rs
PR 56. Unlike its affine representative IDs, Pecan keeps original IDs: min_by
selects the neighbor ID alongside its minimum GF64 priority. For nonzero a,
priorities are a permutation, so equal priorities can only name the same ID.
Duplicate edges therefore cannot introduce an ambiguous min_by tie.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.connect import functions as F

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def representatives(edges: DataFrame, a: int, b: int) -> DataFrame:
    def priority(column: str) -> Column:
        return F.call_function("gf_axpb", F.lit(a).cast("long"), F.col(column), F.lit(b).cast("long"))

    forward = edges.select(F.col("src").alias("vertex"), F.col("dst").alias("neighbor"),
                           priority("dst").alias("priority"))
    reverse = edges.select(F.col("dst").alias("vertex"), F.col("src").alias("neighbor"),
                           priority("src").alias("priority"))
    minima = forward.unionByName(reverse).groupBy("vertex").agg(
        F.min_by("neighbor", "priority").alias("neighbor"),
        F.min("priority").alias("neighbor_priority"))
    return minima.select(F.col("vertex").alias("id"),
                         F.when(priority("vertex") < F.col("neighbor_priority"), F.col("vertex"))
                          .otherwise(F.col("neighbor")).alias("representative"))

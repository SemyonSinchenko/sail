"""Distributed shortest-path certificate without a precomputed full-vector oracle.

Triangle inequalities bound distances from below. Reachability from the source
through tight edges supplies a path witness and bounds them from above. The
second condition is essential: zero-weight disconnected cycles can otherwise
claim distance zero and satisfy every local inequality. Validation is outside
the measured call and may itself require many distributed rounds.
"""
import math
from pyspark.sql.connect import functions as F
from pyspark_pecan import GraphAlgorithms
from pyspark_pecan.types import WccOptions
from pyspark_pecan.algorithms import _check_input_schema
from traversal_parent_witness import parent_witness_depth


def certify(spark, actual, vertices, edges, *, source, weighted, directed=True,
            partitions=4, max_rounds=10000, tolerance=0.0):
    if not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError('certificate tolerance must be finite and nonnegative')
    if not weighted and tolerance != 0:
        raise ValueError('BFS certificate uses exact integer distances')
    if isinstance(max_rounds,bool) or not isinstance(max_rounds,int) or max_rounds < 1:
        raise ValueError('certificate max_rounds must be positive')
    count=vertices.count()
    cardinality=actual.agg(F.count('*').alias('rows'),F.countDistinct('id').alias('unique')).first().asDict()
    assert cardinality==dict(rows=count,unique=count),'output cardinality differs'
    assert not actual.join(vertices,'id','left_anti').limit(1).count(),'output vertex set differs'
    reached=actual.where(F.col('distance').isNotNull()).select('id','distance')
    assert not reached.where(F.isnan('distance') | (F.abs('distance')==float('inf')) |
                             (F.col('distance')<0)).limit(1).count(),'invalid distance'
    if not weighted:
        assert not reached.where(F.col('distance')!=F.floor('distance')).limit(1).count(),'fractional BFS distance'
    root=reached.where(F.col('id')==source).first()
    assert root is not None and root.distance==0.,'source distance must be zero'
    for endpoint in ('src','dst'):
        assert not edges.where(F.col(endpoint).isNull()).limit(1).count(), 'null endpoint'
        assert not edges.join(vertices,edges[endpoint]==vertices.id,'left_anti').limit(1).count(), 'unknown endpoint'
    if not weighted:
        edges=edges.select('src','dst').withColumn('weight',F.lit(1.))
    assert not edges.where(F.col('weight').isNull() | F.isnan('weight') |
        (F.col('weight')<0) | (F.col('weight')==float('inf'))).limit(1).count(),'invalid weight'
    if not directed:
        edges=edges.unionByName(edges.select(F.col('dst').alias('src'),F.col('src').alias('dst'),'weight'))
    src=reached.select(F.col('id').alias('src'),F.col('distance').alias('source_distance'))
    dst=actual.select(F.col('id').alias('dst'),F.col('distance').alias('target_distance'))
    links=edges.join(src,'src').join(dst,'dst','left').withColumn(
        'candidate',F.col('source_distance')+F.col('weight'))
    # Independently verify the finite-path-sum precondition outside the algorithm timer.
    assert not links.where(F.col('candidate')==float('inf')).limit(1).count(),'distance overflow'
    assert not links.where(F.col('target_distance').isNull()).limit(1).count(),'reachable vertex reported unreachable'
    links=links.withColumn('allowed',F.lit(tolerance)+
        F.lit(tolerance)*F.abs('source_distance')+F.lit(tolerance)*F.abs('weight')+
        F.lit(tolerance)*F.abs('target_distance'))
    limits=links.agg(F.max('allowed').alias('slack'),F.max('candidate').alias('largest_sum')).first()
    slack=limits.slack or 0.
    assert math.isfinite(slack),'certificate tolerance overflow'
    assert not links.where(F.col('target_distance')-F.col('candidate')>F.col('allowed')).limit(1).count(), 'triangle inequality violated'
    tight=links.where(F.abs(F.col('target_distance')-F.col('candidate'))<=F.col('allowed')).select('src','dst')
    wanted=reached.count()
    # These preconditions previously ran in _run even for a zero-round witness.
    # Share its validators so choosing a parent witness cannot widen inputs.
    WccOptions(partitions=partitions)
    _check_input_schema(spark,vertices,tight)
    parent_depth=parent_witness_depth(actual,tight,source=source,vertices=count,max_rounds=max_rounds)

    def witness(run, checked_vertices, checked_edges, size):
        path,seen=run.materialize(checked_vertices.where(F.col('id')==source))
        frontier_path,frontier=path,seen
        covered=1
        rounds=0
        while covered < wanted:
            if rounds >= max_rounds:
                raise RuntimeError('certificate reachability round limit exhausted')
            candidates=checked_edges.join(frontier,checked_edges.src==frontier.id).select(
                F.col('dst').alias('id')).distinct().join(seen,'id','left_anti')
            next_frontier_path,next_frontier=run.materialize(candidates)
            added=next_frontier.count()
            assert added, 'finite distance has no source-rooted tight-edge witness'
            next_path,next_seen=run.materialize(seen.unionByName(next_frontier))
            for obsolete in {path,frontier_path}:
                run.remove(obsolete)
            path,seen=next_path,next_seen
            frontier_path,frontier=next_frontier_path,next_frontier
            covered+=added
            rounds+=1
        assert covered==wanted
        run.close()
        return rounds

    if parent_depth is None:
        rounds=GraphAlgorithms(spark)._run(vertices,tight,partitions,None,witness,count_vertices=False)
        witness_method='tight_edge_bfs'
    else:
        rounds=None  # No BFS was run; parent depth is an upper bound, not BFS rounds.
        witness_method='parent_hops'
    # Every simple path has at most V-1 edges. Each local inequality/witness
    # admits at most slack plus one rounded addition; report the accumulated
    # conservative bound, not a full-vector relative-error claim.
    rounding=math.ulp(limits.largest_sum or 0.) if weighted else 0.
    bound=(count-1)*(slack+rounding)
    assert math.isfinite(bound),'certificate error bound overflow'
    return dict(**cardinality,reached=wanted,certificate='all-edge inequalities and rooted tight-edge reachability',
                witness_rounds=rounds,witness_method=witness_method,
                parent_witness_max_hops=parent_depth,relative_edge_tolerance=tolerance,
                max_edge_slack=slack,conservative_absolute_distance_error_bound=bound,
                reference='distributed certificate; no precomputed reference vector')

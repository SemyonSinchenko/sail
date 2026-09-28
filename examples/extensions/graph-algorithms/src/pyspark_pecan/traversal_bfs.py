"""Push/pull relational BFS; pull joins do not promise adjacency early exit."""
from pyspark.sql.connect import functions as F
from .algorithms import ConvergenceError


def execute(graph,run,vertices,adjacency,size,source,limit,alpha=14.,beta=24.):
    degrees=adjacency.groupBy('src').count().select(F.col('src').alias('id'),F.col('count').alias('degree'))
    _,degrees=run.materialize(degrees)
    path,reached=run.materialize(vertices.where(F.col('id')==source).select(
        'id',F.lit(0.).alias('distance'),F.lit(0).cast('long').alias('hops'),F.col('id').alias('parent')))
    frontier_path,frontier=path,reached
    remaining=adjacency.count()
    pull=False
    just_left_pull=False
    for level in range(1,limit+1):
        run.cancellation.check()
        volume=frontier.join(degrees,'id').agg(F.sum('degree')).first()[0] or 0
        remaining=max(0,remaining-volume)
        if not pull and not just_left_pull and volume>remaining/alpha:
            pull=True
        just_left_pull=False
        unvisited=vertices.join(reached.select('id'),'id','left_anti')
        if pull:
            # Restrict destination rows before testing frontier membership.
            # Unlike native BFS, this ordinary relational plan cannot stop an
            # adjacency scan immediately on finding its first parent.
            candidates=adjacency.join(unvisited,adjacency.dst==unvisited.id).select('src','dst')
            candidates=candidates.join(frontier.select(F.col('id').alias('active')),
                                       F.col('src')==F.col('active'),'left_semi')
        else:
            candidates=adjacency.join(frontier,adjacency.src==frontier.id).select('src','dst')
            candidates=candidates.join(unvisited,candidates.dst==unvisited.id,'left_semi')
        next_frontier_path,next_frontier=run.materialize(candidates.groupBy('dst').agg(
            F.min('src').alias('parent')).select(F.col('dst').alias('id'),
                F.lit(float(level)).alias('distance'),F.lit(level).cast('long').alias('hops'),'parent'))
        count=next_frontier.count()
        graph._observe(run,'bfs-push-pull',level,'iteration_end',direction='pull' if pull else 'push',
                       frontier_edges=volume,discovered=count,pull_early_exit=False)
        if not count:
            output_path,output=run.materialize(vertices.join(reached,'id','left'),expected_rows=size)
            return run.finish(output_path,output,algorithm='bfs-push-pull',iterations=level,converged=True)
        next_path,next_reached=run.materialize(reached.unionByName(next_frontier))
        for obsolete in {path,frontier_path}:
            run.remove(obsolete)
        path,reached=next_path,next_reached
        frontier_path,frontier=next_frontier_path,next_frontier
        if pull and count<size/beta:
            pull=False
            just_left_pull=True
    raise ConvergenceError(f'bfs-push-pull did not converge in {limit} iterations')

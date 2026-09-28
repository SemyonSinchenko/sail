"""Version-2 phase schedule over the shared bounded lazy-view lifecycle."""
from contextlib import contextmanager

from argentea_delta_client import ArgenteaDeltaRelation, _request
from argentea_views import compose_views


@contextmanager
def compose_plan(spark, vertices, edges, *, cancellation, **options):
    request = _request(**options)
    phases = [('init',0)]
    for phase in range(2*request['max_pushes']+1):
        phases.extend([('decide',phase),('apply',phase)])
    phases.append(('result',2*request['max_pushes']+1))
    with compose_views(spark,vertices,edges,request=request,phases=phases,
                       relation_type=ArgenteaDeltaRelation,cancellation=cancellation) as plan:
        yield plan

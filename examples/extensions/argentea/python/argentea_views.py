"""Bounded lazy session views for one Argentea native query.

Each registration contains one extension envelope and shallow table references.
Sail resolves/stores its logical plan; native execution starts only when the
terminal relation is acted on. This uses the ordinary view API and unchanged
wire guard, with at most 32 registered views per composition.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import sys
import uuid

from pyspark.sql.connect.dataframe import DataFrame


@dataclass(frozen=True)
class ViewPlan:
    frame: DataFrame
    request: dict
    registrations: tuple


def _cleanup_details(error, errors, uncertain):
    if errors:
        error.view_cleanup_errors = errors
    if uncertain:
        error.uncertain_view_names = tuple(uncertain)
    error.view_cleanup_deferred = bool(errors or uncertain)


@contextmanager
def compose_views(spark, vertices, edges, *, request, phases, relation_type, cancellation):
    """Keep all confirmed views alive until the caller's terminal action ends.

    Unique aliases use createTempView, never replacement. Drop only confirmed
    creations: a failed create could name an existing view or could have reached
    the server without an acknowledgement. Its alias is exposed on the original
    error as uncertain_view_names and left to session cleanup. Failed drops also
    expose view_cleanup_deferred. Cleanup attempts every confirmed alias and
    preserves an existing registration/execution/cancellation error.
    """
    phases = tuple(phases)
    if not 1 <= len(phases) <= 32 or phases[0] != ('init', 0):
        raise ValueError('view composition requires 1..32 phases beginning with init0')
    prefix = 'argentea_phase_'+uuid.uuid4().hex
    owned, registrations, uncertain = [], [], []

    def register(verb, phase, inputs):
        cancellation.check()
        frame = DataFrame(relation_type(dict(request,verb=verb,phase=phase),inputs),spark)
        alias = f'{prefix}_{len(registrations):02d}_{verb}_{phase}'
        data = frame._plan.to_proto(spark.client).SerializeToString()
        try:
            frame.createTempView(alias)
        except BaseException:
            # Without a successful acknowledgement, deletion would not be an
            # ownership proof. Session teardown also handles a lost create reply.
            uncertain.append(alias)
            raise
        owned.append(alias)
        registrations.append(dict(alias=alias,verb=verb,phase=phase,plan_bytes=data,
                                  plan_size=len(data),plan_sha256=hashlib.sha256(data).hexdigest()))
        cancellation.check()
        return spark.table(alias)

    try:
        frame = register(*phases[0],(vertices,edges))
        for verb,phase in phases[1:]:
            frame = register(verb,phase,(frame,))
        yield ViewPlan(frame,request,tuple(registrations))
    finally:
        active_error = sys.exc_info()[1]
        errors = []
        for alias in reversed(owned):
            try:
                spark.catalog.dropTempView(alias)
            except BaseException as error:
                errors.append(dict(alias=alias,error=repr(error)))
        if active_error is not None:
            _cleanup_details(active_error,errors,uncertain)
        elif errors or uncertain:
            error = RuntimeError('Argentea temporary view cleanup is deferred to session teardown')
            _cleanup_details(error,errors,uncertain)
            raise error

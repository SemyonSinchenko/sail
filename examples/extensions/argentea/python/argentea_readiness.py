"""Qualification-only readiness; graph placement is audited separately afterwards."""
import math
import time


WORKERS_SQL = '''
    SELECT CAST(worker_id AS BIGINT) AS worker_id, host,
           CAST(port AS INT) AS port, status
    FROM system.cluster.workers
'''


def wait_for_workers(spark, *, evidence, minimum=2, timeout=90.0):
    """Wait in the graph's own session, retaining every observed state change.

    A listening driver is not proof that its asynchronously launched workers
    registered. The tiny ordinary query starts that session's worker pool.
    This does not impose a worker-count requirement on the production client,
    nor replace the later native owner/physical-host/cross-host-edge audit.
    """
    if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 1:
        raise ValueError('minimum must be a positive integer')
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('timeout must be positive and finite')
    started = time.monotonic()
    readiness = evidence['worker_readiness'] = dict(
        minimum=minimum, timeout_seconds=timeout, outcome='starting', snapshots=[])
    try:
        # The Spark Connect retry policy belongs to the caller. Do not hide a
        # failed query by retrying it here or create a different session.
        assert spark.range(0, 1, numPartitions=1).count() == 1
        while True:
            rows = [row.asDict() for row in spark.sql(WORKERS_SQL).collect()]
            rows.sort(key=lambda row: row['worker_id'])
            elapsed = time.monotonic() - started
            snapshots = readiness['snapshots']
            if not snapshots or snapshots[-1]['workers'] != rows:
                snapshots.append(dict(elapsed_seconds=elapsed, workers=rows))
            running = [row for row in rows
                       if row['status'] == 'RUNNING' and row['host']
                       and isinstance(row['port'], int) and 0 < row['port'] <= 65535]
            ids = {row['worker_id'] for row in running}
            endpoints = {(row['host'], row['port']) for row in running}
            if len(ids) >= minimum and len(endpoints) >= minimum and elapsed < timeout:
                readiness.update(outcome='ready', elapsed_seconds=elapsed)
                return rows
            if elapsed >= timeout:
                raise TimeoutError(f'{minimum} distinct Sail workers did not become ready')
            time.sleep(min(0.1, timeout - elapsed))
    except BaseException as error:
        readiness.update(outcome='failed', error=repr(error),
                         elapsed_seconds=time.monotonic() - started)
        raise

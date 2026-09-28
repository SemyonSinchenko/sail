"""Independent small-graph reference and strict native execution receipts."""
from collections import defaultdict
import json
import math
import re


def pagerank_reference(ids, edges, iterations, damping):
    adjacency = {node: [] for node in ids}
    for source, target in edges:
        if target not in adjacency:
            raise ValueError('unknown destination')
        adjacency[source].append(target)
    ranks = {node: 1.0 / len(ids) for node in ids}
    for _ in range(iterations):
        dangling = math.fsum(ranks[node] for node in ids if not adjacency[node])
        next_ranks = {node: (1-damping + damping*dangling)/len(ids) for node in ids}
        for source, targets in adjacency.items():
            for target in targets:
                next_ranks[target] += damping*ranks[source]/len(targets)
        ranks = next_ranks
    return ranks


def validate_rows(rows, ids, edges, request, iterations):
    expected = pagerank_reference(ids, edges, iterations, request['damping'])
    actual = {}
    for row in rows:
        node = row['id']
        assert node not in actual, 'duplicate output vertex'
        actual[node] = row
    assert actual.keys() == expected.keys(), 'output vertex set differs'
    for node, rank in expected.items():
        observed = actual[node]
        assert math.isfinite(observed['pagerank']) and observed['pagerank'] >= 0
        assert math.isclose(observed['pagerank'], rank, rel_tol=1e-11, abs_tol=1e-13), (node, observed, rank)
        assert observed['owner'] == node % request['partitions'], 'owner mapping differs from pmod'
        assert observed['round'] == iterations-1, 'wrong final round'
    assert math.isclose(math.fsum(row['pagerank'] for row in rows), 1.0, abs_tol=1e-12, rel_tol=0)
    return dict(vertices=len(actual), fixed_iterations=iterations, converged=None,
                reference='independent Python full-power PageRank', normalized=True)


def parse_log(text):
    receipts, supervisors = [], []
    for line in text.splitlines():
        if 'ARGENTEA_RECEIPT ' in line:
            receipts.append(json.loads(line.split('ARGENTEA_RECEIPT ', 1)[1]))
        elif line.startswith('{'):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get('event') == 'sail_remote_started':
                supervisors.append(record)
    return receipts, supervisors


def parse_worker_tasks(text):
    """Retain failed/running attempts too, so a later success cannot hide retry."""
    pattern = re.compile(r'worker_task_status worker_id=(\d+) job_id=(\d+) stage=(\d+) '
                         r'partition=(\d+) attempt=(\d+) status=([A-Z_]+)\b')
    fields = ('worker_id', 'job_id', 'stage', 'partition', 'attempt')
    tasks = []
    for line in text.splitlines():
        if 'worker_task_status ' not in line:
            continue
        match = pattern.search(line)
        assert match is not None, 'malformed worker task status receipt'
        tasks.append(dict(zip(fields, map(int, match.groups()[:5])), status=match[6]))
    return tasks


def validate_native_stages(stages, task_statuses, *, session, job, owners, iterations, partitions):
    job_stages = [stage for stage in stages if stage.get('session_id') == session and stage['job_id'] == job]
    assert job_stages, 'native job has no retained stage inventory'
    assert all('slot_group' in stage and 'mode' in stage for stage in job_stages), 'stage group/mode evidence is missing'
    assert len({stage['stage'] for stage in job_stages}) == len(job_stages), 'duplicate native-job stage inventory'
    # The qualifier submits one operation per job. Sail marks precisely those
    # stages containing WorkerExtensionExec; ordinary scans/sinks may have P=1.
    native = [stage for stage in job_stages if stage['slot_group'].startswith('worker-extension:')]
    ordinary = [stage for stage in job_stages if stage not in native]
    assert len(native) == iterations + 1, 'native init/round/result stage count differs'
    assert len({stage['slot_group'] for stage in native}) == 1, 'native phases use different slot-sharing groups'
    assert all(stage['partitions'] == partitions for stage in native), 'native stage has wrong owner width'
    assert all(stage['placement'] == 'Worker' for stage in native), 'native stage is not placed on workers'
    assert all(stage['mode'] == 'Pipelined' for stage in native), 'native stage is not pipelined'
    native_ids = {stage['stage'] for stage in native}
    selected = [task for task in task_statuses if task['job_id'] == job and task['stage'] in native_ids]
    grouped = defaultdict(list)
    for task in selected:
        assert task['partition'] in owners, 'native task has unexpected owner partition'
        assert task['attempt'] == 0, 'native task was retried'
        assert task['worker_id'] == owners[task['partition']]['worker_id'], 'native task ran on the wrong owner worker'
        assert task['status'] in ('RUNNING', 'SUCCEEDED'), 'native task reported an unsuccessful status'
        grouped[(task['stage'], task['partition'])].append(task)
    expected = {(stage, partition) for stage in native_ids for partition in range(partitions)}
    assert grouped.keys() == expected, 'native stage/owner task receipts are missing'
    for key, tasks in grouped.items():
        assert sum(task['status'] == 'SUCCEEDED' for task in tasks) == 1, f'native task {key} did not succeed exactly once'
    return native, ordinary, selected


def validate_audit(records, rows, request, iterations, *, minimum_workers=2,
                   worker_endpoints=(), supervisors=(), required_hosts=(), stages=(), task_statuses=()):
    records = [record for record in records if record.get('operation_id') == request['operation_id']]
    assert records, 'no native execution receipts for the operation'
    assert len({(record['session_id'], record['job_id']) for record in records}) == 1, 'native rounds spanned multiple jobs'
    for key in ('snapshot_id', 'generation'):
        assert all(record[key] == request[key] for record in records), f'{key} changed'
    grouped = defaultdict(list)
    for record in records:
        grouped[record['partition']].append(record)
    assert set(grouped) == set(range(request['partitions'])), 'missing or unexpected owner, including empty partitions'
    owners = {}
    for partition, records in grouped.items():
        identity = {(r['worker_id'], r['pid'], r['adjacency_id']) for r in records}
        assert len(identity) == 1, f'owner {partition} moved or rebuilt adjacency'
        worker, pid, adjacency = next(iter(identity))
        owners[partition] = dict(worker_id=worker, pid=pid, adjacency_id=adjacency)
        events = defaultdict(list)
        for record in records:
            events[record['event']].append(record)
        assert len(events['init']) == 1, f'owner {partition} was not built exactly once'
        assert len(events['result']) == 1 and events['result'][0]['round'] == iterations-1
        assert len(events['close']) == 1, f'owner {partition} did not close exactly once'
        for kind in ('consume', 'emit'):
            assert sorted(record['round'] for record in events[kind]) == list(range(iterations)), f'{kind} rounds incomplete/replayed'
        for row in rows:
            if row['owner'] == partition:
                assert (row['worker_id'], row['pid'], row['adjacency_id']) == (worker, pid, adjacency), 'result provenance differs from execution receipts'
    workers = {owner['worker_id'] for owner in owners.values()}
    pids = {owner['pid'] for owner in owners.values()}
    assert len(workers) >= minimum_workers and len(pids) >= minimum_workers, 'native kernel did not execute on enough worker processes'
    endpoints = {row['worker_id']: row for row in worker_endpoints}
    assert workers <= endpoints.keys(), 'native worker is absent from Sail worker inventory'
    job, session = records[0]['job_id'], records[0]['session_id']
    native_stages, ordinary_stages, native_tasks = validate_native_stages(stages, task_statuses,
        session=session, job=job, owners=owners, iterations=iterations, partitions=request['partitions'])
    host_proof = []
    if required_hosts:
        started = {(int(record['worker_id']), record['pid']): record['hostname'] for record in supervisors if record.get('worker_id') is not None}
        for owner in owners.values():
            host = started.get((owner['worker_id'], owner['pid']))
            assert host is not None, 'native PID has no supervised host identity'
            host_proof.append(host)
        assert set(required_hosts) <= set(host_proof), 'native execution did not cover every required physical host'
    return dict(session_id=session, job_id=job, owners=owners, native_workers=sorted(workers), native_pids=sorted(pids),
                hosts=sorted(set(host_proof)), stages=native_stages, ordinary_stages=ordinary_stages,
                native_task_statuses=native_tasks, native_phases=iterations+1,
                same_adjacency_across_rounds=True, empty_owners_checked=True, closed_all_owners=True)

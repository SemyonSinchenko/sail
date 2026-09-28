"""Independent small-graph reference and strict native execution receipts."""
from collections import defaultdict
import json
import math


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


def validate_audit(records, rows, request, iterations, *, minimum_workers=2,
                   worker_endpoints=(), supervisors=(), required_hosts=(), stages=()):
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
    job = records[0]['job_id']
    native_stages = [stage for stage in stages if stage['job_id'] == job and stage['placement'] == 'Worker']
    assert native_stages and all(stage['partitions'] == request['partitions'] for stage in native_stages), 'native job contains missing or unequal-width worker stages'
    host_proof = []
    if required_hosts:
        started = {(int(record['worker_id']), record['pid']): record['hostname'] for record in supervisors if record.get('worker_id') is not None}
        for owner in owners.values():
            host = started.get((owner['worker_id'], owner['pid']))
            assert host is not None, 'native PID has no supervised host identity'
            host_proof.append(host)
        assert set(required_hosts) <= set(host_proof), 'native execution did not cover every required physical host'
    return dict(job_id=job, owners=owners, native_workers=sorted(workers), native_pids=sorted(pids),
                hosts=sorted(set(host_proof)), stages=native_stages,
                same_adjacency_across_rounds=True, empty_owners_checked=True, closed_all_owners=True)

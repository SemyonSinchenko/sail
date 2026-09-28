"""Independent traversal oracles and full-vector/parent validation for small gates.

This intentionally uses Python adjacency lists and heap Dijkstra. It is a
correctness oracle, not a timed contender or a billion-edge preparation path.
"""
from collections import deque
import heapq
import math


def distances(ids, edges, source, *, weighted, directed=True):
    adjacency = {node: [] for node in ids}
    if source not in adjacency:
        raise ValueError('source missing')
    for a,b,w in edges:
        if a not in adjacency or b not in adjacency:
            raise ValueError('unknown endpoint')
        if not math.isfinite(w) or w < 0:
            raise ValueError('weights must be finite and nonnegative')
        adjacency[a].append((b, w if weighted else 1.))
        if not directed:
            adjacency[b].append((a, w if weighted else 1.))
    result = dict.fromkeys(adjacency, None)
    result[source] = 0.
    if not weighted:
        queue = deque([source])
        while queue:
            node = queue.popleft()
            for target,_ in adjacency[node]:
                if result[target] is None:
                    result[target] = result[node] + 1
                    queue.append(target)
    else:
        queue = [(0.,source)]
        while queue:
            cost,node = heapq.heappop(queue)
            if cost != result[node]:
                continue
            for target,weight in adjacency[node]:
                candidate = cost + weight
                if not math.isfinite(candidate):
                    raise OverflowError('distance overflow')
                if result[target] is None or candidate < result[target]:
                    result[target] = candidate
                    heapq.heappush(queue, (candidate,target))
    return result


def validate_rows(rows, expected, edges, source, *, weighted, directed=True, parents=True):
    actual = {}
    for row in rows:
        node = row['id']
        assert node not in actual, f'duplicate output vertex {node}'
        actual[node] = row
    assert actual.keys() == expected.keys(), 'output vertex set differs'
    links = {}
    for a,b,w in edges:
        w = w if weighted else 1.
        links.setdefault((a,b),set()).add(w)
        if not directed:
            links.setdefault((b,a),set()).add(w)
    for node, distance in expected.items():
        row = actual[node]
        if distance is None:
            assert row['distance'] is None, f'unreachable vertex {node}'
            if parents:
                assert row['parent'] is None and row['hops'] is None
            continue
        observed = row['distance']
        assert observed is not None and math.isfinite(observed) and observed >= 0
        assert math.isclose(observed,distance,rel_tol=1e-12,abs_tol=1e-12), (node,observed,distance)
        if parents:
            if node == source:
                assert row['parent'] == source and row['hops'] == 0
            else:
                parent = actual[row['parent']]
                assert parent['distance'] is not None
                assert row['hops'] == parent['hops'] + 1
                assert 0 < row['hops'] < len(expected)
                assert any(math.isclose(parent['distance']+w,observed,rel_tol=1e-12,abs_tol=1e-12)
                           for w in links.get((row['parent'],node),())), 'invalid parent edge'
    return dict(vertices=len(expected),reached=sum(v is not None for v in expected.values()),
                distances='independent BFS/heap-Dijkstra',parent_tree_checked=parents)

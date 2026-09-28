"""Require nonempty native graph work and crossing edges in a two-host proof."""
from collections import Counter


def validate_host_graph(rows, edges, supervisors, *, required_hosts):
    """Map already-validated rank provenance to supervised physical hosts.

    An empty owner still proves protocol participation in validate_audit; it does
    not establish graph data on that host. Parallel directed arcs count separately.
    This tiny-fixture check is functional evidence, not a scaling measurement.
    """
    required = set(required_hosts)
    assert len(required)>=2, 'physical graph proof requires at least two named hosts'
    origins = {}
    for record in supervisors:
        if record.get('event')!='sail_remote_started' or record.get('worker_id') is None:
            continue
        key = (int(record['worker_id']),int(record['pid']))
        host = record['hostname']
        assert isinstance(host,str) and host, 'supervised worker lacks a hostname'
        assert key not in origins or origins[key]==host, 'conflicting supervised worker host identity'
        origins[key] = host
    locations = {}
    for row in rows:
        node = row['id']
        assert node not in locations, 'duplicate validated vertex in host proof'
        key = (row['worker_id'],row['pid'])
        assert key in origins, 'vertex has no supervised worker/PID host identity'
        locations[node] = origins[key]
    vertices = Counter(locations.values())
    assert set(vertices)<=required, 'graph vertex ran on an unexpected physical host'
    assert all(vertices[host]>0 for host in required), 'a required host has only empty owners or no graph vertices'
    outgoing = Counter()
    crossing = []
    edge_count = 0
    for source,target in edges:
        assert source in locations and target in locations, 'edge endpoint missing from validated host graph'
        source_host,target_host = locations[source],locations[target]
        outgoing[source_host] += 1
        edge_count += 1
        if source_host!=target_host:
            crossing.append(dict(src=source,dst=target,source_host=source_host,target_host=target_host))
    assert crossing, 'fixture has no graph edge crossing between physical hosts'
    return dict(vertices=len(locations),directed_edges=edge_count,
                vertices_by_host={host:vertices[host] for host in sorted(required)},
                source_edges_by_host={host:outgoing[host] for host in sorted(required)},
                cross_host_edge_count=len(crossing),cross_host_edges=crossing,
                nonempty_vertices_on_every_host=True,
                boundary='tiny validated graph and supervised physical-host provenance; not a scaling result')

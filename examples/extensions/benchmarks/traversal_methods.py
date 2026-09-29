"""Explicit traversal method mappings; no engine imports needed for planning."""

ARGENTEA = {
    'bfs': {'reference': 'reference', 'frontier': 'frontier', 'push_pull': 'direction'},
    'sssp': {'reference': 'reference', 'delta_star': 'delta_star'},
}


def method(engine, algorithm, variant):
    if engine == 'argentea':
        try:
            return ARGENTEA[algorithm][variant]
        except KeyError as error:
            raise ValueError(f'unsupported traversal method: {engine}/{algorithm}/{variant}') from error
    choices = {
        'bfs': {'reference': ('reference','bfs'), 'frontier': ('frontier','bfs'), 'push_pull': ('push_pull','bfsDirection')},
        'sssp': {'reference': ('reference','bellmanFord'), 'frontier': ('frontier','dijkstra'),
                 'delta_star': ('delta_star','ssspDeltaStar')},
    }
    try:
        selected=choices[algorithm][variant][engine == 'nutmeg-native']
        if selected is None:
            raise KeyError(variant)
        return selected
    except KeyError as error:
        raise ValueError(f'unsupported traversal method: {engine}/{algorithm}/{variant}') from error

"""Import the exact ASCII fixtures used by an external graph benchmark.

The header declares V and E; each subsequent line contains one directed edge.
Conversion preserves row order, duplicate edges, loops and isolated vertices.
The caller must supply a trusted SHA256, not a hash inferred from the input.
"""
from pathlib import Path
import re

import numpy as np


def read_edges(path, expected_sha256, vertices):
    from graph_fixtures import _check_edges, _file_details

    if path is None or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256 or ""):
        raise ValueError("import requires edge_file and a trusted edge_sha256")
    path = Path(path)
    before = _file_details(path)
    if before["sha256"] != expected_sha256:
        raise ValueError("input edge file SHA256 differs from the pinned fixture")
    with path.open("r", encoding="ascii") as stream:
        header = stream.readline().split()
        if len(header) != 2:
            raise ValueError("expected a V E header")
        count, edges = map(int, header)
        if count != vertices or count < 1 or edges < 0:
            raise ValueError("fixture header differs from the requested vertex count")
        rows = np.loadtxt(stream, dtype=np.int64, ndmin=2, comments=None)
    if edges == 0 and rows.size == 0:
        rows = np.empty((0, 2), dtype=np.int64)
    if rows.shape != (edges, 2):
        raise ValueError("edge rows differ from the declared E or have extra columns")
    source, target = rows[:, 0].copy(), rows[:, 1].copy()
    _check_edges(vertices, source, target)
    if _file_details(path) != before:
        raise ValueError("input edge file changed during import")
    return source, target, dict(name=path.name, source_path=str(path), **before, format="ASCII V E; source target",
                                row_order="preserved", declared_vertices=count, declared_edges=edges)

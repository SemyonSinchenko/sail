"""Pin graph identity at the interchange boundary, including invalid inputs."""
import hashlib

import numpy as np
import pyarrow.parquet as pq
import pytest

from graph_fixtures import prepare
from imported_fixture import read_edges


def fixture(tmp_path, text):
    path = tmp_path / "input.edges"
    path.write_text(text)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_preserves_ids_edge_order_duplicates_loops_and_isolates(tmp_path):
    path, sha = fixture(tmp_path, "5 4\n2 1\n1 1\n2 1\n0 2\n")
    manifest = prepare(tmp_path / "out", vertices=5, family="edge-list",
                       edge_file=path, edge_sha256=sha)
    assert manifest["input"]["sha256"] == sha
    assert manifest["counts"]["isolates"] == 2
    edges = pq.read_table(tmp_path / "out/edges.parquet").to_pydict()
    assert edges == {"src": [2, 1, 2, 0], "dst": [1, 1, 1, 2]}
    reference = pq.read_table(tmp_path / "out/reference.parquet").to_pydict()
    assert reference["component"] == [0, 0, 0, 3, 4]
    assert manifest["seed"] is None


@pytest.mark.parametrize("text", ["5 2\n0 1\n", "5 1\n0 5\n", "5 1\n0 1 2\n", "6 1\n0 1\n"])
def test_rejects_incomplete_or_malformed_graph(tmp_path, text):
    path, sha = fixture(tmp_path, text)
    with pytest.raises(ValueError):
        read_edges(path, sha, 5)


def test_rejects_different_bytes_even_for_same_graph(tmp_path):
    path, sha = fixture(tmp_path, "5 1\n0 1\n")
    path.write_text("5 1\n0  1\n")
    with pytest.raises(ValueError, match="SHA256"):
        read_edges(path, sha, 5)


def test_empty_edge_list_retains_vertices(tmp_path):
    path, sha = fixture(tmp_path, "5 0\n")
    source, target, _ = read_edges(path, sha, 5)
    np.testing.assert_array_equal(source, [])
    np.testing.assert_array_equal(target, [])

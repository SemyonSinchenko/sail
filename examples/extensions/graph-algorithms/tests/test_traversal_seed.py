"""The typed singleton uses lazy literals, never local data conversion."""
from types import SimpleNamespace
from typing import Any

import pytest
from pyspark_pecan.traversal_state import initial_state


@pytest.mark.parametrize('source', [-(2**63), -1, 0, 2**63 - 1])
def test_source_seed_is_one_exact_typed_row(source: int) -> None:
    calls = []
    expected = object()
    def select(*columns: Any) -> object:
        calls.append(('select', columns))
        return expected
    def one_row(end: int) -> Any:
        calls.append(('range', end))
        return SimpleNamespace(select=select)
    spark = SimpleNamespace(range=one_row,
        createDataFrame=lambda *args, **kwargs: pytest.fail('unexpected local data conversion'))
    assert initial_state(spark, source) is expected
    assert len(calls) == 2 and calls[0] == ('range', 1)
    assert calls[1][0] == 'select'
    # Serialize the real expressions with a stub client: no RPC or input frame
    # is available, and no createDataFrame/configuration path may be used.
    plans = [column.to_plan(object()) for column in calls[1][1]]
    assert [list(plan.alias.name) for plan in plans] == [['id'], ['distance'], ['hops'], ['parent']]
    for index, value in ((0, source), (2, 0), (3, source)):
        cast = plans[index].alias.expr.cast
        assert cast.type_str == 'long'
        literal = cast.expr.literal
        kind = literal.WhichOneof('literal_type')
        assert kind in ('integer', 'long')
        assert getattr(literal, kind) == value
    distance = plans[1].alias.expr.literal
    assert distance.WhichOneof('literal_type') == 'double'
    assert distance.double == 0.0

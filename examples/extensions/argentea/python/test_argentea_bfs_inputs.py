"""Malformed wire controls never qualify unrelated or unclean failures."""
import pytest
from qualify_bfs_inputs import accept_error, INVALID_FIELD


def test_specific_wire_rejection_with_completed_owned_cleanup() -> None:
    error = RuntimeError(f'unknown field `{INVALID_FIELD}`')
    error.cleanup_deferred = False
    accept_error('unknown-field', error)


@pytest.mark.parametrize('fault', ['cancelled', 'quota', 'wrong_field', 'deferred', 'unrelated_schema'])
def test_unrelated_or_unclean_error_never_passes(fault: str) -> None:
    message = {'cancelled': 'operation cancelled', 'quota': 'native quota exceeded',
               'wrong_field': 'unknown field `other`', 'unrelated_schema': 'unrelated schema problem'}
    error = RuntimeError(message.get(fault, f'unknown field `{INVALID_FIELD}`'))
    error.cleanup_deferred = fault == 'deferred'
    with pytest.raises(AssertionError):
        accept_error('unknown-field', error)


def test_missing_source_is_not_a_supported_rejection_contract() -> None:
    error = ValueError('BFS source must occur exactly once in the vertex snapshot')
    error.cleanup_deferred = False
    with pytest.raises(AssertionError, match='outside the public contract'):
        accept_error('missing-source', error)

"""Negative controls accept the specific pre-execution cause, never arbitrary failure."""
import pytest
from qualify_bfs_inputs import accept_error,INVALID_FIELD


@pytest.mark.parametrize('case',['missing-source','unknown-field'])
def test_specific_input_rejection_with_completed_owned_cleanup(case):
    error=ValueError('BFS source must occur exactly once in the vertex snapshot') if case=='missing-source' else RuntimeError(f'unknown field `{INVALID_FIELD}`')
    error.cleanup_deferred=False
    accept_error(case,error)


@pytest.mark.parametrize('case',['missing-source','unknown-field'])
@pytest.mark.parametrize('fault',['cancelled','quota','wrong_field','deferred','wrong_type'])
def test_unrelated_or_unclean_error_never_passes_as_pre_execution_control(case,fault):
    message='BFS source must occur exactly once in the vertex snapshot' if case=='missing-source' else f'unknown field `{INVALID_FIELD}`'
    if fault=='cancelled':message='operation cancelled'
    if fault=='quota':message='native quota exceeded'
    if fault=='wrong_field':message='unknown field `other`'
    error=(ValueError if case=='missing-source' and fault!='wrong_type' else RuntimeError)(message)
    error.cleanup_deferred=fault=='deferred'
    if fault=='wrong_type' and case=='unknown-field':error=RuntimeError('unrelated schema problem');error.cleanup_deferred=False
    with pytest.raises(AssertionError):accept_error(case,error)

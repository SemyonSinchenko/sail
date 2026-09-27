from summarize import aggregate, cell_row


def trial(outcome, seconds, pss=None):
    cell = dict(cell_id=outcome, sequence=1, repeat=1, suite='sample', dataset='sparse', mode='local',
                algorithm='pagerank', engine='pecan', variant='optimized', expected_outcome='passed')
    summary = dict(outcome=outcome, expected_outcome_observed=outcome == 'passed')
    receipt = dict(end_to_end_seconds=seconds, memory={'phase_peaks': {'execute': {'pss_bytes': pss}}})
    return cell_row(cell, summary, receipt)


def test_failures_remain_counted_but_cannot_improve_summary():
    group, = aggregate([trial('passed', 5, 10), trial('passed', 7, 20), trial('mismatch', 0.01, 1)])
    assert group['outcomes'] == {'passed': 2, 'mismatch': 1}
    assert group['metrics']['seconds'] == dict(samples=2, median=6, minimum=5, maximum=7)
    assert group['metrics']['execution_pss_bytes']['median'] == 15


def test_missing_memory_and_failed_only_groups_are_not_zero():
    group, = aggregate([trial('passed', 5)])
    assert group['metrics']['execution_pss_bytes'] is None
    group, = aggregate([trial('nonconverged', 2)])
    assert group['passed'] == 0
    assert all(metric is None for metric in group['metrics'].values())

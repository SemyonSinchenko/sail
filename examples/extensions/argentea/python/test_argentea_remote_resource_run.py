"""Adapter must audit every owned run and retain a cleanup failure."""
from types import SimpleNamespace
import pytest
import argentea_remote_resource_run as module


@pytest.mark.parametrize('defect', [None, 'missing-run', 'duplicate-run', 'leftovers'])
def test_adapter_audits_all_five_storage_runs(monkeypatch, defect):
    import qualify_graph_resources
    observed = []
    receipt = dict(configuration=dict(driver={}))
    paths = [f's3://bucket/root/run-{i}' for i in range(5)]
    if defect == 'duplicate-run': paths[-1] = paths[0]
    if defect == 'missing-run': paths.pop()
    def exercise(endpoint, args, check, **kwargs):
        assert callable(kwargs['remote_workers'])
        check.update(operations=[dict(run_path=p) for p in paths], cleanup_errors=[])
    def storage(driver, path):
        observed.append(path)
        if defect == 'leftovers': raise AssertionError('owned objects remain')
        return dict(run_path=path, passed=True)
    def launch(args, result, *, exercise_fn, audit_fn, fault_control):
        assert fault_control
        exercise_fn('endpoint', args)
    monkeypatch.setattr(qualify_graph_resources, 'exercise', exercise)
    monkeypatch.setattr(module, 'audit_remote_storage', storage)
    monkeypatch.setattr(module, 'two_hosts', launch)
    args = SimpleNamespace(algorithm='wcc_star')
    if defect:
        with pytest.raises((ValueError, AssertionError)):
            module.run_remote_resources(args, receipt)
        assert 'post_session_storage' not in receipt
    else:
        module.run_remote_resources(args, receipt)
        assert observed == paths
        assert len(receipt['post_session_storage']) == 5

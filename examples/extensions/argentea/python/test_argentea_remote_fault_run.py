"""Read-only remote storage audit must stay inside the owned run prefix."""
import json
import sys
from types import SimpleNamespace

import pytest

from argentea_remote_fault_run import STORAGE_CHECK


@pytest.mark.parametrize('run', ['s3://other/root/run', 's3://bucket/root',
                                 's3://bucket/root/', 's3://bucket/root-other/run',
                                 'file:///root/run'])
def test_storage_audit_rejects_unowned_prefix(tmp_path, monkeypatch, run):
    config = tmp_path/'environment.json'
    config.write_text(json.dumps(dict(SAIL_GRAPH_UTILS_ROOT='s3://bucket/root')))
    monkeypatch.setattr(sys, 'argv', ['audit', str(config), run])
    with pytest.raises(ValueError, match='root'):
        exec(compile(STORAGE_CHECK, '<remote storage audit>', 'exec'), {})


@pytest.mark.parametrize('remaining', [[], ['bucket/root/run/part.parquet']])
def test_storage_audit_reports_remaining_objects(tmp_path, monkeypatch, capsys, remaining):
    import pyarrow.fs as fs
    import time
    config = tmp_path/'environment.json'
    config.write_text(json.dumps(dict(SAIL_GRAPH_UTILS_ROOT='s3://bucket/root',
        AWS_ENDPOINT_URL='http://localhost:9000', AWS_ACCESS_KEY_ID='test',
        AWS_SECRET_ACCESS_KEY='test', AWS_REGION='test')))
    calls = []
    def inspect(selector):
        calls.append(selector)
        return [SimpleNamespace(path=p, type=fs.FileType.File) for p in remaining]
    monkeypatch.setattr(fs, 'S3FileSystem', lambda **_: SimpleNamespace(get_file_info=inspect))
    clock = iter([0, 31, 32])
    monkeypatch.setattr(time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(sys, 'argv', ['audit', str(config), 's3://bucket/root/run'])
    exec(compile(STORAGE_CHECK, '<remote storage audit>', 'exec'), {})
    result = json.loads(capsys.readouterr().out)
    assert result['passed'] == (not remaining)
    assert result['remaining_objects'] == remaining
    assert calls[0].base_dir == 'bucket/root/run'
    assert calls[0].recursive and calls[0].allow_not_found

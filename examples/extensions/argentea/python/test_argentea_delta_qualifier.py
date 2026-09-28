import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).parent))


@pytest.mark.parametrize('custom',[False,True])
def test_two_host_launcher_preserves_default_v1_or_uses_explicit_callbacks(tmp_path,monkeypatch,custom):
    import qualify
    driver=dict(python='/python',repo='/source',sail='/sail',gateway_port=100,advertise='127.0.0.1',connect_port=101)
    workers=[dict(driver,host='a'),dict(driver,host='b')]
    config=tmp_path/'config.json'; config.write_text(json.dumps(dict(driver=driver,workers=workers)))
    args=SimpleNamespace(two_host_config=config,native_quota=100,worker_task_slots=32,partitions=5,
                         sail_pool_bytes=1000,threads=2,output=tmp_path,iterations=2)
    monkeypatch.setattr(qualify,'target_python',lambda target,*a:dict(source_commit='source',source_dirty='',
        binary_sha256='binary',packages={},host=target.get('host','driver')))
    monkeypatch.setattr(qualify,'launch',lambda *a,**kw:SimpleNamespace(returncode=0))
    monkeypatch.setattr(qualify,'stop',lambda process:None)
    monkeypatch.setattr(qualify,'wait_server',lambda *a:None)
    monkeypatch.setattr(qualify,'process_cleanup',lambda *a:{})
    calls=[]
    def old_exercise(endpoint,output,*,iterations,partitions):
        calls.append(('v1',iterations,partitions)); return dict(ok=True)
    def new_exercise(endpoint,settings):
        assert settings is args
        calls.append(('v2',)); return dict(ok=True)
    def old_audit(receipt,log,**kwargs): calls.append(('audit-v1',kwargs['required_hosts']))
    def new_audit(receipt,log,**kwargs): calls.append(('audit-v2',kwargs['required_hosts']))
    monkeypatch.setattr(qualify,'exercise',old_exercise)
    monkeypatch.setattr(qualify,'audit',old_audit)
    receipt={}
    if custom: qualify.two_hosts(args,receipt,exercise_fn=new_exercise,audit_fn=new_audit)
    else: qualify.two_hosts(args,receipt)
    assert calls==([('v2',),('audit-v2',{'a','b'})] if custom else [('v1',2,5),('audit-v1',{'a','b'})])
    assert receipt['checks']=={'ok':True} and receipt['driver_supervisor_returncode']==0


def test_explicit_phase_count_does_not_mislabel_reference_iterations():
    from argentea_evidence import validate_native_stages
    with pytest.raises(AssertionError,match='not both'):
        validate_native_stages([],[],session='s',job=1,owners={},partitions=5,
                               iterations=31,expected_native_phases=32)

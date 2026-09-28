import pytest
from argentea_remote_resource_control import validate_remote_owners, live_remote_workers


def fixture():
    workers = [dict(worker_id=i, pid=123, host=h, session_id='s') for i,h in enumerate(('a','b'))]
    records = [dict(operation_id='op', event=e, partition=i, worker_id=i,
                    pid=123, session_id='s', job_id=7) for e in ('init','close') for i in range(2)]
    return records, dict(operation_id='op', partitions=2), workers


def test_binds_equal_pids_to_distinct_hosts():
    assert [r['host'] for r in validate_remote_owners(*fixture())] == ['a','b']


@pytest.mark.parametrize('defect',['host','worker','pid','session','job','owner','missing'])
def test_rejects_unbound_owner_evidence(defect):
    records, request, workers = fixture()
    if defect=='host': workers[1]['host']='a'
    elif defect=='worker': records[-1]['worker_id']=9
    elif defect=='pid': records[-1]['pid']=9
    elif defect=='session': records[-1]['session_id']='other'
    elif defect=='job': records[-1]['job_id']=9
    elif defect=='owner': records[1]['partition']=0
    elif defect=='missing': records.pop(0)
    with pytest.raises(ValueError): validate_remote_owners(records,request,workers)


@pytest.mark.parametrize('status,alive,pgid,valid', [('R',True,123,True),('S',True,123,True),
    ('T',True,123,False),('Z',True,123,False),('',False,123,False),('R',True,9,False)])
def test_liveness_rejects_unusable_worker(monkeypatch,status,alive,pgid,valid):
    import argentea_remote_resource_control as module
    monkeypatch.setattr(module,'control_worker',lambda *_: dict(pid=123,alive=alive,status=status,pgid=pgid))
    workers=fixture()[2]
    if valid:
        assert [s['host'] for s in live_remote_workers(workers)]==['a','b']
    else:
        with pytest.raises(ValueError):live_remote_workers(workers)

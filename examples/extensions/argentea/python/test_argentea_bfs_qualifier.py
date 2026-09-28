"""CLI exercise controls preserve strict negative classification and readiness."""
import json
from types import SimpleNamespace

import pytest

import qualify_bfs as qualifier


@pytest.mark.parametrize('mode,case',[('local','graph'),('process-cluster','cap')])
@pytest.mark.parametrize('method',qualifier.METHODS)
def test_exercise_waits_only_for_worker_mode_and_does_not_accept_rpc_cancel_as_cap(tmp_path,monkeypatch,mode,case,method):
    from pyspark.sql.connect.session import SparkSession
    import argentea_readiness
    events=[];called={}
    class Spark:
        client=SimpleNamespace(set_retry_policies=lambda policies:None)
        def createDataFrame(self,rows,schema):
            events.append('input');return (rows,schema)
        def stop(self):events.append('stop')
    spark=Spark()
    monkeypatch.setattr(SparkSession,'builder',SimpleNamespace(remote=lambda endpoint:SimpleNamespace(create=lambda:spark)))
    monkeypatch.setattr(argentea_readiness,'wait_for_workers',lambda session,**kw:events.append('ready'))
    monkeypatch.setattr(qualifier,'inventory',lambda session:([],[]))
    class Client:
        def __init__(self,session,**kwargs):assert session is spark
        def bfs(self,nodes,edges,**kwargs):
            called.update(kwargs)
            raise RuntimeError('requires distributed Sail execution' if mode=='local' else 'operation cancelled')
    monkeypatch.setattr(qualifier,'ArgenteaBfs',Client)
    args=SimpleNamespace(mode=mode,case=case,method=method,partitions=5,alpha=14,beta=24,batch_rows=2,output=tmp_path)
    result=qualifier.exercise('sc://fixture',args)
    assert called['method']==method and called['source']==-5 and called['directed'] is True
    assert called['max_levels']==(0 if case=='cap' else 14)
    assert events[-1]=='stop'
    if mode=='local':
        assert 'ready' not in events and result['outcome']=='expected-local-rejection'
    else:
        assert events[0]=='ready' and result['outcome']=='failed-query-awaiting-cap-audit'
        assert result['query_failed'] is True and result['error']=='operation cancelled'
        assert result['outcome']!='expected-cap'
    assert json.loads((tmp_path/'exercise.json').read_text())['outcome']==result['outcome']

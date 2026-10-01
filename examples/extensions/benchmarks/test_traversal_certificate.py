"""Adversarial certificates against an actual Sail server when configured."""
import os
import pytest


@pytest.fixture(scope='module')
def spark():
    endpoint=os.environ.get('SAIL_GRAPH_TEST_REMOTE')
    if not endpoint:
        pytest.skip('set SAIL_GRAPH_TEST_REMOTE for distributed certificate checks')
    from pyspark.sql.connect.session import SparkSession
    session=SparkSession.builder.remote(endpoint).create()
    yield session
    session.stop()


def fixture(spark, distances, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    # Parquet is the benchmark input boundary; no client createDataFrame config
    # negotiation is needed to exercise the certificate on an actual server.
    root=tmp_path/'input';root.mkdir()
    pq.write_table(pa.table({'id':list(range(6))}),root/'vertices.parquet')
    pq.write_table(pa.table({'src':[0,1,2,3,4],'dst':[1,2,1,4,3],
                             'weight':[1.,0.,0.,0.,0.]}),root/'edges.parquet')
    pq.write_table(pa.table({'id':list(range(6)),
                             'distance':pa.array(distances,type=pa.float64())}),root/'actual.parquet')
    return tuple(spark.read.parquet((root/name).as_uri()) for name in
                 ['vertices.parquet','edges.parquet','actual.parquet'])



@pytest.mark.parametrize('weighted,distances',[(True,[0.,1.,1.,None,None,None]),
                                               (False,[0.,1.,2.,None,None,None])])
def test_valid_full_certificate(spark,tmp_path,weighted,distances):
    from traversal_certificate import certify
    v,e,a=fixture(spark,distances,tmp_path)
    result=certify(spark,a,v,e,source=0,weighted=weighted,partitions=2)
    assert result['reached']==3 and result['witness_rounds']==2
    if not weighted:
        assert result['conservative_absolute_distance_error_bound']==0


@pytest.mark.parametrize('distances,message',[
    ([0.,1.,1.,0.,0.,None],'tight-edge witness'),
    ([0.,0.,0.,None,None,None],'tight-edge witness'),
    ([0.,2.,2.,None,None,None],'triangle'),
    ([0.,None,None,None,None,None],'reachable'),
    ([0.,1.,float('nan'),None,None,None],'invalid distance'),
    ([0.,1.,1.,None,None,0.],'tight-edge witness'),
])
def test_bad_distances_cannot_pass_local_constraints_alone(spark,tmp_path,distances,message):
    from traversal_certificate import certify
    v,e,a=fixture(spark,distances,tmp_path)
    with pytest.raises(AssertionError,match=message):
        certify(spark,a,v,e,source=0,weighted=True,partitions=2)


def test_certificate_cap_is_failure_and_ownership_is_released(spark, tmp_path, monkeypatch):
    from traversal_certificate import certify
    from pyspark_pecan.utils import GraphUtils
    allocated=[]
    original=GraphUtils.allocate
    def track(utils,**kwargs):
        owned=original(utils,**kwargs)
        allocated.append((utils,owned))
        return owned
    monkeypatch.setattr(GraphUtils,'allocate',track)
    v,e,a=fixture(spark,[0.,1.,1.,None,None,None],tmp_path)
    with pytest.raises(RuntimeError,match='round limit'):
        certify(spark,a,v,e,source=0,weighted=True,partitions=2,max_rounds=1)
    assert len(allocated)==1
    utils,owned=allocated[0]
    assert utils.remove(*owned)==0

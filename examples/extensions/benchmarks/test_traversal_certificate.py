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


def fixture(spark, distances):
    vertices=spark.createDataFrame([(i,) for i in range(6)],'id long')
    edges=spark.createDataFrame([(0,1,1.),(1,2,0.),(2,1,0.),(3,4,0.),(4,3,0.)],
                               'src long,dst long,weight double')
    actual=spark.createDataFrame(list(enumerate(distances)),'id long,distance double')
    return vertices,edges,actual


@pytest.mark.parametrize('weighted,distances',[(True,[0.,1.,1.,None,None,None]),
                                               (False,[0.,1.,2.,None,None,None])])
def test_valid_full_certificate(spark,weighted,distances):
    from traversal_certificate import certify
    v,e,a=fixture(spark,distances)
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
def test_bad_distances_cannot_pass_local_constraints_alone(spark,distances,message):
    from traversal_certificate import certify
    v,e,a=fixture(spark,distances)
    with pytest.raises(AssertionError,match=message):
        certify(spark,a,v,e,source=0,weighted=True,partitions=2)


def test_certificate_cap_is_failure_and_ownership_is_released(spark, monkeypatch):
    from traversal_certificate import certify
    from pyspark_pecan.utils import GraphUtils
    allocated=[]
    original=GraphUtils.allocate
    def track(utils,**kwargs):
        owned=original(utils,**kwargs)
        allocated.append((utils,owned))
        return owned
    monkeypatch.setattr(GraphUtils,'allocate',track)
    v,e,a=fixture(spark,[0.,1.,1.,None,None,None])
    with pytest.raises(RuntimeError,match='round limit'):
        certify(spark,a,v,e,source=0,weighted=True,partitions=2,max_rounds=1)
    assert len(allocated)==1
    utils,owned=allocated[0]
    assert utils.remove(*owned)==0

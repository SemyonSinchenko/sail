"""Actual SQL/oracle and fallback controls for the optional parent witness."""
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture(scope='module')
def spark():
    endpoint=os.environ.get('SAIL_GRAPH_TEST_REMOTE')
    if not endpoint:
        pytest.skip('set SAIL_GRAPH_TEST_REMOTE for actual parent-witness checks')
    from pyspark.sql.connect.session import SparkSession
    session=SparkSession.builder.remote(endpoint).create()
    yield session
    session.stop()


def fixture(spark,root,rows=None,edges=None,hop_type=None):
    rows=rows or [(0,0.,0,0),(1,1.,0,1),(2,2.,1,2),(3,None,None,None)]
    if edges is None:edges=[(0,1,1.),(1,2,1.)]
    root.mkdir()
    pq.write_table(pa.table({'id':pa.array([r[0] for r in rows],type=pa.int64())}),root/'vertices.parquet')
    actual=pa.table({name:pa.array([row[i] for row in rows],type=kind)
        for i,(name,kind) in enumerate([('id',pa.int64()),('distance',pa.float64()),
                                       ('parent',pa.int64()),('hops',hop_type or pa.int64())])})
    pq.write_table(actual,root/'actual.parquet')
    pq.write_table(pa.table({name:pa.array([edge[i] for edge in edges],type=kind)
        for i,(name,kind) in enumerate([('src',pa.int64()),('dst',pa.int64()),('weight',pa.float64())])}),root/'edges.parquet')
    return tuple(spark.read.parquet((root/name).as_uri()) for name in
                 ['vertices.parquet','edges.parquet','actual.parquet'])


def certify(spark,frames,**options):
    from traversal_certificate import certify as implementation
    v,e,a=frames
    return implementation(spark,a,v,e,source=options.pop('source',0),
                           weighted=options.pop('weighted',True),partitions=options.pop('partitions',2),**options)


def validate(spark,root,**options):
    from traversal_cell import validate as implementation
    return implementation(spark,root/'actual.parquet',root,'sssp',4,0,True,False,
                          policy='certificate',partitions=2,**options)


@pytest.mark.parametrize('weighted',[False,True])
@pytest.mark.parametrize('hop_type',[pa.int8(),pa.int16(),pa.int32(),pa.int64()])
def test_integral_parent_tree_matches_distance_only_certificate_and_oracle(spark,tmp_path,weighted,hop_type):
    from traversal_reference import distances
    frames=fixture(spark,tmp_path/'data',hop_type=hop_type)
    v,e,a=frames
    proof=certify(spark,frames,weighted=weighted)
    reference=certify(spark,(v,e,a.select('id','distance')),weighted=weighted)
    assert proof['witness_method']=='parent_hops' and proof['witness_rounds'] is None
    assert proof['parent_witness_max_hops']==2
    assert reference['witness_method']=='tight_edge_bfs' and reference['witness_rounds']==2
    for key in ('rows','unique','reached','max_edge_slack','conservative_absolute_distance_error_bound'):
        assert proof[key]==reference[key]
    assert {r.id:r.distance for r in a.collect()}==distances([0,1,2,3],[(0,1,1.),(1,2,1.)],0,weighted=weighted)


def test_cap_trap_retains_shorter_bfs_fallback(spark,tmp_path):
    frames=fixture(spark,tmp_path/'data',rows=[(0,0.,0,0),(1,0.,0,1),(2,0.,1,2)],
                   edges=[(0,1,0.),(1,2,0.),(0,2,0.)])
    proof=certify(spark,frames,max_rounds=1)
    assert proof['witness_method']=='tight_edge_bfs' and proof['witness_rounds']==1
    assert proof['parent_witness_max_hops'] is None


def test_old_parent_tolerance_does_not_prove_exact_tight_edge(spark,tmp_path):
    # Chosen root->2 parent is allowed by old parent validation but not tight at
    # tolerance0; actual tight path needs two rounds, beyond the cap of one.
    frames=fixture(spark,tmp_path/'data',rows=[(0,0.,0,0),(1,1.,0,1),(2,1.,0,1)],
                   edges=[(0,1,1.),(1,2,0.),(0,2,1.+5e-13)])
    with pytest.raises(RuntimeError,match='round limit'):
        certify(spark,frames,max_rounds=1)


def test_stricter_existing_parent_output_rejection_is_preserved(spark,tmp_path):
    root=tmp_path/'data'
    frames=fixture(spark,root,rows=[(0,0.,0,0),(1,1.,0,1),(2,2.+4e-12,1,2),(3,None,None,None)])
    assert certify(spark,frames,tolerance=1e-12)['witness_method']=='parent_hops'
    with pytest.raises(AssertionError,match='invalid parent edge'):
        validate(spark,root)


@pytest.mark.parametrize('case',['null_parent','null_hops','missing_parent','cycle','unreachable_metadata',
                                  'root_parent','root_hops','zero_nonroot_hops','oversized_hops','missing_edge'])
def test_invalid_parent_metadata_falls_back_and_caller_still_rejects(spark,tmp_path,case):
    rows=[[0,0.,0,0],[1,1.,0,1],[2,2.,1,2],[3,None,None,None]]
    if case=='null_parent':rows[1][2]=None
    if case=='null_hops':rows[1][3]=None
    if case=='missing_parent':rows[1][2]=99
    if case=='cycle':rows[1][2]=2
    if case=='unreachable_metadata':rows[3][2]=0
    if case=='root_parent':rows[0][2]=1
    if case=='root_hops':rows[0][3]=1
    if case=='zero_nonroot_hops':rows[1][3]=0
    if case=='oversized_hops':rows[2][3]=4
    if case=='missing_edge':rows[2][2:]=[0,1]
    root=tmp_path/'data';frames=fixture(spark,root,rows=rows)
    proof=certify(spark,frames)
    assert proof['witness_method']=='tight_edge_bfs' and proof['witness_rounds']==2
    with pytest.raises(AssertionError):validate(spark,root)


def test_floating_hops_keep_existing_fallback_acceptance(spark,tmp_path):
    root=tmp_path/'data';frames=fixture(spark,root,hop_type=pa.float64())
    assert certify(spark,frames)['witness_method']=='tight_edge_bfs'
    assert validate(spark,root)['parent_tree_checked']


@pytest.mark.parametrize('missing',['parent','hops'])
def test_one_missing_optional_native_field_keeps_original_contract(spark,tmp_path,missing):
    from traversal_cell import validate_parents
    root=tmp_path/'data';v,e,a=fixture(spark,root);a=a.drop(missing)
    assert certify(spark,(v,e,a))['witness_method']=='tight_edge_bfs'
    assert validate_parents(spark,a,root,'sssp',4,0,True,True) is False
    with pytest.raises(AssertionError,match='portable traversal'):
        validate_parents(spark,a,root,'sssp',4,0,True,False)


def test_disconnected_zero_cycle_cannot_be_parent_proof(spark,tmp_path):
    frames=fixture(spark,tmp_path/'data',rows=[(0,0.,0,0),(1,0.,2,1),(2,0.,1,2)],
                   edges=[(1,2,0.),(2,1,0.)])
    with pytest.raises(AssertionError,match='tight-edge witness'):certify(spark,frames)


@pytest.mark.parametrize('case',['shortcut','unreachable','overflow'])
def test_parent_tree_never_bypasses_all_edge_checks(spark,tmp_path,case):
    rows=[(0,0.,0,0),(1,1.,0,1),(2,2.,1,2),(3,None,None,None)]
    edges=[(0,1,1.),(1,2,1.)]
    if case=='shortcut':edges.append((0,2,.5));message='triangle'
    if case=='unreachable':edges.append((1,3,1.));message='reachable'
    if case=='overflow':
        rows=[(0,0.,0,0),(1,1e308,0,1),(2,1e308,1,2),(3,None,None,None)]
        edges=[(0,1,1e308),(1,2,1e308)];message='overflow'
    with pytest.raises(AssertionError,match=message):certify(spark,fixture(spark,tmp_path/'data',rows,edges))


@pytest.mark.parametrize('case',['duplicate','missing','unknown'])
def test_parent_metadata_never_bypasses_output_coverage(spark,tmp_path,case):
    from pyspark.sql.connect import functions as F
    v,e,a=fixture(spark,tmp_path/'data')
    if case=='duplicate':a=a.unionByName(a.where(F.col('id')==1))
    if case=='missing':a=a.where(F.col('id')!=2)
    if case=='unknown':a=a.withColumn('id',F.when(F.col('id')==2,99).otherwise(F.col('id')))
    with pytest.raises(AssertionError,match='output cardinality|output vertex set'):
        certify(spark,(v,e,a))


def test_signed_ids_parallel_edges_and_undirected_orientation(spark,tmp_path):
    ids=[-(1<<63),-1,(1<<63)-1]
    frames=fixture(spark,tmp_path/'data',rows=[(ids[0],0.,ids[0],0),(ids[1],1.,ids[0],1),(ids[2],2.,ids[1],2)],
                   edges=[(ids[1],ids[0],8.),(ids[1],ids[0],1.),(ids[1],ids[2],1.)])
    assert certify(spark,frames,source=ids[0],directed=False)['witness_method']=='parent_hops'


def test_float_expression_order_and_isolated_root(spark,tmp_path):
    frames=fixture(spark,tmp_path/'rounding',rows=[(0,0.,0,0),(1,1.,0,1),(2,1.,1,2)],
                   edges=[(0,1,1.),(1,2,2.**-53)])
    assert certify(spark,frames,tolerance=0.)['witness_method']=='parent_hops'
    singleton=fixture(spark,tmp_path/'singleton',rows=[(0,0.,0,0)],edges=[])
    proof=certify(spark,singleton)
    assert proof['parent_witness_max_hops']==0 and proof['witness_rounds'] is None


@pytest.mark.parametrize('cap',[0,-1,True,1.5])
def test_invalid_cap_rejected_before_any_fast_path(cap):
    from traversal_certificate import certify as implementation
    with pytest.raises(ValueError,match='max_rounds'):
        implementation(None,None,None,None,source=0,weighted=True,max_rounds=cap)


@pytest.mark.parametrize('case',['partitions0','partitions_bool','partitions_float',
                                  'vertices_int32','edges_int32','duplicate_names'])
def test_original_graph_run_preconditions_apply_before_parent_selection(spark,tmp_path,case):
    from pyspark.sql.connect import functions as F
    v,e,a=fixture(spark,tmp_path/'data')
    partitions=2;message='partitions'
    if case=='partitions0':partitions=0
    if case=='partitions_bool':partitions=True
    if case=='partitions_float':partitions=1.5
    if case=='vertices_int32':v=v.select(F.col('id').cast('int').alias('id'));message='BIGINT'
    if case=='edges_int32':e=e.select(F.col('src').cast('int').alias('src'),'dst','weight');message='BIGINT'
    if case=='duplicate_names':v=v.select('id',F.lit(1).alias('extra'),F.lit(2).alias('extra'));message='unique column'
    with pytest.raises(ValueError,match=message):
        certify(spark,(v,e,a),partitions=partitions)

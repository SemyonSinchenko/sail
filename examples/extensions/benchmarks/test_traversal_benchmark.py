import copy
import json
from pathlib import Path
import pytest
from traversal_reference import distances, validate_rows
from traversal_fixture import prepare
from traversal_methods import method
from run_matrix import plan_cells, cell_command


EDGES=[(0,1,8.),(0,2,1.),(2,1,1.),(1,3,0.),(3,1,0.),(0,2,1.)]
IDS=[0,1,2,3,4]


def test_independent_oracles_keep_unreachable_and_zero_cycles():
    assert distances(IDS,EDGES,0,weighted=False)=={0:0.,1:1.,2:1.,3:2.,4:None}
    assert distances(IDS,EDGES,0,weighted=True)=={0:0.,1:2.,2:1.,3:2.,4:None}
    assert distances(IDS,EDGES,3,weighted=True,directed=False)[0]==2.


def valid_rows():
    return [dict(id=i,distance=d,parent=p,hops=h) for i,d,p,h in
            [(0,0.,0,0),(1,2.,2,2),(2,1.,0,1),(3,2.,1,3),(4,None,None,None)]]


def test_parent_certificate_accepts_valid_tree():
    result=validate_rows(valid_rows(),distances(IDS,EDGES,0,weighted=True),EDGES,0,weighted=True)
    assert result['parent_tree_checked'] and result['reached']==4


@pytest.mark.parametrize('mutation', ['distance','duplicate','missing','cycle','unreachable'])
def test_parent_certificate_rejects_corrupt_results(mutation):
    rows=valid_rows()
    if mutation=='distance': rows[1]['distance']=8.
    if mutation=='duplicate': rows.append(rows[0])
    if mutation=='missing': rows.pop()
    if mutation=='cycle': rows[1]['parent']=3
    if mutation=='unreachable': rows[4]['distance']=0.
    with pytest.raises(AssertionError):
        validate_rows(rows,distances(IDS,EDGES,0,weighted=True),EDGES,0,weighted=True)


def test_fixture_hashes_and_deterministic_weights(tmp_path):
    a=prepare(tmp_path/'a',vertices=32,degree=2)
    b=prepare(tmp_path/'b',vertices=32,degree=2)
    assert a['files']==b['files']
    assert a['family']=='traversal' and a['traversal']['source']==0
    with pytest.raises(ValueError,match='oracle'):
        prepare(tmp_path/'too-big',vertices=1_000_000)


def test_matrix_covers_each_execution_path_and_method():
    config=json.loads(Path(__file__).with_name('traversal-matrix.example.json').read_text())
    cells=plan_cells(config)
    assert len(cells)==36
    assert {c['engine'] for c in cells}=={'pecan','nutmeg-native','nutmeg-datafusion'}
    assert {c['mode'] for c in cells}=={'local','process-cluster'}
    for cell in cells:
        command=cell_command(config,cell)
        assert command[command.index('--source')+1]=='0'
        assert command[command.index('--delta')+1]=='4.0'
        assert '--directed' in command
    assert method('nutmeg-native','sssp','delta_star')=='ssspDeltaStar'
    assert method('nutmeg-native','bfs','push_pull')=='bfsDirection'
    assert method('nutmeg-datafusion','sssp','delta_star')=='delta_star'
    with pytest.raises(ValueError): method('pecan','bfs','delta_star')
    broken=copy.deepcopy(config);broken['datasets']['skew-small']['source']=999
    with pytest.raises(ValueError,match='source'):
        plan_cells(broken)


def test_matrix_rejects_fixture_without_matching_reference_columns():
    config=json.loads(Path(__file__).with_name('traversal-matrix.example.json').read_text())
    config['datasets']['skew-small']['family']='sparse'
    with pytest.raises(ValueError,match='reference family'):
        plan_cells(config)

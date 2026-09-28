import copy,json,sys,tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).parent))
from audit_parquet import audit,sha
with tempfile.TemporaryDirectory() as tmp:
 root=Path(tmp);dataset=root/'dataset';result=root/'result';dataset.mkdir();result.mkdir()
 pq.write_table(pa.table({'id':[0,1,2,3]}),dataset/'vertices.parquet')
 pq.write_table(pa.table({'src':[0,1,0],'dst':[1,2,2],'weight':[2.,3.,7.]}),dataset/'edges.parquet')
 def write(values):pq.write_table(pa.table({'id':[0,1,2,3],'distance':pa.array(values,type=pa.float64())}),result/'part.parquet')
 def record():
  return dict(outcome='passed',arguments=dict(source=0,algorithm='sssp',directed=True),dataset=dict(counts=dict(vertices=4,edges=3),files={p.name:dict(bytes=p.stat().st_size,sha256=sha(p)) for p in dataset.iterdir()}),result_files=[dict(name=p.name,bytes=p.stat().st_size,sha256=sha(p)) for p in result.iterdir()])
 write([0,2,5,None]);r=record();assert audit(r,dataset,result)['reached']==3
 write([0,1,1,None]);r=record();r['arguments']['algorithm']='bfs';assert audit(r,dataset,result)['exact']
 def reject(r):
  try:audit(r,dataset,result)
  except AssertionError:return
  raise AssertionError('invalid output accepted')
 write([0,1,2,None]);r=record();r['arguments']['algorithm']='bfs';reject(r)
 write([0,2,4,None]);reject(record())
 write([0,2,5,1]);reject(record())
 write([0,2,5,None]);r=record();write([0,2,6,None]);reject(r)
 write([0,2,5,None]);r=record();r['dataset']['files']['edges.parquet']['sha256']='0'*64;reject(r)
 pq.write_table(pa.table({'id':[0,1,1,3],'distance':[0.,2.,5.,None]}),result/'part.parquet');reject(record())
print('PASS Parquet BFS/SSSP including unreachable vertex; 6 invalid outputs/hash/cardinality controls rejected')

"""Read-only independent audit for positive-integer campaign outputs."""
import hashlib,json
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
from positive_integer_certificate import Certificate

def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def pin(path,record):
 assert path.is_file() and not path.is_symlink()
 assert path.stat().st_size==record['bytes'] and sha(path)==record['sha256'],str(path)
def files(root):
 return sorted(root.rglob('*.parquet')) if root.is_dir() else [root]
def audit(receipt,dataset,result):
 n=receipt['dataset']['counts']['vertices'];d=np.full(n,np.nan);seen=np.zeros(n,dtype=bool)
 expected={x['name']:x for x in receipt['result_files']};outputs=files(result)
 assert len(expected)==len(receipt['result_files'])==len(outputs)
 assert {p.name for p in outputs}==set(expected)
 for p in outputs:
  pin(p,expected[p.name])
  for batch in pq.ParquetFile(p).iter_batches(columns=['id','distance']):
   ids=batch.column(0).to_numpy(zero_copy_only=False);values=batch.column(1).to_numpy(zero_copy_only=False)
   assert ids.dtype.kind in 'iu' and np.all((ids>=0)&(ids<n))
   assert len(np.unique(ids))==len(ids) and not np.any(seen[ids])
   seen[ids]=True;d[ids]=values
 assert np.all(seen)
 source=receipt['arguments']['source'];c=Certificate(d,source)
 pins=receipt['dataset']['files'];edgefiles=files(dataset/'edges.parquet');vertexfiles=files(dataset/'vertices.parquet')
 graph_names={str(p.relative_to(dataset)) for p in edgefiles+vertexfiles}
 assert graph_names<=set(pins)
 for name in set(pins)-graph_names:
  relative=Path(name);assert not relative.is_absolute() and '..' not in relative.parts
  pin(dataset/relative,pins[name])
 vertex_seen=np.zeros(n,dtype=bool)
 for p in vertexfiles:
  pin(p,pins[str(p.relative_to(dataset))])
  for b in pq.ParquetFile(p).iter_batches(columns=['id']):
   ids=b.column(0).to_numpy(zero_copy_only=False)
   assert ids.dtype.kind in 'iu' and np.all((ids>=0)&(ids<n))
   assert len(np.unique(ids))==len(ids) and not np.any(vertex_seen[ids]);vertex_seen[ids]=True
 assert np.all(vertex_seen)
 input_edges=0
 for p in edgefiles:
  pin(p,pins[str(p.relative_to(dataset))])
  for b in pq.ParquetFile(p).iter_batches(columns=['src','dst','weight']):
   u,v,w=[b.column(i).to_numpy(zero_copy_only=False) for i in range(3)]
   # Validate input weight policy even when BFS substitutes unit weights.
   assert np.all(np.isfinite(w)&(w>=1)&(w<=16)&(w==np.floor(w)))
   if receipt['arguments']['algorithm']=='bfs':w=np.ones(len(w))
   c.add(u,v,w);input_edges+=len(u)
   if not receipt['arguments']['directed']:c.add(v,u,w)
 assert input_edges==receipt['dataset']['counts']['edges']
 out=c.finish();out.update(input_edges=input_edges,result_files=len(outputs),input_files=len(pins),receipt_metadata_outcome=receipt['outcome'])
 return out

if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser();p.add_argument('receipt',type=Path);p.add_argument('dataset',type=Path);p.add_argument('result',type=Path);a=p.parse_args()
 print(json.dumps(audit(json.loads(a.receipt.read_text()),a.dataset,a.result),indent=2))

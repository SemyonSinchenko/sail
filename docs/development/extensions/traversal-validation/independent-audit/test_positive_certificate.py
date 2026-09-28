import heapq,random,sys
from pathlib import Path
import numpy as np
sys.path.insert(0,str(Path(__file__).parent))
from positive_integer_certificate import Certificate
rng=random.Random(42)
for trial in range(100):
 n=rng.randrange(2,80);edges=[(rng.randrange(n),rng.randrange(n),rng.randrange(1,17)) for _ in range(n*3)]
 adjacency=[[] for _ in range(n)]
 for u,v,w in edges:adjacency[u].append((v,w))
 distances=[float('inf')]*n;distances[0]=0;queue=[(0,0)]
 while queue:
  cost,u=heapq.heappop(queue)
  if cost!=distances[u]:continue
  for v,w in adjacency[u]:
   if cost+w<distances[v]:distances[v]=cost+w;heapq.heappush(queue,(cost+w,v))
 d=[np.nan if x==float('inf') else x for x in distances]
 c=Certificate(d,0)
 for start in range(0,len(edges),7):
  a,b,w=zip(*edges[start:start+7]);c.add(a,b,w)
 assert c.finish()['edges']==len(edges)
def rejects(d,edges):
 try:
  c=Certificate(d,0)
  if edges:c.add(*zip(*edges))
  c.finish()
 except AssertionError:return
 raise AssertionError((d,edges))
rejects([0,2],[(0,1,1)])
rejects([0,0],[(0,1,1)])
rejects([0,1],[])
rejects([0,np.nan],[(0,1,1)])
rejects([0,1,1],[(1,2,1),(2,1,1)])
rejects([0,1],[(0,1,0)])
rejects([0,1.5],[(0,1,1)])
rejects([0,float('inf')],[])
print('PASS 100 seeded independent Dijkstra comparisons, streamed chunks, 8 invalid certificates rejected')

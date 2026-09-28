"""Independent streamed certificate for this campaign's positive integer weights.

All-edge inequalities give a lower bound on every path length. A tight incoming
edge for each reached non-root vertex gives a witness: positive integer weights
strictly decrease distance on predecessor steps, so the chain terminates at the
unique zero-distance source. No graph kernel or Sail execution is reused.
Reject zero weights; this proof must not be applied to general SSSP fixtures.
"""
import numpy as np

class Certificate:
 def __init__(self, distances, source):
  self.d=np.asarray(distances,dtype=np.float64)
  assert self.d.ndim==1 and 0<=source<len(self.d)
  self.finite=np.isfinite(self.d)
  assert np.all(self.finite | np.isnan(self.d)), 'only null/NaN represents unreachable'
  values=self.d[self.finite]
  assert np.all((values>=0)&(values==np.floor(values))&(values<2**52))
  assert self.d[source]==0 and np.count_nonzero(self.d==0)==1
  self.witness=np.zeros(len(self.d),dtype=bool);self.witness[source]=True
  self.edges=0
 def add(self,src,dst,weight):
  src=np.asarray(src);dst=np.asarray(dst);w=np.asarray(weight,dtype=np.float64)
  assert src.ndim==dst.ndim==w.ndim==1 and len(src)==len(dst)==len(w)
  assert src.dtype.kind in 'iu' and dst.dtype.kind in 'iu'
  assert np.all((src>=0)&(src<len(self.d))) and np.all((dst>=0)&(dst<len(self.d)))
  assert np.all(np.isfinite(w)&(w>=1)&(w<=16)&(w==np.floor(w)))
  active=self.finite[src];u=src[active];v=dst[active];cost=self.d[u]+w[active]
  assert np.all(self.finite[v]), 'reachable target reported unreachable'
  assert np.all(self.d[v]<=cost), 'edge inequality violated'
  tight=self.d[v]==cost
  self.witness[v[tight]]=True
  self.edges+=len(src)
 def finish(self):
  assert np.all(self.witness[self.finite]), 'finite vertex lacks descending source witness'
  return {'vertices':len(self.d),'reached':int(self.finite.sum()),'edges':self.edges,'exact':True,'weight_domain':'integers 1..16; distances < 2**52'}

#!/usr/bin/env python3
"""Prepare bounded correctness fixtures; never load a giant graph with this oracle."""
import argparse
import json
from pathlib import Path
import random
import pyarrow as pa
import pyarrow.parquet as pq
from traversal_reference import distances
from runtime import sha256


def prepare(output,vertices=256,degree=8,seed=42,source=0,directed=True):
    if not 8 <= vertices <= 100_000 or not 1 <= degree <= 64:
        raise ValueError('oracle fixture requires 8..100000 vertices and degree 1..64')
    if not 0 <= source < vertices:
        raise ValueError('source outside graph')
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    rng=random.Random(seed)
    # Keep an isolate, a zero-weight cycle, ties, duplicates and skew. Integer
    # weights are exactly representable doubles and suitable for GAP controls.
    edges=[(0,1,8.),(0,2,1.),(2,1,1.),(1,3,0.),(3,1,0.),(0,2,1.)]
    for node in range(vertices-1):
        for _ in range(degree):
            edges.append((node,rng.randrange(vertices-1),float(rng.randrange(16))))
        if node%3==0:
            edges.append((node,0,1.))
    ids=list(range(vertices))
    pq.write_table(pa.table({'id':pa.array(ids,type=pa.int64())}),output/'vertices.parquet')
    pq.write_table(pa.table({'src':pa.array([e[0] for e in edges],type=pa.int64()),
                            'dst':pa.array([e[1] for e in edges],type=pa.int64()),
                            'weight':pa.array([e[2] for e in edges],type=pa.float64())}),output/'edges.parquet')
    refs={kind:distances(ids,edges,source,weighted=kind=='sssp',directed=directed) for kind in ('bfs','sssp')}
    pq.write_table(pa.table({'id':pa.array(ids,type=pa.int64()),**{
        kind:pa.array([values[i] for i in ids],type=pa.float64()) for kind,values in refs.items()}}),output/'reference.parquet')
    manifest=dict(family='traversal',seed=seed,parameters=dict(degree=degree,source=source,directed=directed),counts=dict(vertices=vertices,edges=len(edges)),
                  traversal=dict(source=source,directed=directed,seed=seed,weight_policy='seeded integers 0..15 stored as float64'),
                  purpose='bounded correctness fixture; not a Graph500 input',
                  files={p.name:dict(sha256=sha256(p),bytes=p.stat().st_size) for p in output.glob('*.parquet')})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--vertices',type=int,default=256)
    parser.add_argument('--degree',type=int,default=8)
    parser.add_argument('--source',type=int,default=0)
    parser.add_argument('--directed',action=argparse.BooleanOptionalAction,default=True)
    parser.add_argument('--seed',type=int,default=42)
    args=parser.parse_args()
    prepare(args.output,vertices=args.vertices,seed=args.seed,degree=args.degree,source=args.source,directed=args.directed)

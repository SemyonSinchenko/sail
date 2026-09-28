"""Export complete benchmark diagnostics, omitting Parquet bytes but hashing them."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys

source,target=map(Path,sys.argv[1:3])
assert source.is_dir() and not target.exists()
target.mkdir(parents=True)
entries=[]
for path in sorted(source.rglob('*')):
    if path.is_symlink():
        raise RuntimeError('refusing to follow source symlink: '+str(path))
    if not path.is_file():
        continue
    relative=path.relative_to(source)
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            digest.update(block)
    include=path.suffix!='.parquet'
    entry=dict(path=str(relative),bytes=path.stat().st_size,sha256=digest.hexdigest(),included=include)
    if include:
        destination=target/relative
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(path,destination)
        copied=hashlib.sha256(destination.read_bytes()).hexdigest()
        assert copied==entry['sha256'], relative
    else:
        entry['omission']='Parquet bytes omitted; original remains at source_root, hash retained here and in per-cell result/dataset receipts'
    entries.append(entry)
manifest=dict(recorded_utc=datetime.now(timezone.utc).isoformat(),source_root=str(source),
              boundary='All original regular files inventoried; logs, memory samples, configuration, plans and receipts copied byte-for-byte; Parquet bytes omitted.',
              included_files=sum(e['included'] for e in entries),omitted_parquet_files=sum(not e['included'] for e in entries),files=entries)
(target/'export-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({k:v for k,v in manifest.items() if k!='files'},indent=2))

(target/'raw-inventory.json').write_text(json.dumps(dict(source_root=str(source), files=[entry for entry in entries if not entry['included']]),indent=2)+'\n')

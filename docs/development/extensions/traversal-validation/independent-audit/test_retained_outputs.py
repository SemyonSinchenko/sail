"""Reproduce the independent audit using the adjacent committed evidence archive."""
import json
import sys
import tarfile
import tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from audit_parquet import audit
here=Path(__file__).parent
expected=json.loads((here/'retained-linux-output-controls.json').read_text())['cases']
with tempfile.TemporaryDirectory() as tmp:
 root=Path(tmp)
 with tarfile.open(here.parent/'linux-release-538b-evidence.tar.gz') as archive:
  for member in archive.getmembers():
   path=Path(member.name)
   assert not path.is_absolute() and '..' not in path.parts and member.isfile()
   if not member.name.startswith('qualification/'):continue
   target=root/path;target.parent.mkdir(parents=True,exist_ok=True)
   target.write_bytes(archive.extractfile(member).read())
 base=root/'qualification'
 for case in expected:
  cell=base/case['cell'];receipt=json.loads((cell/'receipt.json').read_text())
  result=audit(receipt,base/'imported-dataset',cell/'result')
  assert dict(cell=case['cell'],**result)==case
print(f'PASS {len(expected)} archived Linux output cases independently reaudited')

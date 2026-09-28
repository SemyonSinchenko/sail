import copy
import hashlib
import json
import tempfile
from pathlib import Path

from audit_campaign import ENGINES, audit_cells, validate_matrix


config = {'suites': [dict(name='small', repetitions=1, datasets=['graph'],
                          algorithms=['bfs'], variants=['frontier'])]}
fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
rows = [dict(suite='small', repeat=1, dataset='graph', engine=engine,
             algorithm='bfs', variant='frontier', sequence=i + 1,
             cell_id='cell-' + str(i), outcome='passed',
             configuration_sha256=fingerprint) for i, engine in enumerate(ENGINES)]
matrix = dict(results=rows, outcome_counts={'passed': 3})
assert validate_matrix(config, matrix, fingerprint) == rows


def reject(changed, pin=fingerprint):
    try:
        validate_matrix(config, changed, pin)
    except ValueError:
        return
    raise AssertionError('invalid matrix accepted')


changed = copy.deepcopy(matrix)
changed['results'].pop()
reject(changed)
changed = copy.deepcopy(matrix)
changed['results'].append(changed['results'][0])
reject(changed)
changed = copy.deepcopy(matrix)
changed['results'][0]['cell_id'] = '../escape'
reject(changed)
changed = copy.deepcopy(matrix)
changed['outcome_counts']['passed'] = 4
reject(changed)
reject(matrix, '0' * 64)
changed = copy.deepcopy(matrix)
for row in changed['results']:
    row['configuration_sha256'] = '0' * 64
reject(changed)

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    def receipt(row, **updates):
        record = dict(outcome=row['outcome'], arguments={
            k: row[k] for k in ('engine', 'algorithm', 'variant', 'repeat', 'dataset')
        }, result_files=[{'name': 'part.parquet'}])
        record.update(updates)
        path = root / 'cells' / row['cell_id'] / 'artifacts' / 'receipt.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record))
        return path

    for row in rows:
        receipt(row)
    calls = []
    def check(record, dataset, result):
        calls.append(result)
        if record['arguments']['engine'] == ENGINES[1]:
            raise AssertionError('deliberately invalid vector')
        return {'exact': True}
    audited = audit_cells(rows, root, root / 'datasets', checker=check)
    assert [r['audit_outcome'] for r in audited] == ['passed', 'error', 'passed']
    assert len(calls) == 3
    assert all(r['outcome'] == 'passed' for r in audited)

    receipt(rows[0], result_files=[])
    audited = audit_cells(rows, root, root / 'datasets', checker=check)
    assert audited[0]['audit_outcome'] == 'error'
    changed = copy.deepcopy(rows)
    changed[0]['outcome'] = 'timeout'
    receipt(changed[0], result_files=[])
    audited = audit_cells(changed, root, root / 'datasets', checker=check)
    assert audited[0]['audit_outcome'] == 'no_result'
    assert audited[0]['outcome'] == 'timeout'

    path = receipt(rows[0])
    record = json.loads(path.read_text())
    record['arguments']['engine'] = 'different-engine'
    path.write_text(json.dumps(record))
    audited = audit_cells(rows, root, root / 'datasets', checker=check)
    assert audited[0]['audit_outcome'] == 'error'

print('PASS campaign coverage/pins/path controls; retained failures, missing outputs, and receipt identity controls')

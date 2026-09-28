"""Audit a terminal campaign without changing its recorded outcomes.

Run only after the campaign and its containers have stopped. Inputs may be
mounted read-only; the report must be a separate, previously absent file.
"""
import argparse
import collections
import hashlib
import itertools
import json
from pathlib import Path

from audit_parquet import audit, sha


KEYS = ('suite', 'repeat', 'dataset', 'engine', 'algorithm', 'variant')
ENGINES = ('pecan', 'nutmeg-native', 'nutmeg-datafusion')


def validate_matrix(config, matrix, fingerprint):
    actual = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if actual != fingerprint:
        raise ValueError('configuration does not match independently supplied pin')
    expected = set()
    for suite in config['suites']:
        for values in itertools.product(
            range(1, suite['repetitions'] + 1), suite['datasets'], ENGINES,
            suite['algorithms'], suite['variants'],
        ):
            expected.add((suite['name'], *values))
    seen, sequences, ids = set(), set(), set()
    for row in matrix['results']:
        key = tuple(row[k] for k in KEYS)
        if key not in expected or key in seen:
            raise ValueError('unexpected or duplicate campaign cell')
        seen.add(key)
        if row['sequence'] in sequences or row['cell_id'] in ids:
            raise ValueError('duplicate sequence or cell ID')
        sequences.add(row['sequence'])
        ids.add(row['cell_id'])
        for name in (row['cell_id'], row['dataset']):
            if not isinstance(name, str) or Path(name).name != name or name in ('.', '..'):
                raise ValueError('unsafe cell or dataset path')
        if row['configuration_sha256'] != fingerprint:
            raise ValueError('cell configuration mismatch')
    if seen != expected or sequences != set(range(1, len(expected) + 1)):
        raise ValueError('campaign incomplete')
    counts = dict(collections.Counter(r['outcome'] for r in matrix['results']))
    if counts != matrix['outcome_counts']:
        raise ValueError('outcome count mismatch')
    return sorted(matrix['results'], key=lambda r: r['sequence'])


def audit_cells(rows, campaign, datasets, checker=audit):
    results = []
    for row in rows:
        item = {k: row[k] for k in ('cell_id', 'sequence', 'outcome')}
        receipt_path = campaign / 'cells' / row['cell_id'] / 'artifacts' / 'receipt.json'
        result_path = receipt_path.parent / 'result'
        try:
            receipt = json.loads(receipt_path.read_text())
            item['receipt_sha256'] = sha(receipt_path)
            if receipt['outcome'] != row['outcome']:
                raise ValueError('receipt/matrix outcome mismatch')
            for key in ('engine', 'algorithm', 'variant', 'repeat'):
                if receipt['arguments'][key] != row[key]:
                    raise ValueError('receipt/matrix identity mismatch: ' + key)
            if Path(receipt['arguments']['dataset']).name != row['dataset']:
                raise ValueError('receipt/matrix dataset mismatch')
            if receipt.get('result_files'):
                item['certificate'] = checker(receipt, datasets / row['dataset'], result_path)
                item['audit_outcome'] = 'passed'
            elif row['outcome'] == 'passed':
                raise ValueError('successful campaign cell has no retained result pins')
            else:
                item['audit_outcome'] = 'no_result'
        except Exception as error:
            # Retain each failure and continue, rather than losing later cells.
            item['audit_outcome'] = 'error'
            item['error'] = type(error).__name__ + ': ' + str(error)
        results.append(item)
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('campaign', type=Path)
    p.add_argument('datasets', type=Path)
    p.add_argument('report', type=Path)
    p.add_argument('--configuration-sha256', required=True)
    a = p.parse_args()
    if a.report.exists():
        p.error('refusing to overwrite an existing audit report')
    config_path = a.campaign / 'configuration.json'
    matrix_path = a.campaign / 'matrix-results.json'
    config = json.loads(config_path.read_text())
    matrix = json.loads(matrix_path.read_text())
    rows = validate_matrix(config, matrix, a.configuration_sha256)
    results = audit_cells(rows, a.campaign, a.datasets)
    counts = dict(collections.Counter(r['audit_outcome'] for r in results))
    import datetime
    report = dict(
        recorded_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        configuration_sha256=a.configuration_sha256,
        matrix_file_sha256=sha(matrix_path),
        campaign_outcomes=matrix['outcome_counts'], audit_outcomes=counts,
        results=results,
        scope='Independent output certificates; source provenance, measurements, and cleanup require separate audits.',
    )
    with a.report.open('x') as f:
        f.write(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'report': str(a.report), 'audit_outcomes': counts}))
    return 1 if counts.get('error') else 0


if __name__ == '__main__':
    raise SystemExit(main())

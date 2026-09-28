#!/usr/bin/env python3
"""Verify delivered report/evidence bytes and receipts; does not re-audit omitted raw graphs."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import statistics
import tarfile

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
ROOT = BASE.parents[3]
spec = importlib.util.spec_from_file_location('proof', HERE / 'package_graphframes_large.py')
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)
MAIN_HASH = '5cf0f2b14e9792c25c7fb1957324f5edca9170049c56cd98f8eb6e469814002f'
AUDITOR_HASH = '85070d03778ad1dd2e578835032fda7f000c0b4fcde8f9d1f74d2d3a5791935a'


def close(left, right):
    assert math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-9), (left, right)


def verify_summary():
    summary = proof.read(BASE / 'summary/summary.json')
    audit = proof.read(BASE / 'audit/audit.json')
    config = proof.read(BASE / 'configuration.json')
    rows = list(csv.DictReader((BASE / 'summary/cells.csv').open()))
    assert len(rows) == len({row['cell_id'] for row in rows}) == summary['planned_cells'] == audit['planned'] == 180
    assert dict(Counter(row['outcome'] for row in rows)) == summary['outcomes'] == audit['outcomes'] == {'passed': 179, 'timeout': 1}
    assert audit['audit_outcome'] == 'passed' and not audit['issues'] and not summary['integrity_errors']
    assert audit['raw_coverage'] == {'raw_full_vector': 179, 'receipt_only': 1}
    assert audit['reconstructed_samples'] == 117958 and audit['auditor_sha256'] == AUDITOR_HASH
    assert proof.sha(BASE.parent / 'pecan-nutmeg-benchmark/reproduce/audit_matrix.py') == AUDITOR_HASH
    assert summary['sources'] == audit['sources']
    assert all(config[key] == value for key, value in summary['sources'].items())
    assert config['limits'] == audit['limits'] and config['defaults'] == audit['defaults']
    assert audit['steal_observations'] == dict(observations=180, minimum=0., median=0., maximum=0.)
    groups = ('suite', 'dataset', 'mode', 'algorithm', 'variant', 'engine')
    assert len(summary['groups']) == 60
    for group in summary['groups']:
        selected = [row for row in rows if all(row[key] == group[key] for key in groups)]
        assert len(selected) == group['planned'] == 3
        assert {int(row['repeat']) for row in selected} == {1, 2, 3}
        assert dict(Counter(row['outcome'] for row in selected)) == group['outcomes']
        passed = [row for row in selected if row['outcome'] == 'passed']
        assert len(passed) == group['passed']
        for name, metric in group['metrics'].items():
            values = [float(row[name]) for row in passed if row.get(name) not in ('', None)]
            if metric is None:
                assert not values, f'{name} incorrectly reported unavailable'
                continue
            assert len(values) == metric['samples']
            close(statistics.median(values), metric['median'])
            close(min(values), metric['minimum'])
            close(max(values), metric['maximum'])
    failed = [row for row in rows if row['outcome'] != 'passed']
    assert failed[0]['cell_id'] == 'large-wcc-r2-uniform-2097152-pecan-wcc-reference'
    assert failed[0]['seconds'] == ''
    close(failed[0]['elapsed_until_error_seconds'], 1800.0833901160004)
    return summary, audit


def verify_archive(directory, exact):
    archive_path = directory / 'evidence.tar.gz'
    bundle, scan = proof.read(directory / 'bundle.json'), proof.read(directory / 'credential-scan.json')
    manifest_bytes = (directory / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    digest = proof.sha(archive_path)
    assert digest == bundle['archive_sha256'] == scan['archive_sha256']
    assert scan['outcome'] == 'passed' and scan['matches'] == []
    if directory.name == 'proof':
        assert digest == MAIN_HASH and len(manifest['included']) == 1109 and len(manifest['omitted']) == 728
        assert bundle['packager_sha256'] == scan['scanner_sha256'] == proof.sha(HERE / 'package_graphframes_large.py')
    expected = {entry['path']: entry for entry in manifest['included']}
    assert len(expected) == len(manifest['included']) == bundle['included_files']
    expected['manifest.json'] = dict(bytes=len(manifest_bytes), sha256=hashlib.sha256(manifest_bytes).hexdigest())
    seen, matches = set(), []
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive:
            assert member.isfile() and member.name in expected and member.name not in seen
            entry = expected[member.name]
            assert member.size == entry['bytes']
            with archive.extractfile(member) as stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == entry['sha256'], member.name
            with archive.extractfile(member) as stream:
                matches.extend(proof.scan_stream(stream, f'{directory.name}/{member.name}', exact))
            seen.add(member.name)
    assert seen == expected.keys() and len(seen) == scan['scanned_files']
    assert len(manifest['omitted']) == bundle['omitted_files']
    assert all(entry['path'].endswith('.parquet') for entry in manifest['omitted'])
    return len(seen), matches


def verify_figures():
    receipt = proof.read(BASE / 'figures/render-receipt.json')
    assert receipt['summary']['sha256'] == proof.sha(BASE / 'summary/summary.json')
    assert receipt['configuration']['sha256'] == proof.sha(BASE / 'configuration.json')
    assert receipt['renderer']['sha256'] == proof.sha(ROOT / 'examples/extensions/benchmarks/render_large.py')
    for entry in receipt['outputs']:
        file = BASE / 'figures' / Path(entry['path']).name
        assert file.stat().st_size == entry['bytes'] and proof.sha(file) == entry['sha256']


def verify_supplement():
    assert proof.read(BASE / 'supplement/capacity/summary.json')['outcomes'] == {'error': 1, 'not_run': 29}
    assert proof.read(BASE / 'supplement/qualified-pilots/summary.json')['outcomes'] == {'passed': 30}
    control = proof.read(BASE / 'supplement/timeout-control/cell-summary.json')
    assert control['outcome'] == 'passed'
    close(control['end_to_end_seconds'], 32.0890961130026)
    original = proof.read(BASE / 'configuration.json')
    changed = proof.read(BASE / 'supplement/timeout-control/configuration.json')
    assert {key for key in original.keys() | changed.keys() if original.get(key) != changed.get(key)} == {'run_id', 'host_output', 'container_root'}


def delivered_files():
    excluded = {BASE / 'publication-manifest.json', BASE / 'publication-scan.json'}
    files = [p for p in BASE.rglob('*') if p.is_file() and p not in excluded and '__pycache__' not in p.parts]
    files += [BASE.with_suffix('.md')]
    files += [ROOT / 'examples/extensions/benchmarks' / name for name in ('README.md', 'LARGE-GRAPHS.md', 'render_large.py')]
    return sorted(files)


def check_links(files):
    for file in files:
        if file.suffix != '.md':
            continue
        for match in re.finditer(r'\]\(([^)]+)\)', file.read_text()):
            target = match[1].split('#', 1)[0]
            if not target or '://' in target or target.startswith('mailto:'):
                continue
            assert (file.parent / target).resolve().exists(), (file.relative_to(ROOT), target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write-manifest', action='store_true', help='Create a new local publication inventory and actual scan receipt')
    args = parser.parse_args()
    summary, audit = verify_summary()
    verify_figures()
    verify_supplement()
    files = delivered_files()
    check_links(files)
    exact, matches, scanned = proof.credentials(), [], 0
    for directory in ('proof', 'supplement'):
        count, found = verify_archive(BASE / directory, exact)
        scanned += count
        matches.extend(found)
    entries = []
    for file in files:
        assert not file.is_symlink()
        relative = file.relative_to(ROOT).as_posix()
        entries.append(dict(path=relative, bytes=file.stat().st_size, sha256=proof.sha(file)))
        if file.suffix != '.gz':
            with file.open('rb') as stream:
                matches.extend(proof.scan_stream(stream, relative, exact))
            scanned += 1
    manifest_path = BASE / 'publication-manifest.json'
    scan_path = BASE / 'publication-scan.json'
    if args.write_manifest:
        manifest = dict(recorded_utc=datetime.now(timezone.utc).isoformat(), files=entries,
                        measured_sources=summary['sources'], main_archive_sha256=MAIN_HASH,
                        auditor_sha256=AUDITOR_HASH, audit_recorded_utc=audit['recorded_utc'])
        manifest_path.write_bytes(proof.encoded(manifest))
        scan_path.write_bytes(proof.encoded(dict(recorded_utc=datetime.now(timezone.utc).isoformat(),
            manifest_sha256=proof.sha(manifest_path), scanned_files=scanned+2, matches=matches,
            outcome='failed' if matches else 'passed', pattern_rules=sorted(proof.PATTERNS),
            credential_fields_checked=sorted(exact), scanner_sha256=proof.sha(Path(proof.__file__)),
            boundary='Every public non-archive file and every decompressed archive member checked; listed patterns and available exact values only, not exhaustive secret detection.')))
    manifest, scan = proof.read(manifest_path), proof.read(scan_path)
    assert manifest['files'] == entries
    assert scan['manifest_sha256'] == proof.sha(manifest_path)
    for path in (manifest_path, scan_path):
        with path.open('rb') as stream:
            matches.extend(proof.scan_stream(stream, path.relative_to(ROOT).as_posix(), exact))
        scanned += 1
    assert scan['scanned_files'] == scanned
    assert scan['outcome'] == 'passed' and not scan['matches'] and not matches, matches
    print(f'PASS large publication: {len(entries)} pinned files; 180 outcomes (179 passed, 1 timeout); '
          f'179 raw-vector audit records; {scanned} scanned files; original proof preserved')


if __name__ == '__main__':
    main()

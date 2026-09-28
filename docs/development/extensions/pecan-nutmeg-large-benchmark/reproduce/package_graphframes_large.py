#!/usr/bin/env python3
"""Build and verify a deterministic local proof archive after the raw audit passes.

No remote operations, site edits or deployment. Exact credential values are
read only from explicitly named environment fields or an optional JSON map;
neither those values nor matching lines are written to receipts.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import zlib

CREDENTIAL_FIELDS = (
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
    'AZURE_STORAGE_KEY', 'AZURE_STORAGE_SAS_TOKEN', 'GOOGLE_API_KEY',
    'GITHUB_TOKEN', 'GH_TOKEN', 'VERCEL_TOKEN',
)
PATTERNS = {
    'private-key': re.compile(rb'-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----'),
    'aws-access-key': re.compile(rb'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
    'github-token': re.compile(rb'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b'),
    'authorization-bearer': re.compile(rb'\bauthorization["\s]*[:=]["\s]*bearer\s+[^"\s,]+', re.I),
    'credential-field-assignment': re.compile(
        rb'\b(?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AZURE_STORAGE_KEY|'
        rb'AZURE_STORAGE_SAS_TOKEN|GITHUB_TOKEN|GH_TOKEN|VERCEL_TOKEN)'
        rb'(?:\s*=\s*[^"\s,}\]]+|"\s*:\s*"[^"\r\n]+")', re.I),
    'json-secret-field': re.compile(rb'"(?:password|passwd|client_secret|api_key|access_token|refresh_token)"\s*:\s*"[^"\s]+"', re.I),
}


def read(path):
    return json.loads(Path(path).read_text())


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            result.update(block)
    return result.hexdigest()


def safe_path(root, relative):
    parts = PurePosixPath(relative)
    if parts.is_absolute() or any(p in ('', '.', '..') for p in relative.split('/')):
        raise ValueError(f'unsafe evidence path: {relative}')
    path = root.joinpath(*parts.parts)
    if path.resolve() != root.resolve().joinpath(*parts.parts) or not path.is_file():
        raise ValueError(f'missing/nonregular/symlinked evidence path: {relative}')
    return path


def credentials(filename=None):
    values = {key: os.environ[key] for key in CREDENTIAL_FIELDS if os.environ.get(key)}
    if filename:
        supplied = read(filename)
        if not isinstance(supplied, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in supplied.items()):
            raise ValueError('credential file must be a JSON object of field names and exact string values')
        values.update({key: value for key, value in supplied.items() if value})
    return {key: value.encode() for key, value in values.items()}


def scan_stream(stream, relative, exact):
    """Overlap chunks so credentials crossing read boundaries are still found."""
    overlap = max([8192, *(len(value) for value in exact.values())])
    tail, found = b'', set()
    for block in iter(lambda: stream.read(1 << 20), b''):
        data = tail + block
        for rule, pattern in PATTERNS.items():
            if pattern.search(data):
                found.add(rule)
        for field, value in exact.items():
            if value in data:
                found.add(f'exact-value:{field}')
        tail = data[-overlap:]
    return [dict(path=relative, rule=rule) for rule in sorted(found)]


def tar_entry(archive, relative, stream, size):
    item = tarfile.TarInfo(relative)
    item.size, item.mode, item.mtime = size, 0o644, 0
    item.uid = item.gid = 0
    item.uname = item.gname = ''
    archive.addfile(item, stream)


def package(export, summary, audit_base, output, exact):
    export, summary, output = map(Path, (export, summary, output))
    audit_path = Path(str(audit_base) + '.json')
    if not audit_path.exists():
        raise ValueError('independent audit is pending')
    audit = read(audit_path)
    if audit.get('audit_outcome') != 'passed' or audit.get('issues'):
        raise ValueError(f'independent audit did not pass: {audit.get("issues")}')
    original = read(export / 'export-manifest.json')
    reported = read(summary / 'summary.json')
    matrix = read(export / 'matrix-results.json')
    assert audit['kind'] == 'large'
    assert matrix['planned_cells'] == matrix['completed_cells'] == audit['planned'] == reported['planned_cells'] == 180
    assert len(matrix['results']) == 180 and len({row['cell_id'] for row in matrix['results']}) == 180
    assert dict(Counter(row['outcome'] for row in matrix['results'])) == matrix['outcome_counts']
    assert matrix['outcome_counts'] == audit['outcomes'] == reported['outcomes']
    assert audit['sources'] == reported['sources'] and not reported['integrity_errors']
    included, omitted, paths = [], [], {}
    for entry in original['files']:
        relative = entry['path']
        if not entry['included']:
            assert relative.endswith('.parquet')
            omitted.append(entry)
            continue
        source = safe_path(export, relative)
        assert source.stat().st_size == entry['bytes'] and sha(source) == entry['sha256'], relative
        archive_path = 'evidence/' + relative
        assert archive_path not in paths
        paths[archive_path] = source
        included.append(dict(path=archive_path, origin='original-export/' + relative,
                             bytes=entry['bytes'], sha256=entry['sha256']))
    assert len(included) == original['included_files']
    assert len(omitted) == original['omitted_parquet_files']
    assert read(export / 'raw-inventory.json')['files'] == omitted
    extras = {
        'evidence/export-manifest.json': export / 'export-manifest.json',
        'evidence/raw-inventory.json': export / 'raw-inventory.json',
        **{'summary/' + name: summary / name for name in ('summary.json', 'cells.csv', 'tables.md')},
        'audit/audit.json': audit_path, 'audit/audit.md': Path(str(audit_base) + '.md'),
    }
    for relative, source in extras.items():
        assert relative not in paths and source.is_file() and not source.is_symlink(), relative
        paths[relative] = source
        included.append(dict(path=relative, origin=relative, bytes=source.stat().st_size, sha256=sha(source)))
    included.sort(key=lambda entry: entry['path'])
    manifest = dict(format='sail-large-original-proof-v1', sources=audit['sources'],
                    original_source_root=original['source_root'],
                    source_export_manifest_sha256=sha(export / 'export-manifest.json'),
                    source_audit_sha256=sha(audit_path), source_audit_recorded_utc=audit['recorded_utc'],
                    included=included, omitted=omitted,
                    boundary='Original included diagnostics copied byte-for-byte; Parquet omitted with original sizes and hashes. Independent raw audit was performed on retained originals.')
    manifest_bytes = encoded(manifest)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'manifest.json').write_bytes(manifest_bytes)
    archive_path = output / 'evidence.tar.gz'
    with archive_path.open('wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0, compresslevel=6) as compressed:
            with tarfile.open(mode='w|', fileobj=compressed, format=tarfile.PAX_FORMAT) as archive:
                for relative in sorted([*paths, 'manifest.json']):
                    if relative == 'manifest.json':
                        tar_entry(archive, relative, io.BytesIO(manifest_bytes), len(manifest_bytes))
                    else:
                        source = paths[relative]
                        with source.open('rb') as stream:
                            tar_entry(archive, relative, stream, source.stat().st_size)
    specs = {entry['path']: entry for entry in included}
    specs['manifest.json'] = dict(bytes=len(manifest_bytes), sha256=hashlib.sha256(manifest_bytes).hexdigest())
    matches, seen = [], set()
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive:
            assert member.isfile() and member.name in specs and member.name not in seen, member.name
            spec = specs[member.name]
            assert member.size == spec['bytes']
            with archive.extractfile(member) as stream:
                result = hashlib.sha256()
                for block in iter(lambda: stream.read(1 << 20), b''):
                    result.update(block)
                assert result.hexdigest() == spec['sha256'], member.name
            with archive.extractfile(member) as stream:
                matches.extend(scan_stream(stream, member.name, exact))
            seen.add(member.name)
    assert seen == specs.keys()
    archive_hash = sha(archive_path)
    bundle = dict(generated_utc=datetime.now(timezone.utc).isoformat(), archive_sha256=archive_hash,
                  archive_bytes=archive_path.stat().st_size, included_files=len(included), omitted_files=len(omitted),
                  hash_verification='passed', sources=audit['sources'],
                  deterministic_format='sorted PAX tar; uid/gid/mtime=0; gzip mtime=0, empty filename, level6',
                  python=sys.version, zlib=zlib.ZLIB_VERSION, packager_sha256=sha(Path(__file__)))
    scan = dict(recorded_utc=datetime.now(timezone.utc).isoformat(), archive_sha256=archive_hash,
                scanned_files=len(seen), credential_fields_checked=sorted(exact), pattern_rules=sorted(PATTERNS),
                matches=matches, outcome='failed' if matches else 'passed', scanner_sha256=sha(Path(__file__)),
                boundary='Scanned every decompressed archive file for listed credential patterns and exact nonempty supplied/environment credential values. Match receipts omit values and lines. This is not exhaustive secret detection.')
    (output / 'bundle.json').write_bytes(encoded(bundle))
    (output / 'credential-scan.json').write_bytes(encoded(scan))
    return dict(outcome=scan['outcome'], output=str(output), archive_sha256=archive_hash,
                archive_bytes=bundle['archive_bytes'], included_files=len(included), omitted_files=len(omitted),
                scanned_files=len(seen), matches=matches)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('export', 'summary', 'audit', 'output'):
        parser.add_argument('--' + option, required=True, type=Path)
    parser.add_argument('--credential-file', type=Path)
    args = parser.parse_args()
    result = package(args.export, args.summary, args.audit, args.output, credentials(args.credential_file))
    print(json.dumps(result, indent=2))
    return int(result['outcome'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())

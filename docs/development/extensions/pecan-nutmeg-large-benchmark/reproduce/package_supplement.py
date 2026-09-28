#!/usr/bin/env python3
"""Archive original capacity pilots and the separate timeout control, without Parquet."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile

spec = importlib.util.spec_from_file_location('proof', Path(__file__).with_name('package_graphframes_large.py'))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


def package(inputs, output):
    paths, omitted, originals = {}, [], {}
    for name, root in inputs:
        root = Path(root)
        inventory = proof.read(root / 'export-manifest.json')
        originals[name] = dict(source_root=inventory['source_root'],
                               export_manifest_sha256=proof.sha(root / 'export-manifest.json'))
        for entry in inventory['files']:
            if entry['included']:
                source = proof.safe_path(root, entry['path'])
                assert source.stat().st_size == entry['bytes'] and proof.sha(source) == entry['sha256']
            else:
                assert entry['path'].endswith('.parquet')
                omitted.append(dict(entry, export=name))
        # Sidecars added after the original export are retained and explicitly
        # distinguishable through the original unchanged export inventory.
        for source in sorted(root.rglob('*')):
            if source.is_dir():
                continue
            relative = source.relative_to(root).as_posix()
            source = proof.safe_path(root, relative)
            assert source.suffix != '.parquet', 'supplement must omit graph bytes'
            paths[f'{name}/{relative}'] = source
    included = [dict(path=name, bytes=path.stat().st_size, sha256=proof.sha(path)) for name, path in sorted(paths.items())]
    manifest = dict(format='sail-large-supplement-v1', original_exports=originals, included=included, omitted=omitted,
                    boundary='Original pilot/control diagnostics and sidecars; separate from the audited 180-cell campaign. No new raw-vector audit is claimed for these runs.')
    data = proof.encoded(manifest)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / 'manifest.json').write_bytes(data)
    archive_path = output / 'evidence.tar.gz'
    with archive_path.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0, compresslevel=6) as compressed:
        with tarfile.open(mode='w|', fileobj=compressed, format=tarfile.PAX_FORMAT) as archive:
            for name in sorted([*paths, 'manifest.json']):
                if name == 'manifest.json':
                    proof.tar_entry(archive, name, io.BytesIO(data), len(data))
                else:
                    with paths[name].open('rb') as stream:
                        proof.tar_entry(archive, name, stream, paths[name].stat().st_size)
    expected = {entry['path']: entry for entry in included}
    expected['manifest.json'] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    seen, matches, exact = set(), [], proof.credentials()
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive:
            assert member.isfile() and member.name in expected and member.name not in seen
            entry = expected[member.name]
            assert member.size == entry['bytes']
            with archive.extractfile(member) as stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == entry['sha256']
            with archive.extractfile(member) as stream:
                matches.extend(proof.scan_stream(stream, member.name, exact))
            seen.add(member.name)
    assert seen == expected.keys()
    digest = proof.sha(archive_path)
    bundle = dict(recorded_utc=datetime.now(timezone.utc).isoformat(), archive_sha256=digest,
                  archive_bytes=archive_path.stat().st_size, included_files=len(included), omitted_files=len(omitted),
                  packager_sha256=proof.sha(Path(__file__)), hash_verification='passed')
    scan = dict(recorded_utc=datetime.now(timezone.utc).isoformat(), archive_sha256=digest,
                scanned_files=len(seen), matches=matches, outcome='failed' if matches else 'passed',
                credential_fields_checked=sorted(exact), pattern_rules=sorted(proof.PATTERNS),
                scanner_sha256=proof.sha(Path(proof.__file__)),
                boundary='Every decompressed file scanned; pattern and supplied-value coverage only, not exhaustive secret detection.')
    (output / 'bundle.json').write_bytes(proof.encoded(bundle))
    (output / 'credential-scan.json').write_bytes(proof.encoded(scan))
    assert not matches, json.dumps(matches)
    print(json.dumps(bundle, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export', action='append', required=True, help='NAME=LOCAL_DIRECTORY')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    package([value.split('=', 1) for value in args.export], args.output)

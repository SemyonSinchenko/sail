#!/usr/bin/env python3
"""Build thin fixed-source drivers against pinned, unmodified upstream code."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess

PINS = {'gap': '2972aeb2703165bafd921222f4ed7196f542d3a8',
        'parallel': 'a160e5eaf5bed40e2c2626d6d46e526129b2f274'}
PARLAY = '9e6078709438072b8e46e2922939af0c0555be83'


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_source(path, expected):
    if subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], text=True).strip() != expected:
        raise ValueError(f'wrong source commit: {path}')
    if subprocess.check_output(['git', '-C', str(path), 'status', '--porcelain', '--untracked-files=no'], text=True).strip():
        raise ValueError(f'modified upstream source: {path}')


def build(kind, source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    check_source(source, PINS[kind])
    if kind == 'parallel':
        check_source(source / 'parlaylib', PARLAY)
    adapter = Path(__file__).with_name(f'{kind}_control.cc')
    flags = shlex.split(os.environ.get('CONTROL_CXXFLAGS', '-O3 -fopenmp' if kind == 'gap' else '-O3 -pthread'))
    command = [os.environ.get('CXX', 'c++'), '-std=c++17', *flags, '-I', str(source / 'src' if kind == 'gap' else source)]
    if kind == 'parallel':
        command.extend(['-I', str(source / 'parlaylib/include')])
    command.extend([str(adapter), '-o', str(output)])
    subprocess.run(command, check=True)
    check_source(source, PINS[kind])
    receipt = dict(schema_version=1, control=kind, source_commit=PINS[kind],
                   parlay_commit=PARLAY if kind == 'parallel' else None,
                   built_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   compile_argv=command, compiler_version=subprocess.check_output([command[0], '--version'], text=True).strip(),
                   platform=platform.platform(), machine=platform.machine(),
                   adapter_sha256=sha256(adapter), builder_sha256=sha256(__file__), binary_sha256=sha256(output))
    Path(str(output) + '.build.json').write_text(json.dumps(receipt, sort_keys=True, indent=2) + '\n')
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=PINS)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    print(json.dumps(build(**vars(args)), sort_keys=True, indent=2))

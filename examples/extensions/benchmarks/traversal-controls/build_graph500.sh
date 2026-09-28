#!/usr/bin/env bash
set -euo pipefail
# Usage: CC=cc CFLAGS='-O3 -fopenmp' build_graph500.sh UPSTREAM_CHECKOUT OUTPUT_BINARY
if [[ $# != 2 ]]; then
  echo 'usage: build_graph500.sh UPSTREAM_CHECKOUT OUTPUT_BINARY' >&2
  exit 2
fi
source_dir=$(cd "$1" && pwd)
output=$2
expected=f89d643ce4aaae9a823d310c6ab2dd10e3d2982c
[[ $(git -C "$source_dir" rev-parse HEAD) == "$expected" ]]
[[ -z $(git -C "$source_dir" status --porcelain --untracked-files=no) ]]
script_dir=$(cd "$(dirname "$0")" && pwd)
# CFLAGS is deliberately a compiler-option list, not evaluated shell code.
read -r -a flags <<< "${CFLAGS:--O3}"
command=("${CC:-cc}" -std=c11 -DSSSP "${flags[@]}" -I"$source_dir/generator" \
 "$script_dir/graph500_stream.c" "$source_dir/generator/graph_generator.c" \
 "$source_dir/generator/splittable_mrg.c" "$source_dir/generator/utils.c" -lm -o "$output")
"${command[@]}"
# The fixture preparer requires this receipt and verifies its binary hash. It is
# provenance, not a signature: keep it with the source checkout and gate record.
python3 - "$source_dir" "$output" "$script_dir" "$expected" "${command[@]}" <<'PY'
import datetime
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys

source, output, adapter, commit = map(str, sys.argv[1:5])
command = sys.argv[5:]
def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

assert subprocess.check_output(['git', '-C', source, 'rev-parse', 'HEAD'], text=True).strip() == commit
assert not subprocess.check_output(['git', '-C', source, 'status', '--porcelain', '--untracked-files=no'], text=True).strip()
receipt = dict(
    schema_version=1,
    built_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    generator_repository='https://github.com/graph500/graph500',
    generator_commit=commit,
    generator_sources={p.name: digest(p) for p in sorted((Path(source) / 'generator').glob('*')) if p.is_file()},
    adapter_sha256=digest(Path(adapter) / 'graph500_stream.c'),
    builder_sha256=digest(Path(adapter) / 'build_graph500.sh'),
    binary_sha256=digest(output),
    compiler_version=subprocess.check_output([command[0], '--version'], text=True).strip(),
    compile_argv=command,
    platform=platform.platform(),
    machine=platform.machine(),
)
Path(output + '.build.json').write_text(json.dumps(receipt, sort_keys=True, indent=2) + '\n')
PY

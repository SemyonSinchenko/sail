#!/usr/bin/env bash
# Reproducible local PoC: one host executable, two independent native wheels.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
base="$repo/examples/extensions"
venv=${SAIL_EXTENSION_VENV:-"$repo/.venv"}
target=${SAIL_EXTENSION_TARGET:-"$repo/target/extensions-poc"}
python=${SAIL_EXTENSION_PYTHON:-python3.12}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
df -h "$repo"
if [[ ! -x "$venv/bin/python" ]]; then
    uv venv --python "$python" "$venv"
fi
uv pip sync --python "$venv/bin/python" "$base/requirements.lock"
export PYO3_PYTHON="$venv/bin/python"
if [[ "$(uname -s)" == Darwin ]]; then
    export DYLD_LIBRARY_PATH=$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
fi
"$venv/bin/python" "$base/sedona/scripts/prepare.py"
for package in sedona nutmeg; do
    CARGO_TARGET_DIR="$target/$package" "$venv/bin/python" -m maturin build \
        --manifest-path "$base/$package/Cargo.toml" --locked --profile dev \
        --interpreter "$venv/bin/python" --out "$target/wheels" --auditwheel repair
 done
uv pip install --python "$venv/bin/python" --reinstall "$target"/wheels/*.whl
CARGO_TARGET_DIR="$target/host" cargo build --manifest-path "$repo/Cargo.toml" --locked -p sail-cli
printf 'Host: %s\nPython: %s\nWheels: %s\n' "$target/host/debug/sail" "$venv/bin/python" "$target/wheels"

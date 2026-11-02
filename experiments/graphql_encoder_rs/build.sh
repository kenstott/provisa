#!/bin/bash
# Builds the extension module into build/ (put build/ on PYTHONPATH), and extracts the reference
# product (the exact-values contract, J497b b2b50fcb7) into build/ref for python_ref. Uses the
# worktree venv's interpreter only to read its config; nothing is installed into any venv.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
ref="${REF_SHA:-b2b50fcb77a0a003120a2a0b9f89a47fe5dc9091}"
cd "$here"
PYO3_PYTHON="$root/.venv/bin/python" cargo build --release
mkdir -p build
cp target/release/libgraphql_encoder_rs.dylib build/graphql_encoder_rs.so
if [ ! -f build/ref/.sha ] || [ "$(cat build/ref/.sha)" != "$ref" ]; then
  rm -rf build/ref && mkdir -p build/ref
  git -C "$root" archive "$ref" provisa | tar -x -C build/ref
  echo "$ref" > build/ref/.sha
fi

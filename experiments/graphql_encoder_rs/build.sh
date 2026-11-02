#!/bin/bash
# Builds the extension module into build/ (put build/ on PYTHONPATH). Uses the worktree venv's
# interpreter only to read its config; nothing is installed into any venv.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$here"
PYO3_PYTHON="$root/.venv/bin/python" cargo build --release
mkdir -p build
cp target/release/libgraphql_encoder_rs.dylib build/graphql_encoder_rs.so

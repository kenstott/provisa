#!/bin/bash
# Builds the extension module into build/ (put build/ on PYTHONPATH). Uses the worktree venv's
# interpreter only to read its config; nothing is installed into any venv.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$here"
# The benchmark build is tuned for this CPU. RUSTFLAGS replaces .cargo/config.toml's flags, so it
# carries the link arguments too. The shipped wheels are built without target-cpu (pyproject.toml).
RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup -C target-cpu=native" \
  PYO3_PYTHON="$root/.venv/bin/python" cargo rustc --release --lib --crate-type cdylib
mkdir -p build
cp target/release/libpgwire_encoder_rs.dylib build/pgwire_encoder_rs.so

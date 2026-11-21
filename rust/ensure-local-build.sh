#!/bin/bash
# Builds provisa._encoders with rust/build-local.sh when provisa/_encoders.so is missing or older
# than any Rust source under rust/ (a Cargo.toml, Cargo.lock, build.rs, rust/.cargo/config.toml, or
# a file under a src/ or third_party/ directory); otherwise does nothing. The start scripts that run
# the API from a source checkout call it before the API starts: pgwire, gRPC and JSON encode through
# the module, with no Python fallback, so a missing cargo or a failed build stops the start.
#   rust/ensure-local-build.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
so="$root/provisa/_encoders.so"

if [ -f "$so" ]; then
  newer="$(find "$root/rust" -name target -prune -o -type f \
    \( -name Cargo.toml -o -name Cargo.lock -o -name build.rs -o -path '*/.cargo/config.toml' \
       -o -path '*/src/*' -o -path '*/third_party/*' \) -newer "$so" -print -quit)"
  [ -z "$newer" ] && exit 0
  echo "provisa._encoders is older than $newer: rebuilding it (rust/build-local.sh)"
else
  echo "provisa._encoders is not built ($so is missing): building it (rust/build-local.sh)"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo not found. provisa._encoders (the Rust encoders pgwire, gRPC and JSON use)" >&2
  echo "       must be built from rust/ to run from source. Install Rust (https://rustup.rs or" >&2
  echo "       'brew install rust') and start again." >&2
  exit 1
fi
if ! "$root/rust/build-local.sh"; then
  echo "ERROR: building provisa._encoders failed (rust/build-local.sh, output above);" >&2
  echo "       $so was not built. Fix the build and start again." >&2
  exit 1
fi

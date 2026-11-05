#!/bin/bash
# Builds provisa._encoders (rust/encoders, with rust/pgwire linked in) for this machine and copies
# the module to <out dir>/_encoders.so (default: provisa/, where an editable install puts it; git
# ignores it there). A scratch <out dir> keeps the build out of the tree: link
# provisa/_encoders.so to it. Nothing is installed into any venv.
#   rust/build-local.sh [out dir]        NATIVE=1 tunes the build for this CPU (benchmarks only;
#                                        the shipped wheels are built without target-cpu).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-$root/provisa}"
cd "$root/rust/encoders"
if [ "${NATIVE:-0}" = 1 ]; then
  # RUSTFLAGS replaces rust/.cargo/config.toml's flags, so it carries the link arguments too.
  export RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup -C target-cpu=native"
fi
PYO3_PYTHON="${PYO3_PYTHON:-$root/.venv/bin/python}" cargo build --release --lib
mkdir -p "$out"
for lib in target/release/lib_encoders.dylib target/release/lib_encoders.so; do
  if [ -f "$lib" ]; then cp "$lib" "$out/_encoders.so"; exit 0; fi
done
echo "no lib_encoders built" >&2
exit 1

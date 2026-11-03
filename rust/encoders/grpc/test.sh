#!/bin/bash
# The gRPC encoder crate's own tests (cargo test). The crate is a Python extension (pyo3's
# extension-module), which leaves libpython unlinked; the test binary links the venv's.
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
py="${PYO3_PYTHON:-$root/.venv/bin/python}"
libdir="$("$py" -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")"
version="$("$py" -c "import sysconfig; print(sysconfig.get_config_var('LDVERSION'))")"
cd "$(dirname "$0")"
RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup -L $libdir -l python$version" \
  DYLD_LIBRARY_PATH="$libdir" LD_LIBRARY_PATH="$libdir" PYO3_PYTHON="$py" cargo test --release "$@"

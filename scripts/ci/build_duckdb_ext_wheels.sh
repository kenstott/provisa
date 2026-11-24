#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# Build provisa-duckdb-ext's wheels from a mirror (REQ-294): one wheel per version part
# <release>.<part> per platform label, each tagged for its platform and holding only that part of
# that platform's blobs (packaging/duckdb-ext/collect.py).
#
# Usage: build_duckdb_ext_wheels.sh <mirror-root> <out-dir> [label ...]
#   <mirror-root>/<label>/  what scripts/ci/mirror_duckdb_extensions.py wrote for that platform
#   label ...               the platforms to build; none named builds every one collect.py knows
#
# Called by build-duckdb-extensions.yml (every platform, the release's wheels) and by
# .github/actions/duckdb-ext-wheels (the runner's own platform, for the images CI builds).
set -euo pipefail

mirror=$1
out=$2
shift 2
collect=packaging/duckdb-ext/collect.py
if [ "$#" -eq 0 ]; then
  # shellcheck disable=SC2046
  set -- $(python "$collect" labels)
fi

# set-version rewrites the project's version for each part; the checkout gets its own back, and
# loses the per-label build directories (an image built from the checkout would send them).
pyproject=packaging/duckdb-ext/pyproject.toml
saved=$(mktemp)
cp "$pyproject" "$saved"
trap 'cp "$saved" "$pyproject"; rm -rf "$saved" wheel-build' EXIT

mkdir -p "$out"
for part in $(python "$collect" parts); do
  python "$collect" set-version "$part"
  for label in "$@"; do
    python "$collect" collect "$mirror" "$label" "$part"
    # setuptools reuses build/ and *.egg-info between builds: the previous platform's blobs
    # would ride along in this one's wheel.
    rm -rf packaging/duckdb-ext/build packaging/duckdb-ext/*.egg-info "wheel-build/$label"
    python -m build --wheel --outdir "wheel-build/$label" packaging/duckdb-ext
    python -m wheel tags --remove \
      --platform-tag "$(python "$collect" wheel-tag "$label")" \
      "wheel-build/$label"/provisa_duckdb_ext-*-py3-none-any.whl
    mv "wheel-build/$label"/*.whl "$out"/
  done
done
ls -l "$out"

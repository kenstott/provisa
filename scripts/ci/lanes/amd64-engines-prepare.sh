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
# The amd64-only engines lane, before its tests (lane.yml `prepare`).
set -euo pipefail

# docker-compose.core.yml bind-mounts ./sharepoint.pfx into Trino. The file is a real
# certificate on a developer box and is gitignored, so it is absent here — and Docker
# silently creates a DIRECTORY in place of a missing bind-mount source, leaving an
# untracked directory named like a cert in the checkout. Create the empty file instead;
# no catalog in this lane reads it.
touch sharepoint.pfx

arch="$(uname -m)"
echo "runner arch: $arch"
# A silent arch change here would turn every test in this lane into a skip, i.e. a
# green run that executed nothing — exactly what this workflow exists to prevent.
[ "$arch" = "x86_64" ] || { echo "::error::amd64 lane running on $arch"; exit 1; }

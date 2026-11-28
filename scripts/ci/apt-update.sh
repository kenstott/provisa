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
# `apt-get update` that does not depend on the runner image's preconfigured Microsoft source.
# The image (ubuntu 26.04 "resolute") ships packages.microsoft.com, and a 403 from it fails the
# update (run 45900004821) although the step installs nothing from it. The steps that call this
# install only Ubuntu packages; the mssql-odbc action, which needs msodbcsql18, refreshes its own
# list and does not use this script.
set -euo pipefail

sudo rm -f /etc/apt/sources.list.d/microsoft-prod.list /etc/apt/sources.list.d/microsoft-prod.sources
# Any other list or deb822 file that names Microsoft's repository.
for f in $(sudo grep -rlE 'packages\.microsoft\.com' /etc/apt/sources.list.d/ /etc/apt/sources.list 2>/dev/null || true); do
  sudo rm -f "$f"
done
sudo apt-get "$@" update -q

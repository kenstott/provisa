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
# A container-suite lane, before it runs (lane.yml `prepare`, given the lane's name): how many
# stacks this runner holds.
set -euo pipefail

uv run python -c "from tests.itest_stack import concurrent_session_limit as n; \
  print('docker stack slots on this runner:', n()); assert n() >= 1"

# REQ-1992: tests/integration/test_org_folder_crypt.py mounts an org's encrypted folder with
# gocryptfs over FUSE, as a node does.
sudo apt-get update -qq
sudo apt-get install -y -qq gocryptfs fuse3

# REQ-2016: the vector lane's tests embed with a local sentence-transformers model, the vector
# extra. Of the container-suite lanes it is installed in those that select a requires_vector test
# alone (scripts/ci/install_vector_extra.sh, CPU-only torch). The warehouse lane is one: a demo
# test on a live warehouse that embeds locally (the audit demo's) carries both markers, and only
# the warehouse lane selects it. The UI lanes install it too (ui-core-prepare.sh).
case "${1:-}" in vector | warehouse)
  bash scripts/ci/install_vector_extra.sh
  ;;
esac

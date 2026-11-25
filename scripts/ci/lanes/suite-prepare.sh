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
# extra. Every lane's `uv sync --extra dev` already has it (dev carries provisa[vector], torch from
# PyTorch's CPU-only index through [tool.uv.sources]); the lanes that select a requires_vector test
# check it here, with the requirements the sonnets demo's demo.yaml names. The warehouse lane is
# one: a demo test on a live warehouse that embeds locally (the audit demo's) carries both markers,
# and only the warehouse lane selects it.
case "${1:-}" in vector | warehouse)
  uv pip install --index-url https://download.pytorch.org/whl/cpu torch
  # The extra's own requirements, as the sonnets demo's demo.yaml names it.
  # shellcheck disable=SC2046
  uv pip install $(uv run python scripts/demo_extras.py demo/named/sonnets)
  uv run python -c "import sentence_transformers, torch; print('vector extra, torch', torch.__version__)"
  ;;
esac

# REQ-294: a running Provisa never fetches a connector bundle, and an AskAmerica source's server is
# the pgwire-govdata bundle (pgwire_bundles.py CONNECTOR_RELEASE["govdata"]). The warehouse lane
# runs the AskAmerica tests (test_govdata_source.py, test_govdata_trino_source_e2e.py) with the
# ASKAMERICA_API_KEY its job requires, so it stages the bundle here, from the pinned GitHub release,
# as the salesforce and servicenow jobs stage theirs. Unstaged, the source is refused with
# BundleNotStaged.
if [ "${1:-}" = warehouse ]; then
  uv run --no-sync python -m provisa.runtime_deps.stage_pgwire_bundles --only govdata --github-release
fi

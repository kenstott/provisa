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
# ci-leaf-check.yml's lane, after its command whatever its outcome (lane.yml `after`): leaves a
# mark, and runs the not-executed report on a small made-up Playwright report.
set -euo pipefail

mkdir -p "results/$LANE_SLUG"
echo "after ran; secret=${ANTHROPIC_API_KEY:-empty}" > "results/$LANE_SLUG/after.txt"
python3 scripts/ci/lanes/playwright_not_executed.py scripts/ci/lanes/leafcheck-playwright.json \
  > "results/$LANE_SLUG/not-executed.txt"

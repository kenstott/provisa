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
# ci-leaf-check.yml's lane, when its command failed (lane.yml `on-failure`).
set -euo pipefail

mkdir -p "results/$LANE_SLUG"
echo "the lane failed and this ran" > "results/$LANE_SLUG/on-failure.txt"

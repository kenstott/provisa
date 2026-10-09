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
# ci-leaf-check.yml's lane, before its command (lane.yml `prepare`): leaves a mark the check
# reads back from the lane's uploaded results, with the argument it was given and what the
# lane's environment carried.
set -euo pipefail

mkdir -p "results/$LANE_SLUG"
echo "prepared with: ${1:-no argument}" > "results/$LANE_SLUG/prepare.txt"
echo "LEAF_ONE=${LEAF_ONE:-unset} secret=${ANTHROPIC_API_KEY:-empty}" >> "results/$LANE_SLUG/prepare.txt"

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
# A demo reset's model history (start-ui-install.sh --demo). The reset returns the demo to its first
# start: its control-plane database is dropped, and every environment with it (REQ-1535). Each org's
# model repository (<data dir>/orgs/<org>/repo.git) holds those environments' branches; left, the
# next start's environments meet branches with no environment behind them and are refused by name
# (REQ-1554). So they go too -- from the demo's own data directory, the one argument, and nothing
# else under it.
#
# Usage: demo_reset_model_history.sh DEMO_DATA_DIR (the demo data directory)
set -euo pipefail

data_dir="${1:?usage: demo_reset_model_history.sh DEMO_DATA_DIR}"
shopt -s nullglob
for repo in "$data_dir"/orgs/*/repo.git; do
  rm -rf -- "$repo"
done

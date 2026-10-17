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
# The test instance's store credentials (tests/store_credentials.py, their one home) as KEY=VALUE
# lines: appended to $GITHUB_ENV by the CI lanes, so every compose command a lane runs outside
# pytest (its prepare, its on-failure dump, a build) interpolates docker-compose.core.yml with them.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
python3 -c 'from tests.store_credentials import STORE_ENV
for name, value in STORE_ENV.items():
    print(f"{name}={value}")'

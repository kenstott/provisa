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
# What the Trino-backed UI lanes leave behind when they fail (lane.yml `on-failure`).
set -euo pipefail

docker ps -a
docker compose -f docker-compose.core.yml logs --tail=200
# A connector that fails to instantiate does so minutes before the spec's wait expires,
# so the tail above scrolls the stack trace off. Keep the whole Trino log as an artifact.
docker compose -f docker-compose.core.yml logs --no-color trino > trino-container.log

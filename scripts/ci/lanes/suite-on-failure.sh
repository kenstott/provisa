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
# A container-suite lane's containers when it fails (lane.yml `on-failure`).
set -euo pipefail

docker ps -a --format '{{.Names}}\t{{.Status}}'
for c in $(docker ps -a --filter health=unhealthy --filter status=exited --format '{{.Names}}'); do
  echo "=== $c"; docker logs --tail 200 "$c" 2>&1
done

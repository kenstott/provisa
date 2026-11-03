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
# A Conformance (functions) leg's engine server (REQ-2167): the docker-compose.test.yml service the
# integration tests start for that engine (tests/conftest.py _MARKER_SERVICES), in a compose project
# of its own, on the test instance's store credentials (scripts/ci/test-store-env.sh).
#
#   engine_service.sh up <service> <wait-seconds>   start it and wait for its healthcheck, if any
#   engine_service.sh logs <service> <file>         its log, for the leg's failure reason
#
# The leg then waits for the engine to answer SQL over its engine URL (function_legs.py wait).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

project=conformance-functions
compose=(docker compose -p "$project" -f docker-compose.core.yml -f docker-compose.test.yml)

if [ -z "${PROVISA_PG_PASSWORD:-}" ]; then
  set -a
  # shellcheck disable=SC1090
  . <(bash scripts/ci/test-store-env.sh)
  set +a
fi

case "${1:-}" in
  up)
    service=$2
    wait_seconds=$3
    "${compose[@]}" pull --quiet "$service"
    "${compose[@]}" up -d --wait --wait-timeout "$wait_seconds" "$service"
    ;;
  logs)
    "${compose[@]}" logs --no-color --tail 200 "$2" > "$3" 2>&1 || true
    "${compose[@]}" ps --all "$2" >> "$3" 2>&1 || true
    ;;
  *)
    echo "usage: $0 up <service> <wait-seconds> | logs <service> <file>" >&2
    exit 2
    ;;
esac

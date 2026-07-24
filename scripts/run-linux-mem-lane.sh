#!/usr/bin/env bash
# linux-mem lane: run the RLIMIT_AS memory-bounded tests for real on a macOS host.
#
# tests/integration/test_streaming_memory_bounded_e2e.py @linux_only-skips on macOS because
# Darwin ignores RLIMIT_AS for the large pyarrow/libpq arena maps the streaming paths use. On
# a Linux CI host the default/core lane already runs them natively, so this lane is a no-op
# there. On macOS it runs the test process inside an arm64-native Linux container (the only way
# RLIMIT_AS discriminates) against the SAME postgres service the isolated-stack harness uses —
# docker-compose.core.yml, brought up here via docker compose, NOT a hand-rolled initdb. The
# test connects to it over host.docker.internal; PROVISA_MEM_TEST_DSN also makes it skip
# embedded pgserver (no linux/aarch64 wheel).
#
# Own compose project name (provisa-mem) so this never tears down a concurrently-running
# test-all's provisa-itest stack. Extra pytest args are forwarded to the in-container run.
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "linux-mem lane: host is Linux — default/core lane runs these natively; skipping"
  exit 0
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "linux-mem lane: docker CLI not found; cannot run the Linux memory tests" >&2
  exit 1
fi

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
IMG=provisa-mem-base:local
PROJECT=provisa-mem
COMPOSE=(docker compose -p "$PROJECT" -f docker-compose.core.yml -f docker-compose.test.yml)

case "$(uname -m)" in
  arm64|aarch64) PLAT=linux/arm64 ;;
  x86_64|amd64)  PLAT=linux/amd64 ;;
  *) echo "linux-mem lane: unsupported arch $(uname -m)" >&2; exit 1 ;;
esac

# Publish the harness postgres on a free host port (compose maps ${PG_PORT}:5432).
PG_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("",0));print(s.getsockname()[1]);s.close()')
export PG_PORT

cleanup() { "${COMPOSE[@]}" down --volumes >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "linux-mem lane: building $IMG ($PLAT)"
docker build --platform "$PLAT" -f "$REPO/docker/mem-lane.Dockerfile" -t "$IMG" "$REPO"

echo "linux-mem lane: starting harness postgres (project $PROJECT, host port $PG_PORT)"
"${COMPOSE[@]}" up -d --wait postgres

echo "linux-mem lane: running memory tests in container against host.docker.internal:$PG_PORT"
# --memory=6g: materialized-path test peaks ~1.6 GiB; give headroom without letting a
#   regression balloon unbounded. -v mounts the live source tree so no rebuild per edit.
docker run --rm --platform "$PLAT" --memory=6g \
  -v "$REPO":/repo -w /repo \
  -e PROVISA_MEM_TEST_DSN="postgresql://provisa:provisa@host.docker.internal:${PG_PORT}/provisa" \
  "$IMG" bash /repo/scripts/_mem-lane-entrypoint.sh "$@"

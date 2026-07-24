#!/usr/bin/env bash
# linux-mem lane: run the RLIMIT_AS memory-bounded tests for real on a macOS host.
#
# tests/integration/test_streaming_memory_bounded_e2e.py @linux_only-skips on macOS because
# Darwin ignores RLIMIT_AS for the large pyarrow/libpq arena maps the streaming paths use. On
# a Linux CI host the default/core lane already runs them natively, so this lane is a no-op
# there. On macOS it runs them inside an arm64-native Linux container against a real apt
# PostgreSQL (docker/mem-lane.Dockerfile + scripts/_mem-lane-entrypoint.sh).
#
# Extra pytest args are forwarded to the in-container run.
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
IMG=provisa-mem-base:local

case "$(uname -m)" in
  arm64|aarch64) PLAT=linux/arm64 ;;
  x86_64|amd64)  PLAT=linux/amd64 ;;
  *) echo "linux-mem lane: unsupported arch $(uname -m)" >&2; exit 1 ;;
esac

echo "linux-mem lane: building $IMG ($PLAT)"
docker build --platform "$PLAT" -f "$REPO/docker/mem-lane.Dockerfile" -t "$IMG" "$REPO"

echo "linux-mem lane: running memory tests in container"
# --memory=6g: materialized-path test peaks ~1.6 GiB; give headroom without letting a
#   regression balloon unbounded. -v mounts the live source tree so no rebuild per edit.
exec docker run --rm --platform "$PLAT" --memory=6g \
  -v "$REPO":/repo -w /repo \
  "$IMG" bash /repo/scripts/_mem-lane-entrypoint.sh "$@"

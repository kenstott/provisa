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
# Device 1 of the three-device optimistic benchmark (../METHODOLOGY.md): Provisa only. Every
# backend — the sources, the engine (pg by default; duckdb runs in-process here), the control
# plane, Redis and the telemetry pipeline — is on device 3. Runs until interrupted (SIGINT/SIGTERM),
# then stops Provisa and the host agent. Started through `orchestrate.sh serve`.
#
# Usage: serve_three_device.sh --telemetry off|on [--engine pg|duckdb|trino]
# Needs:
#   PROVISA_BENCH_BACKENDS_HOST  device 3's private address
#   PROVISA_BENCH_AGENT_LISTEN   HOST:PORT for host_agent.py, device 1's private address
# Optional:
#   PROVISA_WORKERS (default: this host's hardware threads), PROVISA_REQUEST_THREADS
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
PYTHON_BIN="$REPO_ROOT/.venv/bin/python3"
API_PORT=8001

ENGINE=pg
TELEMETRY=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --engine) ENGINE="$2"; shift 2 ;;
    --telemetry) TELEMETRY="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
[ -n "$TELEMETRY" ] || { echo "--telemetry off|on is required"; exit 2; }
: "${PROVISA_BENCH_BACKENDS_HOST:?the private address of device 3}"
: "${PROVISA_BENCH_AGENT_LISTEN:?HOST:PORT for the host agent, a private address}"
WORKERS="${PROVISA_WORKERS:-$(nproc)}"
STATE_DIR="${PROVISA_BENCH_STATE_DIR:-$HOME/.provisa-bench}"
mkdir -p "$STATE_DIR"

_threads=()
[ -n "${PROVISA_REQUEST_THREADS:-}" ] && _threads=(--request-threads "$PROVISA_REQUEST_THREADS")
eval "$("$PYTHON_BIN" "$SCRIPT_DIR/three_device.py" serve-env \
  --engine "$ENGINE" --telemetry "$TELEMETRY" --backends-host "$PROVISA_BENCH_BACKENDS_HOST" \
  --workers "$WORKERS" "${_threads[@]}" --config-out "$STATE_DIR/config-$TELEMETRY.yaml")"
if [ "$TELEMETRY" = off ]; then
  unset OTEL_EXPORTER_OTLP_ENDPOINT
fi

ulimit -n "$(ulimit -Hn)"
# Several workers on Linux: each opens its own SO_REUSEPORT socket on the public port so the
# kernel spreads connections over them (REQ-1900); uvicorn's supervisor gets a unix socket.
export PROVISA_HTTP_LISTEN="0.0.0.0:$API_PORT"
# Each server process records its collections and stalls here (stall_probe.py), for the host
# agent's /stalls; a fresh directory per serve.
STALL_DIR="$STATE_DIR/stalls-$TELEMETRY"
rm -rf "$STALL_DIR"
mkdir -p "$STALL_DIR"
( cd "$REPO_ROOT" && PROVISA_BENCH_STALL_DIR="$STALL_DIR" \
    PYTHONPATH="$SCRIPT_DIR/stallprobe${PYTHONPATH:+:$PYTHONPATH}" \
    exec "$REPO_ROOT/.venv/bin/uvicorn" main:app --workers "$WORKERS" \
    --uds "$STATE_DIR/provisa-supervisor.sock" ) > "$STATE_DIR/provisa.log" 2>&1 &
PROVISA_PID=$!
AGENT_PID=""
stop() {
  [ -n "$AGENT_PID" ] && kill "$AGENT_PID" 2>/dev/null || true
  kill -INT "$PROVISA_PID" 2>/dev/null || true
  wait "$PROVISA_PID" 2>/dev/null || true
}
trap stop EXIT INT TERM

# Ready when every worker serves: /health reports workers.ready of workers.expected (REQ-1900).
deadline=$(( $(date +%s) + 420 ))
until seen="$(curl -sf "http://localhost:$API_PORT/health" | "$PYTHON_BIN" -c '
import json, sys
w = json.load(sys.stdin)["workers"]
print(w["ready"], w["expected"])' 2>/dev/null)" && [ "${seen% *}" -ge "${seen#* }" ]; do
  if [ "$(date +%s)" -ge "$deadline" ] || ! kill -0 "$PROVISA_PID" 2>/dev/null; then
    echo "Provisa did not serve with every worker — see $STATE_DIR/provisa.log"
    exit 1
  fi
  sleep 2
done
echo "Provisa serving: $WORKERS workers, engine $ENGINE, telemetry $TELEMETRY"

"$PYTHON_BIN" "$SCRIPT_DIR/host_agent.py" --listen "$PROVISA_BENCH_AGENT_LISTEN" \
  --pid "$PROVISA_PID" --fact "telemetry=$TELEMETRY" --fact "engine=$ENGINE" \
  --fact "workers=$WORKERS" --fact "request_threads=${PROVISA_REQUEST_THREADS:-default}" \
  --rtt-target "backends=$PROVISA_BENCH_BACKENDS_HOST:25632" --stall-dir "$STALL_DIR" &
AGENT_PID=$!
wait "$PROVISA_PID"

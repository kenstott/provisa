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
# Full duckdb/pg/trino benchmark sweep: for each engine, start `start-ui-install.sh --demo perf`
# fresh with PROVISA_ENGINE set, wait for readiness, run run_benchmark.py + the Artillery
# saturation test, then stop Provisa (SIGINT — the same signal Ctrl+C sends, so its own
# `trap cleanup EXIT INT TERM` (start-ui-install.sh:830) tears everything down properly) before
# moving to the next engine.
#
# YOU run this script yourself — it starts/stops/restarts Provisa (local-dev) repeatedly by
# design, which is exactly what an AI assistant working on this repo must never do. That's why
# this exists as a script instead of a series of assistant-run commands.
#
# The demo/named/perf/docker-compose.yml data stack (4 DB containers) is assumed to already be
# up and seeded (`docker compose -f demo/named/perf/docker-compose.yml up -d`, then wait for the
# `seeder` service to exit 0). This script never touches that stack — only start-ui-install.sh's
# own process/Docker lifecycle for Provisa itself, once per engine.
#
# Usage:
#   ./orchestrate.sh [engine ...]
#   (engines default to: duckdb pg trino)
#
# No login/credentials: --demo perf runs with auth.provider: none (config/provisa-install*.yaml,
# checked in) — every request is auto-identified with no token at all (verified in
# provisa/auth/middleware.py:417-441). A demo's whole point is a clean, predictable reset every
# time, including that posture, so this script doesn't add a login step that doesn't exist.
#
# Each engine's results land in results/<engine>/ (run_benchmark.py's JSON report +
# artillery-saturation.json), so a run can be repeated for one engine without disturbing the
# others' results.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
LOG_DIR="$SCRIPT_DIR/orchestrate-logs"
mkdir -p "$LOG_DIR"

API_PORT="${PROVISA_API_PORT:-8001}"
HTTP_BASE_URL="http://localhost:$API_PORT"
# run_benchmark.py needs psycopg2/neo4j/httpx/pyarrow, only installed in the repo's own venv —
# the bare `python3` on PATH is the system interpreter and has none of them (confirmed live: all
# 4 transports reported UNAVAILABLE with ModuleNotFoundError, producing an empty results matrix).
PYTHON_BIN="$REPO_ROOT/.venv/bin/python3"
if [ ! -x "$PYTHON_BIN" ]; then
  echo "Expected venv python at $PYTHON_BIN (not found/executable)"
  exit 1
fi

if [ "$#" -gt 0 ]; then
  ENGINES=("$@")
else
  ENGINES=(duckdb pg trino)
fi

for e in "${ENGINES[@]}"; do
  case "$e" in
    duckdb|pg|trino) ;;
    *) echo "Unknown engine: $e (must be duckdb, pg, or trino)"; exit 1 ;;
  esac
done

wait_for_ready() {
  # 420s: matches PROVISA_ENGINE_READY_TIMEOUT (provisa/ui_server.py) — the backend's own budget
  # for startup phases past pg+schema+seed (flight/minio/results infra, MCP/gRPC readiness, etc.),
  # observed live taking well past 18s on a native boot. 180s cut off mid-phase with no error.
  local deadline=$(( $(date +%s) + 420 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -sf "$HTTP_BASE_URL/health" > /dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

stop_provisa() {
  local pid="$1"
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  kill -INT "$pid" 2>/dev/null || true
  local deadline=$(( $(date +%s) + 60 ))
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "start-ui-install.sh did not exit within 60s of SIGINT — sending SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
      break
    fi
    sleep 2
  done
  wait "$pid" 2>/dev/null || true
}

# Kill any Provisa left running from a previous ungraceful stop (e.g. this script killed
# mid-round) or a manual start the caller forgot about — makes every round start from a known
# state regardless of what was running before. Tries start-ui-install.sh's own processes by name
# first (SIGINT, so its real cleanup trap runs — Docker teardown etc., not just the process), then
# falls back to whatever still holds the API port, same as start-ui-install.sh's own startup does
# for itself (see its "Stopping any previous UI/backend processes" step).
kill_stale_instance() {
  local stale_pids
  stale_pids="$(pgrep -f 'start-ui-install\.sh' || true)"
  if [ -n "$stale_pids" ]; then
    echo "Found a Provisa instance already running (pid(s): $stale_pids) — stopping it first..."
    echo "$stale_pids" | xargs -r kill -INT 2>/dev/null || true
    local deadline=$(( $(date +%s) + 30 ))
    while [ -n "$(pgrep -f 'start-ui-install\.sh' || true)" ] && [ "$(date +%s)" -lt "$deadline" ]; do
      sleep 2
    done
    pgrep -f 'start-ui-install\.sh' | xargs -r kill -9 2>/dev/null || true
  fi
  local port_pids
  port_pids="$(lsof -ti ":$API_PORT" 2>/dev/null || true)"
  if [ -n "$port_pids" ]; then
    echo "Port $API_PORT still held (pid(s): $port_pids) — killing"
    echo "$port_pids" | xargs -r kill -9 2>/dev/null || true
  fi
}

# CPU steal-time logging (Linux only — steal time is a hypervisor-visibility concept `vmstat`
# only reports under Linux; no-ops elsewhere). Answers "was this cloud VM's result polluted by
# noisy-neighbor contention" with data instead of assumption — a real concern on a shared
# (non-sole-tenant) cloud node, checked live and found clean at idle, but idle isn't the phase
# that matters; this covers the whole benchmark window including the CPU-saturating ramp.
start_steal_logging() {
  local out_dir="$1"
  mkdir -p "$out_dir"
  STEAL_LOG="$out_dir/steal_time.log"
  STEAL_PID=""
  if [ "$(uname -s)" = "Linux" ] && command -v vmstat > /dev/null 2>&1; then
    vmstat -t 1 > "$STEAL_LOG" 2>&1 &
    STEAL_PID=$!
  fi
}

stop_steal_logging() {
  if [ -n "${STEAL_PID:-}" ]; then
    kill "$STEAL_PID" 2>/dev/null || true
    wait "$STEAL_PID" 2>/dev/null || true
  fi
  if [ -n "${STEAL_LOG:-}" ] && [ -f "$STEAL_LOG" ]; then
    # `st` is vmstat's steal-time column; position varies by version, so find it by header
    # rather than a fixed index.
    local col
    col="$(awk '/^procs|--cpu--/{for(i=1;i<=NF;i++) if($i=="st"){print i; exit}}' "$STEAL_LOG")"
    if [ -n "$col" ]; then
      local max_steal
      max_steal="$(awk -v c="$col" 'NR>2 && $c ~ /^[0-9]+$/ {if($c>m) m=$c} END{print m+0}' "$STEAL_LOG")"
      echo "CPU steal time this round: max ${max_steal}% (log: $STEAL_LOG)"
      if [ "$max_steal" -gt 1 ] 2>/dev/null; then
        echo "WARNING: steal time exceeded 1% — results this round may be polluted by noisy-neighbor contention on this cloud node"
      fi
    fi
  fi
}

# set -e means a failure in any step below (run_benchmark.py, artillery) aborts the script
# immediately, before the explicit stop_provisa call at the bottom of the loop would run — this
# trap guarantees Provisa still gets stopped (not left orphaned in the background) no matter how
# a round ends. Harmless to fire again at normal script exit (stop_provisa is a no-op once the
# pid is already dead).
trap 'stop_provisa "${provisa_pid:-}"' EXIT

for engine in "${ENGINES[@]}"; do
  echo ""
  echo "=== engine=$engine ==="
  start_log="$LOG_DIR/start-$engine.log"

  kill_stale_instance
  echo "Starting Provisa (PROVISA_ENGINE=$engine, --demo perf)... log: $start_log"
  # cd into REPO_ROOT first: start-ui-install.sh (and code it shells out to, e.g. db/init.sql
  # for the embedded control plane) resolves relative paths against the CALLER's cwd, not its
  # own script directory — invoking it by absolute path alone while cwd is still bench/ breaks
  # those (confirmed live: "FileNotFoundError: db/init.sql").
  # exec (not just a plain invocation) so the subshell's process image becomes start-ui-install.sh
  # itself — $! then captures the actual script's pid, so stop_provisa's SIGINT reaches its real
  # `trap cleanup EXIT INT TERM` directly rather than an intermediate subshell wrapper.
  ( cd "$REPO_ROOT" && PROVISA_ENGINE="$engine" exec ./start-ui-install.sh --demo perf ) > "$start_log" 2>&1 &
  provisa_pid=$!

  if ! wait_for_ready; then
    echo "Provisa did not become healthy within 180s (engine=$engine) — see $start_log"
    stop_provisa "$provisa_pid"
    exit 1
  fi
  echo "Provisa ready at $HTTP_BASE_URL"

  mkdir -p "$SCRIPT_DIR/results/$engine"
  start_steal_logging "$SCRIPT_DIR/results/$engine"

  echo "Running multi-transport benchmark..."
  ( cd "$SCRIPT_DIR" && "$PYTHON_BIN" run_benchmark.py \
      --engine "$engine" \
      --http-base-url "$HTTP_BASE_URL" )

  echo "Running Artillery saturation test..."
  ( cd "$SCRIPT_DIR" && PROVISA_HTTP_BASE_URL="$HTTP_BASE_URL" \
      npx artillery run --output "results/$engine/artillery-saturation.json" \
      artillery-concurrency-ramp.yml )

  stop_steal_logging

  echo "Stopping Provisa (engine=$engine)..."
  stop_provisa "$provisa_pid"
  echo "=== engine=$engine done ==="
done

echo ""
echo "Sweep complete. Results: $SCRIPT_DIR/results/{${ENGINES[*]}}/"

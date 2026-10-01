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

# One Python process executes on one core at a time, so the server under test runs a worker
# process per core; a single `uvicorn --reload` process caps every transport at one core no matter
# how many request threads it has (measured: a cached GraphQL hit held ~60 req/s flat from 1 to 200
# clients). Override with PROVISA_WORKERS=N.
export PROVISA_WORKERS="${PROVISA_WORKERS:-$(nproc)}"

# Workers share one response cache: the `redis` service of this demo's compose stack. The native
# demo's default fakeredis is private to each process.
BENCH_REDIS_PORT="${PROVISA_BENCH_REDIS_PORT:-26379}"
export PROVISA_SHARED_REDIS_URL="redis://localhost:$BENCH_REDIS_PORT"
if ! (exec 3<>"/dev/tcp/127.0.0.1/$BENCH_REDIS_PORT") 2>/dev/null; then
  echo "No Redis on localhost:$BENCH_REDIS_PORT — start it: docker compose -p perf -f $SCRIPT_DIR/../docker-compose.yml up -d redis"
  exit 1
fi

# Trace collector: in production the collector and the trace database run on other machines, so
# on this one VM they are replaced by a no-op OTLP receiver (noop_otlp_receiver.py): Provisa still
# serializes and exports every batch, and nothing downstream of the wire competes for the vCPUs.
# It listens where `--demo` points the exporters (OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4319).
# The real collector must not be running: PROVISA_OPS_DB_URL (the trace database otlp2sql writes)
# is unset for the server, and a foreign listener on the port stops the run.
OTLP_PORT=4319
unset PROVISA_OPS_DB_URL
export PROVISA_BENCH_TRACE_COLLECTOR="noop"
NOOP_COLLECTOR_PID=""
start_noop_collector() {
  if (exec 3<>"/dev/tcp/127.0.0.1/$OTLP_PORT") 2>/dev/null; then
    echo "Port $OTLP_PORT already has a listener (a real trace collector?) — stop it; the benchmark runs against the no-op receiver only"
    exit 1
  fi
  "$PYTHON_BIN" "$SCRIPT_DIR/noop_otlp_receiver.py" "$OTLP_PORT" > "$LOG_DIR/noop-otlp-receiver.log" 2>&1 &
  NOOP_COLLECTOR_PID=$!
  local deadline=$(( $(date +%s) + 10 ))
  until (exec 3<>"/dev/tcp/127.0.0.1/$OTLP_PORT") 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "no-op OTLP receiver did not start — see $LOG_DIR/noop-otlp-receiver.log"
      exit 1
    fi
    sleep 0.2
  done
  echo "trace collector: noop (pid $NOOP_COLLECTOR_PID, port $OTLP_PORT)"
}
stop_noop_collector() {
  if [ -n "$NOOP_COLLECTOR_PID" ]; then
    kill "$NOOP_COLLECTOR_PID" 2>/dev/null || true
    wait "$NOOP_COLLECTOR_PID" 2>/dev/null || true
    NOOP_COLLECTOR_PID=""
  fi
}

# Every in-flight request holds a socket and its thread's event loop; the shell default of 1024
# descriptors is reached at a few hundred concurrent requests ("Too many open files" -> HTTP 500).
ulimit -n "$(ulimit -Hn)"

# The data source each engine federates through. start-ui-install.sh passes PROVISA_ENGINE and
# the caller's environment to the server.
engine_env() {
  case "$1" in
    pg)
      export PROVISA_ENGINE_URL="postgresql://provisa:provisa@localhost:${PROVISA_BENCH_POSTGRESQL_PORT:-25632}/provisa_bench"
      ;;
    *) unset PROVISA_ENGINE_URL ;;
  esac
}

if [ "$#" -gt 0 ]; then
  ENGINES=("$@")
else
  ENGINES=(duckdb pg trino)
fi

for e in "${ENGINES[@]}"; do
  case "$e" in
    duckdb|pg|trino) ;;
    optimistic-only) ENGINES=() ; break ;;
    *) echo "Unknown engine: $e (must be duckdb, pg, trino, or optimistic-only)"; exit 1 ;;
  esac
done

wait_for_ready() {
  # 420s: matches PROVISA_ENGINE_READY_TIMEOUT (provisa/ui_server.py) — the backend's own budget
  # for startup phases past pg+schema+seed (flight/minio/results infra, MCP/gRPC readiness, etc.),
  # observed live taking well past 18s on a native boot. 180s cut off mid-phase with no error.
  local deadline=$(( $(date +%s) + 420 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -sf "$HTTP_BASE_URL/health" > /dev/null 2>&1; then
      wait_for_all_workers
      return $?
    fi
    sleep 2
  done
  return 1
}

# /health answers as soon as ONE worker is up. The workers boot one after another (the boot
# sequence writes the control plane and holds a lock, REQ-1900), so a load test started at the
# first 200 runs against a single worker and overloads it before the rest arrive. Each worker logs
# "startup phase warmup ready" when it starts serving: wait until every worker has.
BACKEND_LOG="$REPO_ROOT/.logs/backend.log"
backend_log_mark() {
  if [ -f "$BACKEND_LOG" ]; then wc -l < "$BACKEND_LOG"; else echo 0; fi
}
wait_for_all_workers() {
  local deadline=$(( $(date +%s) + 60 * PROVISA_WORKERS ))
  local up=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    up="$(tail -n "+$(( BACKEND_LOG_MARK + 1 ))" "$BACKEND_LOG" | grep -cE 'startup phase warmup +ready' || true)"
    if [ "$up" -ge "$PROVISA_WORKERS" ]; then
      echo "all $PROVISA_WORKERS workers serving"
      return 0
    fi
    sleep 5
  done
  echo "only $up of $PROVISA_WORKERS workers came up — see $BACKEND_LOG"
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
trap 'stop_provisa "${provisa_pid:-}"; stop_noop_collector' EXIT

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
  engine_env "$engine"
  start_noop_collector
  BACKEND_LOG_MARK="$(backend_log_mark)"
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
      npx --yes artillery@2.0.21 run --output "results/$engine/artillery-saturation.json" \
      artillery-concurrency-ramp.yml )

  stop_steal_logging

  echo "Stopping Provisa (engine=$engine)..."
  stop_provisa "$provisa_pid"
  stop_noop_collector
  echo "=== engine=$engine done ==="
done

# Most-optimistic-case phase (artillery-optimistic-tx.yml: one byte-identical cached GraphQL query
# on the pg engine). To measure the app/protocol ceiling rather than host contention, every
# container other than the benchmark Postgres is PAUSED (frozen, no CPU) for its duration: the
# other data sources and the Trino/Zaychik engine are idle for a pg-engine run anyway. Paused only
# AFTER Provisa is ready (its --demo perf boot registers every source), and always unpaused on
# exit. OPTIMISTIC=0 skips the phase.
OPTIMISTIC_PAUSE=(perf-mongodb-1 perf-clickhouse-1 perf-neo4j-1 trino-bench zaychik-bench)

unpause_others() {
  for c in "${OPTIMISTIC_PAUSE[@]}"; do
    if [ "$(docker inspect -f '{{.State.Paused}}' "$c" 2>/dev/null)" = "true" ]; then
      docker unpause "$c" > /dev/null
    fi
  done
}

if [ "${OPTIMISTIC:-1}" = "1" ]; then
  echo ""
  echo "=== optimistic (pg engine, non-PG containers paused) ==="
  start_log="$LOG_DIR/start-optimistic.log"
  kill_stale_instance
  engine_env pg
  start_noop_collector
  BACKEND_LOG_MARK="$(backend_log_mark)"
  ( cd "$REPO_ROOT" && PROVISA_ENGINE=pg exec ./start-ui-install.sh --demo perf ) > "$start_log" 2>&1 &
  provisa_pid=$!
  if ! wait_for_ready; then
    echo "Provisa did not become healthy (optimistic, engine=pg) — see $start_log"
    stop_provisa "$provisa_pid"
    exit 1
  fi
  trap 'unpause_others; stop_provisa "${provisa_pid:-}"; stop_noop_collector' EXIT
  for c in "${OPTIMISTIC_PAUSE[@]}"; do
    docker pause "$c" > /dev/null
    echo "paused $c"
  done
  mkdir -p "$SCRIPT_DIR/results/optimistic"
  start_steal_logging "$SCRIPT_DIR/results/optimistic"
  ( cd "$SCRIPT_DIR" && PROVISA_HTTP_BASE_URL="$HTTP_BASE_URL" \
      npx --yes artillery@2.0.21 run --output "results/optimistic/artillery-optimistic-tx.json" \
      artillery-optimistic-tx.yml )
  # The same most-optimistic request on EVERY transport (queries.OPTIMISTIC), closed-loop from
  # several client processes: results/optimistic/<transport>.json, one summary line per transport
  # here, and the server CPU per request at concurrency 1 — the per-transport overhead ranking.
  echo "Running per-transport optimistic ramp..."
  ( cd "$SCRIPT_DIR" && "$PYTHON_BIN" run_benchmark.py --engine pg --optimistic \
      --http-base-url "$HTTP_BASE_URL" --server-pid "$provisa_pid" --output-dir results )
  stop_steal_logging
  unpause_others
  echo "unpaused ${OPTIMISTIC_PAUSE[*]}"
  stop_provisa "$provisa_pid"
  stop_noop_collector
  echo "trace collector: noop" > "$SCRIPT_DIR/results/optimistic/trace-collector.txt"
  echo "=== optimistic done ==="
fi

echo ""
echo "Sweep complete. Results: $SCRIPT_DIR/results/{${ENGINES[*]}}/ and results/optimistic/"

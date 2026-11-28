#!/usr/bin/env bash
# Run the demo GraphQL + Petstore OpenAPI servers as host processes (no Docker).
# Both import from the repo .venv (strawberry, starlette, uvicorn are already
# backend dependencies). Replaces the graphql-demo / petstore-mock containers.
#
# `start` returns only once every server it started answers; a server that does not answer in
# DEMO_SERVER_WAIT_SECONDS (default 30) fails the start, naming it, so the caller never starts
# Provisa before the sources its demo config registers at boot (graphql-demo is introspected once,
# app_startup._auto_register_graphql_demo) can be reached.
#
# Usage: run-demo-servers.sh {start|stop}
#   DEMO_PYTHON overrides the interpreter (default: <repo>/.venv/bin/python)
#   DEMO_PETSTORE_PORT, DEMO_GRAPHQL_PORT, DEMO_GRPC_PORT override the ports (18080, 4000, 50071)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${DEMO_PYTHON:-$ROOT/.venv/bin/python}"
LOG_DIR="${DEMO_LOG_DIR:-$ROOT/.logs}"
mkdir -p "$LOG_DIR"

PET_PORT="${DEMO_PETSTORE_PORT:-18080}"
GQL_PORT="${DEMO_GRAPHQL_PORT:-4000}"
GRPC_PORT="${DEMO_GRPC_PORT:-50071}"
WAIT_SECONDS="${DEMO_SERVER_WAIT_SECONDS:-30}"
PET_PID="$LOG_DIR/petstore-server.pid"
GQL_PID="$LOG_DIR/graphql-server.pid"
GRPC_PID="$LOG_DIR/grpc-server.pid"

_listening() {
  lsof -iTCP:"$1" -sTCP:LISTEN -t >/dev/null 2>&1
}

_kill_port() {
  lsof -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null || true
}

# The previous run's server must have let go of its port before the new one binds it: one still
# listening answers the readiness probe below in place of the new server, which then fails to bind.
_wait_port_free() {
  local name="$1" port="$2"
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    _listening "$port" || return 0
    sleep 1
  done
  echo "Demo server $name: port $port is still held by an earlier process after ${WAIT_SECONDS}s" >&2
  exit 1
}

_fail() {
  local name="$1" log="$2" why="$3"
  echo " FAILED" >&2
  echo "Demo server $name $why. Its log ($log):" >&2
  tail -20 "$log" >&2 || true
  exit 1
}

# Wait for one server: its own process must still be running and the probe must succeed.
_wait() {
  local name="$1" pidfile="$2" log="$3"
  shift 3
  echo -n "  Waiting for $name"
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    kill -0 "$(cat "$pidfile")" 2>/dev/null || _fail "$name" "$log" "exited before it answered"
    if "$@" >/dev/null 2>&1; then echo " OK"; return 0; fi
    echo -n "."
    sleep 1
  done
  _fail "$name" "$log" "did not answer in ${WAIT_SECONDS}s"
}

_petstore_answers() {
  curl -sf --max-time 2 "http://localhost:$PET_PORT/api/v3/pet/findByStatus?status=available"
}

# The request Provisa's registration makes: an introspection POST.
_graphql_answers() {
  curl -sf --max-time 2 -X POST "http://localhost:$GQL_PORT/graphql" \
    -H "Content-Type: application/json" -d '{"query":"{ __schema { queryType { name } } }"}'
}

# gRPC has no HTTP health endpoint; the port listening is the answer.
_grpc_answers() {
  _listening "$GRPC_PORT"
}

start() {
  _kill_port "$PET_PORT"
  _kill_port "$GQL_PORT"
  _kill_port "$GRPC_PORT"
  _wait_port_free petstore-server "$PET_PORT"
  _wait_port_free graphql-server "$GQL_PORT"
  _wait_port_free grpc-server "$GRPC_PORT"
  echo "Starting demo servers (host processes)..."
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/petstore_server" \
    --host 0.0.0.0 --port "$PET_PORT" >>"$LOG_DIR/petstore-server.log" 2>&1 &
  echo $! >"$PET_PID"
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/graphql_server" \
    --host 0.0.0.0 --port "$GQL_PORT" >>"$LOG_DIR/graphql-server.log" 2>&1 &
  echo $! >"$GQL_PID"
  # Demo gRPC server for the random_grpc_set command (REQ-885). Run from repo root so
  # `python -m demo.grpc_server.server` resolves the demo package. exec: the recorded pid is the
  # server's own, which the readiness wait checks is still running.
  ( cd "$ROOT" && DEMO_GRPC_PORT="$GRPC_PORT" exec "$PY" -m demo.grpc_server.server \
    >>"$LOG_DIR/grpc-server.log" 2>&1 ) &
  echo $! >"$GRPC_PID"
  _wait petstore-server "$PET_PID" "$LOG_DIR/petstore-server.log" _petstore_answers
  _wait graphql-server "$GQL_PID" "$LOG_DIR/graphql-server.log" _graphql_answers
  _wait grpc-server "$GRPC_PID" "$LOG_DIR/grpc-server.log" _grpc_answers
}

stop() {
  for f in "$PET_PID" "$GQL_PID" "$GRPC_PID"; do
    [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  _kill_port "$PET_PORT"
  _kill_port "$GQL_PORT"
  _kill_port "$GRPC_PORT"
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 {start|stop}" >&2; exit 1 ;;
esac

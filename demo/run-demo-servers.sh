#!/usr/bin/env bash
# Run the demo GraphQL + Petstore OpenAPI servers as host processes (no Docker).
# Both import from the repo .venv (strawberry, starlette, uvicorn are already
# backend dependencies). Replaces the graphql-demo / petstore-mock containers.
#
# Usage: run-demo-servers.sh {start|stop}
#   DEMO_PYTHON overrides the interpreter (default: <repo>/.venv/bin/python)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${DEMO_PYTHON:-$ROOT/.venv/bin/python}"
LOG_DIR="$ROOT/.logs"
mkdir -p "$LOG_DIR"

PET_PORT=18080
GQL_PORT=4000
GRPC_PORT="${DEMO_GRPC_PORT:-50071}"
PET_PID="$LOG_DIR/petstore-server.pid"
GQL_PID="$LOG_DIR/graphql-server.pid"
GRPC_PID="$LOG_DIR/grpc-server.pid"

_kill_port() {
  lsof -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null || true
}

_wait() {
  local name="$1" url="$2"
  echo -n "  Waiting for $name"
  for _ in $(seq 1 30); do
    if curl -sf "$url" >/dev/null 2>&1; then echo " OK"; return 0; fi
    echo -n "."
    sleep 1
  done
  echo " TIMEOUT"
  return 1
}

start() {
  _kill_port "$PET_PORT"
  _kill_port "$GQL_PORT"
  _kill_port "$GRPC_PORT"
  sleep 1
  echo "Starting demo servers (host processes)..."
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/petstore_server" \
    --host 0.0.0.0 --port "$PET_PORT" >>"$LOG_DIR/petstore-server.log" 2>&1 &
  echo $! >"$PET_PID"
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/graphql_server" \
    --host 0.0.0.0 --port "$GQL_PORT" >>"$LOG_DIR/graphql-server.log" 2>&1 &
  echo $! >"$GQL_PID"
  # Demo gRPC server for the random_grpc_set command (REQ-885). Run from repo root so
  # `python -m demo.grpc_server.server` resolves the demo package.
  ( cd "$ROOT" && DEMO_GRPC_PORT="$GRPC_PORT" "$PY" -m demo.grpc_server.server \
    >>"$LOG_DIR/grpc-server.log" 2>&1 ) &
  echo $! >"$GRPC_PID"
  _wait "petstore-server" "http://localhost:$PET_PORT/api/v3/pet/findByStatus?status=available"
  _wait "graphql-server" "http://localhost:$GQL_PORT/graphql?query=%7B__typename%7D"
  # gRPC has no HTTP health endpoint; confirm the port is listening.
  echo -n "  Waiting for grpc-server"
  for _ in $(seq 1 30); do
    if lsof -iTCP:"$GRPC_PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then echo " OK"; break; fi
    echo -n "."; sleep 1
  done
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

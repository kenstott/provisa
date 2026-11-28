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
#   DEMO_LOG_DIR overrides the log and pid-file directory (default: <repo>/.logs)
#
# `stop`, and `start` before it starts anything, signal only the servers this checkout's pid files
# name. A port held by any other process fails the start, naming its pid, and that process is left
# running: another checkout's demo servers are another instance's.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${DEMO_PYTHON:-$ROOT/.venv/bin/python}"
LOG_DIR="${DEMO_LOG_DIR:-$ROOT/.logs}"
mkdir -p "$LOG_DIR"

PET_PORT="${DEMO_PETSTORE_PORT:-18080}"
GQL_PORT="${DEMO_GRAPHQL_PORT:-4000}"
GRPC_PORT="${DEMO_GRPC_PORT:-50071}"
WAIT_SECONDS="${DEMO_SERVER_WAIT_SECONDS:-30}"
# A server is this checkout's only through its pid file in $LOG_DIR: line 1 its pid, line 2 its
# start time (ps lstart) when it was started. stop and start signal only a pid whose start time
# still matches, so a server another checkout started on the same port, or a reused pid, is never
# touched. Nothing is ever killed by port.
SERVERS="petstore-server graphql-server grpc-server"
KILLED=""

_port() {
  case "$1" in
    petstore-server) echo "$PET_PORT" ;;
    graphql-server) echo "$GQL_PORT" ;;
    grpc-server) echo "$GRPC_PORT" ;;
  esac
}

_pidfile() {
  echo "$LOG_DIR/$1.pid"
}

# The pids listening on a port. lsof exits 1 when none listens, which is the free-port answer.
_holders() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | tr '\n' ' ' | sed 's/ $//' || true
}

_listening() {
  [ -n "$(_holders "$1")" ]
}

# Record a just-started server. One already gone has no start time: it exited before it answered.
_record() {
  local name="$1" pid="$2" started
  if ! started=$(ps -o lstart= -p "$pid"); then
    _fail "$name" "$LOG_DIR/$name.log" "exited before it answered"
  fi
  printf '%s\n%s\n' "$pid" "$started" >"$(_pidfile "$name")"
}

# Prints the pid a pid file names when that process is still the one this checkout started.
_owned_pid() {
  local file="$1" pid started now
  pid=$(sed -n 1p "$file")
  started=$(sed -n 2p "$file")
  [ -n "$pid" ] && [ -n "$started" ] || return 1
  now=$(ps -o lstart= -p "$pid") || return 1
  [ "$now" = "$started" ] || return 1
  echo "$pid"
}

# Signal the servers this checkout started and forget their pid files. A pid file whose process
# is gone, or whose pid now belongs to another process, is removed without signalling anything.
_stop_owned() {
  local name file pid
  for name in $SERVERS; do
    file=$(_pidfile "$name")
    [ -f "$file" ] || continue
    if pid=$(_owned_pid "$file"); then
      kill "$pid"
      KILLED="$KILLED $pid"
    else
      echo "Demo server $name: pid file $file names no process this checkout started; signalling nothing" >&2
    fi
    rm -f "$file"
  done
}

# Each port must be free before its server binds it. A holder this checkout just signalled is
# waited for (one still listening would answer the readiness probe in place of the new server);
# any other holder fails the start at once, naming it, and is left running.
_wait_port_free() {
  local name="$1" port="$2" holders pid
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    holders=$(_holders "$port")
    [ -n "$holders" ] || return 0
    for pid in $holders; do
      case " $KILLED " in
        *" $pid "*) ;;
        *)
          echo "Demo server $name: port $port is held by another process (pid $holders)" >&2
          exit 1
          ;;
      esac
    done
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
    kill -0 "$(sed -n 1p "$pidfile")" 2>/dev/null || _fail "$name" "$log" "exited before it answered"
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
  local name
  _stop_owned
  for name in $SERVERS; do
    _wait_port_free "$name" "$(_port "$name")"
  done
  echo "Starting demo servers (host processes)..."
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/petstore_server" \
    --host 0.0.0.0 --port "$PET_PORT" >>"$LOG_DIR/petstore-server.log" 2>&1 &
  _record petstore-server $!
  "$PY" -m uvicorn server:app --app-dir "$ROOT/demo/graphql_server" \
    --host 0.0.0.0 --port "$GQL_PORT" >>"$LOG_DIR/graphql-server.log" 2>&1 &
  _record graphql-server $!
  # Demo gRPC server for the random_grpc_set command (REQ-885). Run from repo root so
  # `python -m demo.grpc_server.server` resolves the demo package. exec: the recorded pid is the
  # server's own, which the readiness wait checks is still running.
  ( cd "$ROOT" && DEMO_GRPC_PORT="$GRPC_PORT" exec "$PY" -m demo.grpc_server.server \
    >>"$LOG_DIR/grpc-server.log" 2>&1 ) &
  _record grpc-server $!
  _wait petstore-server "$(_pidfile petstore-server)" "$LOG_DIR/petstore-server.log" _petstore_answers
  _wait graphql-server "$(_pidfile graphql-server)" "$LOG_DIR/graphql-server.log" _graphql_answers
  _wait grpc-server "$(_pidfile grpc-server)" "$LOG_DIR/grpc-server.log" _grpc_answers
}

stop() {
  _stop_owned
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 {start|stop}" >&2; exit 1 ;;
esac

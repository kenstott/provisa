#!/usr/bin/env bash
# Kill every process start-ui-install.sh (--demo/--native) starts on this host: the
# uvicorn backend (reloader + multiprocessing worker) and the Vite UI dev server.
# Same pattern-match + port-fallback teardown start-ui-install.sh runs on its own
# restart (Ctrl-R/Ctrl-U) and on startup to clear a previous run — extracted here so
# both the script and an admin-triggered shutdown call one piece of logic.
#
# macOS, Linux, and WSL only (Windows runs this launcher under WSL, never natively —
# see is_wsl() in start-ui-install.sh) — pkill/lsof/nc are present on all three.
set -uo pipefail

PROVISA_API_PORT="${PROVISA_API_PORT:-8001}"

lsof_pids() {
  local port="$1" tmp lp _w
  tmp=$(mktemp 2>/dev/null) || tmp="${TMPDIR:-/tmp}/.provisa-lsof.$$"
  lsof -S 2 -bnP -i ":$port" -t >"$tmp" 2>/dev/null &
  lp=$!
  for _w in $(seq 1 6); do
    kill -0 "$lp" 2>/dev/null || break
    sleep 0.5
  done
  kill -9 "$lp" 2>/dev/null || true
  cat "$tmp" 2>/dev/null || true
  rm -f "$tmp" 2>/dev/null || true
}

port_in_use() {
  nc -z -G 1 127.0.0.1 "$1" >/dev/null 2>&1
}

echo -n "Stopping all Provisa services (ports 3000/$PROVISA_API_PORT)"

pkill -CONT -f "uvicorn main:app" 2>/dev/null || true
for _pid in $(pgrep -f "uvicorn main:app" 2>/dev/null); do
  pkill -9 -P "$_pid" 2>/dev/null || true
  kill -9 "$_pid" 2>/dev/null || true
done

pkill -CONT -f "node.*vite" 2>/dev/null || true
pkill -9 -f "node.*vite" 2>/dev/null || true
port_in_use 3000 && lsof_pids 3000 | xargs kill -9 2>/dev/null || true
port_in_use "$PROVISA_API_PORT" && lsof_pids "$PROVISA_API_PORT" | xargs kill -9 2>/dev/null || true

for _i in $(seq 1 10); do
  port_in_use "$PROVISA_API_PORT" || break
  echo -n "."
  sleep 1
done
echo " OK"

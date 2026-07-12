#!/bin/sh
# Start the containerd daemon inside the Provisa WSL2 distro if not already up.
# WSL2 has no systemd by default, so containerd runs as a backgrounded process
# with output to a log. Idempotent — a no-op when the socket is already live.
set -eu

SOCK="/run/containerd/containerd.sock"
LOG="/var/log/containerd.log"

if [ -S "$SOCK" ] && /usr/local/bin/nerdctl info >/dev/null 2>&1; then
  exit 0
fi

mkdir -p /run/containerd /var/lib/containerd /var/log
# nohup + setsid so the daemon survives the launching `wsl` invocation exiting.
setsid /usr/local/bin/containerd >>"$LOG" 2>&1 &

# Wait for the socket to accept connections.
i=0
while [ "$i" -lt 30 ]; do
  if [ -S "$SOCK" ] && /usr/local/bin/nerdctl info >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
  i=$((i + 1))
done

echo "[start-containerd] containerd did not become ready — see $LOG" >&2
exit 1

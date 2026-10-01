#!/usr/bin/env bash
# Idle auto-shutdown for the benchmark VM: a running VM bills by the hour whether or not a
# benchmark is using it, a stopped one bills only its disk.
#
#   sudo ./vm_idle_shutdown.sh install   # on the VM: installs a systemd timer (every 5 min)
#   ./vm_idle_shutdown.sh check          # what the timer runs
#   ./vm_idle_shutdown.sh status         # print what it sees and how long it has been idle
#
# The VM counts as in use while any of these holds: an SSH session or remote command is
# connected, a benchmark or Provisa server process is running, or the hold file is fresh
# (`touch /var/lib/provisa-idle/hold` keeps it up for HOLD_SECONDS). After IDLE_SECONDS with
# none of them it powers off, which leaves the instance TERMINATED. Start it again with
#   gcloud compute instances start provisa-perf-bench --zone=us-east1-b
set -euo pipefail

STATE_DIR=/var/lib/provisa-idle
LAST_ACTIVE="$STATE_DIR/last_active"
HOLD="$STATE_DIR/hold"
IDLE_SECONDS=1800
HOLD_SECONDS=43200
# Process command lines that mean a benchmark or a server under test is running.
BUSY_PATTERN='uvicorn main:app|start-ui-install|orchestrate\.sh|run_benchmark|optimistic_load|sizing_|artillery|pytest'

busy_reason() {
    if pgrep -f 'sshd: .*@' >/dev/null; then
        echo "ssh session"
    elif pgrep -f "$BUSY_PATTERN" >/dev/null; then
        echo "benchmark process"
    elif [[ -e "$HOLD" ]] && (( $(date +%s) - $(stat -c %Y "$HOLD") < HOLD_SECONDS )); then
        echo "hold file"
    fi
}

idle_for() {
    echo $(( $(date +%s) - $(stat -c %Y "$LAST_ACTIVE") ))
}

case "${1:?usage: vm_idle_shutdown.sh install|check|status}" in
install)
    install -d -m 0755 "$STATE_DIR"
    install -m 0755 "$0" /usr/local/sbin/provisa-idle-shutdown
    cat >/etc/systemd/system/provisa-idle-shutdown.service <<'UNIT'
[Unit]
Description=Power off the benchmark VM when it has been idle

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/provisa-idle-shutdown check
UNIT
    cat >/etc/systemd/system/provisa-idle-shutdown.timer <<'UNIT'
[Unit]
Description=Check every 5 minutes whether the benchmark VM is idle

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
UNIT
    # A boot counts as activity, so a freshly started VM gets the full idle window.
    cat >/etc/systemd/system/provisa-idle-mark-boot.service <<UNIT
[Unit]
Description=Mark the benchmark VM as active at boot

[Service]
Type=oneshot
ExecStart=/usr/bin/touch $LAST_ACTIVE

[Install]
WantedBy=multi-user.target
UNIT
    touch "$LAST_ACTIVE"
    systemctl daemon-reload
    systemctl enable --now provisa-idle-shutdown.timer
    systemctl enable provisa-idle-mark-boot.service
    systemctl list-timers provisa-idle-shutdown.timer --no-pager
    ;;
check)
    reason=$(busy_reason)
    if [[ -n "$reason" ]]; then
        touch "$LAST_ACTIVE"
        exit 0
    fi
    if (( $(idle_for) >= IDLE_SECONDS )); then
        logger -t provisa-idle "idle for $(idle_for)s, powering off"
        systemctl poweroff
    fi
    ;;
status)
    reason=$(busy_reason)
    echo "in use: ${reason:-no}"
    echo "idle for: $(idle_for)s of ${IDLE_SECONDS}s"
    ;;
*)
    echo "usage: vm_idle_shutdown.sh install|check|status" >&2
    exit 2
    ;;
esac

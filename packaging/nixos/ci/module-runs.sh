#!/usr/bin/env bash
# On a host that enables the root flake's nixosModules.provisa (packaging/nixos/host's `ci`), the
# provisa service runs the release as the unprivileged `provisa` user: it is active, and the API
# and the UI answer. The service's first start stages the release, so it is given minutes.
set -euo pipefail

groups="$(id -nG provisa)"
if [ "$groups" != "provisa" ]; then
  echo "::error::the service user is in groups '${groups}', expected only provisa"
  exit 1
fi

for _ in $(seq 1 240); do
  if curl -sf -o /dev/null http://127.0.0.1:8000/health; then
    systemctl is-active provisa
    curl -sf http://127.0.0.1:8000/health
    curl -sf -o /dev/null http://127.0.0.1:3000/
    # Both servers run as the service user.
    servers="$(ps -eo user=,args= | grep '[u]vicorn provisa')"
    echo "$servers"
    if [ "$(awk '$1 != "provisa"' <<< "$servers")" != "" ] || [ "$(wc -l <<< "$servers")" -ne 2 ]; then
      echo "::error::expected the API and the UI server running as provisa"
      exit 1
    fi
    exit 0
  fi
  sleep 5
done
echo "::error::the provisa service did not answer on :8000/health"
systemctl status provisa --no-pager -l || true
journalctl -u provisa --no-pager -n 500 || true
sudo tail -n 200 /var/lib/provisa/.provisa/.logs/native-api.log /var/lib/provisa/.provisa/.logs/native-ui.log /var/lib/provisa/.provisa/.logs/control-plane-pg.log || true
exit 1

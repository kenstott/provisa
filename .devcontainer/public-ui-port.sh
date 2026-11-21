#!/bin/bash
# Makes the UI's forwarded port public in a Codespace (maintainer decision 2026-11-05). devcontainer.json
# has no field for port visibility, and a public port reverts to private whenever the codespace
# restarts, so this is the postStartCommand and runs on every start. The UI binds the port only after
# start-ui-install.sh runs, so it waits, bounded, for the port to be listed before setting it.
# Outside a Codespace it does nothing. A refusal (for example the built-in token lacking permission, or
# an organization policy against public ports) is reported and does not fail the container.
set -uo pipefail

UI_PORT=3000
DEADLINE_SECONDS="${PUBLIC_UI_PORT_WAIT_SECONDS:-3600}"

[ -n "${CODESPACE_NAME:-}" ] || exit 0

warn() { echo "public-ui-port: WARNING: $*" >&2; }

listed() {
  gh codespace ports -c "$CODESPACE_NAME" --json sourcePort --jq '.[].sourcePort' 2>/dev/null \
    | grep -qx "$UI_PORT"
}

end=$((SECONDS + DEADLINE_SECONDS))
until listed; do
  if [ "$SECONDS" -ge "$end" ]; then
    warn "port $UI_PORT was not forwarded within ${DEADLINE_SECONDS}s; it stays private." \
      "Set it by hand: gh codespace ports visibility $UI_PORT:public -c \"\$CODESPACE_NAME\""
    exit 0
  fi
  sleep 3
done

if out=$(gh codespace ports visibility "$UI_PORT:public" -c "$CODESPACE_NAME" 2>&1); then
  echo "public-ui-port: port $UI_PORT is public"
else
  warn "'gh codespace ports visibility $UI_PORT:public' failed, so port $UI_PORT stays private: $out"
fi
exit 0

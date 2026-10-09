#!/usr/bin/env bash
# On a NixOS host with Provisa's settings (the `ci` guest, which imports preinstall.nix),
# install.sh installs: the command is on PATH and the services it starts answer.
set -euo pipefail

./install.sh --non-interactive
test "$(command -v provisa)" = "$HOME/.local/bin/provisa"
test ! -e ~/.provisa/nixos-preinstall.nix

for _ in $(seq 1 120); do
  if curl -sf -o /dev/null http://127.0.0.1:8000/health; then
    curl -sf http://127.0.0.1:8000/health
    curl -sf -o /dev/null http://127.0.0.1:3000/
    provisa status
    exit 0
  fi
  sleep 5
done
provisa status
exit 1

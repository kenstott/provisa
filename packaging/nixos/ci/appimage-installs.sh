#!/usr/bin/env bash
# On a NixOS host with Provisa's settings and nothing else (the `prepared` guest: stock NixOS
# after the steps the installer names), the download installs and starts: a first launch as a
# desktop user makes it -- Enter at each question, so the default native tier -- then
# `provisa start`, and the API and the UI answer.
set -euo pipefail
. packaging/nixos/ci/appimage-runtime.sh

rm -rf ~/.provisa
# PROVISA_INSTALL_SOURCE=bundled: install the wheels this AppImage carries -- this build's --
# not a release from PyPI.
answers="$(mktemp)"
printf '\n%.0s' $(seq 1 20) > "$answers"
if ! PROVISA_INSTALL_SOURCE=bundled "$APPIMAGE" start < "$answers"; then
  echo "::error::first launch, or the start after it, failed"
  exit 1
fi
test -x ~/.local/bin/provisa
test "$(command -v provisa)" = "$HOME/.local/bin/provisa"
test ! -e ~/.provisa/nixos-preinstall.nix
grep -q "^runtime: native" ~/.provisa/config.yaml

for _ in $(seq 1 120); do
  if curl -sf -o /dev/null http://127.0.0.1:8000/health; then
    curl -sf http://127.0.0.1:8000/health
    curl -sf -o /dev/null http://127.0.0.1:3000/
    provisa status
    exit 0
  fi
  sleep 5
done
provisa status || true
tail -n 200 ~/.provisa/.logs/native-api.log ~/.provisa/.logs/native-ui.log || true
exit 1

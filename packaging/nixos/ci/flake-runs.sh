#!/usr/bin/env bash
# On stock NixOS (the `bare` guest: no nix-ld, no Docker, flakes not enabled), `nix run` of the
# repository's root flake starts Provisa as `nix run github:kenstott/provisa` does: the Linux
# release in an FHS environment. Enter at each first-launch question, so the default native tier;
# then the API and the UI answer.
set -euo pipefail

nix_() { nix --extra-experimental-features "nix-command flakes" "$@"; }

rm -rf ~/.provisa
answers="$(mktemp)"
printf '\n%.0s' $(seq 1 20) > "$answers"
if ! nix_ run --print-build-logs . < "$answers"; then
  echo "::error::nix run of the root flake failed"
  exit 1
fi

for _ in $(seq 1 120); do
  if curl -sf -o /dev/null http://127.0.0.1:8000/health; then
    curl -sf http://127.0.0.1:8000/health
    curl -sf -o /dev/null http://127.0.0.1:3000/
    nix_ run . -- status
    exit 0
  fi
  sleep 5
done
nix_ run . -- status || true
tail -n 200 ~/.provisa/.logs/native-api.log ~/.provisa/.logs/native-ui.log ~/.provisa/.logs/control-plane-pg.log || true
exit 1

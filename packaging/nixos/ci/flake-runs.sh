#!/usr/bin/env bash
# On stock NixOS (the `bare` guest: no nix-ld, no Docker, flakes not enabled), `nix run` of the
# repository's root flake starts Provisa as `nix run github:kenstott/provisa` does: the Linux
# release in an FHS environment. Enter at each first-launch question, so the default native tier;
# then the API and the UI answer.
set -euo pipefail

nix_() { nix --extra-experimental-features "nix-command flakes" "$@"; }

# Run from the home directory, as a user runs `nix run github:kenstott/provisa`, not from the
# checkout vm-run starts in: the pinned release's first launch checks its install with
# `python3 -c "import provisa._encoders.pgwire"`, which from the checkout imports its provisa/ source
# tree (no compiled encoders) and fails (run 45680556710). The flake is this checkout, by path.
flake="$PWD"
cd ~

rm -rf ~/.provisa
answers="$(mktemp)"
printf '\n%.0s' $(seq 1 20) > "$answers"
if ! nix_ run --print-build-logs "$flake" < "$answers"; then
  echo "::error::nix run of the root flake failed"
  exit 1
fi

for _ in $(seq 1 120); do
  if curl -sf -o /dev/null http://127.0.0.1:8000/health; then
    curl -sf http://127.0.0.1:8000/health
    curl -sf -o /dev/null http://127.0.0.1:3000/
    nix_ run "$flake" -- status
    exit 0
  fi
  sleep 5
done
nix_ run "$flake" -- status || true
tail -n 200 ~/.provisa/.logs/native-api.log ~/.provisa/.logs/native-ui.log ~/.provisa/.logs/control-plane-pg.log || true
exit 1

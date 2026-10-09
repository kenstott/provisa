#!/usr/bin/env bash
# On a NixOS host without Provisa's settings (the `bare` guest), install.sh names the steps and
# stops, having installed nothing: asked interactively and not.
set -euo pipefail

out="$(mktemp)"
for flag in --non-interactive --interactive; do
  rm -rf ~/.provisa
  # --interactive is no flag of the installer: it is the default, here with Enter on stdin.
  if ./install.sh "$flag" > "$out" 2>&1 <<< ""; then
    cat "$out"
    echo "install.sh installed on a host without the settings"
    exit 1
  fi
  cat "$out"
  grep -q "sudo cp $HOME/.provisa/nixos-preinstall.nix /etc/nixos/provisa.nix" "$out"
  grep -q "users.users.provisa.extraGroups" "$out"
  grep -q "sudo nixos-rebuild switch" "$out"
  cmp ~/.provisa/nixos-preinstall.nix packaging/nixos/preinstall.nix
  # The settings file is the only thing it wrote.
  test "$(ls -A ~/.provisa)" = "nixos-preinstall.nix"
  test ! -e ~/.local/bin/provisa
  # Only a user who can answer is asked.
  if [ "$flag" = --interactive ]; then
    grep -q "Press Enter to list the steps" "$out"
  elif grep -q "Press Enter" "$out"; then
    exit 1
  fi
done

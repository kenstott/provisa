#!/usr/bin/env bash
# On a NixOS host without Provisa's settings (the `bare` guest -- stock NixOS), the download
# starts, names the steps and stops, having installed nothing: asked interactively and not.
set -euo pipefail
. packaging/nixos/ci/appimage-runtime.sh

out="$(mktemp)"
for flag in --non-interactive --interactive; do
  rm -rf ~/.provisa
  # --interactive is no flag of the AppImage: it is the default, here with Enter on stdin.
  if "$APPIMAGE" "$flag" > "$out" 2>&1 <<< ""; then
    cat "$out"
    echo "the AppImage installed on a host without the settings"
    exit 1
  fi
  cat "$out"
  grep -q "sudo cp $HOME/.provisa/nixos-preinstall.nix /etc/nixos/provisa.nix" "$out"
  grep -q "sudo nixos-rebuild switch" "$out"
  # The default install needs no Docker, so the user is not sent to set it up.
  if grep -q "extraGroups" "$out"; then exit 1; fi
  # The settings it hands out are the repository's, carried inside the AppImage.
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

# The root flake's app: `nix run` with no command starts Provisa; with one, it runs that provisa
# command (status, stop, logs, ...). Takes the package, so the NixOS workflow's `nix run` leg runs
# this same app on the AppImage its commit builds (packaging/nixos/this-commit).
{
  lib,
  writeShellScriptBin,
  provisa,
}:
writeShellScriptBin "provisa-run" ''
  # REQ-1900: `nix run` with no command starts Provisa.
  if [ "$#" -eq 0 ]; then set -- start; fi
  exec ${lib.getExe provisa} "$@"
''

{
  description = "Provisa: its NixOS module, its release as a package and an app, and a development shell";

  # The commit packaging/nixos/flake.nix pins (nixos-26.05): move both together.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/7c8764b7c7b09b34f632464276218ef9090eaa11";

  outputs =
    { self, nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;

      # The Linux release is built for x86_64 only.
      releaseSystems = [ "x86_64-linux" ];
      shellSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          # Provisa is under the Business Source License, which nixpkgs counts as unfree.
          config.allowUnfreePredicate = pkg: lib.getName pkg == "provisa";
        };
    in
    {
      # A host flake imports it as inputs.provisa.nixosModules.provisa.
      nixosModules.provisa = ./packaging/nixos/configuration.nix;

      packages = lib.genAttrs releaseSystems (system: {
        default = (pkgsFor system).callPackage ./packaging/nixos/package.nix { };
      });

      # `nix run github:kenstott/provisa` starts Provisa; `nix run github:kenstott/provisa -- <command>`
      # runs any other provisa command (status, stop, logs, ...).
      apps = lib.genAttrs releaseSystems (
        system:
        let
          provisa = self.packages.${system}.default;
          run = (pkgsFor system).writeShellScriptBin "provisa-run" ''
            # REQ-1900: `nix run` with no command starts Provisa.
            if [ "$#" -eq 0 ]; then set -- start; fi
            exec ${lib.getExe provisa} "$@"
          '';
        in
        {
          default = {
            type = "app";
            program = lib.getExe run;
            meta.description = "Start Provisa, or run a provisa command";
          };
        }
      );

      devShells = lib.genAttrs shellSystems (system: {
        default = (pkgsFor system).callPackage ./packaging/nixos/dev-shell.nix { };
      });
    };
}

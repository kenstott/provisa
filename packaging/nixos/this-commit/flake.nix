{
  description = "CI only: the root flake's package and app on the AppImage this commit builds, in place of the pinned release";

  # The NixOS workflow's `nix run` leg (packaging/nixos/ci/flake-runs.sh) puts the commit's
  # Provisa.AppImage beside this file and runs this flake against its own checkout:
  #   nix run path:packaging/nixos/this-commit --override-input provisa "path:$PWD" --no-write-lock-file
  # A user's flake never names it: theirs runs the root flake's pinned release.
  inputs.provisa.url = "github:kenstott/provisa";

  outputs =
    { provisa, ... }:
    let
      system = "x86_64-linux";
      inherit (provisa.inputs.nixpkgs) lib;
      pkgs = provisa.inputs.nixpkgs.legacyPackages.${system};
      # The version names the store path. It starts with a digit, so the name parses as "provisa",
      # which is the one unfree name the root flake allows.
      package = provisa.packages.${system}.default.override {
        releaseVersion = "0-this-commit";
        releaseAppImage = ./Provisa.AppImage;
      };
    in
    {
      packages.${system}.default = package;

      apps.${system}.default = {
        type = "app";
        program = lib.getExe (
          pkgs.callPackage "${provisa}/packaging/nixos/app.nix" { provisa = package; }
        );
      };
    };
}

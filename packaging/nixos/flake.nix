{
  description = "The NixOS workflow's hosts: the CI host (configuration.nix) and the guests it is tested in. A user's host imports the root flake's nixosModules.provisa instead";

  # nixos-26.05 at a fixed commit, so every run builds the same system; move it deliberately.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/7c8764b7c7b09b34f632464276218ef9090eaa11";

  outputs =
    { nixpkgs, ... }:
    {
      # The same host as a QEMU guest, for .github/workflows/nixos.yml:
      #   nix build path:packaging/nixos#nixosConfigurations.ci.config.system.build.vm
      nixosConfigurations.ci = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          ./configuration.nix
          ./ci-vm.nix
        ];
      };

      # Stock NixOS after the steps an installer names -- the settings and nothing more -- as the
      # same guest: where the download has to install and start.
      nixosConfigurations.prepared = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          ./bare.nix
          ./preinstall.nix
          ./ci-vm.nix
        ];
      };

      # A host without the settings, as the same guest: what an installer says to a new NixOS user.
      nixosConfigurations.bare = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          ./bare.nix
          ./ci-vm.nix
        ];
      };
    };
}

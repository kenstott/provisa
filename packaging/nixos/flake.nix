{
  description = "Provisa on NixOS: the host configuration, and the VM the NixOS workflow tests it in";

  # nixos-26.05 at a fixed commit, so every run builds the same system; move it deliberately.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/7c8764b7c7b09b34f632464276218ef9090eaa11";

  outputs =
    { nixpkgs, ... }:
    {
      # The host itself: toolchain, nix-ld, Docker, and the provisa service. Reusable on any NixOS
      # machine (an EC2 instance imports this beside its own hardware module).
      nixosModules.provisa = ./configuration.nix;

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

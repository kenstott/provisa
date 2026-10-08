{
  description = "Provisa on NixOS: the host configuration, and the VM the NixOS workflow tests it in";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

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
    };
}

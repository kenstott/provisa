{
  description = "A NixOS host flake that imports Provisa's module from the repository's root flake";

  # What a host flake names. The NixOS workflow builds this host against its own checkout:
  #   nix build path:packaging/nixos/host#nixosConfigurations.host.config.system.build.toplevel \
  #     --override-input provisa "git+file://$PWD" --no-write-lock-file
  inputs.provisa.url = "github:kenstott/provisa";
  inputs.nixpkgs.follows = "provisa/nixpkgs";

  outputs =
    { nixpkgs, provisa, ... }:
    {
      nixosConfigurations.host = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          provisa.nixosModules.provisa
          {
            networking.hostName = "provisa-host";
            # A real host's hardware-configuration.nix declares these.
            fileSystems."/" = {
              device = "/dev/disk/by-label/nixos";
              fsType = "ext4";
            };
            boot.loader.grub.device = "nodev";
          }
        ];
      };
    };
}

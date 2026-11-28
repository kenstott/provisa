{
  description = "A NixOS host flake that runs Provisa with the module from the repository's root flake";

  # What a host flake names. The NixOS workflow builds this host against its own checkout:
  #   nix build path:packaging/nixos/host#nixosConfigurations.host.config.system.build.toplevel \
  #     --override-input provisa "git+file://$PWD" --no-write-lock-file
  inputs.provisa.url = "github:kenstott/provisa";
  inputs.nixpkgs.follows = "provisa/nixpkgs";

  outputs =
    { nixpkgs, provisa, ... }:
    let
      host = [
        provisa.nixosModules.provisa
        {
          system.stateVersion = "26.05";
          networking.hostName = "provisa-host";
          services.provisa.enable = true;
          # A real host's hardware-configuration.nix declares these.
          fileSystems."/" = {
            device = "/dev/disk/by-label/nixos";
            fsType = "ext4";
          };
          boot.loader.grub.device = "nodev";
        }
      ];
    in
    {
      nixosConfigurations.host = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = host;
      };

      # The same host as the NixOS workflow's QEMU guest (.github/actions/nixos-vm with
      # flake: packaging/nixos/host), where it checks the provisa service. The workflow logs in as
      # `ci` with the key it writes beside this file; the service's own user has no login. Its
      # service runs the AppImage the workflow's commit builds, which the workflow puts beside
      # this file, in place of the pinned release: CI tests what ships.
      nixosConfigurations.ci = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = host ++ [
          "${provisa}/packaging/nixos/vm-guest.nix"
          {
            # The version is digit-first, so the name parses as "provisa": the unfree name the
            # root flake allows.
            services.provisa.package = provisa.packages.x86_64-linux.default.override {
              releaseVersion = "0-this-commit";
              releaseAppImage = ./Provisa.AppImage;
            };
            users.users.ci = {
              isNormalUser = true;
              extraGroups = [ "wheel" ];
              openssh.authorizedKeys.keyFiles = [ ./ci_authorized_key.pub ];
            };
            security.sudo.wheelNeedsPassword = false;
          }
        ];
      };
    };
}

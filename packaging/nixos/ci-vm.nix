# The host of configuration.nix as a QEMU guest on a GitHub-hosted runner (.github/actions/nixos-vm).
{ ... }:
{
  imports = [ ./vm-guest.nix ];

  networking.hostName = "provisa-nixos";

  # Never booted from: the VM build replaces both. A NixOS system must declare them to evaluate.
  fileSystems."/" = {
    device = "/dev/disk/by-label/nixos";
    fsType = "ext4";
  };
  boot.loader.grub.device = "nodev";

  # The workflow logs in as provisa with the key it generates for the run.
  users.users.provisa.openssh.authorizedKeys.keyFiles = [ ./ci_authorized_key.pub ];
}

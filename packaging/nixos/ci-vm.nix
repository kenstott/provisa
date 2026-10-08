# The host of configuration.nix as a QEMU guest on a GitHub-hosted runner (.github/actions/nixos-vm).
{ ... }:
{
  networking.hostName = "provisa-nixos";

  # Never booted from: the VM build replaces both. A NixOS system must declare them to evaluate.
  fileSystems."/" = {
    device = "/dev/disk/by-label/nixos";
    fsType = "ext4";
  };
  boot.loader.grub.device = "nodev";

  # The workflow drives the guest over ssh with a key it generates for the run and writes beside
  # this file before the build.
  services.openssh = {
    enable = true;
    extraConfig = "AcceptEnv SPLUNKBASE_USERNAME SPLUNKBASE_PASSWORD";
  };
  users.users.provisa.openssh.authorizedKeys.keyFiles = [ ./ci_authorized_key.pub ];

  virtualisation.vmVariant.virtualisation = {
    # A hosted runner has 4 cores and 16 GB; the guest gets all but what QEMU and the runner need.
    cores = 4;
    memorySize = 12288;
    # MB, allocated as it is written: the lanes' images and the checkout's environments.
    diskSize = 40960;
    graphics = false;
    forwardPorts = [
      {
        from = "host";
        host.port = 2222;
        guest.port = 22;
      }
    ];
    # The runner's checkout, read by the guest and copied onto its own disk.
    sharedDirectories.src = {
      source = ''"$PROVISA_SRC"'';
      target = "/mnt/src";
    };
  };
}

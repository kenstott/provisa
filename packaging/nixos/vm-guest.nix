# A NixOS host as a QEMU guest on a GitHub-hosted runner (.github/actions/nixos-vm): what every
# guest the NixOS workflow boots has, whichever host it is. The host names the user the workflow
# logs in as, with the key the workflow writes beside the host's flake.
{ lib, ... }:
{
  # A hosted runner's zone. NixOS declares none by default, and a library that looks for the
  # host's zone then warns on stderr, ahead of the output a test reads from a process it starts.
  time.timeZone = "UTC";

  # The workflow drives the guest over ssh.
  services.openssh = {
    enable = true;
    extraConfig = "AcceptEnv SPLUNKBASE_USERNAME SPLUNKBASE_PASSWORD FREE_ASKAMERICA_KEY";
  };

  # The swap an Ubuntu runner has (4 GB beside its 16 GB), and the 4 GB of memory the guest leaves
  # QEMU and the runner: 20 GB in all, as there. Without it Trino's 5 GB heap, kept after its text
  # search tests, and ClickHouse's own text search left the guest out of memory, and the kernel
  # killed clickhouse-server mid-test (run 45940557737, core 2/21).
  # qemu-vm.nix empties swapDevices at mkVMOverride (priority 10) whenever the VM uses its default
  # file systems, which beats a plain definition and mkForce (50): run 45997449404 booted with
  # "Swap: 0" and the kernel killed clickhouse-server again. Priority 9 is the one that wins.
  virtualisation.vmVariant.swapDevices = lib.mkOverride 9 [
    {
      device = "/var/lib/swapfile";
      size = 8192;
    }
  ];

  virtualisation.vmVariant.virtualisation = {
    # A hosted runner has 4 cores and 16 GB; the guest gets all but what QEMU and the runner need.
    cores = 4;
    memorySize = 12288;
    # MB, allocated as it is written: the lanes' images and the checkout's environments.
    # A core shard's images and volumes filled 40 GB; the runner has about 100 GB free.
    diskSize = 81920;
    # What the guest adds to /nix/store goes on that disk. By default it goes to a tmpfs (half the
    # memory), and `nix run` filled it unpacking the AppImage: "No space left on device"
    # (run 45720472876, nix run leg).
    writableStoreUseTmpfs = false;
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

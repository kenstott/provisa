# Provisa on a user's own NixOS host (REQ-1900): the root flake's nixosModules.provisa.
#
#   inputs.provisa.url = "github:kenstott/provisa";
#   modules = [ inputs.provisa.nixosModules.provisa { services.provisa.enable = true; } ];
#
# It runs the flake's package -- the Linux release in an FHS environment -- as the unprivileged
# system user `provisa`, whose home is the data directory: no login, no root, no source checkout.
# The CI host in packaging/nixos (configuration.nix) is not this module.
{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.provisa;
  inherit (lib)
    mkEnableOption
    mkOption
    mkIf
    types
    ;

  # The settings every Provisa install needs of a NixOS host, as install.sh hands them out. Read
  # here rather than imported: a module imports nothing unless it is enabled.
  preinstall = import ./preinstall.nix { inherit pkgs; };

  # The release's first launch asks its questions on stdin: these are its defaults (DuckDB, the
  # built-in observability, no demo, no data-quality checker, this host's address, API port 8000),
  # the native tier. The ports are set in its config.yaml afterwards.
  firstLaunchAnswers = pkgs.writeText "provisa-first-launch-answers" "\n\n\n\n\n\n";

  # `provisa start` starts the API and the UI server in the background and returns. The service
  # stays up while both run: it exits, and systemd restarts it, when either stops.
  start = pkgs.writeShellScript "provisa-service-start" ''
    set -euo pipefail
    provisa=${lib.getExe cfg.package}
    home="$HOME/.provisa"

    # The first launch of a new version stages the release into ~/.provisa; then the help text.
    "$provisa" help < ${firstLaunchAnswers}

    sed -i \
      -e 's/^api_port:.*/api_port: ${toString cfg.port}/' \
      -e 's/^ui_port:.*/ui_port: ${toString cfg.uiPort}/' \
      "$home/config.yaml"

    "$provisa" start
    read -r -a pids < "$home/.native.pid"
    while kill -0 "''${pids[@]}" 2>/dev/null; do sleep 5; done
    echo "provisa: a server process exited; see $home/.logs" >&2
    exit 1
  '';
in
{
  options.services.provisa = {
    enable = mkEnableOption "Provisa, from its Linux release, as a system service";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "provisa.packages.\${system}.default";
      description = "The Provisa package the service runs: the root flake's release package.";
    };

    port = mkOption {
      type = types.port;
      default = 8000;
      description = "The API's port.";
    };

    uiPort = mkOption {
      type = types.port;
      default = 3000;
      description = "The UI's port.";
    };

    dataDir = mkOption {
      type = types.path;
      default = "/var/lib/provisa";
      description = "The service user's home. Provisa keeps its runtime, configuration and data in .provisa under it.";
    };

    openFirewall = mkOption {
      type = types.bool;
      default = false;
      description = "Open the API and UI ports in the firewall.";
    };
  };

  config = mkIf cfg.enable {
    # The libraries the release's binaries load, for any of them started outside the package's FHS
    # environment, and the marker its first launch checks for them.
    programs.nix-ld = {
      enable = true;
      inherit (preinstall.programs.nix-ld) libraries;
    };
    environment.etc."provisa/nixos-preinstall".text =
      preinstall.environment.etc."provisa/nixos-preinstall".text;

    users.groups.provisa = { };
    users.users.provisa = {
      isSystemUser = true;
      group = "provisa";
      home = cfg.dataDir;
      createHome = true;
    };

    networking.firewall.allowedTCPPorts = mkIf cfg.openFirewall [
      cfg.port
      cfg.uiPort
    ];

    # The federation engine runs in a compose network and dials the servers Provisa starts on the
    # host: a code source's sidecar, bound at the default bridge's gateway. The NixOS firewall
    # drops those connections, so Docker's own bridges (docker0 and each compose network's br-*)
    # are trusted, as on the CI host (configuration.nix); every other interface keeps the firewall
    # (REQ-1900, maintainer 2026-11-26).
    networking.firewall.trustedInterfaces = [
      "docker0"
      "br-+"
    ];

    systemd.services.provisa = {
      description = "Provisa";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment = {
        HOME = cfg.dataDir;
        SSL_CERT_FILE = "/etc/ssl/certs/ca-certificates.crt";
        # A system service gets no login session, so no XDG_RUNTIME_DIR: platformdirs then
        # picked /tmp/runtime-<uid>, which it refuses when another uid owns it, and the embedded
        # control plane did not start (run 45680556710). RuntimeDirectory below makes this one,
        # owned by the service user.
        XDG_RUNTIME_DIR = "/run/provisa";
      };
      serviceConfig = {
        Type = "simple";
        User = "provisa";
        Group = "provisa";
        RuntimeDirectory = "provisa";
        RuntimeDirectoryMode = "0700";
        WorkingDirectory = cfg.dataDir;
        ExecStart = start;
        ExecStop = "${lib.getExe cfg.package} stop";
        Restart = "on-failure";
        RestartSec = 10;
      };
    };
  };
}

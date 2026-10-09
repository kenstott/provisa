# A NixOS host that runs Provisa's native tier from a source checkout and runs its test suites.
#
# The checkout lives at /home/provisa/provisa and its environment is the repo's own `uv sync` venv,
# exactly as on every other Linux host. What NixOS changes is that nothing prebuilt finds a loader
# or a library at an FHS path, so the interpreter uv downloads, the wheels' native modules, the
# embedded PostgreSQL and Playwright's Chromium all load through nix-ld.
{ pkgs, lib, ... }:
let
  repo = "/home/provisa/provisa";
  nixLd = "/run/current-system/sw/share/nix-ld/lib";
  caBundle = "/etc/ssl/certs/ca-certificates.crt";
in
{
  # What any Provisa install needs of a NixOS host; the installer hands a user the same file.
  imports = [ ./preinstall.nix ];

  system.stateVersion = "26.05";

  # Microsoft's ODBC driver is the one unfree package (the SQL Server warehouse replica target).
  nixpkgs.config.allowUnfreePredicate = pkg: builtins.elem (lib.getName pkg) [ "msodbcsql18" ];

  users.users.provisa = {
    isNormalUser = true;
    extraGroups = [
      "docker"
      "wheel"
    ];
  };
  security.sudo.wheelNeedsPassword = false;

  # Docker's own default, on every other distro. NixOS sends container output to journald, which
  # drops lines past its rate limit: `docker logs` of a busy container is then missing some.
  virtualisation.docker.logDriver = "json-file";

  # pyproject pins Python to 3.12; uv supplies that interpreter itself, as it does in CI.
  environment.variables.UV_PYTHON_PREFERENCE = "only-managed";

  environment.systemPackages = with pkgs; [
    git
    uv
    nodejs_22
    maven
    # tests/unit renders the chart with `helm template`.
    kubernetes-helm
    curl
    openssl
    zstd
    # pg_dump and pg_restore for the integration suites' snapshots, at the major the
    # postgres:16 image they dump runs.
    postgresql_16
    # The worker-boot integration tests read a process's listening sockets with it.
    lsof
    # The embedded PostgreSQL's contrib FDWs are built from the PostgreSQL 16.2 source, which is
    # C17: the default compiler's C23 does not build it.
    gcc14
    gnumake
    unixodbc
  ];

  # Registers "ODBC Driver 18 for SQL Server" in /etc/odbcinst.ini.
  environment.unixODBCDrivers = [ pkgs.unixodbcDrivers.msodbcsql18 ];

  # The DuckDB firebird extension is given the client library by path, and looks for it where a
  # distro package puts it.
  systemd.tmpfiles.rules = [
    "d /usr/lib 0755 root root -"
    "L+ /usr/lib/libfbclient.so.2 - - - - ${pkgs.firebird}/lib/libfbclient.so.2"
  ];

  networking.firewall.allowedTCPPorts = [
    3000
    8000
  ];

  # The native tier, started the way a user starts it: `provisa run`. The unit is skipped until the
  # checkout has its venv.
  systemd.services.provisa = {
    description = "Provisa (native tier, demo config)";
    wantedBy = [ "multi-user.target" ];
    wants = [ "network-online.target" ];
    after = [ "network-online.target" ];
    unitConfig.ConditionPathExists = "${repo}/.venv/bin/provisa";
    # A unit's PATH is a handful of core tools. The adapter servers the service starts are
    # `#!/usr/bin/env bash` launchers, so it gets the PATH a login has.
    path = [ "/run/current-system/sw" ];
    # A service does not get the login environment nix-ld is configured through.
    environment = {
      NIX_LD = "${nixLd}/ld.so";
      NIX_LD_LIBRARY_PATH = nixLd;
      SSL_CERT_FILE = caBundle;
      # An adapter server's Python starts its JVM through JPype, which finds libjvm.so by
      # JAVA_HOME or in /usr/lib/jvm, not in the JRE its own bundle ships.
      JAVA_HOME = pkgs.jdk21.home;
    };
    serviceConfig = {
      User = "provisa";
      WorkingDirectory = repo;
      StateDirectory = "provisa";
      ExecStart = "${repo}/.venv/bin/provisa run --demo --host 0.0.0.0 --no-browser --data-dir /var/lib/provisa";
      Restart = "on-failure";
    };
  };
}

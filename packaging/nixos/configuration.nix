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
in
{
  system.stateVersion = "25.11";

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

  # The container-backed lanes provision their sources with docker compose.
  virtualisation.docker.enable = true;

  programs.nix-ld = {
    enable = true;
    # What a manylinux binary expects the distro to provide. A library missing from this list is a
    # NixOS difference: add it here when a lane names it.
    libraries = with pkgs; [
      stdenv.cc.cc.lib
      zlib
      openssl
      krb5
      icu
      libxml2
      libuuid
      curl
      unixODBC
      # Playwright's Chromium.
      glib
      nss
      nspr
      at-spi2-core
      cups
      dbus
      libdrm
      expat
      libxkbcommon
      libgbm
      pango
      cairo
      alsa-lib
      systemd
      fontconfig
      freetype
      xorg.libX11
      xorg.libXcomposite
      xorg.libXdamage
      xorg.libXext
      xorg.libXfixes
      xorg.libXrandr
      xorg.libxcb
    ];
  };

  # pyproject pins Python to 3.12; uv supplies that interpreter itself, as it does in CI.
  environment.variables.UV_PYTHON_PREFERENCE = "only-managed";

  programs.java = {
    enable = true;
    package = pkgs.jdk21;
  };

  environment.systemPackages = with pkgs; [
    git
    uv
    nodejs_22
    maven
    curl
    openssl
    zstd
    unixODBC
  ];

  # Registers "ODBC Driver 18 for SQL Server" in /etc/odbcinst.ini.
  environment.unixODBCDrivers = [ pkgs.unixODBCDrivers.msodbcsql18 ];

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
    # A service does not get the login environment nix-ld is configured through.
    environment = {
      NIX_LD = "${nixLd}/ld.so";
      NIX_LD_LIBRARY_PATH = nixLd;
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

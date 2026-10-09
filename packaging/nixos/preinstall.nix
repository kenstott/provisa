# What Provisa needs of a NixOS host before it can be installed. install.sh checks for these
# settings and, where they are missing, hands the user this file and stops.
#
#   1. Copy this file to /etc/nixos/provisa.nix.
#   2. Add ./provisa.nix to the `imports` of /etc/nixos/configuration.nix.
#   3. sudo nixos-rebuild switch
{ pkgs, ... }:
{
  # The version of these settings a host has. install.sh reads it: keep it equal to the
  # installer's NIXOS_PREINSTALL_VERSION, and raise both when a setting is added.
  environment.etc."provisa/nixos-preinstall".text = "1\n";

  # Nothing prebuilt finds a loader or a library at an FHS path on NixOS, so the Python runtime,
  # its wheels' native modules, the embedded PostgreSQL and Chromium all load through nix-ld.
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
      unixodbc
      # The embedded PostgreSQL's sqlite_fdw.
      sqlite
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
      libx11
      libxcomposite
      libxdamage
      libxext
      libxfixes
      libxrandr
      libxcb
    ];
  };

  # Provisa's services, and the sources of its demo, run under docker compose.
  virtualisation.docker.enable = true;

  # A Python built for other distros looks for its CA store at /etc/ssl/cert.pem or in a hashed
  # /etc/ssl/certs; NixOS has neither, so every https call from the standard library fails
  # verification unless it is told where the bundle is.
  environment.variables.SSL_CERT_FILE = "/etc/ssl/certs/ca-certificates.crt";

  # An adapter server's Python starts its JVM through JPype, which finds it by JAVA_HOME.
  programs.java = {
    enable = true;
    package = pkgs.jdk21;
  };

  # The `provisa` command reads `docker compose ps` with python3.
  environment.systemPackages = [ pkgs.python3 ];

  # The installer puts the `provisa` command in ~/.local/bin: NixOS has no /usr/local/bin.
  environment.localBinInPath = true;
}

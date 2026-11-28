# The toolchain for working on Provisa (`nix develop`): the one configuration.nix gives the NixOS
# host the CI runs its suites on.
{
  pkgs,
  lib,
  stdenv,
}:
let
  linux = stdenv.hostPlatform.isLinux;
  # What a manylinux wheel's native module expects the distro to provide: the list preinstall.nix
  # gives nix-ld. A shell has no nix-ld of its own, so the libraries are named to the loader.
  libraries = (import ./preinstall.nix { inherit pkgs; }).programs.nix-ld.libraries;
in
pkgs.mkShell {
  packages =
    with pkgs;
    [
      # pyproject pins Python to 3.12; uv builds the project's environment on this interpreter.
      python312
      uv
      nodejs_24
      jdk21
      maven
      # tests/unit renders the chart with `helm template`.
      kubernetes-helm
      git
      curl
      openssl
      zstd
      # pg_dump and pg_restore for the integration suites' snapshots.
      postgresql_16
      lsof
      gnumake
      unixodbc
      # `uv sync` compiles provisa._encoders (rust/encoders) into provisa.
      cargo
      rustc
    ]
    # The embedded PostgreSQL's contrib FDWs are built from C17 source on Linux.
    ++ lib.optionals linux [ gcc14 ];

  UV_PYTHON = "${pkgs.python312}/bin/python3.12";
  UV_PYTHON_DOWNLOADS = "never";
  JAVA_HOME = pkgs.jdk21.home;

  shellHook = lib.optionalString linux ''
    export LD_LIBRARY_PATH=${lib.makeLibraryPath libraries}
  '';
}

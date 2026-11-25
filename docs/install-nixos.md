# Install with Nix

The repository's root is a flake. It gives you Provisa's release as a package you can run, the NixOS module Provisa's own NixOS host is built from, and a shell with the toolchain for working on Provisa.

## Run Provisa

On x86_64 Linux with Nix and flakes enabled:

```bash
nix run github:kenstott/provisa
```

The first run downloads the Linux release and asks the same setup questions as the download from the site. Press Enter at each one for the default: DuckDB on the native tier, with no Docker. Provisa then starts in the background, with the UI on port 3000 and the API on port 8000.

Anything after `--` goes to the `provisa` command:

```bash
nix run github:kenstott/provisa -- status
nix run github:kenstott/provisa -- stop
```

The release runs inside a sandbox that puts its loader and libraries where a prebuilt Linux binary expects them. A NixOS host therefore needs no nix-ld and no other settings for it. Start Provisa through the flake each time. The `provisa` command the first run copies to `~/.local/bin` runs outside that sandbox, and on NixOS it needs the host settings described below.

The package is built for x86_64 Linux only, because the release has no Linux build for other processors.

## Import the module into a NixOS host

A host flake names the repository as an input and imports its module:

```nix
{
  inputs.provisa.url = "github:kenstott/provisa";
  inputs.nixpkgs.follows = "provisa/nixpkgs";

  outputs = { nixpkgs, provisa, ... }: {
    nixosConfigurations.my-host = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./hardware-configuration.nix
        provisa.nixosModules.provisa
      ];
    };
  };
}
```

The module makes the host Provisa's NixOS CI runs on. It turns on nix-ld with the libraries prebuilt binaries need, turns on Docker and adds a JDK. It also creates a `provisa` user in the `wheel` and `docker` groups, with sudo that asks for no password. Its `provisa` service runs `provisa run --demo` from a source checkout at `/home/provisa/provisa`, and it stays skipped until that checkout has its `.venv`. Read `packaging/nixos/configuration.nix` before you import it into a host you share.

## Work on Provisa

```bash
nix develop
```

The shell has Python 3.12 with uv, Node 24, JDK 21, Maven, Helm, Rust, PostgreSQL 16's client tools and the ODBC driver manager. uv builds the project's environment on the shell's Python. On Linux the shell also points the loader at the libraries that prebuilt wheels expect.

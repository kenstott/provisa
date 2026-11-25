# Install with Nix

The repository's root is a flake. It gives you Provisa's release as a package you can run, a NixOS module that runs that package as a system service, and a shell with the toolchain for working on Provisa.

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

The release runs inside a sandbox that puts its loader and libraries where a prebuilt Linux binary expects them. A NixOS host therefore needs no nix-ld and no other settings for it. Start Provisa through the flake each time. The `provisa` command the first run copies to `~/.local/bin` runs outside that sandbox, and on NixOS it needs nix-ld with the libraries in `packaging/nixos/preinstall.nix`.

The package is built for x86_64 Linux only, because the release has no Linux build for other processors.

## Run Provisa as a NixOS service

A host flake names the repository as an input, imports its module and turns the service on:

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
        { services.provisa.enable = true; }
      ];
    };
  };
}
```

After `sudo nixos-rebuild switch`, the `provisa` systemd service runs the flake's package as the system user `provisa`. That user has no login shell, no password and no sudo, and is in no group but its own. No source checkout is needed. The service's first start stages the release with the setup defaults (DuckDB on the native tier, no Docker, no demo), so it takes a few minutes before the API answers.

The module also turns on nix-ld with the libraries the release's binaries load, for any of them started outside the package's sandbox.

| Option | Default | What it sets |
|---|---|---|
| `services.provisa.enable` | `false` | Runs the service. |
| `services.provisa.port` | `8000` | The API's port. |
| `services.provisa.uiPort` | `3000` | The UI's port. |
| `services.provisa.dataDir` | `/var/lib/provisa` | The service user's home. Provisa keeps its runtime, configuration and data in `.provisa` under it. |
| `services.provisa.openFirewall` | `false` | Opens the API and UI ports. |
| `services.provisa.package` | the flake's package | The release the service runs. |

The service stays active while the API and the UI server run. When either stops, systemd restarts the service. Its logs are in `journalctl -u provisa` and in `.provisa/.logs` under the data directory.

`packaging/nixos/configuration.nix` is a different thing: the host Provisa's NixOS CI runs its test suites on. It is not exported by the flake.

## Work on Provisa

```bash
nix develop
```

The shell has Python 3.12 with uv, Node 24, JDK 21, Maven, Helm, Rust, PostgreSQL 16's client tools and the ODBC driver manager. uv builds the project's environment on the shell's Python. On Linux the shell also points the loader at the libraries that prebuilt wheels expect.

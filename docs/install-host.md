# Install on a Linux Host with Trino

`scripts/install-host.sh` installs Provisa on a Linux machine you already run, a virtual machine or bare metal, with Trino as its federation engine. Run it on the host, or from your workstation over SSH. Run it again with a newer release and it upgrades the install in place.

## What you need

- Linux on x86_64 or aarch64, with systemd.
- Docker, with the daemon running. The script checks `docker info` and stops if the daemon does not answer.
- `curl` and `tar`.
- Root on the host. Over SSH, your account needs `sudo`.
- Outbound HTTPS to GitHub (the release assets, uv and its Python build), Docker Hub (the Trino image) and PyPI (Provisa's third-party dependencies). With `--assets-dir` the release assets come from a local directory and GitHub is reached only for uv.

The host needs no Python of its own. The script fetches uv, and uv fetches the CPython 3.12 that Provisa runs on.

## Install

On the host:

```bash
sudo scripts/install-host.sh --version v0.1.0-alpha.511
```

From your workstation:

```bash
scripts/install-host.sh --version v0.1.0-alpha.511 --ssh admin@db-host-1
```

Over SSH the script sends itself to the host and runs there under `sudo`, with every other option you gave it. Nothing is installed on your workstation.

When it finishes, open `http://<host>:3000`. The first visit runs the setup wizard, where you choose how people sign in.

## What it installs

| Piece | Where |
| --- | --- |
| Provisa, from the release's own wheels | `/opt/provisa/venvs/<version>`, with `/opt/provisa/current` pointing at it |
| Trino plugins, from the release's plugin tarball | `/opt/provisa/trino/plugins` |
| Trino coordinator | Docker container `provisa-trino`, host network, restarts with Docker |
| Trino engine config and its shared secret | `/etc/provisa/trino/` |
| Provisa config | `/etc/provisa/provisa.yaml` |
| Service environment | `/etc/provisa/provisa.env` |
| Control plane and Trino catalog files | `/var/lib/provisa` |
| Service | `provisa.service`, run as the `provisa` system user |

Provisa is installed from three wheels attached to the GitHub release: `provisa`, `provisa_notebook` and `provisa_duckdb_ext`. The script names them as files, so the provisa package never comes from a public registry. Their third-party dependencies come from PyPI.

The config selects Trino:

```yaml
federation_engine: trino
federation_engine_host: 127.0.0.1
federation_engine_port: 8080
```

`provisa.env` does not set `PROVISA_ENGINE`. The embedded launch profile names DuckDB only when neither the environment nor the config file names an engine, so the config's `federation_engine` selects Trino.

Trino runs with Docker's host network. It reaches your sources over the same network Provisa uses, and listens on the Trino port directly on the host. Provisa creates catalogs in Trino at runtime, so the engine config holds no catalog files.

## Health check

The last step checks the install and names each thing that failed. It waits up to `--health-timeout` seconds (300 by default) for each service to come up. It checks:

1. The `provisa-trino` container is running.
2. Trino's `/v1/info` reports it has finished starting.
3. `provisa.service` is active.
4. Provisa's `/ready` reports ready.
5. Provisa's `/health` reports status `ok`.
6. The UI answers.
7. Trino has run a query as user `provisa`. Provisa runs `SELECT 1` through its engine at startup, so a Trino that has seen no query from Provisa means Provisa is not using it.

A failure prints a list and the script exits non-zero:

```text
[install-host] health check FAILED:
  - systemd service provisa is not active (journalctl -u provisa)
  - Provisa API at http://127.0.0.1:8000/ready did not report ready within 300s
```

## Upgrade

Run the script again with the new tag:

```bash
sudo scripts/install-host.sh --version v0.1.0-alpha.512
```

The upgrade builds a fresh virtualenv for the new release, points `/opt/provisa/current` at it, replaces the Trino plugins and the Trino container, and restarts the service. After the health check passes it deletes the previous release's virtualenv.

These survive an upgrade:

- `/etc/provisa/provisa.yaml`, including whatever the setup wizard and the admin UI wrote to it.
- The control plane and everything else in `/var/lib/provisa`.
- The Trino shared secret in `/etc/provisa/trino/trino.env`.

`provisa.env`, the Trino engine config and the systemd unit are written again on every run from the options you pass. Change them through the options and re-run.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--version TAG` | required | Release tag to install, such as `v0.1.0-alpha.511` |
| `--ssh USER@HOST` | | Install on that host over SSH |
| `--assets-dir DIR` | | Take the release assets from `DIR` on the installing machine |
| `--repo OWNER/NAME` | `kenstott/provisa` | Repository the release belongs to |
| `--prefix DIR` | `/opt/provisa` | Virtualenvs, uv, Python and plugins |
| `--data-dir DIR` | `/var/lib/provisa` | Provisa state |
| `--config-dir DIR` | `/etc/provisa` | Config, service environment, Trino config |
| `--bind ADDR` | `0.0.0.0` | Address Provisa listens on |
| `--api-port PORT` | `8000` | Provisa API |
| `--ui-port PORT` | `3000` | Provisa UI |
| `--trino-port PORT` | `8080` | Trino coordinator, on the host |
| `--trino-heap-gb N` | `5` | Trino JVM heap |
| `--trino-image IMAGE` | `trinodb/trino:481` | Trino image |
| `--health-timeout SECS` | `300` | How long the health check waits for each service |

For `--assets-dir`, put these files from the release in one directory. Names are shown for `v0.1.0-alpha.511` on x86_64:

- `provisa-0.1.0a511-cp312-cp312-manylinux_2_28_x86_64.whl`
- `provisa_notebook-0.1.0a511-py3-none-any.whl`
- `provisa_duckdb_ext-<version>-py3-none-manylinux_2_28_x86_64.whl`
- `provisa-trino-plugins-v0.1.0-alpha.511.tar.gz`

With `--ssh`, copy the directory to the host and run the script there instead. The script refuses `--assets-dir` together with `--ssh` because the directory would have to exist on the remote machine.

## Operate

```bash
systemctl status provisa
journalctl -u provisa -f
docker logs -f provisa-trino
```

Stop everything with `systemctl stop provisa` and `docker stop provisa-trino`.

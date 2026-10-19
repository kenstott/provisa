# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3c5a71-4b2d-48f6-a0c8-1d7e6b2f9c34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A node's provisa.env keeps the settings first launch does not generate.

``scripts/deploy-cloud.sh`` adds keys to the node's systemd EnvironmentFile that first launch does
not know (the store bind address, the telemetry object store, Grafana's admin password, the billing
keys). First launch truncated the file and wrote its own list, so a reset or a version bump, which
reruns it, dropped them and the node came up without them. ``write_env_file`` writes what the run
sets and keeps every other line.

The function is run here as the script runs it (``bash -euo pipefail``)."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

_FIRST_LAUNCH = (Path(__file__).parents[2] / "packaging/linux/first-launch.sh").read_text()


def _function(name: str) -> str:
    start = _FIRST_LAUNCH.index(f"{name}() {{")
    return _FIRST_LAUNCH[start : _FIRST_LAUNCH.index("\n}\n", start) + 3]


def _write(env_file: Path, run_env: dict[str, str]) -> None:
    done = subprocess.run(
        [
            "bash",
            "-euo",
            "pipefail",
            "-c",
            _function("write_env_file") + 'write_env_file "$1"\n',
            "_",
            str(env_file),
        ],
        # This process's own PATH: a fixed one names directories that hold no bash on NixOS.
        env={"PATH": os.environ["PATH"], **run_env},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr


def _keys(env_file: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in env_file.read_text().splitlines() if "=" in line)


def test_a_rerun_keeps_the_keys_it_does_not_generate_and_updates_the_ones_it_does(tmp_path):
    env_file = tmp_path / "provisa.env"
    env_file.write_text(
        "PROVISA_ENGINE=duckdb\n"
        "PROVISA_IDP=firebase\n"
        "PROVISA_STORE_BIND_IP=10.0.0.5\n"
        "PROVISA_OTEL_S3_ENDPOINT=https://storage.googleapis.com\n"
        "PROVISA_OTEL_S3_BUCKET=otel-signals\n"
        "LEMONSQUEEZY_API_KEY=ls-key\n"
        "LEMONSQUEEZY_STORE_ID=4711\n"
        "PROVISA_GRAFANA_ADMIN_PASSWORD=kept-from-before\n"
    )
    env_file.chmod(0o600)

    _write(
        env_file,
        {"PROVISA_ENGINE": "trino", "PROVISA_IDP": "firebase", "CONFIG_DB_HOST": "10.1.0.3"},
    )

    keys = _keys(env_file)
    # Set by this run: this run's value, once.
    assert keys["PROVISA_ENGINE"] == "trino"
    assert keys["CONFIG_DB_HOST"] == "10.1.0.3"
    assert env_file.read_text().count("PROVISA_ENGINE=") == 1
    assert env_file.read_text().count("PROVISA_IDP=") == 1
    # Not known to first launch: kept as they were.
    assert keys["PROVISA_OTEL_S3_ENDPOINT"] == "https://storage.googleapis.com"
    assert keys["PROVISA_OTEL_S3_BUCKET"] == "otel-signals"
    assert keys["LEMONSQUEEZY_API_KEY"] == "ls-key"
    assert keys["LEMONSQUEEZY_STORE_ID"] == "4711"
    # Known to first launch but not given this run: kept, never blanked.
    assert keys["PROVISA_STORE_BIND_IP"] == "10.0.0.5"
    assert keys["PROVISA_GRAFANA_ADMIN_PASSWORD"] == "kept-from-before"
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert [p.name for p in tmp_path.iterdir()] == ["provisa.env"]  # no temporary file left


def test_a_first_run_writes_only_what_it_was_given(tmp_path):
    env_file = tmp_path / "provisa.env"

    _write(env_file, {"PROVISA_IDP": "keycloak", "PROVISA_MULTITENANCY": "true"})

    assert _keys(env_file) == {"PROVISA_IDP": "keycloak", "PROVISA_MULTITENANCY": "true"}
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_install_systemd_writes_the_environment_file_through_the_merge():
    body = _function("install_systemd")
    assert 'write_env_file "${PROVISA_HOME}/provisa.env"' in body
    assert ': > "$env_file"' not in _FIRST_LAUNCH

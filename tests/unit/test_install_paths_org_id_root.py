# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d3b52-8a47-4c0e-9d25-b3a7c1e04f68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1296: every install path that writes the service environment sets ORG_ID=root.

The resolver's own default (provisa/core/models.py) is unchanged; an install names the bootstrap
org itself."""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path

import yaml

_ROOT = Path(__file__).parents[2]
_FIRST_LAUNCH = (_ROOT / "packaging/linux/first-launch.sh").read_text()


def test_install_host_env_file_sets_org_id_root():
    script = _ROOT / "scripts" / "install-host.sh"
    done = subprocess.run(
        [
            "bash",
            "-euo",
            "pipefail",
            "-c",
            f"source {shlex.quote(str(script))}\nrender_env_file /c /d 8080 5432 pw 9000 mpw 8480\n",
        ],
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr
    assert "ORG_ID=root" in done.stdout.splitlines()


def _first_launch_write(env_file: Path) -> list[str]:
    start = _FIRST_LAUNCH.index("write_env_file() {")
    fn = _FIRST_LAUNCH[start : _FIRST_LAUNCH.index("\n}\n", start) + 3]
    done = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", fn + 'write_env_file "$1"\n', "_", str(env_file)],
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr
    return env_file.read_text().splitlines()


def test_first_launch_env_file_sets_org_id_root_on_a_fresh_file(tmp_path):
    lines = _first_launch_write(tmp_path / "provisa.env")
    assert lines.count("ORG_ID=root") == 1


def test_first_launch_env_file_keeps_an_existing_org_id(tmp_path):
    env_file = tmp_path / "provisa.env"
    env_file.write_text("ORG_ID=default\nPROVISA_ENGINE=duckdb\n")
    env_file.chmod(0o600)
    lines = _first_launch_write(env_file)
    assert lines.count("ORG_ID=default") == 1
    assert not any(line.startswith("ORG_ID=") and line != "ORG_ID=default" for line in lines)
    assert "PROVISA_ENGINE=duckdb" in lines


def test_first_launch_primary_overlay_forwards_org_id_to_the_container():
    assert re.search(r'^\s+ORG_ID: "\\\$\{ORG_ID:-\}"$', _FIRST_LAUNCH, re.MULTILINE)


def test_nixos_module_sets_org_id_root():
    nix = (_ROOT / "packaging/nixos/configuration.nix").read_text()
    assert re.search(r'^\s+ORG_ID = "root";$', nix, re.MULTILINE)


def test_helm_values_set_org_id_root():
    values = yaml.safe_load((_ROOT / "helm/provisa/values.yaml").read_text())
    assert values["provisa"]["env"]["ORG_ID"] == "root"


def test_windows_native_launch_sets_org_id_root():
    ps1 = (_ROOT / "packaging/windows/provisa-native.ps1").read_text()
    assert "$e['ORG_ID'] = 'root'" in ps1


def test_install_host_env_file_keeps_an_existing_org_id(tmp_path):
    script = _ROOT / "scripts" / "install-host.sh"
    env_file = tmp_path / "provisa.env"
    env_file.write_text("ORG_ID=acme\nOTHER=1\n")
    done = subprocess.run(
        [
            "bash",
            "-euo",
            "pipefail",
            "-c",
            f"source {shlex.quote(str(script))}\n"
            f'org="$(existing_org_id {shlex.quote(str(env_file))})"\n'
            'render_env_file /c /d 8080 5432 pw 9000 mpw 8480 "$org"\n',
        ],
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    assert lines.count("ORG_ID=acme") == 1
    assert "ORG_ID=root" not in lines

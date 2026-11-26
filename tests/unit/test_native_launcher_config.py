# Copyright (c) 2026 Kenneth Stott
# Canary: 6a096d85-6662-444a-8a35-7913fae72993
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The native launcher (scripts/provisa, `provisa start` on a native install) names the config file
the API loads (REQ-528).

PROVISA_CONFIG has no default in the app (provisa/core/config_location.py), so a native start that
sets it only for the demo started a non-demo API that died at create_app (NixOS prepared host, run
45655242557). A non-demo native install loads ${PROVISA_HOME}/provisa.yaml: absent on the first
start, where the setup wizard writes it from the shipped skeleton (REQ-164)."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_WRAPPER = _REPO / "scripts" / "provisa"


def _env_pairs(tmp_path: Path, demo: bool) -> dict[str, str]:
    """native_env_pairs from the wrapper itself, run against a stub runtime in ``tmp_path``."""
    home = tmp_path / "provisa-home"
    runtime = home / "venv"
    (runtime / "bin").mkdir(parents=True)
    py = runtime / "bin" / "python3"
    # The demo branch asks the runtime for the bundled demo config and its data dir.
    py.write_text("#!/bin/sh\necho /bundled/demo-path\n")
    py.chmod(0o755)
    script = f"""
set -euo pipefail
PROVISA_HOME={home}
RUNTIME_DIR={runtime}
DEPLOY_ENGINE=duckdb CONTROL_PLANE_URL=postgresql://cp API_PORT=8000
ENGINE_URL= MATERIALIZE_URL= TRINO_HOST= TRINO_PORT= OTLP_ENDPOINT=
DEMO={"true" if demo else "false"}
err() {{ echo "$*" >&2; }}
eval "$(sed -n '/^native_python() {{/,/^}}/p; /^native_env_pairs() {{/,/^}}/p' {_WRAPPER})"
native_env_pairs
"""
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True)
    return dict(line.split("=", 1) for line in out.stdout.splitlines())


def test_a_native_start_that_is_not_a_demo_names_the_installs_own_config(tmp_path):
    env = _env_pairs(tmp_path, demo=False)
    assert env["PROVISA_CONFIG"] == str(tmp_path / "provisa-home" / "provisa.yaml")
    assert "PROVISA_DEMO" not in env


def test_a_native_demo_start_names_the_bundled_demo_config(tmp_path):
    env = _env_pairs(tmp_path, demo=True)
    assert env["PROVISA_CONFIG"] == "/bundled/demo-path"
    assert env["PROVISA_DEMO"] == "1"


def test_every_run_of_the_runtime_python_keeps_the_working_directory_off_its_path():
    """Each `-m` or `-c` run of the native runtime passes -P. Without it Python puts the working
    directory first on sys.path, and `provisa start` from a directory holding a provisa/ source tree
    (a checkout) imported that tree, which has no compiled provisa._encoders: the API died at
    startup (NixOS download leg, run 45680556710)."""
    runs = [
        line.strip()
        for line in _WRAPPER.read_text().splitlines()
        if re.search(r'"\$(py|\(native_python\))" +-', line)
    ]
    assert len(runs) == 5, runs
    for run in runs:
        assert re.search(r'"\$(py|\(native_python\))" -P -[mc] ', run), run


def test_each_uvicorn_server_takes_its_app_directory_from_the_install_not_the_working_directory():
    """uvicorn inserts its --app-dir (default: the working directory) first on sys.path, which
    undoes -P: from a checkout the API imported the checkout's provisa/, which has no compiled
    provisa._encoders, and died at startup (NixOS download leg, run 45720472876)."""
    text = _WRAPPER.read_text()
    starts = re.findall(r"-m uvicorn [^\n]*", text)
    assert len(starts) == 2, starts
    for start in starts:
        assert '--app-dir "${PROVISA_HOME}/native"' in start, start

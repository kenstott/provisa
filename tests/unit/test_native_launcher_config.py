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

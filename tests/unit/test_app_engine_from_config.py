# Copyright (c) 2026 Kenneth Stott
# Canary: 4f5acdc7-35b6-47e7-a706-f48bf6c01398
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2269: the application's deployment engine is the config file's ``federation_engine``.

The deployment engine is built when ``provisa.api.app`` is imported (``state = AppState()``), so
the persisted-config provider the engine layer reads must be registered before that. Imported in a
fresh interpreter: the module's state is built once per process.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_PROBE = "from provisa.api.app import state\nprint(state.federation_engine.engine.selected_key)\n"


def _engine_on_import(tmp_path: Path, config_body: str) -> str:
    cfg = tmp_path / "provisa.yaml"
    cfg.write_text(config_body)
    env = {k: v for k, v in os.environ.items() if k != "PROVISA_ENGINE"}
    env["PROVISA_CONFIG"] = str(cfg)
    out = subprocess.run(
        [sys.executable, "-c", _PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert out.returncode == 0, out.stderr[-4000:]
    return out.stdout.strip().splitlines()[-1]


def test_config_federation_engine_selects_the_deployment_engine(tmp_path: Path) -> None:
    assert _engine_on_import(tmp_path, "federation_engine: trino\n") == "trino"


def test_config_without_an_engine_gets_the_embedded_default(tmp_path: Path) -> None:
    assert _engine_on_import(tmp_path, "sources: []\n") == "duckdb"

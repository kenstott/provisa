# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2f4c61-0b7a-4d93-a5e8-3c19d7f0b42a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""First launch of the macOS Core DMG stages the DuckDB extensions as every installer does.

REQ-294 (amended 2026-11-22): no installer carries the extensions as a separate release asset. The
Core DMG ships none (codesign cannot sign them); first-launch.sh copies the runtime and runs the one
stage command, ``provisa.runtime_deps.stage_assets``, which fetches them with pip from the release's
Pages index. The runtime's version stamp is written only once that command has succeeded, so a
failed stage is retried on the next launch rather than taken as done.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_FIRST_LAUNCH = (_REPO / "packaging/macos/first-launch.sh").read_text()


def _functions() -> str:
    """first-launch.sh's payload lookup and native-runtime staging, without the rest."""
    start = _FIRST_LAUNCH.index("_find_payload() {")
    end = _FIRST_LAUNCH.index("# ── REQ-1443: the operator's chosen data-quality checker")
    return _FIRST_LAUNCH[start:end]


def _setup(tmp_path: Path, *, stage_exit: int) -> dict[str, Path]:
    """A DMG payload ``runtime`` beside the .app whose python3 records its arguments and exits
    ``stage_exit`` for the stage command, and stub xattr/codesign."""
    paths = {
        "bundle": tmp_path / "dmg/Provisa.app/Contents",
        "runtime": tmp_path / "dmg/Provisa.app/runtime",
        "home": tmp_path / "home/.provisa",
        "bin": tmp_path / "bin",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    python = paths["runtime"] / "bin/python3"
    python.parent.mkdir()
    python.write_text(
        "#!/bin/bash\n"
        f'echo "$*" >> "{tmp_path}/python.log"\n'
        '[ "$1" = "-c" ] && exit 0\n'
        f"exit {stage_exit}\n"
    )
    python.chmod(0o755)
    (paths["runtime"] / ".runtime-version").write_text("v9")
    for tool in ("xattr", "codesign"):
        (paths["bin"] / tool).write_text("#!/bin/bash\nexit 0\n")
        (paths["bin"] / tool).chmod(0o755)
    return paths


def _launch(paths: dict[str, Path]) -> subprocess.CompletedProcess:
    # BUNDLE_DIR is Contents; _find_payload looks first in its parent, where the runtime is placed.
    program = (
        "set -euo pipefail\n"
        'info() { echo "INFO $*"; }; ok() { echo "OK $*"; }; err() { echo "$*" >&2; }\n'
        "install_dq_checker() { :; }\n"
        f'BUNDLE_DIR="{paths["bundle"]}"\n'
        f'PROVISA_HOME="{paths["home"]}"\n'
        'PROVISA_VERSION="v9"\n'
        f"{_functions()}\n"
        "stage_native_runtime\n"
    )
    return subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{paths['bin']}:/usr/bin:/bin", "HOME": str(paths["home"].parent)},
    )


def _python_calls(tmp_path: Path) -> list[str]:
    log = tmp_path / "python.log"
    return log.read_text().splitlines() if log.exists() else []


def test_no_extension_asset_is_restored_at_first_launch():
    assert "duckdb-extensions.sha256" not in _FIRST_LAUNCH
    assert "stage_duckdb_extensions" not in _FIRST_LAUNCH
    assert "provisa-duckdb-extensions-" not in _FIRST_LAUNCH


def test_first_launch_runs_the_one_stage_command_then_stamps_the_runtime(tmp_path):
    paths = _setup(tmp_path, stage_exit=0)
    result = _launch(paths)
    assert result.returncode == 0, result.stderr
    assert f"-I -m provisa.runtime_deps.stage_assets {paths['home']}" in _python_calls(tmp_path)
    assert (paths["home"] / "venv/.runtime-version").read_text() == "v9"


def test_a_failed_stage_leaves_no_stamp_so_the_next_launch_stages_again(tmp_path):
    paths = _setup(tmp_path, stage_exit=1)
    result = _launch(paths)
    assert result.returncode == 1
    assert "did not stage" in result.stderr
    # The runtime's copied stamp is gone: the next launch restages and reruns the stage command.
    assert not (paths["home"] / "venv/.runtime-version").exists()
    again = _launch(paths)
    assert again.returncode == 1
    stages = [c for c in _python_calls(tmp_path) if "stage_assets" in c]
    assert len(stages) == 2

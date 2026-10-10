# Copyright (c) 2026 Kenneth Stott
# Canary: 921cea92-db5a-4cfa-a99b-48b930d12830
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Linux AppImage's first launch finds the payload the AppImage carries (#207).

The default install — the native tier — stopped at ``name: unbound variable`` on any
distribution: ``_find_payload`` declared ``name`` and read it in the same ``local`` statement,
whose words are all expanded before any of it is assigned, and the script treats an unset
variable as an error. The message that followed blamed a missing interpreter that was there.

The function is run here as the script runs it (``bash -euo pipefail``)."""

from __future__ import annotations

import subprocess
from pathlib import Path

_FIRST_LAUNCH = Path(__file__).parents[2] / "packaging/linux/first-launch.sh"


def _find_payload_function() -> str:
    script = _FIRST_LAUNCH.read_text()
    start = script.index("_find_payload() {")
    return script[start : script.index("\n}\n", start) + 3]


def _run(appdir: Path, calls: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _find_payload_function() + calls],
        env={"APPDIR": str(appdir), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )


def test_the_script_treats_an_unset_variable_as_an_error():
    assert "\nset -euo pipefail\n" in _FIRST_LAUNCH.read_text()


def test_the_interpreter_the_appimage_carries_is_found(tmp_path):
    (tmp_path / "python-base/bin").mkdir(parents=True)
    (tmp_path / "python-base/bin/python3").write_text("")
    done = _run(tmp_path, "_find_payload python-base bin/python3\n")
    assert done.returncode == 0, done.stderr
    assert done.stdout == f"{tmp_path}/python-base"


def test_the_ui_the_appimage_carries_is_found(tmp_path):
    """Asked for with no file to test for: the directory is enough."""
    (tmp_path / "ui-dist").mkdir()
    done = _run(tmp_path, '_find_payload ui-dist ""\n')
    assert done.returncode == 0, done.stderr
    assert done.stdout == f"{tmp_path}/ui-dist"


def test_wheels_that_are_not_there_are_not_found_and_nothing_else_goes_wrong(tmp_path):
    (tmp_path / "wheels").mkdir()  # the directory, holding no wheel
    done = _run(tmp_path, '_find_payload wheels "*.whl" || echo absent\n')
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "absent" and done.stderr == ""
    done = _run(tmp_path / "nowhere", '_find_payload wheels "*.whl" || echo absent\n')
    assert done.stdout.strip() == "absent" and done.stderr == ""

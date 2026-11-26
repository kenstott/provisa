# Copyright (c) 2026 Kenneth Stott
# Canary: e0baca91-1788-40ac-9853-b78693e91cd7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every platform package that bundles Python packages ships exactly uv.lock's set, the one CI tests.

An unconstrained ``pip download`` resolved the newest versions at build time, so the AppImage shipped
platformdirs 4.14 against the lock's 4.10: a dependency set no test had run. Each packager exports
uv.lock hash-pinned and fetches or installs that export with nothing resolved beyond it
(``--require-hashes --no-deps -r``); provisa and provisa-notebook, built here, go in without deps.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]

#: packager -> the extras its export names.
_PACKAGERS = {
    "packaging/linux/build-appimage.sh": "--extra embedded --extra desktop",
    "packaging/macos/build-dmg.sh": "--extra embedded --extra desktop",
    "packaging/windows/build-sfx.ps1": "--extra vector --extra desktop",
}

#: The jobs that build a packager's bundled Python set (build-dmg.sh's verify and dmg steps, and
#: its container edition, install none).
_BUILD_JOBS = (
    (".github/workflows/release-linux.yml", "build-linux", "build-appimage.sh"),
    (".github/workflows/nixos.yml", "appimage-build", "build-appimage.sh"),
    (".github/workflows/release-macos.yml", "build-macos-core", "build-dmg.sh stage"),
    (".github/workflows/release-windows.yml", "build-windows-core", "build-sfx.ps1"),
)

_EXPORT_FLAGS = (
    "--frozen",
    "--no-dev",
    "--no-emit-project",
    "--no-emit-package provisa-notebook",
    "--format requirements-txt",
)


def _text(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def _joined(text: str) -> str:
    """The script with its line continuations (bash ``\\``, PowerShell's backtick) joined."""
    return re.sub(r"[ \t]*[\\`]\n[ \t]*", " ", text)


def _export_command(script: str) -> str:
    commands = [line for line in _joined(script).splitlines() if "uv export --" in line]
    assert len(commands) == 1, commands
    return commands[0]


@pytest.mark.parametrize("packager", sorted(_PACKAGERS))
def test_the_packager_exports_uv_lock_hash_pinned(packager):
    extras = _PACKAGERS[packager]
    command = _export_command(_text(packager))
    for flag in (*_EXPORT_FLAGS, extras):
        assert flag in command, (packager, flag)
    # The hashes are what make the fetch or install take exactly the locked files.
    assert "--no-hashes" not in command


@pytest.mark.parametrize("packager", sorted(_PACKAGERS))
def test_the_packager_takes_the_export_and_nothing_resolved_beyond_it(packager):
    script = _joined(_text(packager))
    takes = [
        line
        for line in script.splitlines()
        if re.search(r"pip (install|wheel)\b.*--require-hashes --no-deps -r ", line)
    ]
    assert len(takes) == 1, (packager, takes)
    assert script.index("uv export") < script.index(takes[0])
    # No third-party package is named to pip beside the export: that resolves it at PyPI's newest.
    for line in script.splitlines():
        if re.search(r"pip (install|download|wheel)\b", line) and not line.lstrip().startswith("#"):
            for name in ("uvicorn", "mcp-proxy", "[embedded]", "[vector]"):
                assert name not in line or "--dry-run" in line, (packager, line)
            assert " -c " not in line, (packager, line)  # constraints leave unlocked packages free


@pytest.mark.parametrize("packager", sorted(_PACKAGERS))
def test_the_packager_installs_provisa_and_the_notebook_without_deps(packager):
    script = _joined(_text(packager))
    if packager.endswith("build-appimage.sh"):
        # The wheelhouse: both built wheels are copied in, then the whole set must resolve from it.
        assert 'cp "$built_wheel" "$notebook_dir"/provisa_notebook-*.whl "$wheels/"' in script
        assert '--dry-run --quiet --ignore-installed --no-index --find-links "$wheels"' in script
    elif packager.endswith("build-dmg.sh"):
        assert (
            'pip install --quiet --no-deps "$built_wheel" "$notebook_dir"/provisa_notebook-*.whl'
            in script
        )
        assert '"$py" -m pip check' in script
    else:
        assert "pip install --quiet --no-deps $RepoRoot $NotebookWheel" in script
        assert "& $RuntimePy -m pip check" in script


def test_the_linux_first_launch_installs_the_wheelhouse_as_it_is():
    """Every wheel the AppImage carries is installed with no resolution and no index; the venv's
    pip is not upgraded from PyPI first, so the locked pip in the wheelhouse is the one shipped."""
    script = _text("packaging/linux/first-launch.sh")
    body = script[script.index("setup_native_venv() {") :]
    body = body[: body.index("\n}\n")]
    install = body.index('"$pip" install --quiet --no-index --no-deps "$wheels"/*.whl')
    check = body.index('"$pip" check')
    assert install < check
    assert "--upgrade pip" not in body


@pytest.mark.parametrize(("workflow", "job", "command"), _BUILD_JOBS)
def test_every_job_that_builds_a_bundled_set_installs_uv_first(workflow, job, command):
    steps = yaml.safe_load(_text(workflow))["jobs"][job]["steps"]
    builds = [i for i, step in enumerate(steps) if command in str(step.get("run", ""))]
    installs_uv = [
        i for i, step in enumerate(steps) if step.get("uses") == "./.github/actions/setup-python"
    ]
    assert builds, (workflow, job)
    assert installs_uv and installs_uv[0] < builds[0], (workflow, job)


def test_the_export_pins_every_package_with_its_hashes():
    """The packagers' export holds nothing a hash-checked, no-deps fetch cannot take: no editable or
    path requirement, every requirement ``==`` pinned and hashed."""
    uv = shutil.which("uv")
    assert uv, "uv is not on PATH"
    command = _export_command(_text("packaging/linux/build-appimage.sh"))
    flags = command[command.index("uv export") + len("uv export") : command.index(" -o ")].split()
    out = subprocess.run(
        [uv, "export", *flags], cwd=_ROOT, capture_output=True, text=True, check=True
    ).stdout
    requirements = [r for r in re.split(r"\n(?=\S)", out.strip()) if r.strip()]
    assert requirements
    for requirement in requirements:
        head = requirement.splitlines()[0]
        assert re.match(r"^[A-Za-z0-9_.\-]+==\S+", head), head
        assert "--hash=sha256:" in requirement, head
    names = {r.split("==")[0] for r in requirements}
    assert {"uvicorn", "mcp-proxy", "torch", "sentence-transformers", "platformdirs"} <= names

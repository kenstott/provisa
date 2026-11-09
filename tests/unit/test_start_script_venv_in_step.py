# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1f3c52-9a4e-4d86-b0c7-3e58a2d6f914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2223: a start refuses a virtual environment out of step with uv.lock.

Each test builds a throwaway project, lock and venv in a temp directory, from local path packages so
nothing is fetched. The start scripts are run from a copy cut off just after their check, so the
real argument handling and the real call run and nothing is started.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_CHECK = _REPO / "scripts" / "ensure-venv-in-step.sh"
_SCRIPTS = ["start-ui-install.sh", "start-ui.sh"]
_CALL = '"$SCRIPT_DIR/scripts/ensure-venv-in-step.sh" "$SCRIPT_DIR" || exit 1\n'
_FIX = re.compile(r"^    (cd .* uv sync --locked --inexact)$", re.MULTILINE)
_PROCEEDED = "PROCEEDED PAST THE CHECK"

_ROOT_TOML = """
[build-system]
requires = ["uv_build"]
build-backend = "uv_build"

[project]
name = "probe"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [{deps}]

[project.optional-dependencies]
opt = ["optdep"]

[tool.uv]
cache-keys = [{{ file = "pyproject.toml" }}, {{ file = "key.txt" }}]

[tool.uv.sources]
dep = {{ path = "dep" }}
optdep = {{ path = "optdep" }}
"""


def _env() -> dict[str, str]:
    env = {
        k: v for k, v in os.environ.items() if k not in ("UV_PROJECT_ENVIRONMENT", "VIRTUAL_ENV")
    }
    env["UV_OFFLINE"] = "1"
    env["UV_PYTHON"] = sys.executable
    return env


def _uv(project: Path, *args: str) -> None:
    subprocess.run(["uv", *args], cwd=project, env=_env(), check=True, capture_output=True)


def _write_package(directory: Path, name: str, version: str) -> None:
    (directory / "src" / name).mkdir(parents=True, exist_ok=True)
    (directory / "src" / name / "__init__.py").write_text("")
    (directory / "pyproject.toml").write_text(
        '[build-system]\nrequires = ["uv_build"]\nbuild-backend = "uv_build"\n\n'
        f'[project]\nname = "{name}"\nversion = "{version}"\nrequires-python = ">=3.12"\n'
    )


def _write_root(project: Path, deps: str = '"dep"') -> None:
    (project / "pyproject.toml").write_text(_ROOT_TOML.format(deps=deps))


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project whose .venv is in step with its uv.lock."""
    root = tmp_path / "checkout"
    (root / "src" / "probe").mkdir(parents=True)
    (root / "src" / "probe" / "__init__.py").write_text("")
    (root / "key.txt").write_text("one")
    _write_package(root / "dep", "dep", "1.0.0")
    _write_package(root / "optdep", "optdep", "1.0.0")
    _write_root(root)
    _uv(root, "lock")
    _uv(root, "sync", "--locked")
    return root


def _make_stale(project: Path) -> None:
    """The lock moves on to dep 2.0.0 while .venv keeps dep 1.0.0."""
    _write_package(project / "dep", "dep", "2.0.0")
    _uv(project, "lock")


def _venv_state(project: Path) -> dict[str, tuple[int, int]]:
    state: dict[str, tuple[int, int]] = {}
    for path in sorted((project / ".venv").rglob("*")):
        stat = path.lstat()
        state[str(path.relative_to(project))] = (stat.st_size, stat.st_mtime_ns)
    return state


def _check(project: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [shutil.which("bash") or "/bin/bash", str(_CHECK), str(project)],
        env=_env() if env is None else env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _assert_refused_unchanged_with_command(
    project: Path, before: dict[str, tuple[int, int]], result: subprocess.CompletedProcess[str]
) -> str:
    assert result.returncode == 1, result.stdout + result.stderr
    assert "out of step with uv.lock" in result.stderr
    assert _venv_state(project) == before
    match = _FIX.search(result.stderr)
    assert match, result.stderr
    return match.group(1)


def test_a_stale_venv_is_refused_with_the_command_and_left_unchanged(project: Path) -> None:
    _make_stale(project)
    before = _venv_state(project)

    result = _check(project)

    _assert_refused_unchanged_with_command(project, before, result)
    assert "- dep==1.0.0" in result.stderr
    assert "+ dep" in result.stderr


def test_the_printed_command_brings_the_venv_in_step(project: Path) -> None:
    _make_stale(project)
    before = _venv_state(project)

    command = _assert_refused_unchanged_with_command(project, before, _check(project))
    subprocess.run(["bash", "-c", command], env=_env(), check=True, capture_output=True)

    assert _check(project).returncode == 0


def test_an_in_step_venv_proceeds_unchanged(project: Path) -> None:
    before = _venv_state(project)

    result = _check(project)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stderr == ""
    assert _venv_state(project) == before


def test_installed_extras_and_packages_outside_the_lock_are_not_reported(project: Path) -> None:
    _uv(project, "sync", "--locked", "--extra", "opt")
    _write_package(project.parent / "demo_req", "demo_req", "1.0.0")
    _uv(
        project,
        "pip",
        "install",
        "--python",
        ".venv/bin/python",
        "--no-deps",
        str(project.parent / "demo_req"),
    )

    result = _check(project)

    assert result.returncode == 0, result.stdout + result.stderr


def test_a_changed_project_cache_key_is_not_a_difference_from_the_lock(project: Path) -> None:
    (project / "key.txt").write_text("two")

    result = _check(project)

    assert result.returncode == 0, result.stdout + result.stderr


def test_a_lock_behind_pyproject_is_refused(project: Path) -> None:
    before = _venv_state(project)
    _write_root(project, deps='"dep", "optdep"')

    result = _check(project)

    _assert_refused_unchanged_with_command(project, before, result)
    assert "needs to be updated" in result.stderr


def test_a_missing_venv_is_refused_and_not_created(project: Path) -> None:
    shutil.rmtree(project / ".venv")

    result = _check(project)

    assert result.returncode == 1, result.stdout + result.stderr
    assert _FIX.search(result.stderr), result.stderr
    assert not (project / ".venv").exists()


def test_a_machine_without_uv_is_stopped_and_told(project: Path, tmp_path: Path) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    env = _env()
    env["PATH"] = str(empty)

    result = _check(project, env)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "uv not found" in result.stderr
    assert "Would make no changes" not in result.stdout + result.stderr


@pytest.fixture(params=_SCRIPTS)
def script(request: pytest.FixtureRequest) -> str:
    return request.param


def _prefix(script: str) -> str:
    text = (_REPO / script).read_text()
    assert _CALL in text, f"{script} does not call the check"
    return text[: text.index(_CALL) + len(_CALL)]


def _run_cut_script(project: Path, script: str) -> subprocess.CompletedProcess[str]:
    copy = project / script
    copy.write_text(_prefix(script) + f'echo "{_PROCEEDED}"\n')
    (project / "scripts").mkdir(exist_ok=True)
    shutil.copy2(_CHECK, project / "scripts" / _CHECK.name)
    return subprocess.run(
        ["bash", str(copy)], env=_env(), capture_output=True, text=True, timeout=60
    )


def test_the_start_script_stops_on_a_stale_venv(project: Path, script: str) -> None:
    _make_stale(project)
    before = _venv_state(project)

    result = _run_cut_script(project, script)

    _assert_refused_unchanged_with_command(project, before, result)
    assert _PROCEEDED not in result.stdout


def test_the_start_script_proceeds_on_an_in_step_venv(project: Path, script: str) -> None:
    result = _run_cut_script(project, script)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _PROCEEDED in result.stdout


@pytest.mark.parametrize(
    "started", ["docker compose", ".venv/bin", "uvicorn", 'mkdir -p "$LOG_DIR"']
)
def test_nothing_is_started_before_the_check(script: str, started: str) -> None:
    commands = [line for line in _prefix(script).splitlines() if not line.lstrip().startswith("#")]
    assert not [line for line in commands if started in line]

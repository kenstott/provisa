# Copyright (c) 2026 Kenneth Stott
# Canary: 174f8caa-3e90-496a-9914-5dbdc27011be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PyPI wheel distribution packaging (REQ-1126–REQ-1130).

Verifies the pip-installable embedded tier's packaging contract: the console
entry point, the [embedded] extra, the precompiled-UI/config packaging pointers,
and that a built wheel is self-contained (embeds provisa/_ui and provisa/_config,
never leaks node_modules or the sibling provisa_client project). The full build is
deselected by default (pytest -m packaging).
"""

from __future__ import annotations

import shutil
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.packaging

_REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads((_REPO / "pyproject.toml").read_text())


def test_console_entry_point_declared(pyproject: dict) -> None:  # REQ-1128
    scripts = pyproject["project"]["scripts"]
    assert scripts["provisa"] == "provisa.cli:main"


def test_embedded_extra_declared(pyproject: dict) -> None:  # REQ-1126, REQ-1129
    extras = pyproject["project"]["optional-dependencies"]
    assert "embedded" in extras
    # Embedded tier = SQLite control plane + embedded DuckDB engine + in-memory cache.
    assert {"duckdb", "aiosqlite", "greenlet", "fakeredis"} <= set(extras["embedded"])


def test_python_pin(pyproject: dict) -> None:  # REQ-1130
    assert pyproject["project"]["requires-python"] == ">=3.12,<3.13"


def test_buenavista_not_external_dependency(pyproject: dict) -> None:  # REQ-1126
    # The Provisa fork of buenavista is bundled in the wheel, never fetched from PyPI
    # (which carries a different upstream package).
    deps = pyproject["project"]["dependencies"]
    assert not any(d == "buenavista" or d.startswith("buenavista") for d in deps)
    find = pyproject["tool"]["setuptools"]["packages"]["find"]
    assert "vendor/buenavista" in find["where"]
    assert "buenavista" in find["include"]


def test_ui_and_config_packaged(pyproject: dict) -> None:  # REQ-1127
    globs = pyproject["tool"]["setuptools"]["package-data"]["provisa"]
    assert "_ui/**/*" in globs
    assert "_config/*.yaml" in globs


def test_mirror_checklist_present() -> None:  # REQ-1130
    d = _REPO / "packaging" / "pypi"
    assert (d / "MIRROR-CHECKLIST.md").is_file()
    assert (d / "requirements-embedded.lock").is_file()


def test_cli_builds_embedded_env(tmp_path: Path) -> None:  # REQ-1126, REQ-1129
    from provisa.cli import _apply_embedded_env

    data_dir = tmp_path / "native"
    data_dir.mkdir()
    import os

    saved = dict(os.environ)
    try:
        for k in ("PROVISA_ENGINE", "PLATFORM_DATABASE_URL", "PROVISA_REDIS_EMBEDDED"):
            os.environ.pop(k, None)
        _apply_embedded_env(data_dir)
        assert os.environ["PROVISA_ENGINE"] == "duckdb"  # embedded DuckDB engine
        assert os.environ["PROVISA_REDIS_EMBEDDED"] == "1"  # in-memory cache
        assert os.environ["PLATFORM_DATABASE_URL"].startswith("sqlite+aiosqlite:///")
    finally:
        os.environ.clear()
        os.environ.update(saved)


def test_ui_server_static_dir_resolution() -> None:  # REQ-1127
    import provisa.ui_server as ui_server

    # The packaged wheel dir (provisa/_ui) wins when present; otherwise the repo/Docker
    # static/ dir is used. The selection is unconditional (not an error-hiding fallback).
    expected = ui_server._PACKAGED_UI if ui_server._PACKAGED_UI.is_dir() else ui_server._REPO_STATIC
    assert ui_server.STATIC_DIR == expected
    assert ui_server._PACKAGED_UI.name == "_ui"
    assert ui_server._PACKAGED_UI.parent.name == "provisa"


@pytest.mark.slow
def test_wheel_is_self_contained(tmp_path: Path) -> None:  # REQ-1126, REQ-1127
    if shutil.which("python") is None:
        pytest.skip("no python on PATH")
    stage_ui = _REPO / "provisa" / "_ui"
    stage_cfg = _REPO / "provisa" / "_config"
    created_ui = not stage_ui.exists()
    created_cfg = not stage_cfg.exists()
    (stage_ui / "assets").mkdir(parents=True, exist_ok=True)
    (stage_ui / "index.html").write_text("<html></html>")
    stage_cfg.mkdir(parents=True, exist_ok=True)
    shutil.copy(_REPO / "config" / "capabilities.yaml", stage_cfg / "capabilities.yaml")
    outdir = tmp_path / "dist"
    try:
        subprocess.run(
            ["python", "-m", "build", "--wheel", "--no-isolation", "--outdir", str(outdir)],
            cwd=_REPO,
            check=True,
            capture_output=True,
            timeout=600,
        )
        wheel = next(outdir.glob("provisa-*.whl"))
        names = zipfile.ZipFile(wheel).namelist()
        assert any(n.startswith("provisa/_ui/") for n in names)
        assert any(n.startswith("provisa/_config/") for n in names)
        assert "provisa/cli.py" in names
        # The buenavista pgwire fork is bundled INTO the wheel (not a PyPI dependency).
        assert "buenavista/core.py" in names
        assert "buenavista/postgres.py" in names
        # No node_modules or sibling client leaked into the wheel.
        assert not any("node_modules" in n for n in names)
        assert not any(n.startswith("provisa_client/") for n in names)
    finally:
        if created_ui:
            shutil.rmtree(stage_ui, ignore_errors=True)
        if created_cfg:
            shutil.rmtree(stage_cfg, ignore_errors=True)

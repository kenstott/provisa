# Copyright (c) 2026 Kenneth Stott
# Canary: ee88a62b-db9c-42b9-a461-e957918cc193
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""provisa-notebook (the Data Science area's in-browser Python) is part of every install: provisa
pins it at its own version, every install path builds or fetches it, and a release publishes it
before provisa, after every artifact exists."""

# Requirements: REQ-2071

from __future__ import annotations

import tomllib
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_provisa_pins_provisa_notebook_at_its_own_version():
    provisa = _toml(REPO / "pyproject.toml")
    notebook = _toml(REPO / "provisa-notebook" / "pyproject.toml")
    version = provisa["project"]["version"]
    assert notebook["project"]["version"] == version
    assert f"provisa-notebook=={version}" in provisa["project"]["dependencies"]
    assert provisa["tool"]["uv"]["sources"]["provisa-notebook"] == {
        "path": "provisa-notebook",
        "editable": True,
    }


def test_the_release_builds_the_notebook_and_stamps_both_pins():
    leg = yaml.safe_load((WORKFLOWS / "release-packages.yml").read_text())["jobs"]
    notebook_steps = " ".join(s.get("run", "") for s in leg["build-provisa-notebook"]["steps"])
    assert "provisa-notebook/pyproject.toml" in notebook_steps
    assert "python -m provisa_notebook.build" in notebook_steps
    wheel_steps = " ".join(s.get("run", "") for s in leg["build-provisa-wheel"]["steps"])
    assert '"provisa-notebook==${{ needs.metadata.outputs.py_version }}"' in wheel_steps


def test_the_notebook_is_published_before_provisa_and_only_after_every_leg():
    jobs = yaml.safe_load((WORKFLOWS / "build-dmg.yml").read_text())["jobs"]
    legs = {"prebuilt", "packages", "macos", "windows", "linux"}
    assert legs <= set(jobs["publish-notebook-pypi"]["needs"])
    assert "publish-notebook-pypi" in jobs["publish-provisa-pypi"]["needs"]
    assert {"publish-notebook-pypi", "publish-provisa-pypi"} <= set(jobs["publish"]["needs"])


def test_every_installer_and_launcher_builds_the_notebook_from_this_repository():
    for path in (
        "packaging/macos/build-dmg.sh",
        "packaging/linux/build-appimage.sh",
    ):
        assert "scripts/build-notebook-wheel.sh" in (REPO / path).read_text(), path
    assert "provisa_notebook.build" in (REPO / "packaging/windows/build-sfx.ps1").read_text()
    for path in ("Dockerfile", "Dockerfile.dev"):
        assert "python -m provisa_notebook.build" in (REPO / path).read_text(), path
    for path in ("start-ui.sh", "start-ui-install.sh"):
        assert "scripts/ensure-notebook.sh" in (REPO / path).read_text(), path

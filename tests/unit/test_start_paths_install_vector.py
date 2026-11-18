# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e1a7d-52b4-4f08-9e6a-d18b07c4e2f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-419: every way of starting Provisa from a source checkout carries the vector extra.

The default config's embedding model is a local sentence-transformers model, and the startup check
refuses a configured model whose library is missing. So every start path syncs the vector extra from
uv.lock, where torch comes from PyTorch's CPU-only index on Linux; none installs torch from plain
PyPI, whose Linux wheel is the CUDA build.
"""

from __future__ import annotations

import importlib.util
import re
import tomllib
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_CHECK = "scripts/ensure-venv-in-step.sh"
_CALL = '"$SCRIPT_DIR/scripts/ensure-venv-in-step.sh" "$SCRIPT_DIR" || exit 1'
# The scripts that start Provisa from a checkout, and those that build its .venv.
_STARTS = ["start-ui-install.sh", "start-ui.sh"]
_VENV_BUILDERS = ["setup.sh", "venv.sh"]
_START_PATHS = [*_STARTS, _CHECK, *_VENV_BUILDERS]
_CPU_INDEX = "https://download.pytorch.org/whl/cpu"


def _commands(path: str) -> str:
    """The script with its comments removed and continued lines joined."""
    text = (_REPO / path).read_text().replace("\\\n", " ")
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _uv_syncs(path: str) -> list[str]:
    return re.findall(r"uv sync[^\n]*", _commands(path))


@pytest.mark.parametrize("script", _STARTS)
def test_each_start_checks_the_venv_before_it_starts_anything(script: str) -> None:
    text = _commands(script)
    assert _CALL in text, f"{script} does not run {_CHECK}"
    assert text.index(_CALL) < text.index("uvicorn")


def test_the_check_compares_and_prints_a_sync_of_the_vector_extra() -> None:
    syncs = _uv_syncs(_CHECK)
    dry = [s for s in syncs if "--dry-run" in s]
    fix = [s for s in syncs if "--dry-run" not in s]
    assert dry and fix, syncs
    for sync in syncs:
        assert "--locked" in sync and "--extra vector" in sync, sync


@pytest.mark.parametrize("script", _VENV_BUILDERS)
def test_each_venv_builder_syncs_from_the_lock_with_the_vector_extra(script: str) -> None:
    syncs = _uv_syncs(script)
    assert syncs, f"{script} does not sync from uv.lock"
    assert all("--locked" in s for s in syncs), syncs
    # The dev extra carries the vector extra (provisa[vector]).
    assert "--extra dev" in _commands(script), script
    dev = tomllib.loads((_REPO / "pyproject.toml").read_text())["project"]["optional-dependencies"][
        "dev"
    ]
    assert any(r.startswith("provisa[") and "vector" in r for r in dev), dev


@pytest.mark.parametrize("script", _START_PATHS)
def test_no_start_path_installs_with_pip(script: str) -> None:
    """pip resolves from plain PyPI, whose torch on Linux is the CUDA build; every install on a
    start path goes through uv.lock."""
    commands = [line for line in _commands(script).splitlines() if "echo " not in line]
    pip = re.compile(r"(?<!uv )\bpip3? install\b|-m pip\b|uv pip install[^\n]*\btorch\b")
    assert not [line for line in commands if pip.search(line)], script
    for sync in _uv_syncs(script):
        assert "--locked" in sync, sync


def test_a_demo_s_extras_are_synced_from_the_lock() -> None:
    text = _commands("start-ui-install.sh")
    assert 'scripts/demo_extras.py" --groups' in text
    demo_syncs = [s for s in _uv_syncs("start-ui-install.sh") if "_DEMO_EXTRA_ARGS" in s]
    assert demo_syncs and all("--locked" in s and "--inexact" in s for s in demo_syncs)


def test_demo_extras_prints_a_demo_s_groups() -> None:
    spec = importlib.util.spec_from_file_location("demo_extras", _REPO / "scripts/demo_extras.py")
    assert spec and spec.loader
    extras = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(extras)
    assert extras.extras(_REPO / "demo" / "named" / "sonnets") == ["vector"]
    assert extras.extras(_REPO / "demo" / "named" / "weblogs") == []


def test_the_lock_takes_linux_torch_from_the_cpu_index() -> None:
    project = tomllib.loads((_REPO / "pyproject.toml").read_text())
    assert set(project["project"]["optional-dependencies"]["vector"]) >= {
        "sentence-transformers",
        "torch",
    }
    assert project["tool"]["uv"]["sources"]["torch"] == [
        {"index": "pytorch-cpu", "marker": "sys_platform == 'linux'"}
    ]
    [index] = [i for i in project["tool"]["uv"]["index"] if i["name"] == "pytorch-cpu"]
    assert index["url"] == _CPU_INDEX
    lock = tomllib.loads((_REPO / "uv.lock").read_text())
    torches = [p for p in lock["package"] if p["name"] == "torch"]
    linux = [p for p in torches if p["source"].get("registry") == _CPU_INDEX]
    assert linux and all(p["version"].endswith("+cpu") for p in linux), torches
    assert all(p["resolution-markers"] == ["sys_platform == 'linux'"] for p in linux), linux
    pypi = [p for p in torches if p["source"].get("registry") != _CPU_INDEX]
    assert all(p["resolution-markers"] == ["sys_platform != 'linux'"] for p in pypi), pypi

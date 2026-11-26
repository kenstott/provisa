# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0f7c1e-3b52-4a8e-9c47-2f1a5e8b9d30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-528: a non-demo ``provisa run`` names the install's own provisa.yaml in PROVISA_CONFIG."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pytest

from provisa import cli


@pytest.fixture
def run_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    monkeypatch.delenv("PROVISA_CONFIG", raising=False)
    monkeypatch.setattr(cli, "_require_supported_interpreter", lambda: None)
    monkeypatch.setattr(cli, "_apply_embedded_env", lambda data_dir: [])
    monkeypatch.setattr(cli, "_control_plane_drift", lambda: [])
    monkeypatch.setattr(cli.asyncio, "run", lambda coro: coro.close())
    return home


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        data_dir=str(tmp_path / "native"),
        reset=False,
        demo=False,
        host="127.0.0.1",
        api_port=8001,
        ui_port=3000,
        no_browser=True,
    )


def test_non_demo_run_sets_config_in_default_home(run_env: Path, tmp_path: Path) -> None:
    assert cli._cmd_run(_args(tmp_path)) == 0
    assert os.environ["PROVISA_CONFIG"] == str(run_env / ".provisa" / "provisa.yaml")


def test_non_demo_run_uses_provisa_home_pointer(run_env: Path, tmp_path: Path) -> None:
    custom = tmp_path / "custom-home"
    (run_env / ".provisa_home").write_text(f"{custom}\n")
    assert cli._cmd_run(_args(tmp_path)) == 0
    assert os.environ["PROVISA_CONFIG"] == str(custom / "provisa.yaml")


def test_operator_config_is_kept(
    run_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROVISA_CONFIG", "/etc/mine.yaml")
    assert cli._cmd_run(_args(tmp_path)) == 0
    assert os.environ["PROVISA_CONFIG"] == "/etc/mine.yaml"

# Copyright (c) 2026 Kenneth Stott
# Canary: 0c8f4f7e-2f55-4d0b-8b0e-6d7b1b9d1a42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""setup.sh's prerequisite logic (scripts/dev_prereqs.py): version parsing, the pins read from the dev
container, and what gets installed given detected versions and answers. Nothing is installed."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

import dev_prereqs as dp  # noqa: E402

PINS = {"uv": "0.11.19", "rust": "1.95.0", "node": "24.22.3", "go": "1.27.3"}
ALL_OK = {
    "uv": (0, 11, 19),
    "rust": (1, 95, 0),
    "node": (24, 22, 3),
    "go": (1, 27, 3),
    "docker": (27, 0, 0),
}


@pytest.mark.parametrize(
    "text,expected",
    [
        ("uv 0.11.19 (abc 2026-01-01)", (0, 11, 19)),
        ("cargo 1.95.0 (x 2026-01-01)", (1, 95, 0)),
        ("v24.22.3", (24, 22, 3)),
        ("go version go1.27.3 darwin/arm64", (1, 27, 3)),
        ("Docker version 27.1, build abc", (27, 1, 0)),
        ("no version here", None),
    ],
)
def test_parse_version(text, expected):
    assert dp.parse_version(text) == expected


def test_pins_come_from_the_dev_container_files():
    pins = dp.read_pins(REPO)
    assert set(pins) == {"uv", "rust", "node", "go"}
    assert pins["rust"] in (REPO / ".devcontainer" / "devcontainer.json").read_text()
    assert pins["uv"] in (REPO / ".devcontainer" / "Dockerfile").read_text()


def test_detect_reads_each_tools_output():
    outputs = {
        "uv": "uv 0.11.19",
        "cargo": "cargo 1.95.0",
        "node": "v24.22.3",
        "go": "go version go1.27.3 linux/amd64",
    }
    got = dp.detect(lambda cmd: outputs.get(cmd[0]))
    assert got["uv"] == (0, 11, 19) and got["go"] == (1, 27, 3) and got["docker"] is None


def _no_ask(_p):
    raise AssertionError("must not ask")


def test_nothing_to_install_when_all_current():
    installs, unresolved = dp.plan(PINS, ALL_OK, "Linux", False, False, _no_ask)
    assert installs == [] and unresolved == []


def test_old_and_missing_are_found():
    det = {**ALL_OK, "node": (22, 0, 0), "go": None}
    got = {f.tool: f.status for f in dp.findings(PINS, det)}
    assert got["node"] == "old" and got["go"] == "missing" and got["uv"] == "ok"


def test_yes_installs_without_asking():
    det = {**ALL_OK, "rust": None}
    installs, unresolved = dp.plan(PINS, det, "Linux", False, True, _no_ask)
    assert [t for t, _ in installs] == ["rust"] and "1.95.0" in installs[0][1] and unresolved == []


def test_answers_decide_per_tool():
    det = {**ALL_OK, "node": None, "go": None}
    prompts = []

    def ask(p):
        prompts.append(p)
        return p.startswith("node")

    installs, unresolved = dp.plan(PINS, det, "Linux", False, False, ask)
    assert [t for t, _ in installs] == ["node"] and [f.tool for f in unresolved] == ["go"]
    assert len(prompts) == 2


def test_macos_with_brew_uses_brew_and_rustup():
    assert dp.install_command("node", "24.22.3", "Darwin", True).startswith("brew install node@24")
    assert dp.install_command("go", "1.27.3", "Darwin", True) == "brew install go"
    assert dp.install_command("uv", "0.11.19", "Darwin", True) == "brew install uv"
    assert dp.install_command("docker", None, "Darwin", True) == "brew install --cask docker"
    assert "rustup" in dp.install_command("rust", "1.95.0", "Darwin", True)


def test_linux_uses_pinned_official_installers():
    assert "astral.sh/uv/0.11.19/install.sh" in dp.install_command("uv", "0.11.19", "Linux", False)
    assert "node-v24.22.3-linux-" in dp.install_command("node", "24.22.3", "Linux", False)
    assert "go1.27.3.linux-" in dp.install_command("go", "1.27.3", "Linux", False)
    assert "get.docker.com" in dp.install_command("docker", None, "Linux", False)


def test_macos_without_brew_names_the_way_out():
    with pytest.raises(ValueError, match="Homebrew"):
        dp.install_command("go", "1.27.3", "Darwin", False)


def test_unsupported_platform_is_refused():
    with pytest.raises(ValueError, match="unsupported"):
        dp.install_command("uv", "0.11.19", "Windows", False)


def test_docker_is_optional_everything_else_is_not():
    f = {x.tool: x.optional for x in dp.findings(PINS, {**ALL_OK, "docker": None})}
    assert f["docker"] is True and f["uv"] is False

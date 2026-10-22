# Copyright (c) 2026 Kenneth Stott
# Canary: c44eb0db-7cfd-4abb-8a8f-3fe5a9afca4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh gives cloud.provisa.dev's sandbox tier its AI (REQ-2006): the overlay it installs
points the tier at OpenRouter, with the key named by reference to the node's own
OPENROUTER_API_KEY, and a node whose provisa.env lacks that key is refused by name before
anything is pushed to it. The key is never written by the deploy, and never printed."""

# Requirements: REQ-2006

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_KEY_VALUE = "sk-or-v1-not-a-real-key"


def _function(name: str) -> str:
    text = _SCRIPT.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _fake_bin(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sudo").write_text('#!/bin/bash\n"$@"\n')
    (bin_dir / "sudo").chmod(0o755)
    return bin_dir


def _bash(tmp_path: Path, lines: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", "\n".join(["set -euo pipefail", *lines])],
        env={"PATH": f"{_fake_bin(tmp_path)}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
    )


def test_the_overlay_points_the_sandbox_tier_at_openrouter_by_reference(tmp_path):
    out = tmp_path / "overlay.yml"
    result = _bash(
        tmp_path,
        [
            f"REPO={_REPO}",
            f"STAGE={tmp_path}",
            "OVERLAY_DIR=/o HF_CACHE_DIR=/h RUNTIME_DEPS_DIR=/r OVERLAY_EXT=/x/overlay.yml",
            "SANDBOX_PUBLISH=127.0.0.1:18000 API_CONTAINER=compose-provisa-1",
            "ssh_node() { :; }",
            f'scp_node() {{ cp "$1" {out}; }}',
            _function("push_overlay"),
            "push_overlay",
        ],
    )
    assert result.returncode == 0, result.stderr
    env = yaml.safe_load(out.read_text())["services"]["provisa-sandbox"]["environment"]
    assert env["PROVISA_SANDBOX_AI_BASE_URL"] == "https://openrouter.ai/api/v1"
    # "$$" is compose's literal "$": the tier's setting is the reference ${env:OPENROUTER_API_KEY},
    # resolved in the container (provisa.core.secrets.resolve_api_key_field).
    assert env["PROVISA_SANDBOX_AI_API_KEY"] == "$${env:OPENROUTER_API_KEY}"
    # Interpolated by compose from the node's environment (its provisa.env).
    assert env["OPENROUTER_API_KEY"] == "${OPENROUTER_API_KEY:-}"
    # The model is the setting's own default, a Claude Haiku through OpenRouter.
    assert "PROVISA_SANDBOX_AI_MODEL" not in env


def _preflight(tmp_path: Path, env_lines: list[str]) -> subprocess.CompletedProcess:
    env_file = tmp_path / "provisa.env"
    env_file.write_text("".join(f"{line}\n" for line in env_lines))
    return _bash(
        tmp_path,
        [
            f"ENV_FILE={env_file}",
            "NODE=provisa-saas-sandbox ZONE=us-central1-a PROJECT=provisa-test-473",
            'ssh_node() { bash -c "$1"; }',
            _function("preflight_sandbox_ai"),
            "preflight_sandbox_ai",
        ],
    )


def test_a_node_with_the_key_passes_without_printing_it(tmp_path):
    result = _preflight(tmp_path, ["PROVISA_ENGINE=duckdb", f"OPENROUTER_API_KEY={_KEY_VALUE}"])
    assert result.returncode == 0, result.stderr
    assert "OPENROUTER_API_KEY=<set>" in result.stdout
    assert _KEY_VALUE not in result.stdout + result.stderr


@pytest.mark.parametrize("lines", [["PROVISA_ENGINE=duckdb"], ["OPENROUTER_API_KEY="]])
def test_a_node_without_the_key_is_refused_by_name_with_the_command_that_adds_it(tmp_path, lines):
    result = _preflight(tmp_path, lines)
    assert result.returncode == 1
    assert "has no OPENROUTER_API_KEY" in result.stderr
    command = next(line.strip() for line in result.stderr.splitlines() if "gcloud compute" in line)
    assert command.startswith("printf 'OPENROUTER_API_KEY=%s\\n' \"$(sed -n ")
    assert "gcloud compute ssh provisa-saas-sandbox --zone us-central1-a" in command
    assert f"tee -a {tmp_path / 'provisa.env'}" in command


def test_every_arm_that_pushes_the_overlay_checks_the_tiers_node_first():
    text = _SCRIPT.read_text()
    arms = text[text.index('case "$TARGET" in\n  ui)') :]
    for arm in re.findall(r"^    (.*;;)$", arms, re.MULTILINE):
        if "push_overlay" in arm or "deploy_sandbox_vm" in arm:
            assert arm.startswith("{ sandbox_on_own_vm || preflight_sandbox_ai; }; "), arm
    vm = _function("deploy_sandbox_vm")
    assert vm.index("preflight_sandbox_ai") < vm.index("push_overlay")

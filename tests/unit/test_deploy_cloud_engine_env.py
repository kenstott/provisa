# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deploy-cloud.sh sets PROVISA_ENGINE_IMAGE and PROVISA_ZAYCHIK_IMAGE to the release version
on each deploy arm (REQ-2194).

The node's provisa.env pins the engine and proxy image at the version terraform first wrote.
A deploy that only asserts the keys but never updates them leaves the node on the old images
after every idle-stop/restart cycle. push_engine_env heals this the same way push_commerce_env
heals the Stripe keys: set_node_env writes the target version into provisa.env; the main restart
causes converge_boot_shard (engine_wake.py:302) to roll the GKE pod to the new images.

The function is run here as the script runs it, with ssh_node replaced by a local shell and
git replaced by a stub that returns a fixed tag."""

# Requirements: REQ-2194

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_LIB = _REPO / "scripts" / "cloud-node-lib.sh"

_VERSION = "v0.1.0-alpha.512"
_ENGINE_IMG = f"ghcr.io/simpleishard-llc/provisa-trino-engine:{_VERSION}"
_ZAYCHIK_IMG = f"ghcr.io/simpleishard-llc/provisa-zaychik:{_VERSION}"


def _source() -> str:
    """deploy-cloud.sh with the node library it sources (its shared functions live there)."""
    return _LIB.read_text() + _SCRIPT.read_text()


def _function(name: str) -> str:
    source = _source()
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", source, re.S | re.M)
    assert found is not None, f"deploy-cloud.sh has no {name}"
    return found.group(0)


def _constant(name: str) -> str:
    """The `NAME=value` assignment for a simple variable (no quoting required)."""
    text = _source()
    match = re.search(rf'^{name}="([^"]*)"$', text, re.MULTILINE)
    assert match, f"deploy-cloud.sh has no {name}= assignment"
    return f'{name}="{match.group(1)}"'


def _run_push_engine_env(
    tmp_path: Path,
    *,
    running_engine: str = "",
    running_zaychik: str = "",
    git_tag: str = _VERSION,
) -> subprocess.CompletedProcess:
    """Run push_engine_env with a stubbed ssh_node and git."""
    env_file = tmp_path / "provisa.env"
    env_file.touch()

    # Stub docker inspect to return whatever the test says is running.
    docker_env = ""
    if running_engine:
        docker_env += f"PROVISA_ENGINE_IMAGE={running_engine}\n"
    if running_zaychik:
        docker_env += f"PROVISA_ZAYCHIK_IMAGE={running_zaychik}\n"

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    # git stub: returns the fixed tag on `describe --tags --abbrev=0 HEAD`
    if git_tag:
        (bin_dir / "git").write_text(f"#!/bin/bash\necho '{git_tag}'\n")
    else:
        (bin_dir / "git").write_text("#!/bin/bash\nexit 1\n")
    (bin_dir / "git").chmod(0o755)

    # sudo stub: pass through to the real command without escalation
    (bin_dir / "sudo").write_text('#!/bin/bash\nexec "$@"\n')
    (bin_dir / "sudo").chmod(0o755)

    script = f"""
set -euo pipefail
REPO={_REPO}
API_CONTAINER=compose-provisa-1
ENV_FILE={env_file}
{_constant("IMAGE_REGISTRY")}
# ssh_node: docker inspect returns the stub env; anything else is a no-op.
ssh_node() {{
  local cmd="$1"
  case "$cmd" in
    *"docker inspect"*) printf '%s' '{docker_env}' ;;
    *) : ;;
  esac
}}
# set_node_env: portable key=value upsert (the real one calls ssh_node with sed -i + tee -a,
# which is Linux-only; replace it with a portable shell that writes to ENV_FILE directly).
set_node_env() {{
  local key="$1" value="$2"
  local tmp
  tmp="$(grep -v "^${{key}}=" "$ENV_FILE" || true)"
  printf '%s\\n%s=%s\\n' "$tmp" "$key" "$value" > "$ENV_FILE"
}}
{_function("push_engine_env")}
push_engine_env
"""
    return subprocess.run(
        ["bash", "-c", script],
        env={"PATH": f"{bin_dir}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"},
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )


def _env_contents(tmp_path: Path) -> str:
    return (tmp_path / "provisa.env").read_text()


class TestPushEngineEnvSetsImages:
    def test_unset_images_are_written_to_env_file(self, tmp_path):
        result = _run_push_engine_env(tmp_path)
        assert result.returncode == 0, result.stderr
        env = _env_contents(tmp_path)
        assert f"PROVISA_ENGINE_IMAGE={_ENGINE_IMG}" in env
        assert f"PROVISA_ZAYCHIK_IMAGE={_ZAYCHIK_IMG}" in env

    def test_stale_images_are_replaced(self, tmp_path):
        result = _run_push_engine_env(
            tmp_path,
            running_engine="ghcr.io/simpleishard-llc/provisa-trino-engine:v0.1.0-alpha.391",
            running_zaychik="ghcr.io/simpleishard-llc/provisa-zaychik:v0.1.0-alpha.344",
        )
        assert result.returncode == 0, result.stderr
        env = _env_contents(tmp_path)
        assert f"PROVISA_ENGINE_IMAGE={_ENGINE_IMG}" in env
        assert f"PROVISA_ZAYCHIK_IMAGE={_ZAYCHIK_IMG}" in env
        assert "v0.1.0-alpha.391" not in env
        assert "v0.1.0-alpha.344" not in env

    def test_already_current_images_are_not_rewritten(self, tmp_path):
        result = _run_push_engine_env(
            tmp_path,
            running_engine=_ENGINE_IMG,
            running_zaychik=_ZAYCHIK_IMG,
        )
        assert result.returncode == 0, result.stderr
        assert "up to date" in result.stdout
        # set_node_env was not called: env file is empty
        assert _env_contents(tmp_path) == ""

    def test_no_tag_skips_without_failing(self, tmp_path):
        """When git cannot find any tag, the function skips without failing."""
        result = _run_push_engine_env(tmp_path, git_tag="")
        assert result.returncode == 0, result.stderr
        assert "skipped" in result.stdout
        # No images written
        assert _env_contents(tmp_path) == ""

    def test_local_commit_on_top_of_tag_still_pins_the_tag_version(self, tmp_path):
        """--abbrev=0 returns the nearest tag even when HEAD is a commit above it."""
        # git stub returns the tag (what --abbrev=0 would return for a worktree one commit
        # ahead of the tag, as the deploy-512 worktree has after a local cherry-pick).
        result = _run_push_engine_env(tmp_path, git_tag=_VERSION)
        assert result.returncode == 0, result.stderr
        assert f"PROVISA_ENGINE_IMAGE={_ENGINE_IMG}" in _env_contents(tmp_path)
        assert f"PROVISA_ZAYCHIK_IMAGE={_ZAYCHIK_IMG}" in _env_contents(tmp_path)

    def test_stale_output_names_old_and_new_image(self, tmp_path):
        old = "ghcr.io/simpleishard-llc/provisa-trino-engine:v0.1.0-alpha.391"
        result = _run_push_engine_env(tmp_path, running_engine=old)
        assert result.returncode == 0, result.stderr
        assert old in result.stdout
        assert _ENGINE_IMG in result.stdout

    def test_image_registry_prefix_is_ghcr_kenstott(self):
        text = _source()
        assert 'IMAGE_REGISTRY="ghcr.io/simpleishard-llc"' in text


def _target_steps(target: str) -> list[str]:
    """The function calls a deploy target runs, in order, from the script's case arm."""
    text = _source()
    arm = re.search(
        rf"^  {re.escape(target)}\)\n(.*?)\n    ;;$|^  {re.escape(target)}\)\n(.*?) ;;$",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert arm, target
    body = arm.group(1) or arm.group(2)
    calls = [line for line in body.splitlines() if not line.strip().startswith("#")]
    return [
        s.strip()
        for s in " ".join(calls).replace("{", " ").replace("}", " ").split(";")
        if s.strip()
    ]


@pytest.mark.parametrize("target", ["api", "cfg", "all", "patch", "reset"])
def test_push_engine_env_runs_on_every_arm_that_restarts(target):
    steps = _target_steps(target)
    assert "push_engine_env" in steps, steps


@pytest.mark.parametrize("target", ["api", "cfg", "all", "patch", "reset"])
def test_push_engine_env_runs_before_restart(target):
    steps = _target_steps(target)
    assert steps.index("push_engine_env") < steps.index("restart"), steps

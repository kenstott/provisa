# Copyright (c) 2026 Kenneth Stott
# Canary: 4f6c1f0e-9d2a-4a38-8a59-6d2f3f1c7b10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A deploy brings the sandbox VM's compose up to what the config it pushes needs (REQ-2006,
REQ-2068). The VM's compose-control-plane.yml is written by first-launch.sh, which runs only when a
release changes; the configs are pushed by every deploy. So a config naming a variable the node's
compose did not set (PROVISA_DEMO_LEDGER_ADDRESS) crash-looped the regions tier at startup, and
every visitor of retail was told it was starting, for ever."""

# Requirements: REQ-2006, REQ-2068

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_REGIONS_CONFIG = _REPO / "config" / "provisa-sandbox-regions.yaml"
_NODE_FILE = "/root/.provisa/compose-control-plane.yml"


def _function(name: str) -> str:
    source = _SCRIPT.read_text()
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", source, re.S | re.M)
    assert found is not None, f"deploy-cloud.sh has no {name}"
    return found.group(0)


def _push(tmp_path: Path, node_sha: str) -> tuple[subprocess.CompletedProcess, str, Path]:
    """Run push_compose against a node whose compose hashes to ``node_sha``; what it ran there."""
    log = tmp_path / "ssh.log"
    log.touch()
    stage = tmp_path / "stage"
    stage.mkdir()
    script = f"""
set -euo pipefail
REPO={_REPO}
STAGE={stage}
CONTROL_PLANE_YML={_NODE_FILE}
ssh_node() {{ echo "ssh $1" >> {log}; case "$1" in *shasum*) echo "{node_sha}  file" ;; esac; }}
scp_node() {{ echo "scp $1 $2" >> {log}; }}
{_function("push_compose")}
push_compose
"""
    done = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)
    return done, log.read_text(), stage / "compose-control-plane.yml"


def _rendered(tmp_path: Path) -> Path:
    done, _log, rendered = _push(tmp_path, "unknown")
    assert done.returncode == 0, done.stderr
    return rendered


def test_every_variable_the_regions_config_names_is_set_by_the_compose_a_deploy_installs(tmp_path):
    named = set(re.findall(r"\$\{env:([A-Z0-9_]+)\}", _REGIONS_CONFIG.read_text()))
    assert "PROVISA_DEMO_LEDGER_ADDRESS" in named
    compose = yaml.safe_load(_rendered(tmp_path).read_text())
    environment = compose["services"]["provisa-sandbox-regions"]["environment"]
    assert not sorted(named - set(environment)), "named by the config, set by no compose"


def test_a_compose_that_differs_is_replaced_and_the_stack_recreated(tmp_path):
    done, log, _rendered_file = _push(tmp_path, "0" * 64)
    assert done.returncode == 0, done.stderr
    assert f"scp {tmp_path}/stage/compose-control-plane.yml /tmp/compose-control-plane.yml" in log
    assert _NODE_FILE in log
    assert "systemctl restart provisa" in log


def test_a_node_already_on_this_compose_is_left_running(tmp_path):
    first = _rendered(tmp_path)
    sha = hashlib.sha256(first.read_bytes()).hexdigest()
    again = tmp_path / "again"
    again.mkdir()
    done, log, _rendered_file = _push(again, sha)
    assert done.returncode == 0, done.stderr
    assert "scp " not in log
    assert "systemctl restart" not in log


def test_the_deploy_replaces_the_vms_compose_before_its_overlay_and_only_there():
    body = _function("deploy_sandbox_vm")
    assert body.index("push_compose") < body.index("push_overlay")
    arms = _SCRIPT.read_text().rsplit('case "$TARGET" in', 1)[1]
    assert "push_compose" not in arms, "the coordinator's compose is not this function's"

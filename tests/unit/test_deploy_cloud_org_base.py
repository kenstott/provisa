# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1e2c84-5d3f-4a6e-9c0a-3f8d2e6b1a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A deploy puts the org base domain where the API containers read it (REQ-1276, REQ-1348).

The API reads an org from a Host only under VITE_ORG_BASE_DOMAIN (provisa/security/sni.py). The
node's compose interpolates it from provisa.env, which nothing writes on cloud.provisa.dev, so the
deploy states it itself — in the overlay — and refuses to finish when the running API container
does not carry it."""

# Requirements: REQ-1276, REQ-1348

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "deploy-cloud.sh"
_FIRST_LAUNCH = _REPO / "packaging" / "linux" / "first-launch.sh"


def _function(name: str) -> str:
    found = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", _SCRIPT.read_text(), re.S | re.M)
    assert found is not None, f"deploy-cloud.sh has no {name}"
    return found.group(0)


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize(
    "site,base",
    [("https://cloud.provisa.dev", "provisa.dev"), ("https://cloud.example.org", "example.org")],
)
def test_the_base_domain_is_the_control_plane_hosts_parent(site, base):
    done = _run(f"SITE={site}\n{_function('org_base_domain')}\norg_base_domain")
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == base


@pytest.mark.parametrize("site", ["https://provisa.dev", "https://cloud.provisa.dev:8443", ""])
def test_a_site_that_is_not_the_control_plane_declares_nothing(site):
    done = _run(f'SITE="{site}"\n{_function("org_base_domain")}\norg_base_domain')
    assert done.returncode != 0
    assert "cannot declare the base domain" in done.stderr


def _overlay(tmp_path: Path) -> dict:
    stage = tmp_path / "stage"
    stage.mkdir()
    script = f"""
set -uo pipefail
REPO={_REPO}
STAGE={stage}
SITE=https://cloud.provisa.dev
OVERLAY_DIR=/o
OVERLAY_EXT=/o/ext.yml
HF_CACHE_DIR=/hf
RUNTIME_DEPS_DIR=/rd
SANDBOX_PUBLISH=127.0.0.1:18000
REGIONS_NODES_OVERLAY=""
API_CONTAINER=compose-provisa-1
ssh_node() {{ exit 0; }}
scp_node() {{ exit 0; }}
{_function("org_base_domain")}
{_function("push_overlay")}
push_overlay
"""
    _run(script)
    return yaml.safe_load((stage / "docker-compose.overlay.yml").read_text())


def test_the_overlay_declares_the_base_domain_to_every_service_that_reads_a_host(tmp_path):
    services = _overlay(tmp_path)["services"]
    for name in ("provisa", "provisa-ui", "provisa-sandbox", "provisa-sandbox-regions"):
        assert services[name]["environment"]["VITE_ORG_BASE_DOMAIN"] == "provisa.dev", name


def _verify(running: str) -> subprocess.CompletedProcess:
    script = f"""
set -uo pipefail
SITE=https://cloud.provisa.dev
API_CONTAINER=compose-provisa-1
ssh_node() {{ printf '%s\\r\\n' "{running}"; }}
{_function("org_base_domain")}
{_function("verify_org_base")}
verify_org_base
"""
    return _run(script)


def test_a_deploy_passes_when_the_api_container_carries_the_base_domain():
    assert _verify("provisa.dev").returncode == 0


@pytest.mark.parametrize("running", ["", "other.example"])
def test_a_deploy_fails_naming_the_variable_when_the_api_container_lacks_it(running):
    done = _verify(running)
    assert done.returncode == 1
    assert "VITE_ORG_BASE_DOMAIN" in done.stderr


def test_every_deploy_arm_that_pushes_the_overlay_verifies_the_base_domain():
    arms = _SCRIPT.read_text().rsplit('case "$TARGET" in', 1)[1]
    for arm in re.findall(r"^  \w[\w-]*\)\n(.*?);;", arms, re.S | re.M):
        if "push_overlay" in arm:
            assert "verify_org_base" in arm


def test_every_api_service_a_node_runs_is_given_the_base_domain_by_first_launch():
    source = _FIRST_LAUNCH.read_text()
    assert source.count('VITE_ORG_BASE_DOMAIN: "\\${VITE_ORG_BASE_DOMAIN:-}"') == 4

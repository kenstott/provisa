# Copyright (c) 2026 Kenneth Stott
# Canary: 5c4ca5c4-8f0c-408a-a74a-771a24d647e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""devcontainer-smoke.yml: a CI check builds the dev container and runs the three Codespace steps
inside it on every change to the container, the script or what the script starts (REQ-2205)."""

# Requirements: REQ-2205

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "devcontainer-smoke.yml"
PATHS = [
    ".devcontainer/**",
    "start-ui-install.sh",
    "rust/build-local.sh",
    "rust/ensure-local-build.sh",
    "provisa/core/control_plane_pg.py",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/devcontainer-smoke.yml",
]


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def _smoke_step() -> dict:
    steps = _workflow()["jobs"]["three-steps"]["steps"]
    found = [s for s in steps if str(s.get("uses", "")).startswith("devcontainers/ci@")]
    assert len(found) == 1, found
    return found[0]


def test_it_runs_on_every_change_to_the_container_the_script_or_what_it_starts():
    triggers = _workflow()[True]  # PyYAML reads the key `on` as True
    assert triggers["pull_request"]["paths"] == PATHS
    assert triggers["push"] == {"branches": ["main"], "paths": PATHS}
    assert "workflow_dispatch" in triggers
    for pattern in PATHS:
        assert list(REPO.glob(pattern)), f"{pattern} names nothing in the repository"


def test_it_reads_the_repository_only_and_is_bounded():
    workflow = _workflow()
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["three-steps"]
    assert "permissions" not in job
    assert 0 < job["timeout-minutes"] <= 120


def test_the_devcontainer_action_is_an_exact_release_that_pushes_nothing():
    step = _smoke_step()
    assert re.fullmatch(r"devcontainers/ci@v\d+\.\d+\.\d+", step["uses"]), step["uses"]
    assert step["with"]["push"] == "never"


def test_it_runs_the_demo_and_reaches_the_page_and_an_api_call_through_the_ui_server():
    run = _smoke_step()["with"]["runCmd"]
    assert run.startswith("set -euo pipefail\n")
    assert re.search(r"^\./start-ui-install\.sh --demo .*&$", run, re.M)
    assert "-H 'Accept: text/html' http://localhost:3000/)" in run
    assert '[ "$page" = 200 ]' in run
    assert "http://localhost:3000/health)" in run
    assert '[ "$api" = 200 ]' in run
    # A failure shows what the script and the servers it started printed.
    assert "trap report EXIT" in run
    assert "for log in .logs/*.log" in run


def test_the_api_call_is_one_the_ui_server_forwards_and_that_needs_no_sign_in():
    vite = (REPO / "provisa-ui" / "vite.config.ts").read_text()
    assert re.search(r'^\s+"/health": BACKEND_TARGET,$', vite, re.M)
    middleware = (REPO / "provisa" / "auth" / "middleware.py").read_text()
    assert '"/health",' in middleware

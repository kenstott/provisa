# Copyright (c) 2026 Kenneth Stott
# Canary: c5ba82e7-9684-430d-83b7-c8666ce3f628
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The integration and e2e suites run on release branches, not on main (REQ-1989).

No push starts a run of them, to main or to release/**; a release is tested by a dispatch on its
branch (a push to release/** starts only lint and the requirements check). They are dispatched on request, and the nightly run is the warehouse lane alone."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
_WORKFLOWS = REPO / ".github" / "workflows"
_SUITE = _WORKFLOWS / "integration-suite.yml"
_SUITES = [_SUITE, *(_WORKFLOWS / f"ui-e2e-{lane}.yml" for lane in ("core", "trino", "swap-amd64"))]


def _triggers(workflow: Path = _SUITE) -> dict:
    return yaml.safe_load(workflow.read_text())[True]  # PyYAML reads the key `on` as True


@pytest.mark.parametrize("workflow", _SUITES, ids=lambda path: path.stem)
def test_no_push_starts_a_suite_and_a_dispatch_does(workflow: Path):
    triggers = _triggers(workflow)
    assert "push" not in triggers
    assert "workflow_dispatch" in triggers


def test_the_suite_is_dispatched_on_request_and_nightly_for_the_warehouse_lane():
    triggers = _triggers()
    assert "workflow_dispatch" in triggers
    assert "paths" not in (triggers["workflow_dispatch"] or {})
    assert triggers["schedule"] == [{"cron": "0 6 * * *"}]

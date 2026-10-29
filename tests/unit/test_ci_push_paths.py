# Copyright (c) 2026 Kenneth Stott
# Canary: c5ba82e7-9684-430d-83b7-c8666ce3f628
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""During alpha the container suite runs only when asked for (maintainer 2026-10-15).

A push to main starts no run of it, whatever the push touches: the suite is dispatched on request,
and the nightly run is the warehouse lane alone."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
_SUITE = REPO / ".github" / "workflows" / "integration-suite.yml"


def _triggers() -> dict:
    return yaml.safe_load(_SUITE.read_text())[True]  # PyYAML reads the key `on` as True


def test_no_push_starts_the_suite():
    assert "push" not in _triggers()


def test_the_suite_is_dispatched_on_request_and_nightly_for_the_warehouse_lane():
    triggers = _triggers()
    assert "workflow_dispatch" in triggers
    assert "paths" not in (triggers["workflow_dispatch"] or {})
    assert triggers["schedule"] == [{"cron": "0 6 * * *"}]

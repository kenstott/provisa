# Copyright (c) 2026 Kenneth Stott
# Canary: 2b6f0c0e-4a52-4f6e-9d43-8c1f7e3d5a10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The lane's own time budget (scripts/ci/run_lane.py loads this plugin with ``-p``).

Each test is bounded (pytest-timeout), but a shard is not: core 3/6 of release 509 ran its tests
one after another until its job limit cancelled it, and a cancelled job leaves no junit, no
``--durations`` report, and sometimes no log at all. Past ``PROVISA_LANE_DEADLINE`` (epoch seconds)
no further test starts: the session ends failed, naming the first test it did not run and how many
it left, and its report (junit, durations: where the time went) is written as for any failure."""

from __future__ import annotations

import os
import time

import pytest

DEADLINE_ENV = "PROVISA_LANE_DEADLINE"


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item) -> None:
    deadline = os.environ.get(DEADLINE_ENV)
    if not deadline or time.time() < float(deadline):
        return
    items = item.session.items
    left = len(items) - items.index(item)
    pytest.exit(
        f"lane budget spent: {left} of {len(items)} tests not run, from {item.nodeid}; "
        "the slowest tests below are where the time went",
        returncode=pytest.ExitCode.TESTS_FAILED,
    )

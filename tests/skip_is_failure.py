# Copyright (c) 2026 Kenneth Stott
# Canary: 878fe343-3659-40b5-ba67-4fcd0a8f1157
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A warehouse test that skips has failed: it says what was missing, by name.

The warehouse lane exists to run against live cloud warehouses, with their credentials supplied
(CI: repo secrets; locally: .env). A test there that skips — a credential not set, a bucket not
named, a provisioning CLI absent — has not run, and a lane that reports it as skipped reports a
pass it did not earn. So every skip of a test marked ``requires_warehouse`` is reported as a
failure, carrying the skip's own reason, which names what was missing.

One skip is deliberate: a calcite adapter bug must not hold Provisa (maintainer 2026-11-21), so a
test it breaks is skipped until a fixed engine is pinned. Such a test carries
``@pytest.mark.calcite_issue("<repo>#<n>")`` beside its skip. Its skip stays a skip, never a pass,
and its reason is rewritten to start with ``KNOWN_CALCITE_SKIP`` and the issue, which
``scripts/ci/lane_report.py`` lists as a known calcite skip. A marked test that runs reports as
usual.
"""

from __future__ import annotations

import pytest

WAREHOUSE_MARKER = "requires_warehouse"
CALCITE_ISSUE_MARKER = "calcite_issue"
# Read back by scripts/ci/lane_report.py (its own copy; tests/unit/test_skip_is_failure.py checks
# the two agree).
KNOWN_CALCITE_SKIP = "known calcite skip"


def _calcite_issue(item) -> str | None:
    marker = item.get_closest_marker(CALCITE_ISSUE_MARKER)
    if marker is None:
        return None
    if len(marker.args) != 1 or not isinstance(marker.args[0], str) or not marker.args[0]:
        raise pytest.UsageError(
            f"{item.nodeid}: @pytest.mark.{CALCITE_ISSUE_MARKER} takes one argument, the calcite "
            f'issue, e.g. "govdata-ops#1416"; got {marker.args!r}'
        )
    return marker.args[0]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    issue = _calcite_issue(item)
    if not report.skipped:
        return
    if issue is not None and isinstance(report.longrepr, tuple):
        filename, lineno, reason = report.longrepr
        reason = reason.removeprefix("Skipped: ")
        report.longrepr = (filename, lineno, f"{KNOWN_CALCITE_SKIP} ({issue}): {reason}")
        return
    if item.get_closest_marker(WAREHOUSE_MARKER) is None:
        return
    reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
    report.outcome = "failed"
    report.longrepr = f"warehouse test did not run: {reason}"

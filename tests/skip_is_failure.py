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
"""

from __future__ import annotations

import pytest

WAREHOUSE_MARKER = "requires_warehouse"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if not report.skipped or item.get_closest_marker(WAREHOUSE_MARKER) is None:
        return
    reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
    report.outcome = "failed"
    report.longrepr = f"warehouse test did not run: {reason}"

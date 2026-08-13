# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1a94c7-58f2-4be1-9d33-2c07e4b81f5a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading the last scan back out of a results table (REQ-1443).

The results table is scan history — ``scan_time`` is the watermark, so scans append — which makes
the rows at the maximum ``scan_time`` the most recent execution.
"""

# Requirements: REQ-982, REQ-1443

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from provisa.api.metadata_export.dq_outcomes import index_outcomes, latest_scan_sql

SCAN_TIME = datetime(2026, 8, 12, 3, 0, tzinfo=UTC)


def _row(**overrides):
    row = {
        "target_table": "sales.orders",
        "column_name": "customer",
        "check_type": "missing",
        "outcome": "fail",
        "scan_id": "scan-9",
        "scan_time": SCAN_TIME,
        "metric_value": 0.07,
        "failed_rows": 7,
    }
    row.update(overrides)
    return row


def test_the_read_selects_only_the_most_recent_scan():
    sql = latest_scan_sql("quality", "orders_scans")
    assert sql == (
        "SELECT target_table, column_name, check_type, outcome, scan_id, scan_time, "
        "metric_value, failed_rows FROM quality.orders_scans "
        "WHERE scan_time = (SELECT MAX(scan_time) FROM quality.orders_scans)"
    )


def test_a_row_indexes_under_the_key_the_builder_reconstructs():
    index = index_outcomes([_row()])
    outcome = index[("sales.orders", "customer", "missing")]
    assert (outcome.status, outcome.scan_id, outcome.failed_rows) == ("fail", "scan-9", 7)
    assert outcome.scan_time == SCAN_TIME
    assert outcome.ran is True


def test_a_dataset_level_check_indexes_under_an_empty_column():
    """A table-level check has no column, and the builder addresses it the same way."""
    index = index_outcomes([_row(column_name=None, check_type="row_count", outcome="pass")])
    assert index[("sales.orders", "", "row_count")].status == "pass"


def test_two_checks_sharing_an_identity_get_no_outcome_rather_than_a_guess():
    """The contract cannot tell them apart, so handing either one a sibling's verdict would
    publish an outcome the check never produced."""
    rows = [_row(scan_id="scan-9"), _row(outcome="pass", metric_value=0.0, failed_rows=0)]
    assert index_outcomes(rows) == {}


def test_a_scan_time_that_is_not_a_timestamp_raises():
    """The results schema declares it a timestamp; a string means the read is wrong, and the
    epoch-millis fields the adapters publish cannot be computed from one."""
    with pytest.raises(TypeError, match="scan_time"):
        index_outcomes([_row(scan_time="2026-08-12T03:00:00+00:00")])

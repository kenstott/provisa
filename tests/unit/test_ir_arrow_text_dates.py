# Copyright (c) 2026 Kenneth Stott
# Canary: 2a2efed8-111f-4dfb-aa79-463473c09d0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A date or timestamp a JSON source gives as ISO-8601 text lands in its typed column.

Elasticsearch returns a ``date`` field as the text it was indexed with ("2025-03-01T09:15:00Z").
The column is declared TIMESTAMP, so the replica batch must carry it as a timestamp: Arrow does
not parse text, and the build failed with "object of type <class 'str'> cannot be converted to
int". A string that is not ISO-8601 still fails loud."""

# Requirements: REQ-1672, REQ-1915

from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa
import pytest

from provisa.core.ir_arrow import arrow_schema, rows_to_batch


def test_iso_text_lands_in_timestamp_and_date_columns():
    columns = [("opened_at", "timestamp"), ("due", "date")]
    batch = rows_to_batch(
        [
            {"opened_at": "2025-03-01T09:15:00Z", "due": "2025-03-08"},
            {"opened_at": "2025-03-02T16:40:00+02:00", "due": None},
        ],
        columns,
        arrow_schema(columns),
    )
    assert batch.column("opened_at").to_pylist() == [
        datetime(2025, 3, 1, 9, 15),
        datetime(2025, 3, 2, 14, 40),  # as UTC, the instant the text names
    ]
    assert batch.column("due").to_pylist() == [date(2025, 3, 8), None]


def test_text_that_is_not_a_date_fails_loud():
    columns = [("opened_at", "timestamp")]
    with pytest.raises((ValueError, pa.ArrowException)):
        rows_to_batch([{"opened_at": "last tuesday"}], columns, arrow_schema(columns))

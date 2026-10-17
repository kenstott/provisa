# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A snapshot view's correction (REQ-1162, REQ-1163). Each refresh appends the whole result, so
a row unchanged since the last refresh is recorded again and is no new version; a row whose
content changed at the same source time is a correction, and the latest content recorded is the
one in force -- as a delta view's is. Its content is compared on the view's declared columns.
Run on DuckDB."""

# Requirements: REQ-1162, REQ-1163

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.bitemporal import MODE_SNAPSHOT, BitemporalSpec, view_read_sql

pytestmark = pytest.mark.unit

_SPEC = BitemporalSpec(
    key=("desk",),
    mode=MODE_SNAPSHOT,
    source_column="valid_from",
    columns=("desk", "exposure", "valid_from", "source_from", "source_to", "recorded_at"),
)


def _log(rows: str):
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE log(desk VARCHAR, exposure INT, valid_from TIMESTAMP, "
        "sys_recorded_at TIMESTAMP)"
    )
    con.execute(f"INSERT INTO log VALUES {rows}")
    return con


def _read(con, spec=_SPEC) -> list[tuple]:
    return con.execute(
        "SELECT exposure, CAST(recorded_at AS DATE) "
        f"FROM ({view_read_sql('log', spec)}) t WHERE desk = 'rates'"
    ).fetchall()


_CORRECTED = """('rates', 1540000, '2026-09-30', '2026-10-01'),
  ('rates', 1550000, '2026-09-30', '2026-10-05')"""


def test_a_correction_at_the_same_source_time_is_the_version_in_force():
    import datetime as dt

    assert _read(_log(_CORRECTED)) == [(1550000, dt.date(2026, 10, 5))]


def test_the_same_content_recorded_every_refresh_is_no_new_version():
    import datetime as dt

    con = _log(
        """('rates', 1540000, '2026-09-30', '2026-10-01'),
  ('rates', 1540000, '2026-09-30', '2026-10-02'),
  ('rates', 1540000, '2026-09-30', '2026-10-03')"""
    )
    # One version, recorded when it was first seen -- not once a refresh.
    assert _read(con) == [(1540000, dt.date(2026, 10, 1))]


def test_a_correction_then_unchanged_refreshes_keeps_the_corrections_record():
    import datetime as dt

    con = _log(
        _CORRECTED
        + """,
  ('rates', 1550000, '2026-09-30', '2026-10-06'),
  ('rates', 1550000, '2026-09-30', '2026-10-07')"""
    )
    assert _read(con) == [(1550000, dt.date(2026, 10, 5))]


def test_a_snapshot_views_read_needs_its_columns():
    spec = BitemporalSpec(key=("desk",), mode=MODE_SNAPSHOT, source_column="valid_from")
    with pytest.raises(ValueError, match="names no columns"):
        view_read_sql("log", spec)

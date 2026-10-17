# Copyright (c) 2026 Kenneth Stott
# Canary: fc8311a8-f2e2-42f3-a280-9a447f18e867
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Time travel on both axes (REQ-1163 as amended): as of a time on the view's clock, as Provisa
had recorded it at another. A correction recorded later changes what the view says about the past,
and a read as recorded before it answers what was said then. Run on DuckDB.

The scenario: the rates desk's exposure, valid from 30 September, was recorded on 1 October
(1540000) and corrected on 5 October (1550000), still valid from 30 September."""

# Requirements: REQ-1163

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    AsOf,
    BitemporalSpec,
    parse_as_of,
    parse_as_recorded,
    request_as_of,
    view_read_sql,
)

pytestmark = pytest.mark.unit

_DELTA = """INSERT INTO log VALUES
  ('rates', 1540000, '2026-09-30', '2026-10-01', 'upsert'),
  ('credit', 750000, '2026-09-30', '2026-10-01', 'upsert'),
  ('rates', 1550000, '2026-09-30', '2026-10-05', 'upsert')"""
# The same history as whole snapshots: 1 October holds both desks, 5 October both, rates corrected.
_SNAPSHOT = """INSERT INTO log VALUES
  ('rates', 1540000, '2026-09-30', '2026-10-01'), ('credit', 750000, '2026-09-30', '2026-10-01'),
  ('rates', 1550000, '2026-09-30', '2026-10-05'), ('credit', 750000, '2026-09-30', '2026-10-05')"""
_MODES = [MODE_DELTA, MODE_SNAPSHOT]


def _log(mode: str):
    con = duckdb.connect()
    op = ", sys_op VARCHAR" if mode == MODE_DELTA else ""
    con.execute(
        "CREATE TABLE log(desk VARCHAR, exposure INT, valid_from TIMESTAMP, "
        f"sys_recorded_at TIMESTAMP{op})"
    )
    con.execute(_DELTA if mode == MODE_DELTA else _SNAPSHOT)
    return con, BitemporalSpec(
        key=("desk",),
        mode=mode,
        source_column="valid_from",
        columns=("desk", "exposure", "valid_from"),
    )


def _rates(con, spec, as_of=None, as_recorded=None) -> list[int]:
    key = request_as_of(as_of, as_recorded)
    travel = AsOf.from_key(key) if key else None
    return [
        r[0]
        for r in con.execute(
            f"SELECT exposure FROM ({view_read_sql('log', spec, travel)}) t "
            "WHERE desk = 'rates' ORDER BY recorded_at"
        ).fetchall()
    ]


@pytest.mark.parametrize("mode", _MODES)
def test_as_recorded_before_the_correction_answers_what_was_said_then(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, "2026-09-30T12:00:00", "2026-10-02T00:00:00") == [1540000]


@pytest.mark.parametrize("mode", _MODES)
def test_as_recorded_after_the_correction_answers_the_correction(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, "2026-09-30T12:00:00", "2026-10-06T00:00:00") == [1550000]


@pytest.mark.parametrize("mode", _MODES)
def test_as_of_alone_answers_the_correction(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, "2026-09-30T12:00:00") == [1550000]


@pytest.mark.parametrize("mode", _MODES)
def test_as_recorded_alone_answers_the_version_in_force_then_as_recorded_then(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, None, "2026-10-02T00:00:00") == [1540000]
    assert _rates(con, spec, None, "2026-10-06T00:00:00") == [1550000]


@pytest.mark.parametrize("mode", _MODES)
def test_as_recorded_before_anything_was_recorded_answers_nothing(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, "2026-09-30T12:00:00", "2026-09-30T00:00:00") == []


@pytest.mark.parametrize("mode", _MODES)
def test_a_range_as_recorded_answers_the_versions_recorded_by_then(mode):
    con, spec = _log(mode)
    assert _rates(con, spec, "2026-09-01/2026-10-31", "2026-10-02T00:00:00") == [1540000]


def test_as_recorded_is_a_point_and_nothing_else():
    assert parse_as_recorded("2026-10-02T00:00:00") == "TIMESTAMP '2026-10-02 00:00:00.000000'"
    with pytest.raises(ValueError, match="a point"):
        parse_as_recorded("2026-10-01/2026-10-05")
    for bad in ["", "yesterday", "2026-13-01"]:
        with pytest.raises(ValueError):
            parse_as_recorded(bad)


def test_both_values_travel_as_one_canonical_key():
    key = request_as_of("2026-09-30T12:00:00", "2026-10-02T00:00:00")
    travel = AsOf.from_key(key)
    assert travel.start_sql == parse_as_of("2026-09-30T12:00:00").start_sql
    assert travel.recorded_sql == "TIMESTAMP '2026-10-02 00:00:00.000000'"
    assert AsOf.from_key(travel.key()) == travel
    alone = AsOf.from_key(request_as_of(None, "2026-10-02T00:00:00"))
    assert alone.start_sql is None and alone.recorded_sql is not None
    # Neither given: no time travel at all, as before.
    assert request_as_of(None, None) is None
    # The as-of alone travels exactly as it did.
    assert request_as_of("2026-09-30T12:00:00", None) == parse_as_of("2026-09-30T12:00:00").key()
    with pytest.raises(ValueError, match="not a canonical as-of"):
        AsOf.from_key("@TIMESTAMP '2026-10-02'; DROP TABLE x")

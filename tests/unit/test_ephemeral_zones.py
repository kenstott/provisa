# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4b2a-93d5-4f60-8a1e-5d2b6c9e0f31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session's date times keep their zone (REQ-2176, REQ-2173): the session's column holds
the instant, the session's graph the zone every value of the column was written in, and a read
gives the value back in it. A column of several zones is read as instants."""

# Requirements: REQ-2176, REQ-2173

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_STOCKHOLM = (
    "datetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, "
    "nanosecond: 645876000, timezone: 'Europe/Stockholm'})"
)


def test_a_stored_date_time_keeps_its_named_zone(monkeypatch):
    # TCK Temporal5 [6]: read back as 'Z' at hour 11, its zone lost to the instant.
    s = Session(monkeypatch, {})
    s.run(f"CREATE (:Val {{date: {_STOCKHOLM}}})")
    [row] = s.run(
        "MATCH (v:Val) WITH v.date AS d "
        "RETURN d.hour AS h, d.timezone AS z, d.offset AS o, d.epochSeconds AS e"
    )
    assert row == {"h": 12, "z": "Europe/Stockholm", "o": "+01:00", "e": 469020674}
    [held] = s.run("MATCH (v:Val) RETURN v.date AS d")
    assert held["d"].isoformat() == "1984-11-11T12:31:14.645876+01:00"
    assert str(held["d"].tzinfo) == "Europe/Stockholm"


def test_a_stored_date_time_keeps_its_offset(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:Val {date: datetime('2015-07-21T21:40:32.142+01:00')})")
    [row] = s.run("MATCH (v:Val) RETURN v.date.hour AS h, v.date.offset AS o")
    assert row == {"h": 21, "o": "+01:00"}


def test_date_times_of_several_zones_in_one_property_read_as_instants(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(f"CREATE (:Val {{date: {_STOCKHOLM}}})")
    s.run("CREATE (:Val {date: datetime('1984-11-11T12:31:14.645876+05:00')})")
    rows = s.run("MATCH (v:Val) RETURN v.date.hour AS h, v.date.offset AS o ORDER BY h")
    assert rows == [{"h": 7, "o": "Z"}, {"h": 11, "o": "Z"}]


def test_a_set_date_time_keeps_its_zone(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:Val {k: 1})")
    s.run(f"MATCH (v:Val) SET v.date = {_STOCKHOLM}")
    [row] = s.run("MATCH (v:Val) RETURN v.date.hour AS h, v.date.timezone AS z")
    assert row == {"h": 12, "z": "Europe/Stockholm"}

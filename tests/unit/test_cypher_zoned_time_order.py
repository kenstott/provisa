# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7c4e52-8a1f-4c6d-9e3b-5f2a7d6c1e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Nodes of several labels sorted by a time with its offset (REQ-2173): an unlabelled MATCH reads
the session's zoned times through its union, typed TIMETZ, and a time is ordered by its instant.
The union had cast to the session's own type name, which no engine parses ("No expression was
parsed from 'zoned_time'"). The scenarios are the openCypher TCK's (WithOrderBy1 [37], [38],
WithOrderBy2 [15], [16]), run on DuckDB over a session graph."""

# Requirements: REQ-2173, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_TIMES = (
    "CREATE (:A {time: time({hour: 10, minute: 35, timezone: '-08:00'})}), "
    "(:B {time: time({hour: 12, minute: 31, second: 14, nanosecond: 645876000, "
    "timezone: '+01:00'})}), "
    "(:C {time: time({hour: 12, minute: 31, second: 14, nanosecond: 645877000, "
    "timezone: '+01:00'})}), "
    "(:D {time: time({hour: 12, minute: 35, second: 15, timezone: '+05:00'})}), "
    "(:E {time: time({hour: 12, minute: 30, second: 14, nanosecond: 645876000, "
    "timezone: '+01:01'})})"
)


def _labels(rows) -> list[str]:
    return [r["a"]["label"] for r in rows]


@pytest.mark.parametrize(
    ("sort", "first"),
    [("time", ["D", "E", "B"]), ("time DESC", ["A", "C", "B"])],
)
def test_a_time_variable_sorts_by_its_instant(monkeypatch, sort, first):
    # WithOrderBy1 [37], [38]
    s = Session(monkeypatch, {})
    s.run(_TIMES)
    rows = s.run(f"MATCH (a) WITH a, a.time AS time WITH a, time ORDER BY {sort} LIMIT 3 RETURN a")
    assert _labels(rows) == first


@pytest.mark.parametrize(
    ("sort", "first"),
    [
        ("a.time + duration({minutes: 6})", ["D", "E", "B"]),
        ("a.time + duration({minutes: 6}) DESC", ["A", "C", "B"]),
    ],
)
def test_a_time_expression_sorts_by_its_instant(monkeypatch, sort, first):
    # WithOrderBy2 [15], [16]
    s = Session(monkeypatch, {})
    s.run(_TIMES)
    rows = s.run(f"MATCH (a) WITH a ORDER BY {sort} LIMIT 3 RETURN a")
    assert _labels(rows) == first

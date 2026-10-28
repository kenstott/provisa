# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bench: the traversal reorder on a large session graph (REQ-2176, "SESSION GRAPHS ARE INDEXED
AND COUNTED"). Thousands of Big nodes and a handful of Small, every Big joined to one Small: the
plan of a path written from Big starts at Small, and its rows are the rows the path read as
written (the reorder turned off) returns."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest
import sqlglot

from provisa.gql import traversal_order
from tests.unit.ephemeral_harness import Session

# The session graph lives in the harness's own DuckDB: no stack.
pytestmark = [pytest.mark.integration, pytest.mark.local_platform, pytest.mark.slow]

_BIG = 5000
_SMALL = 5
_QUERY = "MATCH (a:Big)-[:R]->(b:Small) RETURN a.k, b.k"


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def _first_table(sql: str) -> str:
    return sqlglot.parse_one(sql, read="postgres").args["from_"].this.name


@pytest.fixture
def session(monkeypatch) -> Session:
    s = Session(monkeypatch, {})
    from provisa.core import settings_registry

    # The bench's graph is past the harness's per-table row limit.
    limits = {"temp_tables.max_per_session": 50, "temp_tables.max_rows": 4 * _BIG}
    real = settings_registry.value
    monkeypatch.setattr(
        settings_registry, "value", lambda key: limits[key] if key in limits else real(key)
    )
    s.run(f"UNWIND range(1, {_BIG}) AS i CREATE (:Big {{k: i}})")
    s.run(f"UNWIND range(0, {_SMALL - 1}) AS i CREATE (:Small {{k: i}})")
    s.run(f"MATCH (b:Big), (s:Small) WHERE b.k % {_SMALL} = s.k CREATE (b)-[:R]->(s)")
    return s


def test_a_large_paths_plan_starts_at_small_and_holds_the_written_paths_rows(session, monkeypatch):
    assert _first_table(session.sql(_QUERY)).startswith("__session_node__Small")
    reordered = _values(session.run(_QUERY))
    assert len(reordered) == _BIG

    monkeypatch.setattr(traversal_order, "reordered", lambda ast, _label_map: ast)
    assert _first_table(session.sql(_QUERY)).startswith("__session_node__Big")
    assert _values(session.run(_QUERY)) == reordered

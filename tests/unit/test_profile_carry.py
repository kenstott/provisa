# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's profile runs carried in its bundle are loaded under the ids its template gives their
tables (REQ-2052): each relation replaced whole, its timestamps parsed back, and runs of a table
the template does not register refused by name."""

# Requirements: REQ-2052

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.sql.dml import Insert
from sqlalchemy.sql.selectable import Select

from provisa.profiler import carry

pytestmark = pytest.mark.unit

_RUN_TIME = datetime(2026, 10, 21, 2, 0, tzinfo=UTC)


class _Conn:
    """The template org's connection: its registered tables, and what was run against it."""

    def __init__(self, tables):
        self.tables = tables
        self.ran: list = []
        self.inserted: dict = {}

    async def execute_core(self, stmt):
        if isinstance(stmt, Select):
            rows = [
                SimpleNamespace(id=i, source_id=s, schema_name=sc, table_name=t)
                for i, (s, sc, t) in self.tables.items()
            ]
            return SimpleNamespace(fetchall=lambda: rows)
        self.ran.append((type(stmt).__name__, stmt.element.name))
        return None

    async def execute_core_many(self, stmt, rows):
        assert isinstance(stmt, Insert)
        self.inserted[stmt.table.name] = rows


def _bundle(tmp_path, results):
    path = tmp_path / "profile_results.json"
    path.write_text(json.dumps({"results": results}, default=carry._encode))
    return path


def test_runs_load_under_the_template_s_own_table_id_with_their_timestamps(tmp_path):
    run = {"run_id": "r1", "run_time": _RUN_TIME, "status": "succeeded", "row_count": 5000}
    path = _bundle(
        tmp_path, [{"table": ["crm", "public", "customers"], "kind": "runs", "rows": [run]}]
    )
    conn = _Conn({42: ("crm", "public", "customers")})
    loaded = asyncio.run(carry.load_runs(conn, path))
    assert loaded == ["customers_42_profile_runs"]
    assert conn.ran == [
        ("DropTable", "customers_42_profile_runs"),
        ("CreateTable", "customers_42_profile_runs"),
    ]
    assert conn.inserted["customers_42_profile_runs"] == [run]


def test_a_relation_with_no_rows_is_still_made(tmp_path):
    path = _bundle(tmp_path, [{"table": ["crm", "public", "orders"], "kind": "drift", "rows": []}])
    conn = _Conn({7: ("crm", "public", "orders")})
    assert asyncio.run(carry.load_runs(conn, path)) == ["orders_7_profile_drift"]
    assert conn.inserted == {}


def test_runs_of_a_table_the_template_does_not_register_are_refused_by_name(tmp_path):
    path = _bundle(tmp_path, [{"table": ["crm", "public", "gone"], "kind": "runs", "rows": []}])
    with pytest.raises(ValueError, match=r"crm\.public\.gone"):
        asyncio.run(carry.load_runs(_Conn({7: ("crm", "public", "orders")}), path))


def test_a_value_with_no_json_form_is_refused_by_its_type():
    with pytest.raises(TypeError, match="set"):
        carry._encode({1})


def test_the_bundle_carries_them_beside_the_seed(tmp_path):
    assert carry.results_path(tmp_path) == tmp_path / "seeded" / "profile_results.json"

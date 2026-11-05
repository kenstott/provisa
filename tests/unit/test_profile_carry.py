# Copyright (c) 2026 Kenneth Stott
# Canary: 69f1f5c4-4bdb-4d4d-8524-2929c82e02aa
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


def test_a_demo_started_from_its_bundle_loads_the_runs_the_bundle_carries(tmp_path):
    """REQ-2052 (A FIRST RUN IS SEEDED): a bundle started on its own server has the runs its build
    measured, as its hosted template does."""
    run = {"run_id": "r1", "run_time": _RUN_TIME, "status": "succeeded", "row_count": 5000}
    carry.results_path(tmp_path).parent.mkdir()
    carry.results_path(tmp_path).write_text(
        json.dumps(
            {"results": [{"table": ["crm", "public", "customers"], "kind": "runs", "rows": [run]}]},
            default=carry._encode,
        )
    )
    conn = _Conn({42: ("crm", "public", "customers")})
    assert asyncio.run(carry.load_bundle_runs(conn, tmp_path)) == ["customers_42_profile_runs"]


def test_a_server_started_as_no_demo_or_a_bundle_without_runs_is_refused_by_name(tmp_path):
    with pytest.raises(ValueError, match="started as no named demo"):
        asyncio.run(carry.load_bundle_runs(_Conn({}), None))
    with pytest.raises(ValueError, match=r"carries no profile runs \(profile_results\.json\)"):
        asyncio.run(carry.load_bundle_runs(_Conn({}), tmp_path))


def test_the_route_refuses_a_server_started_as_no_demo_by_name(monkeypatch):
    from contextlib import asynccontextmanager

    from provisa.api.admin import profiler_router
    from provisa.api.errors import ApiError

    class _Db:
        @asynccontextmanager
        async def acquire(self):
            yield _Conn({})

    monkeypatch.setattr(profiler_router, "require_capability_request", lambda *_a: None)
    monkeypatch.setattr(profiler_router, "_db", lambda: _Db())
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    with pytest.raises(ApiError) as refused:
        asyncio.run(profiler_router.load_carried_runs(SimpleNamespace()))
    assert refused.value.status_code == 409 and "no named demo" in str(refused.value)

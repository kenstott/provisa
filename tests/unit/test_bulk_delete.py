# Copyright (c) 2026 Kenneth Stott
# Canary: 75542380-74dc-4951-8182-990b57c28158
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rows of a list deleted together (REQ-2240): one call per list kind, each row through the single
delete's own guard. What can be deleted is deleted; each row refused is named with its reason and
the dependents its single delete named; a caller without the right to delete is refused before any
row is attempted."""

# Requirements: REQ-2240

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin._bulk_delete import delete_each
from provisa.api.admin.types import MutationResult
from provisa.core.database import Database
from provisa.core.schema_org import domains, registered_tables, sources
from tests.unit.gate_identity import serve_every_column


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    async def _invalidate(_state, _table_ids) -> int:
        return 0

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr("provisa.cache.tenancy.invalidate_tables", _invalidate)
    monkeypatch.setattr(appmod.state, "hot_manager", None, raising=False)
    return db


async def _table(db: Database, name: str, view_sql: str | None = None) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                view_sql=view_sql,
            )
        )
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _names(db: Database) -> list[str]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(registered_tables.c.table_name).order_by(registered_tables.c.table_name)
        )
        return [r[0] for r in rows.fetchall()]


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def test_the_deletable_tables_are_deleted_and_each_refused_one_is_named_with_why(plane):
    orders = await _table(plane, "orders")
    view = await _table(plane, "open_orders", "SELECT id FROM orders")
    customers = await _table(plane, "customers")

    mutation: Any = schema_mutation.Mutation()
    result = await mutation.delete_tables(_info(), [orders, customers, 4242])

    assert result.deleted == ["customers"]
    assert [(r.name, r.dependents) for r in result.refused] == [
        (
            "orders",
            [
                {
                    "kind": "table",
                    "id": view,
                    "name": "open_orders",
                    "via": ["registered_tables.view_sql"],
                }
            ],
        ),
        ("4242", []),
    ]
    assert "open_orders" in result.refused[0].reason
    assert result.refused[1].reason == "Table 4242 not found"
    assert await _names(plane) == ["open_orders", "orders"]


async def test_a_caller_without_the_right_to_delete_is_refused_before_any_table(plane, monkeypatch):
    orders = await _table(plane, "orders")
    asked: list[str] = []

    def _require(_info, capability, domain_id=None):
        asked.append(capability)
        raise PermissionError(f"Missing capability: {capability!r}")

    monkeypatch.setattr(schema_mutation, "require_capability", _require)

    with pytest.raises(PermissionError, match="Missing capability: 'table_registration'"):
        mutation: Any = schema_mutation.Mutation()
        await mutation.delete_tables(_info(), [orders])

    assert asked == ["table_registration"]
    async with plane.acquire() as conn:
        count = (
            await conn.execute_core(select(func.count()).select_from(registered_tables))
        ).scalar_one()
    assert count == 1


async def test_a_row_its_own_rights_check_refuses_is_named_and_the_others_go():
    gone: list[str] = []

    async def _name(key: str) -> str:
        return key.upper()

    async def _delete(key: str) -> MutationResult:
        if key == "hr":
            raise PermissionError("Missing capability: 'domain hr'")
        gone.append(key)
        return MutationResult(success=True, message="deleted")

    result = await delete_each(["a", "hr", "b"], _name, _delete)

    assert result.deleted == ["A", "B"]
    assert [(r.name, r.reason, r.dependents) for r in result.refused] == [
        ("HR", "Missing capability: 'domain hr'", [])
    ]
    assert gone == ["a", "b"]

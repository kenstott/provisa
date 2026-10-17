# Copyright (c) 2026 Kenneth Stott
# Canary: c1637a02-dc67-4770-99c3-7658b56a03fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An administrator sets a table's write grants (REQ-2074).

The grants are saved whole for the table, gated as row filters are (masking_config in the
table's domain), and refused by name -- an unknown role, a column the table does not have, a
second grant for one role and operation, a predicate that does not resolve against the table --
before anything is stored."""

# Requirements: REQ-2074, REQ-1676

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import WriteGrantInput, WritePresetInput
from provisa.core.database import Database
from provisa.core.repositories import write_grant as write_grant_repo
from provisa.core.schema_org import domains, registered_tables, roles, sources, table_columns


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="support", capabilities=["write"], domain_access=["sales"])
        )
        await conn.execute_core(
            insert(registered_tables).values(
                id=1, source_id="pg", domain_id="sales", schema_name="public", table_name="orders"
            )
        )
        for column, data_type in (("id", "integer"), ("status", "varchar")):
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=1,
                    domain_id="sales",
                    column_name=column,
                    data_type=data_type,
                    visible_to=["support"],
                )
            )

    async def _pool():
        return db

    rebuilds: list[int] = []

    async def _rebuild():
        rebuilds.append(1)

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    db.rebuilds = rebuilds  # type: ignore[attr-defined]
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


def _update(**kw) -> WriteGrantInput:
    return WriteGrantInput(
        **{
            "role": "support",
            "operation": "update",
            "columns": ["status"],
            "rows": "status IN ('placed', 'packed')",
            "check": None,
            "presets": [],
            **kw,
        }
    )


async def _set(grants: list[WriteGrantInput], table_id: int = 1):
    return await schema_mutation.Mutation().set_table_write_grants(_info(), table_id, grants)


async def _held(db: Database) -> list[dict]:
    async with db.acquire() as conn:
        return await write_grant_repo.for_table(conn, 1)


async def test_grants_are_saved_whole_and_the_schemas_rebuilt(plane):
    stamped = _update(presets=[WritePresetInput(column="id", source="literal", value="1")])
    result = await _set([stamped, _update(operation="delete", columns=[], rows="TRUE")])
    assert (result.success, result.code) == (True, "schema.write_grants_saved")
    held = await _held(plane)
    assert [(g["role"], g["operation"]) for g in held] == [
        ("support", "delete"),
        ("support", "update"),
    ]
    assert held[1]["presets"] == [{"column": "id", "source": "literal", "value": "1"}]
    assert plane.rebuilds == [1]  # type: ignore[attr-defined]
    assert (await _set([])).success and await _held(plane) == []


@pytest.mark.parametrize(
    ("grant", "code"),
    [
        (_update(role="nobody"), "schema.role_not_found"),
        (_update(columns=["nope"]), "schema.write_grant_invalid"),
        (_update(rows="nope = 1"), "schema.write_grant_invalid"),
        (_update(operation="insert", rows="TRUE"), "schema.write_grant_invalid"),
    ],
)
async def test_a_grant_that_does_not_fit_the_table_is_refused_and_nothing_stored(
    plane, grant, code
):
    result = await _set([grant])
    assert (result.success, result.code) == (False, code)
    assert await _held(plane) == []
    assert plane.rebuilds == []  # type: ignore[attr-defined]


async def test_two_grants_for_one_role_and_operation_are_refused(plane):
    result = await _set([_update(), _update(columns=["id"])])
    assert (result.success, result.code) == (False, "schema.write_grant_invalid")
    assert "two update grants" in result.message


async def test_a_table_that_is_not_registered_is_refused(plane):
    result = await _set([_update()], table_id=99)
    assert (result.success, result.code) == (False, "schema.table_id_not_found")

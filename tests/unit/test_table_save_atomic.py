# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One save of a table applies everything, or nothing (REQ-2109, settled).

registerTable and updateTable carry the settings that also have mutations of their own -- the
naming convention, the role TTLs, replicate, the write grants and draft -- and apply all of them
with the table in one control-plane transaction. A refusal of any one leaves nothing stored: no
table row, no columns, no grants on a registration; the stored table as it was on an update."""

# Requirements: REQ-2109, REQ-1907, REQ-826, REQ-2074, REQ-1921

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import _table_ops, schema_mutation, schema_mutation_ops
from provisa.api.admin.types import (
    ColumnInput,
    RoleTtlInput,
    TableInput,
    WriteGrantInput,
)
from provisa.core.database import Database
from provisa.core.models import Column
from provisa.core.repositories import write_grant as write_grant_repo
from provisa.core.schema_org import domains, registered_tables, roles, sources, table_columns


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", cache_ttl=300))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="support", capabilities=["write"], domain_access=["sales"])
        )

    async def _pool():
        return db

    async def _nothing(*_a, **_k):
        return None

    async def _columns(_pool, inp):
        return [
            Column(name=c.name, data_type=c.data_type, visible_to=c.visible_to) for c in inp.columns
        ], None

    engine = types.SimpleNamespace(reconcile_landed_tables=_nothing)
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(appmod.state, "federation_engine", engine, raising=False)
    for module in (schema_mutation, schema_mutation_ops):
        monkeypatch.setattr(module, "_get_pool", _pool)
        monkeypatch.setattr(module, "_rebuild_schemas", _nothing)
        monkeypatch.setattr(module, "_build_columns_for_input", _columns)
        monkeypatch.setattr(module, "column_field_refusal", _nothing)
    monkeypatch.setattr(_table_ops, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation_ops, "reprovision_source_after_registration", _nothing)
    monkeypatch.setattr("provisa.dq.scan.reschedule_org_checkers", _nothing)
    monkeypatch.setattr("provisa.core.deployment_settings.auto_track_fk", lambda: False)
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


def _grant(**kw) -> WriteGrantInput:
    return WriteGrantInput(
        **{
            "role": "support",
            "operation": "update",
            "columns": ["status"],
            "rows": "status = 'placed'",
            "check": None,
            "presets": [],
            **kw,
        }
    )


def _input(**kw) -> TableInput:
    return TableInput(
        **{
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                ColumnInput(name="id", data_type="integer", visible_to=["support"]),
                ColumnInput(name="status", data_type="varchar", visible_to=["support"]),
            ],
            **kw,
        }
    )


EVERYTHING = {
    "gql_naming_convention": "snake",
    "role_ttl": [RoleTtlInput(role="support", ttl=60)],
    "replicate": -1,
    "write_grants": [_grant()],
    "draft": False,
}


async def _stored(db: Database) -> tuple[dict | None, list[str], list[dict]]:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.table_name == "orders")
            )
        ).fetchone()
        if row is None:
            return None, [], []
        table = dict(row._mapping)
        cols = [
            r.column_name
            for r in (
                await conn.execute_core(
                    select(table_columns.c.column_name).where(
                        table_columns.c.table_id == table["id"]
                    )
                )
            ).fetchall()
        ]
        grants = await write_grant_repo.for_table(conn, table["id"])
    return table, sorted(cols), grants


async def _register(**kw):
    return await schema_mutation_ops.register_table(_info(), _input(**kw))


async def test_a_registration_carrying_every_setting_stores_all_of_them(plane):
    result = await _register(**EVERYTHING)
    assert result.success, result.message
    table, cols, grants = await _stored(plane)
    assert table is not None
    assert table["gql_naming_convention"] == "snake"
    assert dict(table["role_ttl"]) == {"support": 60}
    assert table["replicate"] == -1
    assert table["draft"] is False
    assert cols == ["id", "status"]
    assert [(g["role"], g["operation"]) for g in grants] == [("support", "update")]


async def test_left_out_a_registration_takes_a_new_tables_values(plane):
    result = await _register()
    assert result.success, result.message
    table, _cols, grants = await _stored(plane)
    assert table is not None
    assert (table["draft"], table["replicate"], dict(table["role_ttl"])) == (True, None, {})
    assert grants == []


@pytest.mark.parametrize(
    ("refused", "code"),
    [
        ({"write_grants": [_grant(role="nobody")]}, "schema.role_not_found"),
        ({"write_grants": [_grant(columns=["nope"])]}, "schema.write_grant_invalid"),
        # Judged against the table as stored, inside the same transaction.
        ({"write_grants": [_grant(rows="nope = 1")]}, "schema.write_grant_invalid"),
        ({"role_ttl": [RoleTtlInput(role="nobody", ttl=5)]}, "schema.role_ttl_unknown_role"),
        ({"role_ttl": [RoleTtlInput(role="support", ttl=-1)]}, "schema.role_ttl_negative"),
        ({"replicate": -7}, "schema.replicate_invalid"),
        ({"gql_naming_convention": "camelCase"}, "schema.naming_convention_invalid"),
    ],
)
async def test_a_registration_refused_on_any_one_setting_stores_nothing(plane, refused, code):
    result = await _register(**{**EVERYTHING, **refused})
    assert (result.success, result.code) == (False, code)
    assert await _stored(plane) == (None, [], [])


async def test_an_update_carrying_every_setting_stores_all_of_them(plane):
    assert (await _register()).success
    result = await schema_mutation.Mutation().update_table(_info(), _input(**EVERYTHING))
    assert result.success, result.message
    table, cols, grants = await _stored(plane)
    assert table is not None
    assert (table["gql_naming_convention"], table["replicate"], table["draft"]) == (
        "snake",
        -1,
        False,
    )
    assert dict(table["role_ttl"]) == {"support": 60}
    assert cols == ["id", "status"] and len(grants) == 1


async def test_an_update_refused_on_any_one_setting_leaves_the_table_as_it_was(plane):
    assert (await _register(**EVERYTHING)).success
    before = await _stored(plane)
    changed = _input(
        description="changed",
        columns=[ColumnInput(name="id", data_type="integer", visible_to=["support"])],
        **{
            **EVERYTHING,
            "role_ttl": [RoleTtlInput(role="support", ttl=10)],
            "write_grants": [_grant(rows="nope = 1")],
        },
    )
    result = await schema_mutation.Mutation().update_table(_info(), changed)
    assert (result.success, result.code) == (False, "schema.write_grant_invalid")
    assert await _stored(plane) == before


async def test_an_update_leaving_a_setting_out_keeps_the_stored_one(plane):
    assert (await _register(**EVERYTHING)).success
    result = await schema_mutation.Mutation().update_table(_info(), _input(description="x"))
    assert result.success, result.message
    table, _cols, grants = await _stored(plane)
    assert table is not None
    assert (table["gql_naming_convention"], table["replicate"], table["draft"]) == (
        "snake",
        -1,
        False,
    )
    assert dict(table["role_ttl"]) == {"support": 60} and len(grants) == 1

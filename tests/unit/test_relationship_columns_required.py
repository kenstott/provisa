# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f2d61-9e47-4c05-a2d8-6f1c0e7b94a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship joins on its columns: a source column, and a target column unless its target is
a function. The admin save refuses one without them by name, with a catalogued code, and stores
nothing (a stored one would join nothing and show as "schedules." -> "employees.null")."""

# Requirements: REQ-019

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import schema_mutation
from provisa.api.admin.types import RelationshipInput
from provisa.core.database import Database
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    table_columns,
)


@pytest.fixture
async def plane(control_plane, monkeypatch: pytest.MonkeyPatch) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shelter-db", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="shelter"))
        for table_id, table, columns in (
            (1, "schedules", ("id", "employee_id")),
            (2, "employees", ("id",)),
        ):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="shelter-db",
                    domain_id="shelter",
                    schema_name="shelter",
                    table_name=table,
                )
            )
            for column in columns:
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=table_id,
                        domain_id="shelter",
                        column_name=column,
                        data_type="integer",
                    )
                )

    async def _pool() -> Database:
        return db

    async def _no_rebuild() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _no_rebuild)
    return db


@pytest.mark.parametrize(
    ("source_column", "target_column", "code"),
    [
        ("", "id", "schema.relationship_source_column_missing"),
        ("employee_id", "", "schema.relationship_target_column_missing"),
    ],
)
async def test_the_admin_save_refuses_a_relationship_without_its_columns(
    plane: Database, source_column: str, target_column: str, code: str
) -> None:
    result = await schema_mutation.save_relationship(
        RelationshipInput(
            id="schedules-to-employees",
            source_table_id="schedules",
            target_table_id="employees",
            source_column=source_column,
            target_column=target_column,
            cardinality="many-to-one",
        ),
        owner=None,
        needs_review=False,
        creating=True,
    )
    assert (result.success, result.code, result.params) == (
        False,
        code,
        {"relationship": "schedules-to-employees"},
    )
    async with plane.acquire() as conn:
        stored = (await conn.execute_core(select(relationships.c.id))).fetchall()
    assert stored == []

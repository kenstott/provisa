# Copyright (c) 2026 Kenneth Stott
# Canary: 0080f0b6-8bdd-4741-a0a9-792244ad00e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Taking a role off an object's grants, so the role can be deleted (REQ-1918).

``revokeRoleFromTable`` takes it off every column grant of one table (read, write, unmasked);
``revokeRoleFromObject`` off a metric's, command's or webhook's assigned roles. Each is an edit
of that one object through the model store, one commit (REQ-1524), a no-op success when the role
is not on the grant, and refused by name for an object that does not exist. Run on a SQLite
control plane.
"""

# Requirements: REQ-1918, REQ-1524, REQ-1919

from __future__ import annotations

import types
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import GrantKind
from provisa.core import model_change
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.env_deploy import PROJECTED
from provisa.core.model_change import ModelPlane
from provisa.core.models import Function, Role, Webhook
from provisa.core.repositories import function as function_repo
from provisa.core.repositories import role as role_repo
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    sources,
    table_columns,
    tracked_functions,
    tracked_webhooks,
)

ORG = "acme"


@pytest.fixture
async def plane(monkeypatch) -> Database:
    """``leaving`` and ``staying`` granted on a table's columns, a metric, a command and a
    webhook; a config-declared table granting ``leaving`` too."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="revoke-test")
    await _init_schema_portable(db)
    await seed(db)
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", appmod.state.tenant_db, raising=False)
    return db


async def seed(db: Database) -> None:
    """The grants the scenarios take a role off; shared with the PostgreSQL module."""
    both = ["leaving", "staying"]
    async with db.acquire() as conn:
        for role_id in ("leaving", "staying"):
            await role_repo.upsert(
                conn,
                Role(id=role_id, capabilities=["usage"], domain_access=["*"]),
                org_id=ORG,
            )
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        for table_id, name in ((1, "orders"), (2, "declared")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                )
            )
            for column in ("id", "amount"):
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=table_id,
                        column_name=column,
                        visible_to=both,
                        writable_by=both,
                        unmasked_to=both,
                    )
                )
        await conn.execute_core(
            insert(metrics).values(name="revenue", expression="SUM(orders.amount)", visible_to=both)
        )
        await function_repo.upsert_function(
            conn,
            Function(
                name="refund",
                source_id="pg",
                function_name="refund",
                returns="",
                domain_id="sales",
                visible_to=both,
            ),
        )
        await function_repo.upsert_webhook(
            conn,
            Webhook(name="notify", url="http://x", domain_id="sales", visible_to=both),
        )


@pytest.fixture
def rebuilds(monkeypatch) -> list[int]:
    seen: list[int] = []

    async def _rebuild() -> None:
        seen.append(1)

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return seen


@pytest.fixture
def commits(plane) -> Iterator[list[str]]:
    """The commit messages each change asks for, with the plane made acme/prod's model."""
    asked: list[str] = []

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor):
        asked.append(message)
        return "sha"

    plane.model = ModelPlane(ORG, "prod")
    model_change.attach(object(), commit=_write_through, projected=PROJECTED)  # type: ignore[arg-type]
    yield asked
    model_change.detach()


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id=ORG))
    return types.SimpleNamespace(context={"request": request})


async def _table(info, role_id: str, table_id: int):
    async with model_change.scope("POST /admin/graphql"):
        return await schema_mutation.Mutation().revoke_role_from_table(  # type: ignore[call-arg]
            info, role_id=role_id, table_id=table_id
        )


async def _object(info, role_id: str, kind: GrantKind, name: str):
    async with model_change.scope("POST /admin/graphql"):
        return await schema_mutation.Mutation().revoke_role_from_object(  # type: ignore[call-arg]
            info, role_id=role_id, kind=kind, name=name
        )


async def _grants(plane: Database) -> dict[str, list]:
    async with plane.acquire() as conn:
        cols = (
            await conn.execute_core(
                select(
                    table_columns.c.table_id,
                    table_columns.c.visible_to,
                    table_columns.c.writable_by,
                    table_columns.c.unmasked_to,
                )
            )
        ).fetchall()
        out: dict[str, list] = {
            f"table{r.table_id}": [r.visible_to, r.writable_by, r.unmasked_to] for r in cols
        }
        for name, table in (
            ("metric", metrics),
            ("command", tracked_functions),
            ("webhook", tracked_webhooks),
        ):
            out[name] = [
                r[0] for r in (await conn.execute_core(select(table.c.visible_to))).fetchall()
            ]
    return out


async def test_a_table_revoke_takes_the_role_off_every_column_grant_and_only_that_role(
    plane, rebuilds, commits
):
    result = await _table(_info(), "leaving", 1)
    assert (result.success, result.code) == (True, "schema.role_revoked_from_table")
    grants = await _grants(plane)
    assert grants["table1"] == [["staying"], ["staying"], ["staying"]]
    assert grants["table2"] == [["leaving", "staying"]] * 3  # another table is not touched
    assert commits == ["revoke table grants leaving on table 1"]
    assert rebuilds == [1]


@pytest.mark.parametrize("kind", [GrantKind.METRIC, GrantKind.COMMAND, GrantKind.WEBHOOK])
async def test_an_object_revoke_takes_the_role_off_its_assigned_roles(
    plane, rebuilds, commits, kind
):
    name = {GrantKind.METRIC: "revenue", GrantKind.COMMAND: "refund", GrantKind.WEBHOOK: "notify"}[
        kind
    ]
    result = await _object(_info(), "leaving", kind, name)
    assert (result.success, result.code) == (True, "schema.role_revoked_from_object")
    assert (await _grants(plane))[kind.value] == [["staying"]]
    assert commits == [f"revoke {kind.value} grants leaving on {name}"]


async def test_a_revoke_of_a_role_not_on_the_grant_is_a_success_that_changes_nothing(
    plane, rebuilds, commits
):
    await _table(_info(), "leaving", 1)
    again = await _table(_info(), "leaving", 1)
    assert again.success is True
    assert rebuilds == [1]  # the second rebuilt nothing
    assert len(commits) == 1


async def test_an_object_that_does_not_exist_is_refused_by_name(plane, rebuilds):
    missing_table = await _table(_info(), "leaving", 99)
    assert (missing_table.success, missing_table.code) == (False, "schema.table_id_not_found")
    missing = await _object(_info(), "leaving", GrantKind.METRIC, "nope")
    assert (missing.success, missing.code, missing.params) == (
        False,
        "schema.grant_object_not_found",
        {"kind": "metric", "name": "nope"},
    )
    assert rebuilds == []


async def test_a_seeded_table_is_edited_like_any_other_and_nothing_says_whence(plane, rebuilds):
    """REQ-1919: a table a configuration seeded is the store's like any other; the edit stands,
    and the answer carries no notice that a later config load re-applies the file."""
    result = await _table(_info(), "leaving", 2)
    assert result.success is True
    assert not hasattr(result, "warnings")


async def test_once_every_grant_is_revoked_the_role_can_be_deleted(plane, rebuilds):
    info = _info()
    for table_id in (1, 2):
        await _table(info, "leaving", table_id)
    for kind, name in (
        (GrantKind.METRIC, "revenue"),
        (GrantKind.COMMAND, "refund"),
        (GrantKind.WEBHOOK, "notify"),
    ):
        await _object(info, "leaving", kind, name)
    async with plane.acquire() as conn:
        assert await role_repo.delete(conn, "leaving") is True

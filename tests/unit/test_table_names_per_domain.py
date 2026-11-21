# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f1c6a-93b2-4e7f-8a41-2c6e9b7d3f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Table names are per domain, and every table sits in a domain (REQ-433, REQ-1531).

First-come ownership of a source table is withdrawn: the name checked is (domain, table name).
A registration that names no domain is refused by name on every path: registerTable,
registerTables, updateTable, a configuration import and the table repository's write. A table's
domain is changed by saving it into another domain: the save needs the right on both domains,
and its row rules and relationships are checked again.
"""

# Requirements: REQ-433, REQ-1531

from __future__ import annotations

import types
from contextlib import ExitStack
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import ColumnInput, TableInput
from provisa.core import domain_policy
from provisa.core.database import Database
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    rls_rules,
    sources,
    table_columns,
)
from tests.unit.gate_identity import serve_every_column

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

DOMAIN_REQUIRED = "schema.table_domain_required"


@pytest.fixture(autouse=True)
def _inert_policy():
    """The policy the maintainer's org runs under: domains neither required nor single."""
    domain_policy.reset()
    yield
    domain_policy.reset()


def _input(**kw: Any) -> TableInput:
    base: dict[str, Any] = dict(
        source_id="s",
        domain_id="",
        schema_name="public",
        table_name="beer",
        columns=[ColumnInput(name="id", visible_to=["analyst"])],
    )
    base.update(kw)
    return TableInput(**base)


# --- every path refuses a table with no domain --------------------------------------------------


async def test_the_policy_refuses_a_table_with_no_domain():
    with pytest.raises(domain_policy.TableDomainRequired, match="'beer' names no domain"):
        domain_policy.table_domain_id("", "beer")
    assert domain_policy.table_domain_id("pet-store", "beer") == "pet-store"


async def test_single_domain_mode_registers_into_its_one_domain():
    domain_policy.configure(False, "main")
    assert domain_policy.table_domain_id("", "beer") == "main"


async def test_register_table_refuses_a_table_with_no_domain():
    result = await schema_mutation.Mutation().register_table(MagicMock(), _input())  # pyright: ignore[reportCallIssue]
    assert (result.success, result.code, result.params) == (
        False,
        DOMAIN_REQUIRED,
        {"table": "beer"},
    )


async def test_register_tables_refuses_each_table_with_no_domain(monkeypatch):
    from tests.unit.gate_identity import grant

    info, _ = grant(monkeypatch, "table_registration")
    outcomes = await schema_mutation.Mutation().register_tables(  # pyright: ignore[reportCallIssue]
        info, [_input(table_name="beer"), _input(table_name="wine")]
    )
    assert [(o.table_name, o.result.success, o.result.code) for o in outcomes] == [
        ("beer", False, DOMAIN_REQUIRED),
        ("wine", False, DOMAIN_REQUIRED),
    ]


async def test_update_table_refuses_a_save_into_no_domain():
    result = await schema_mutation.Mutation().update_table(MagicMock(), _input())  # pyright: ignore[reportCallIssue]
    assert (result.success, result.code) == (False, DOMAIN_REQUIRED)


async def test_the_table_repository_refuses_a_table_with_no_domain():
    """The last write gate: a configuration apply, an import and the remote-source refreshes
    write through it."""
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo

    table = Table(
        source_id="s",
        domain_id="",
        schema_name="public",
        table_name="beer",
        columns=[Column(name="id", visible_to=["analyst"])],
    )
    with pytest.raises(domain_policy.TableDomainRequired):
        await table_repo.upsert(MagicMock(), table)


async def test_an_import_naming_a_table_with_no_domain_is_refused_by_name(monkeypatch):
    from provisa.api.admin import import_router
    from provisa.api.errors import ApiError

    async def _refuse(config):
        raise domain_policy.TableDomainRequired("beer")

    monkeypatch.setattr("provisa.api.apply_dry_run.dry_run_configuration", _refuse)
    with pytest.raises(ApiError) as err:
        await import_router._dry_run_report(MagicMock())
    assert (err.value.status_code, err.value.code, err.value.params) == (
        422,
        DOMAIN_REQUIRED,
        {"table": "beer"},
    )


# --- moving a table between domains -------------------------------------------------------------

# view_governance: the save keeps no stored grant lists back from these callers (REQ-1958).
RIGHTS = ["table_registration", "view_governance"]
ROLES = {
    "both": {"id": "both", "capabilities": RIGHTS, "domain_access": ["sales", "finance"]},
    "sales_only": {"id": "sales_only", "capabilities": RIGHTS, "domain_access": ["sales"]},
    "finance_only": {"id": "finance_only", "capabilities": RIGHTS, "domain_access": ["finance"]},
    "both_masking": {
        "id": "both_masking",
        "capabilities": [*RIGHTS, "masking_config"],
        "domain_access": ["sales", "finance"],
    },
}
for _role in ROLES.values():
    _role["own_domain_access"] = _role["domain_access"]


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


class _Pool:
    def __init__(self, db: Database):
        self._db = db

    def acquire(self):
        return self._db.acquire()


async def _plane(control_plane, monkeypatch, *, ruled: bool) -> tuple[Database, dict[str, int]]:
    """Domains sales and finance; orders and customers in sales, joined; invoices in finance.
    ``ruled``: orders carries a table-level row rule (of the seeded analyst role)."""
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(
                id="s", type="postgresql", host="h", port=5432, database="d", username="u"
            )
        )
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for table_name, domain_id in (
            ("orders", "sales"),
            ("customers", "sales"),
            ("invoices", "finance"),
        ):
            ids[table_name] = (
                await conn.execute_core(
                    insert(registered_tables)
                    .values(
                        source_id="s",
                        domain_id=domain_id,
                        schema_name="public",
                        table_name=table_name,
                    )
                    .returning(registered_tables.c.id)
                )
            ).scalar_one()
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=ids[table_name],
                    domain_id=domain_id,
                    column_name="id",
                    data_type="integer",
                    visible_to=["analyst"],
                )
            )
        await conn.execute_core(
            insert(relationships).values(
                id="orders_customers",
                source_table_id=ids["orders"],
                target_table_id=ids["customers"],
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        if ruled:
            await conn.execute_core(
                insert(rls_rules).values(
                    table_id=ids["orders"],
                    role_id="analyst",
                    filter_expr=b"id > 0",
                    description="positive ids",
                )
            )
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    return db, ids


def _info(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id], acting_roles=[role_id])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme", role=role_id)
    )
    return types.SimpleNamespace(context={"request": request})


def _move_patches(db: Database):
    from provisa.core.models import Column

    return (
        patch.object(schema_mutation, "_get_pool", new=AsyncMock(return_value=_Pool(db))),
        patch.object(
            schema_mutation,
            "_build_columns_for_input",
            new=AsyncMock(
                return_value=(
                    [Column(name="id", data_type="integer", visible_to=["analyst"])],
                    None,
                )
            ),
        ),
        patch.object(schema_mutation, "_rebuild_schemas", new=AsyncMock()),
        patch.object(schema_mutation, "_remove_view_mv"),
    )


async def _move(db: Database, role_id: str, to: str = "finance"):
    with ExitStack() as stack:
        for p in _move_patches(db):
            stack.enter_context(p)
        return await schema_mutation.Mutation().update_table(  # pyright: ignore[reportCallIssue]
            _info(role_id), _input(table_name="orders", domain_id=to)
        )


async def _domain_of(db: Database, table_id: int) -> str:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.domain_id).where(registered_tables.c.id == table_id)
            )
        ).scalar_one()


async def test_a_move_with_the_right_on_both_domains_moves_the_table(control_plane, monkeypatch):
    db, ids = await _plane(control_plane, monkeypatch, ruled=False)
    result = await _move(db, "both")
    assert result.success is True, result.message
    assert await _domain_of(db, ids["orders"]) == "finance"
    # Its relationship to customers (still in sales) now crosses domains: it lands needs_review.
    async with db.acquire() as conn:
        review = (
            await conn.execute_core(
                select(relationships.c.needs_review).where(relationships.c.id == "orders_customers")
            )
        ).scalar_one()
    assert review is True


@pytest.mark.parametrize("role_id", ["sales_only", "finance_only"])
async def test_a_move_with_the_right_on_one_domain_is_refused(control_plane, monkeypatch, role_id):
    db, ids = await _plane(control_plane, monkeypatch, ruled=False)
    with pytest.raises(PermissionError):
        await _move(db, role_id)
    assert await _domain_of(db, ids["orders"]) == "sales"


async def test_a_move_of_a_ruled_table_needs_the_masking_right_in_the_new_domain(
    control_plane, monkeypatch
):
    db, ids = await _plane(control_plane, monkeypatch, ruled=True)
    with pytest.raises(PermissionError):
        await _move(db, "both")
    assert await _domain_of(db, ids["orders"]) == "sales"
    result = await _move(db, "both_masking")
    assert result.success is True, result.message
    assert await _domain_of(db, ids["orders"]) == "finance"

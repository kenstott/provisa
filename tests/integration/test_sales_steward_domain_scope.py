# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7d2e91-6a3c-4f58-8e17-c0d95a2b6f43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sales steward governs sales and nothing else (REQ-1944), against a real org schema.

The org schema is built by the real ``init_schema`` so schema.sql's seed runs; the sales steward is
a COPY of the seeded data_steward -- its rights, with domain_access narrowed to sales. (Not a child
of it: a child inherits its parent's every-domain reach.) It then masks a sales column, writes a
sales row rule and creates a sales data product, and each of those acts on a finance object is
refused naming finance.
"""

# Requirements: REQ-1944, REQ-1943, REQ-1531

from __future__ import annotations

import os
import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_helpers, schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.schema_org import domains, roles, sources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1944"
_SCHEMA = f"org_{_ORG_ID}"
_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)


async def _drop(db: Database) -> None:
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


@pytest.fixture
async def plane(monkeypatch):
    engine = create_engine_from_url(_ASYNC_URL)
    db = Database(engine, name="tenant", search_path=_SCHEMA)
    await _drop(db)
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        await init_schema(db, fh.read(), org_id=_ORG_ID)
    from provisa.core.models import Column, ProvisaConfig, Table
    from provisa.core.repositories import role as role_repo
    from provisa.core.repositories import table as table_repo

    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for table_name, domain_id in (("orders", "sales"), ("invoices", "finance")):
            ids[table_name] = await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=table_name,
                    governance="pre-approved",
                    columns=[
                        Column(name="id", visible_to=[], data_type="integer"),
                        Column(name="amount", visible_to=[], data_type="integer"),
                    ],
                ),
            )
        seeded = (
            await conn.execute_core(
                select(roles.c.capabilities, roles.c.domain_access).where(
                    roles.c.id == "data_steward"
                )
            )
        ).one()
        # The seeded steward reaches every domain; the sales steward is its copy for sales.
        assert list(seeded[1]) == ["*"]
        await conn.execute_core(
            insert(roles).values(
                id="sales_steward", capabilities=list(seeded[0]), domain_access=["sales"]
            )
        )
        loaded = {r["id"]: r for r in await role_repo.list_all(conn)}
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", loaded, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "config",
        ProvisaConfig.model_validate({"sources": [], "domains": [], "tables": [], "roles": []}),
        raising=False,
    )
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _noop() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _noop)
    monkeypatch.setattr(appmod, "_rebuild_schemas", _noop)
    yield types.SimpleNamespace(db=db, ids=ids)
    await _drop(db)
    engine.dispose()


def _info(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id=_ORG_ID)
    )
    return types.SimpleNamespace(context={"request": request})


async def _masked(table_id: int) -> Any:
    from provisa.api.mcp.table_edit import read_table, table_input

    edited = table_input(await read_table(table_id))
    column = next(c for c in edited.columns if c.name == "amount")
    column.mask_type = "constant"
    column.mask_value = "0"
    return edited


async def test_a_sales_steward_governs_sales_and_is_refused_finance(plane):
    from provisa.api.admin.types import DataProductInput, RLSRuleInput
    from provisa.core.repositories.table import load_columns

    m = schema_mutation.Mutation()
    steward = _info("sales_steward")

    saved = await m.update_table(steward, await _masked(plane.ids["orders"]))
    assert saved.success is True, saved.message
    refused = await m.update_table(steward, await _masked(plane.ids["invoices"]))
    assert refused.success is False and "'finance'" in refused.message
    async with plane.db.acquire() as conn:
        orders = {c["column_name"]: c for c in await load_columns(conn, plane.ids["orders"])}
        invoices = {c["column_name"]: c for c in await load_columns(conn, plane.ids["invoices"])}
    assert orders["amount"]["mask_type"] == "constant"
    assert invoices["amount"]["mask_type"] is None

    rule = RLSRuleInput(table_id="orders", role_id="analyst", filter_expr="1 = 1")
    assert (await m.upsert_rls_rule(steward, rule)).success
    with pytest.raises(PermissionError, match="'finance'"):
        await m.upsert_rls_rule(
            steward, RLSRuleInput(table_id="invoices", role_id="analyst", filter_expr="1 = 1")
        )

    product = DataProductInput(id="sales_kpis", domain_id="sales", name="Sales KPIs")
    assert (await m.create_data_product(steward, product)).success
    with pytest.raises(PermissionError, match="'finance'"):
        await m.create_data_product(
            steward, DataProductInput(id="ledger", domain_id="finance", name="Ledger")
        )

    # The seeded data_steward reaches every domain: it governs finance too.
    assert (
        await m.update_table(_info("data_steward"), await _masked(plane.ids["invoices"]))
    ).success

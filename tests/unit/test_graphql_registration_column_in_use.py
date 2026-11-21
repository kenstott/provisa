# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1c93-2d58-4f6a-9e0b-7a3c5d1f8e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column in use refuses the registration (REQ-1929).

Registering a remote GraphQL table that completes a relationship takes the nested column the
relationship rides on out of the other table. When something uses that column, the registerTable
mutation is refused naming what uses it, and nothing of the registration is stored: not the table,
not its columns, not how it is read, not the relationship, and not in this process either."""

# Requirements: REQ-1929, REQ-313, REQ-1918

from __future__ import annotations

import types
from typing import Any

import pytest
import respx
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation_ops
from provisa.api.admin.types import ColumnInput, TableInput
from provisa.core.models import GraphQLRemoteConfig, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    relationships,
    sources,
    table_columns,
)
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_graphql_table_registration import URL, _mock_schema


async def _nothing(*_a, **_k):
    return None


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organization (REQ-2006)."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane):
    """The deployment org's runtime over a control plane of the test's own, holding the remote
    GraphQL source ``shop``; the steps of a save that reach outside the control plane (the
    engine, the schema build) stood in for."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(
                id="shop", type="graphql_remote", path=URL, mapping={}, cache_ttl=300
            )
        )
        await conn.execute_core(insert(domains).values(id="sales"))
    reg = {
        "source_id": "shop",
        "url": URL,
        "namespace": "shop",
        "domain_id": "",
        "auth": None,
        "tables": [],
    }
    engine = types.SimpleNamespace(
        name="duckdb",
        reconcile_landed_tables=_nothing,
        materialize_store_target=lambda _org: ("mat_store", "views"),
    )
    state = appmod.state
    monkeypatch.setattr(state, "tenant_db", db, raising=False)
    monkeypatch.setattr(state, "model_db", db, raising=False)
    monkeypatch.setattr(state, "source_types", {"shop": "graphql_remote"}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {"shop": reg}, raising=False)
    monkeypatch.setattr(state, "grpc_remote_sources", {}, raising=False)
    monkeypatch.setattr(state, "federation_engine", engine, raising=False)
    config = types.SimpleNamespace(graphql_remote=GraphQLRemoteConfig())
    monkeypatch.setattr(state, "config", config, raising=False)
    monkeypatch.setattr(schema_mutation_ops, "_rebuild_schemas", _nothing)
    monkeypatch.setattr(
        schema_mutation_ops, "require_capability", lambda *_a, **_k: None, raising=False
    )
    monkeypatch.setattr("provisa.api.admin.capabilities.require_capability", lambda *_a, **_k: None)
    monkeypatch.setattr(schema_mutation_ops, "reprovision_source_after_registration", _nothing)
    monkeypatch.setattr("provisa.dq.scan.reschedule_org_checkers", _nothing)
    monkeypatch.setattr("provisa.core.deployment_settings.auto_track_fk", lambda: False)
    return types.SimpleNamespace(db=db, reg=reg)


def _info() -> Any:
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=None, active_org_id="a", role="analyst")
    )
    return types.SimpleNamespace(context={"request": request})


def _table(name: str, *columns: str) -> TableInput:
    return TableInput(
        source_id="shop",
        domain_id="sales",
        schema_name="graphql",
        table_name=name,
        columns=[ColumnInput(name=c, visible_to=["analyst"]) for c in columns],
    )


async def _columns_of(db, table_name: str) -> set[str]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(table_columns.c.column_name)
            .select_from(
                table_columns.join(
                    registered_tables, registered_tables.c.id == table_columns.c.table_id
                )
            )
            .where(registered_tables.c.table_name == table_name)
        )
        return {r.column_name for r in rows.fetchall()}


async def _uses_orders_customer(db, kind: str) -> str:
    """Something that uses ``shop__orders.customer``; its name."""
    async with db.acquire() as conn:
        if kind == "metric":
            await conn.execute_core(
                insert(metrics).values(
                    name="customers_seen", expression="COUNT(shop__orders.customer)"
                )
            )
            return "customers_seen"
        await table_repo.upsert(
            conn,
            Table(
                source_id="shop",
                domain_id="sales",
                schema_name="graphql",
                table_name="orders_with_customer",
                view_sql="SELECT o.id, o.customer FROM shop__orders o",
                columns=[],
            ),
        )
        return "orders_with_customer"


@respx.mock
@pytest.mark.parametrize("kind", ["metric", "view"])
async def test_a_registration_whose_relationship_takes_a_column_in_use_is_refused_whole(
    plane, kind
):
    """REQ-1929 (A COLUMN IN USE REFUSES THE REGISTRATION): orders is registered alone with its
    nested customer column, and something comes to use orders.customer. Registering customers
    would store the relationship over customer and take the column: the registration is refused
    naming what uses it, and customers is not registered -- stored or in this process -- while
    orders keeps its columns."""
    _mock_schema()
    first = await schema_mutation_ops.register_table(
        _info(), _table("shop__orders", "id", "total", "customer_id", "customer")
    )
    assert first.success, first.message
    user = await _uses_orders_customer(plane.db, kind)
    async with plane.db.acquire() as conn:
        mapping_before = (
            await conn.execute_core(select(sources.c.mapping).where(sources.c.id == "shop"))
        ).scalar_one()
    held_before = [(t["sql_name"], [c["name"] for c in t["columns"]]) for t in plane.reg["tables"]]

    refused = await schema_mutation_ops.register_table(
        _info(), _table("shop__customers", "id", "name")
    )

    assert not refused.success
    assert refused.code == "schema.column_has_dependents"
    assert user in refused.message
    assert "customer" in refused.message
    async with plane.db.acquire() as conn:
        names = {
            r.table_name
            for r in (await conn.execute_core(select(registered_tables.c.table_name))).fetchall()
        }
        rels = (await conn.execute_core(select(relationships.c.id))).fetchall()
        mapping_after = (
            await conn.execute_core(select(sources.c.mapping).where(sources.c.id == "shop"))
        ).scalar_one()
    assert "shop__customers" not in names
    assert rels == []
    assert mapping_after == mapping_before
    assert await _columns_of(plane.db, "shop__orders") == {"id", "total", "customer_id", "customer"}
    assert [
        (t["sql_name"], [c["name"] for c in t["columns"]]) for t in plane.reg["tables"]
    ] == held_before
    assert "shop__customers" not in {t["sql_name"] for t in plane.reg["tables"]}

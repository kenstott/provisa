# Copyright (c) 2026 Kenneth Stott
# Canary: 5f2a9c14-7e3b-4d68-9a05-1c8f6e2b7d91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1008: list_schemas/list_tables/search_catalog against a REAL async DB, unmocked.

Regression coverage for a real bug: `_catalog()` used to call the sync `build_catalog_tables`
wrapper via `asyncio.to_thread`, which spins up a brand-new event loop and tries to use
`state.tenant_db`'s pool — bound to the CALLER's event loop — from within it. Async DB drivers
reject that cross-event-loop reuse. Every MCP tool that reached `_catalog()` failed, but the
existing test suite only ever exercised it with `_build_catalog_tables_async` monkeypatched away
entirely (test_mcp_server.py), so the real DB path was never actually run. This file runs it for
real, on a genuine SQLite-backed Database, with no catalog mock."""

from __future__ import annotations

import types

import pytest
from provisa.core.database import create_engine_from_url

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    relationships,
    roles,
    table_columns,
    naming_rules,
)

pytestmark = pytest.mark.asyncio

# table_repo.upsert() (used only by this test's own seeding, not by list_schemas/_catalog
# themselves) touches the glossary tables too — mirrors test_mcp_glossary_protocol.py's fixture.
_TABLES = [
    registered_tables,
    table_columns,
    # The catalog reads a table's references from the declared relationships.
    relationships,
    # naming_rules: the table upsert refuses a taken SQL address under them (REQ-1933).
    naming_rules,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_domains,
]


async def _seeded_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'catalog.db'}")
    with engine.begin() as c:
        registered_tables.metadata.create_all(c, tables=_TABLES)
    db = Database(engine, name="catalog")
    async with db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
                view_sql="SELECT 1 AS id",
            ),
        )
    return engine, db


def _state(db):
    # The analyst's compiled context, as a schema build leaves it for the table seeded above:
    # the tools list what the role is served, so a context with no table is an empty catalog.
    from provisa.compiler.sql_types import TableMeta

    with db._engine.connect() as c:
        (table_id,) = c.execute(
            registered_tables.select().with_only_columns(registered_tables.c.id)
        ).one()
    orders = TableMeta(
        table_id=table_id,
        field_name="orders",
        type_name="Orders",
        source_id="__derived__",
        catalog_name="derived",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    return types.SimpleNamespace(
        contexts={
            "analyst": types.SimpleNamespace(
                tables={"orders": orders}, joins={}, physical_to_sql={(table_id, "id"): "id"}
            )
        },
        roles={"analyst": {"domain_access": ["*"]}},
        config=types.SimpleNamespace(domains=[]),
        model_db=db,
        tenant_db=db,
        engine_conn=None,
    )


async def test_list_schemas_against_real_db_no_event_loop_error(tmp_path):
    from provisa.api.mcp import tools

    engine, db = await _seeded_db(tmp_path)
    try:
        result = await tools.list_schemas(_state(db), "analyst")
        assert result == [{"schema": "sales", "description": "", "table_count": 1}]
    finally:
        engine.dispose()


async def test_list_tables_against_real_db_no_event_loop_error(tmp_path):
    from provisa.api.mcp import tools

    engine, db = await _seeded_db(tmp_path)
    try:
        result = await tools.list_tables(_state(db), "analyst", "sales")
        assert [r["table"] for r in result] == ["orders"]
    finally:
        engine.dispose()


async def test_catalog_returns_empty_list_when_no_tenant_db():
    from provisa.api.mcp import tools

    state = types.SimpleNamespace(model_db=None, tenant_db=None)
    assert await tools._catalog(state, None) == []

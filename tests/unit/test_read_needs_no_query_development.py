# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e7a02-94d3-4f6b-8e21-b7a3d06f4c19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading needs no query_development (REQ-2155).

What a role reads is decided by its grants, row rules, masks and the relationship guard, on every
protocol alike. A role holding no capability reads over GraphQL and over SQL exactly the columns it
is granted, masked and row-ruled, and is refused a column it is not granted. Its statements run on
DuckDB, so what is asserted is the rows the governed statement answers. query_development still
gates what develops a query (the plan a request's stats show, /data/sql/explain), and a write
stays gated by its own right."""

# Requirements: REQ-2155, REQ-868, REQ-2074

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
import pytest
import sqlglot
from httpx import ASGITransport, AsyncClient

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.executor.result import QueryResult
from provisa.security.masking import MaskingRule, MaskType

READER = "reader"
DEVELOPER = "developer"
WRITER = "writer"
_ROLES = {
    READER: {"id": READER, "capabilities": [], "domain_access": ["*"]},
    DEVELOPER: {"id": DEVELOPER, "capabilities": ["query_development"], "domain_access": ["*"]},
    WRITER: {"id": WRITER, "capabilities": ["write"], "domain_access": ["*"]},
}
_GRANTED = [READER, DEVELOPER, WRITER]
_TABLE = {
    "id": 1,
    "source_id": "pg",
    "domain_id": "shop",
    "schema_name": "public",
    "table_name": "customers",
    "columns": [
        {"column_name": "id", "visible_to": _GRANTED, "writable_by": _GRANTED},
        {"column_name": "name", "visible_to": _GRANTED, "writable_by": _GRANTED},
        {"column_name": "region", "visible_to": _GRANTED, "writable_by": _GRANTED},
        {"column_name": "ssn", "visible_to": [DEVELOPER], "writable_by": []},
    ],
    "write_ops": ["insert", "update", "delete"],
    "write_returns_rows": True,
    "write_refused_forms": [],
    "write_grants": [
        {
            "role": role,
            "operation": "update",
            "columns": ["name"],
            "rows": "TRUE",
            "check": "TRUE",
            "presets": [],
        }
        for role in _GRANTED
    ],
}
_TYPES = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="region", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="ssn", data_type="varchar", is_nullable=True),
    ]
}
_MASKED = MaskingRule(mask_type=MaskType.constant, value="***")


def _surface(role: dict):
    si = SchemaInput(
        tables=[_TABLE],
        relationships=[],
        column_types=_TYPES,
        naming_rules=[],
        role=role,
        domains=[{"id": "shop", "description": "Shop"}],
    )
    return generate_schema(si), build_context(si)


@pytest.fixture
def source():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.customers (id INTEGER, name VARCHAR, region VARCHAR, ssn VARCHAR)"
    )
    con.execute(
        "INSERT INTO public.customers VALUES "
        "(1, 'ann', 'east', '111'), (2, 'bob', 'east', '222'), (3, 'cy', 'west', '333')"
    )
    return con


@pytest.fixture
async def client(monkeypatch, source):
    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from provisa.core.config_loader import parse_config_dict
    from provisa.executor.pool import SourcePool

    async def _nothing(*args, **kwargs):
        return []

    async def _executed_on_duckdb(pool, source_id, sql, params=None, *args, **kwargs):
        statement = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
        cursor = source.execute(statement, params or [])
        return QueryResult(
            rows=[tuple(row) for row in cursor.fetchall()],
            column_names=[column[0] for column in cursor.description],
        )

    monkeypatch.setattr("provisa.audit.query_log.log_queries", _nothing)
    monkeypatch.setattr("provisa.core.repositories.source.list_all", _nothing)
    monkeypatch.setattr("provisa.auth.middleware._loaded_derived_roles", lambda: [])
    monkeypatch.setattr("provisa.executor.direct.execute_direct", _executed_on_duckdb)
    # What follows a write reads the registry, which this state does not hold.
    monkeypatch.setattr("provisa.api.data.table_written.after_table_written", _nothing)
    monkeypatch.setattr(
        app_mod.state,
        "config",
        parse_config_dict({"sources": [], "domains": [], "tables": [], "roles": []}),
    )
    state = app_mod.state
    saved = {
        name: getattr(state, name, None)
        for name in (
            "auth_config",
            "tenant_db",
            "record_db",
            "model_db",
            "schemas",
            "contexts",
            "rls_contexts",
            "roles",
            "masking_rules",
            "source_types",
            "source_dialects",
            "tables",
            "relationships",
        )
    }
    state.auth_config = None
    state.tenant_db = state.record_db = state.model_db = MagicMock()
    the_app = create_app()
    surfaces = {role_id: _surface(role) for role_id, role in _ROLES.items()}
    state.schemas = {role_id: schema for role_id, (schema, _ctx) in surfaces.items()}
    state.contexts = {role_id: ctx for role_id, (_schema, ctx) in surfaces.items()}
    state.rls_contexts = {
        READER: RLSContext(rules={1: "region = 'east'"}),
        DEVELOPER: RLSContext.empty(),
        WRITER: RLSContext.empty(),
    }
    state.roles = dict(_ROLES)
    state.masking_rules = {(1, READER): {"name": (_MASKED, "varchar")}}
    state.source_types = {"pg": "postgresql"}
    state.source_dialects = {"pg": "postgres"}
    state.tables = [_TABLE]
    state.relationships = []
    state.source_pools = MagicMock()
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        yield c
    for name, value in saved.items():
        setattr(state, name, value)
    state.source_pools = SourcePool()


async def _graphql(client, query: str, role: str = READER, **headers):
    return await client.post(
        "/data/graphql",
        json={"query": query, "role": role},
        headers={"X-Provisa-Role": role, **headers},
    )


async def _sql(client, sql: str, role: str = READER, **headers):
    return await client.post(
        "/data/sql", json={"sql": sql, "role": role}, headers={"X-Provisa-Role": role, **headers}
    )


_READ = "{ customers { id name region } }"
_UPDATE = 'mutation { updateCustomers(where: {id: {eq: 1}}, set: {name: "x"}) { affected_rows } }'


async def test_graphql_reads_its_granted_columns_masked_and_row_ruled(client):
    resp = await _graphql(client, _READ)
    assert resp.status_code == 200, resp.text
    rows = next(iter(resp.json()["data"].values()))
    assert sorted(rows, key=lambda r: r["id"]) == [
        {"id": 1, "name": "***", "region": "east"},
        {"id": 2, "name": "***", "region": "east"},
    ]


async def test_graphql_refuses_a_column_it_is_not_granted(client):
    resp = await _graphql(client, "{ customers { id ssn } }")
    body = resp.json()
    assert "data" not in body or body["data"] is None
    assert "ssn" in resp.text


async def test_graphql_over_get_reads_as_post_does(client):
    resp = await client.get(
        "/data/graphql", params={"query": _READ}, headers={"X-Provisa-Role": READER}
    )
    assert resp.status_code == 200, resp.text
    rows = next(iter(resp.json()["data"].values()))
    assert sorted(r["id"] for r in rows) == [1, 2]


async def test_a_mutation_is_gated_by_the_write_right_alone(client, source):
    """The reader and the writer hold one write grant; the right is all that tells them apart."""
    name = "SELECT name FROM public.customers WHERE id = 1"
    refused = await _graphql(client, _UPDATE)
    assert refused.status_code == 403, refused.text
    assert "'write' right" in refused.text
    assert source.execute(name).fetchone() == ("ann",)
    written = await _graphql(client, _UPDATE, WRITER)
    assert written.status_code == 200, written.text
    assert source.execute(name).fetchone() == ("x",)


async def test_sql_reads_its_granted_columns_masked_and_row_ruled(client):
    resp = await _sql(client, 'SELECT "id", "name" FROM "shop"."customers" ORDER BY "id"')
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"]["sql"] == [{"id": 1, "name": "***"}, {"id": 2, "name": "***"}]


async def test_sql_refuses_a_column_it_is_not_granted(client):
    resp = await _sql(client, 'SELECT "ssn" FROM "shop"."customers"')
    assert resp.status_code == 403, resp.text


@pytest.mark.parametrize("send", [_graphql, _sql])
async def test_a_reads_stats_show_the_plan_only_to_a_developer(client, send):
    statement = _READ if send is _graphql else 'SELECT "id" FROM "shop"."customers"'
    refused = await send(client, statement, **{"X-Provisa-Stats": "true"})
    assert refused.status_code == 403, refused.text
    answered = await send(client, statement, DEVELOPER, **{"X-Provisa-Stats": "true"})
    assert answered.status_code == 200, answered.text
    assert "provisa_stats" in answered.text


async def test_explain_stays_a_development_feature(client):
    resp = await client.post(
        "/data/sql/explain",
        json={"sql": 'SELECT "id" FROM "shop"."customers"', "role": READER},
        headers={"X-Provisa-Role": READER},
    )
    assert resp.status_code == 403, resp.text

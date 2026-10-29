# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3e5a17-6d28-4b94-a0f1-7e2b8d4c6135
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""deployViewToDb against a real Postgres source: a single-source Provisa view is written back to
its source as a real view carrying its SQL, the source answers it as Provisa did, a view across two
sources is refused naming both, and a role without table_registration is refused. Supported
source types: ``VIEW_DEPLOY_SOURCE_TYPES`` (every other type is refused by name, unit-tested in
tests/unit/test_deploy_view_source_types.py)."""

# Requirements: REQ-2184

import json
import os
from types import SimpleNamespace

import pytest
import pytest_asyncio
import sqlalchemy as sa
from httpx import ASGITransport, AsyncClient

from tests.helpers import registered_id, release_mutation

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_DB = "provisa"
_SCHEMA_A = "j108_dv_a"
_SCHEMA_B = "j108_dv_b"
_SRC_A = "j108-pg-a"
_SRC_B = "j108-pg-b"
_DOMAIN = "j108dv"
_ADMIN = "org_admin"


def _pg_url() -> str:
    return f"postgresql+psycopg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"


@pytest.fixture(scope="module")
def source_db():
    """Two schemas of the real Postgres, one per Provisa source, dropped (views with them) after."""
    os.environ.setdefault("PG_PASSWORD", "provisa")
    engine = sa.create_engine(_pg_url(), isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        for schema in (_SCHEMA_A, _SCHEMA_B):
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(sa.text(f"CREATE SCHEMA {schema}"))
        conn.execute(
            sa.text(f"CREATE TABLE {_SCHEMA_A}.orders (id integer PRIMARY KEY, amount integer)")
        )
        conn.execute(sa.text(f"INSERT INTO {_SCHEMA_A}.orders VALUES (1, 5), (2, 50), (3, 500)"))
        conn.execute(
            sa.text(f"CREATE TABLE {_SCHEMA_B}.customers (id integer PRIMARY KEY, name text)")
        )
        conn.execute(sa.text(f"INSERT INTO {_SCHEMA_B}.customers VALUES (1, 'a'), (2, 'b')"))
    try:
        yield engine
    finally:
        with engine.connect() as conn:
            for schema in (_SCHEMA_A, _SCHEMA_B):
                conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        engine.dispose()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(source_db):
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-Provisa-Role": _ADMIN},
        ) as c:
            yield c


async def _admin(client, query: str) -> dict:
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


async def _ok(client, field: str, body: str) -> dict:
    data = await _admin(client, f"mutation {{ {field}({body}) {{ success message code }} }}")
    result = data[field]
    assert result["success"], result
    return result


_VISIBLE = f'visibleTo: ["{_ADMIN}"]'


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def model(client, source_db):
    """Two Postgres sources, a table on each, in one domain, released for reading."""
    ids = {}
    try:
        await _ok(client, "createDomain", f'input: {{ id: "{_DOMAIN}", description: "j108" }}')
        password = os.environ.get("PG_PASSWORD", "provisa")
        for source_id in (_SRC_A, _SRC_B):
            await _ok(
                client,
                "createSource",
                f'input: {{id: "{source_id}", type: "postgresql", host: "{_PG_HOST}", port: {_PG_PORT}, '
                f'database: "{_PG_DB}", username: "provisa", password: "{password}"}}',
            )
        for source_id, schema, table, columns in (
            (_SRC_A, _SCHEMA_A, "orders", ("id", "amount")),
            (_SRC_B, _SCHEMA_B, "customers", ("id", "name")),
        ):
            cols = ", ".join(
                f'{{name: "{c}", {_VISIBLE}, dataType: "{"text" if c == "name" else "integer"}"'
                f"{', isPrimaryKey: true' if c == 'id' else ''}}}"
                for c in columns
            )
            reg = await _ok(
                client,
                "registerTable",
                f'input: {{sourceId: "{source_id}", domainId: "{_DOMAIN}", schemaName: "{schema}", '
                f'tableName: "{table}", columns: [{cols}]}}',
            )
            released = await _admin(client, release_mutation(registered_id(reg["message"])))
            assert released["setTableDraft"]["success"], released
            ids[table] = registered_id(reg["message"])
            _REGISTERED.append(ids[table])
        yield ids
    finally:
        await _tear_down(client)


# Every table registered through the admin API by this module (views included), for teardown.
_REGISTERED: list[int] = []


async def _tear_down(client) -> None:
    """Delete what the module registered, tables first, then the domain and the sources. Every
    delete is attempted; a refusal is reported once all have run."""
    refused = []
    deletes = [f"deleteTable(id: {i})" for i in reversed(_REGISTERED)]
    deletes.append(f'deleteDomain(id: "{_DOMAIN}")')
    deletes += [f'deleteSource(id: "{s}")' for s in (_SRC_A, _SRC_B)]
    for call in deletes:
        resp = await client.post(
            "/admin/graphql", json={"query": f"mutation {{ {call} {{ success message }} }}"}
        )
        body = resp.json()
        result = (body.get("data") or {}).get(call.split("(")[0])
        if resp.status_code != 200 or not result or not result["success"]:
            refused.append((call, body))
    _REGISTERED.clear()
    assert not refused, refused


async def _register_view(client, name: str, sql: str, columns: list[str]) -> int:
    cols = ", ".join(f'{{name: "{c}", {_VISIBLE}}}' for c in columns)
    reg = await _ok(
        client,
        "registerTable",
        f'input: {{sourceId: "__derived__", domainId: "{_DOMAIN}", schemaName: "views", '
        f'tableName: "{name}", alias: "{name}", viewSql: {json.dumps(sql)}, columns: [{cols}]}}',
    )
    table_id = registered_id(reg["message"])
    _REGISTERED.append(table_id)
    released = await _admin(client, release_mutation(table_id))
    assert released["setTableDraft"]["success"], released
    return table_id


async def _provisa_rows(client, view: str) -> list[dict]:
    resp = await client.post(
        "/data/sql",
        json={
            "sql": f'SELECT id, amount FROM "{_DOMAIN}"."{view}" ORDER BY id',
            "role": _ADMIN,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _view_def(engine, schema: str, view: str) -> str | None:
    with engine.connect() as conn:
        return conn.execute(
            sa.text("SELECT definition FROM pg_views WHERE schemaname = :s AND viewname = :v"),
            {"s": schema, "v": view},
        ).scalar_one_or_none()


async def test_a_single_source_view_is_deployed_and_the_source_answers_as_provisa_did(
    client, model, source_db
):
    name = "j108_big_orders"
    table_id = await _register_view(
        client,
        name,
        f'SELECT id, amount FROM "{_DOMAIN}"."orders" WHERE amount > 10',
        ["id", "amount"],
    )
    before = await _provisa_rows(client, name)
    assert before == [{"id": 2, "amount": 50}, {"id": 3, "amount": 500}]
    assert _view_def(source_db, _SCHEMA_A, name) is None

    deployed = await _ok(client, "deployViewToDb", f"tableId: {table_id}")
    assert deployed["code"] == "schema.view_deployed", deployed

    definition = _view_def(source_db, _SCHEMA_A, name)
    assert definition is not None, "the view does not exist on the source"
    assert f"{_SCHEMA_A}.orders" in definition or "orders" in definition
    assert "amount" in definition and "10" in definition

    with source_db.connect() as conn:
        native = [
            dict(r._mapping)
            for r in conn.execute(sa.text(f"SELECT id, amount FROM {_SCHEMA_A}.{name} ORDER BY id"))
        ]
    assert native == before
    assert await _provisa_rows(client, name) == before


async def test_a_view_across_two_sources_is_refused_naming_both_and_creates_nothing(
    client, model, source_db
):
    name = "j108_cross"
    table_id = await _register_view(
        client,
        name,
        f'SELECT o.id AS id, o.amount AS amount FROM "{_DOMAIN}"."orders" o '
        f'JOIN "{_DOMAIN}"."customers" c ON c.id = o.id',
        ["id", "amount"],
    )
    data = await _admin(
        client, f"mutation {{ deployViewToDb(tableId: {table_id}) {{ success message code }} }}"
    )
    result = data["deployViewToDb"]
    assert result["success"] is False, result
    assert result["code"] == "schema.view_spans_sources"
    assert _SRC_A in result["message"] and _SRC_B in result["message"], result
    for schema in (_SCHEMA_A, _SCHEMA_B):
        assert _view_def(source_db, schema, name) is None


async def test_a_role_without_table_registration_is_refused_and_nothing_is_created(
    client, model, source_db
):
    from provisa.api.admin import schema_mutation_ops
    from provisa.api.app import state

    role = "analyst"
    assert "table_registration" not in (state.roles[role].get("capabilities") or [])
    name = "j108_denied"
    table_id = await _register_view(
        client, name, f'SELECT id, amount FROM "{_DOMAIN}"."orders"', ["id", "amount"]
    )
    identity = SimpleNamespace(user_id="u-j108", roles=[role], acting_roles=[role])
    info = SimpleNamespace(
        context={"request": SimpleNamespace(state=SimpleNamespace(identity=identity, role=role))}
    )
    with pytest.raises(PermissionError, match="Missing capability: 'table_registration'"):
        await schema_mutation_ops.deploy_view_to_db(info, table_id)
    assert _view_def(source_db, _SCHEMA_A, name) is None

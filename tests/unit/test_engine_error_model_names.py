# Copyright (c) 2026 Kenneth Stott
# Canary: b9bbbc7c-6db2-46c9-9897-81b5b1162420
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An engine's error is answered in the model's names (REQ-2152).

The engine runs the statement lowered to physical addresses -- a source's catalog, its schema
and its table -- and its error quotes them: DuckDB's Binder Error carries a ``LINE 1:`` excerpt
of the physical SQL. A physical address is source configuration, and the caller wrote model
names, so the error it is answered names each table as the model does
(``"pet_store"."pets"``), in the one place an error body leaves for the client.
"""

# Requirements: REQ-2152

from __future__ import annotations

import json
import types

import duckdb
import pytest

from provisa.api import error_redaction
from provisa.compiler.sql_types import TableMeta

_ORG = "acme"


def _meta(table_id: int, table: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=f"pet_store__{table}",
        type_name=table.title(),
        source_id="pet-store-sqlite",
        catalog_name="pet_store_sqlite",
        schema_name="pet_store",
        table_name=table,
        domain_id="pet-store",
    )


@pytest.fixture
def org(monkeypatch):
    """An org whose model holds pets and owners, read through the source pet-store-sqlite."""
    ctx = types.SimpleNamespace(tables={"pets": _meta(1, "pets"), "owners": _meta(2, "owners")})
    runtime = types.SimpleNamespace(contexts={"analyst": ctx})
    registry = types.SimpleNamespace(get=lambda org_id: runtime if org_id == _ORG else None)
    monkeypatch.setattr("provisa.api.app.state.org_registry", registry, raising=False)


def _binder_error() -> str:
    """DuckDB's own answer to a misjoined statement over the physical addresses."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pet_store_sqlite")
    con.execute("CREATE SCHEMA pet_store_sqlite.pet_store")
    con.execute("CREATE TABLE pet_store_sqlite.pet_store.pets (id INTEGER, owner_id INTEGER)")
    con.execute("CREATE TABLE pet_store_sqlite.pet_store.owners (id INTEGER)")
    with pytest.raises(duckdb.BinderException) as refused:
        con.execute(
            'SELECT 1 FROM "pet_store_sqlite"."pet_store"."owners" AS "owners" JOIN '
            '"pet_store_sqlite"."pet_store"."pets" AS "pets" ON "pets"."nope" = "owners"."id"'
        )
    return str(refused.value)


def test_a_binder_error_names_its_tables_as_the_model_does(org):
    said = _binder_error()
    assert '"pet_store_sqlite"."pet_store"."pets"' in said  # the engine quotes the physical SQL
    told = error_redaction.model_named(said, _ORG)
    # Its excerpt of the physical SQL is dropped: truncated, it can begin inside a physical
    # address, and its caret counts characters the caller never wrote.
    assert "pet_store_sqlite" not in told and "LINE 1" not in told and "^" not in told
    assert told.startswith('Binder Error: Table "pets" does not have a column named "nope"')


def test_an_unquoted_physical_address_is_named_too(org):
    told = error_redaction.model_named(
        "Catalog Error: Table with name pet_store_sqlite.pet_store.pets does not exist!", _ORG
    )
    assert told == 'Catalog Error: Table with name "pet_store"."pets" does not exist!'


async def test_the_error_body_a_client_is_answered_names_the_model(org):
    said = _binder_error()

    async def app(scope, receive, send):
        scope["state"]["active_org_id"] = _ORG  # the auth middleware binds the org here
        body = json.dumps({"detail": said}).encode()
        await send({"type": "http.response.start", "status": 400, "headers": []})
        await send({"type": "http.response.body", "body": body})

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    await error_redaction.ErrorRedactionMiddleware(app)({"type": "http", "state": {}}, None, send)
    detail = json.loads(sent[-1]["body"])["detail"]
    assert "pet_store_sqlite" not in detail and "LINE 1" not in detail
    assert detail.startswith('Binder Error: Table "pets" does not have a column named "nope"')


async def test_a_physical_address_in_a_json_body_is_named_as_the_model_does(org):
    async def app(scope, receive, send):
        scope["state"]["active_org_id"] = _ORG
        said = 'Catalog Error: Table "pet_store_sqlite"."pet_store"."pets" is gone'
        await send({"type": "http.response.start", "status": 400, "headers": []})
        await send({"type": "http.response.body", "body": json.dumps({"detail": said}).encode()})

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    await error_redaction.ErrorRedactionMiddleware(app)({"type": "http", "state": {}}, None, send)
    assert json.loads(sent[-1]["body"])["detail"] == (
        'Catalog Error: Table "pet_store"."pets" is gone'
    )

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

from provisa.api import engine_error_names, error_redaction
from provisa.compiler.sql_types import TableMeta
from provisa.core.request_context import reset_current_org, set_current_org

_ORG = "acme"
#: The binder error's own sentence, which names the table by the alias the statement gave it.
_TOLD = 'Binder Error: Table "pets" does not have a column named "nope"'
#: An engine error naming a table by its physical address in its own sentence, and as told.
_CATALOG = "Catalog Error: Table with name pet_store_sqlite.pet_store.pets does not exist!"
_CATALOG_TOLD = 'Catalog Error: Table with name "pet_store"."pets" does not exist!'


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
    runtime = types.SimpleNamespace(view_context=ctx)
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
    told = engine_error_names.model_named(said, _ORG)
    # Its excerpt of the physical SQL is dropped: truncated, it can begin inside a physical
    # address, and its caret counts characters the caller never wrote.
    assert "pet_store_sqlite" not in told and "LINE 1" not in told and "^" not in told
    assert told.startswith(_TOLD)


def test_an_unquoted_physical_address_is_named_too(org):
    told = engine_error_names.model_named(
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


async def test_a_json_body_stays_json_when_a_bare_address_is_named(org):
    """A bare physical address is named as the model quotes it; written into a JSON body as text,
    those quotes ended the string, so the body a client was answered was no longer JSON."""

    async def app(scope, receive, send):
        scope["state"]["active_org_id"] = _ORG
        body = {
            "detail": "the replica of pet_store_sqlite.pet_store.pets is still being built",
            "code": "data.replica_building",
            "params": {"replica": "pet_store_sqlite.pet_store.pets", "running_s": 2},
        }
        headers = [(b"content-type", b"application/json")]
        await send({"type": "http.response.start", "status": 503, "headers": headers})
        await send({"type": "http.response.body", "body": json.dumps(body).encode()})

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    await error_redaction.ErrorRedactionMiddleware(app)({"type": "http", "state": {}}, None, send)
    assert json.loads(sent[-1]["body"]) == {
        "detail": 'the replica of "pet_store"."pets" is still being built',
        "code": "data.replica_building",
        "params": {"replica": '"pet_store"."pets"', "running_s": 2},
    }
    length = dict(sent[0]["headers"])[b"content-length"]
    assert int(length) == len(sent[-1]["body"])


def _told(text: str) -> None:
    """``text`` is the binder error as a client is told it: no physical address, no excerpt."""
    assert "pet_store_sqlite" not in text and "LINE 1" not in text, text
    assert _TOLD in text, text


# --- each transport ------------------------------------------------------------------------------


async def test_graphql_answers_an_engine_failure_in_the_models_names(org):
    """A GraphQL statement the engine refused is answered 500 with the engine's text (data
    endpoint), through the error middleware."""
    from fastapi import FastAPI, HTTPException
    from httpx import ASGITransport, AsyncClient

    said = _binder_error()
    app = FastAPI()

    @app.post("/data/graphql")
    async def graphql() -> None:
        raise HTTPException(status_code=500, detail=said)

    @app.middleware("http")
    async def _org(request, call_next):
        request.state.active_org_id = _ORG  # as the auth middleware binds it
        return await call_next(request)

    transport = ASGITransport(app=error_redaction.ErrorRedactionMiddleware(app))
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        answered = await client.post("/data/graphql")
    assert answered.status_code == 500
    _told(answered.json()["detail"])


def test_pgwire_answers_an_engine_failure_in_the_models_names(org):
    from provisa.pgwire.server import _Said

    _told(str(_Said(_binder_error(), _ORG)))


def test_flight_answers_an_engine_failure_in_the_models_names(org):
    import pyarrow.flight as flight

    from provisa.api.flight.redaction import _redacted

    said = f"{_binder_error()}\n{_CATALOG}"

    def do_get(*args):
        # As the handler: it binds the request's org and resets it before the error leaves.
        token = set_current_org(_ORG)
        try:
            raise RuntimeError(said)
        finally:
            reset_current_org(token)

    with pytest.raises(flight.FlightServerError) as answered:  # pyright: ignore[reportPrivateImportUsage]
        _redacted(do_get)()
    _told(str(answered.value))
    # Named by the org the RPC bound, though its binding ended before the error left.
    assert _CATALOG_TOLD in str(answered.value)


def test_mcp_answers_an_engine_failure_in_the_models_names(org):
    from provisa.api.mcp.server import _redacted_error

    answered = _redacted_error(RuntimeError(_binder_error()))
    assert answered is not None
    _told(str(answered))


def test_grpc_answers_an_engine_failure_in_the_models_names(org):
    from provisa.grpc.redaction import _unary

    said = f"{_binder_error()}\n{_CATALOG}"
    aborted: list[str] = []

    class _Context:
        def abort(self, code, details):
            aborted.append(details)
            raise RuntimeError("abort ends the RPC by raising")

    def handler(request, ctx):
        # As the handler: it binds the request's org and resets it before the error leaves.
        token = set_current_org(_ORG)
        try:
            raise RuntimeError(said)
        finally:
            reset_current_org(token)

    with pytest.raises(RuntimeError):
        _unary(handler)(None, _Context())
    _told(aborted[0])
    # Named by the org the RPC bound, though its binding ended before the error left.
    assert _CATALOG_TOLD in aborted[0]


def test_an_error_with_no_org_has_nothing_rewritten_and_its_excerpt_dropped(org):
    """No other org's names are ever written into an error: one raised before a request bound an
    org has its addresses left as they are, and the engine's excerpt dropped all the same."""
    said = _binder_error()
    told = engine_error_names.model_named(said, None)
    assert "LINE 1" not in told and told.startswith(_TOLD)
    plain = 'Table "pet_store_sqlite"."pet_store"."pets" is gone'
    assert engine_error_names.model_named(plain, None) == plain
    assert engine_error_names.model_named(plain, _ORG) == 'Table "pet_store"."pets" is gone'


#: The schema of a demo's template org, as the engine quotes it to a statement run in a sandbox.
_TEMPLATE = "org_sbxtplprofiling"


def _catalog_error(table: str) -> str:
    """DuckDB's own answer to a table missing from the wrong environment's search path."""
    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA {_TEMPLATE}")
    con.execute(f"CREATE TABLE {_TEMPLATE}.customers_266_profile_runs (id INTEGER)")
    with pytest.raises(duckdb.Error) as raised:
        con.execute(f"SELECT * FROM {table}")
    return str(raised.value)


@pytest.mark.parametrize("org_id", [None, _ORG, "sbx0a1b2c3d4e5f"])
def test_a_template_s_schema_never_reaches_a_client_and_the_table_does(org_id):
    told = engine_error_names.client_text(_catalog_error("customers_267_profile_runs"), org_id)
    assert _TEMPLATE not in told
    assert "customers_267_profile_runs" in told
    assert 'Did you mean "customers_266_profile_runs"?' in told


def test_a_missing_internal_schema_is_not_named_either():
    told = engine_error_names.client_text(_catalog_error("org_sbxabc.runs"), None)
    assert "org_sbxabc" not in told
    assert 'Table with name "runs" does not exist because schema' in told


@pytest.mark.parametrize(
    "said, told",
    [
        (f'"{_TEMPLATE}"."runs" is gone', '"runs" is gone'),
        (f'\\"{_TEMPLATE}\\".\\"runs\\" is gone', '\\"runs\\" is gone'),
        (f'Did you mean \\"{_TEMPLATE}.runs\\"?', 'Did you mean \\"runs\\"?'),
        (f"Table 'cat.{_TEMPLATE}.runs' does not exist", "Table 'cat.runs' does not exist"),
        (f"{_TEMPLATE}_replicas.runs is gone", "runs is gone"),
        (f"{_TEMPLATE}_env_dev_mv_cache.runs is gone", "runs is gone"),
        (f"{_TEMPLATE}__pg.public.runs is gone", "public.runs is gone"),
        ("org_sbx0a1b2c3d4e5f.runs is gone", "runs is gone"),
        (f"schema {_TEMPLATE} does not exist", "schema [internal schema] does not exist"),
    ],
)
def test_every_spelling_of_an_internal_schema_is_taken_out(said, told):
    assert engine_error_names.client_text(said) == told


def test_the_acting_org_s_schema_is_taken_out_in_every_environment():
    said = "org_acme.runs and org_acme_env_dev.runs and org_acme_rg_eu_replicas.runs"
    assert engine_error_names.client_text(said, _ORG) == "runs and runs and runs"


@pytest.mark.parametrize(
    "said",
    ['column "org_id" does not exist', "org_unit.name not found", "org_acmeeu.runs is gone"],
)
def test_a_model_s_own_name_beginning_org_stays(said):
    assert engine_error_names.client_text(said, _ORG) == said

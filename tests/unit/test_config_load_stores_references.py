# Copyright (c) 2026 Kenneth Stott
# Canary: 59a5e6a9-c66b-44a7-8d06-4d800f11643c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A config load stores a credential as the reference the config wrote, never its value.

A config names a credential by reference (``${env:NAME}``, ``${secret:NAME}``), and the value is
resolved where it is used. After a load, no column of the control plane holds the value: the
control plane is copied between environments, exported, and read by the admin API."""

from __future__ import annotations

import contextlib

import pytest
from sqlalchemy import select

from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.schema_org import metadata

SECRETS = {
    "IT_PG_PASSWORD": "pg-pw-9f31c7",
    "IT_PG_HOST": "db-host-51ab02.internal",
    "IT_API_TOKEN": "api-tok-77e0d4",
    "IT_GRAPH_TOKEN": "graph-tok-c41b90",
    "IT_SPARQL_TOKEN": "sparql-tok-0d2e6a",
    "IT_HINT_KEY": "hint-key-3ab915",
    "IT_HOOK_TOKEN": "hook-tok-5c2f08",
    "IT_REGISTRY_URL": "https://u:reg-pw-e19a44@registry.internal",
    "IT_ENGINE_PW": "engine-pw-8d03f1",
    "IT_STORE_PW": "store-pw-6b2e97",
    "IT_EXCHANGE_SECRET": "exchange-secret-41fa0c",
    "IT_SMTP_PW": "smtp-pw-a9c357",
    "IT_OIDC_SECRET": "oidc-secret-2e7b88",
    "IT_EXPORT_KEY": "export-key-90d6e1",
}

_SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"id": {"type": "integer"}},
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}


@pytest.fixture
async def db(monkeypatch, tmp_path) -> Database:
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="refs-test")
    await _init_schema_portable(plane)
    return plane


def _config(spec_path: str):
    column = {"name": "id", "data_type": "integer", "visible_to": ["seller"]}
    return parse_config_dict(
        {
            # Sections the load does not store: carried so that one it starts storing is
            # covered by this test the day it does.
            "federation_engine_url": "postgresql://u:${env:IT_ENGINE_PW}@engine.internal/e",
            "materialize_store_url": "postgresql://u:${env:IT_STORE_PW}@store.internal/s",
            "exchange_spool_s3_secret_key": "${env:IT_EXCHANGE_SECRET}",
            "mail": {"smtp": {"host": "smtp.test", "password": "${env:IT_SMTP_PW}"}},
            "auth": {"provider": "oidc", "oidc": {"client_id": "c",
                     "client_secret": "${env:IT_OIDC_SECRET}"}},
            "metadata_export": {"api_key": "${env:IT_EXPORT_KEY}"},
            "domains": [{"id": "sales"}],
            "roles": [{"id": "seller", "capabilities": [], "domain_access": ["sales"]}],
            "sources": [
                {
                    "id": "pg",
                    "type": "postgresql",
                    "host": "${env:IT_PG_HOST}",
                    "port": 5432,
                    "database": "d",
                    "username": "u",
                    "password": "${env:IT_PG_PASSWORD}",
                    "federation_hints": {"api_key": "${env:IT_HINT_KEY}"},
                    "cdc": {
                        "bootstrap_servers": "kafka:9092",
                        "topic_prefix": "pg",
                        "schema_registry_url": "${env:IT_REGISTRY_URL}",
                    },
                },
                {
                    "id": "petstore",
                    "type": "openapi",
                    "path": spec_path,
                    "base_url": "https://pets.test/v1?token=${env:IT_API_TOKEN}",
                },
                {
                    "id": "graph",
                    "type": "neo4j",
                    "host": "neo.test",
                    "port": 7474,
                    "database": "neo4j",
                    "base_url": "https://u:${env:IT_GRAPH_TOKEN}@neo.test:7473",
                },
                {
                    "id": "kg",
                    "type": "sparql",
                    "host": "https://kg.test/sparql?key=${env:IT_SPARQL_TOKEN}",
                },
            ],
            "tables": [
                {"source_id": "pg", "domain_id": "sales", "schema": "public", "table": "orders",
                 "columns": [column]},
                {"source_id": "petstore", "domain_id": "sales", "schema": "openapi",
                 "table": "listPets", "columns": [column]},
                {"source_id": "graph", "domain_id": "sales", "schema": "neo4j", "table": "people",
                 "query_template": "MATCH (p:Person) RETURN p.id AS id", "columns": [column]},
                {"source_id": "kg", "domain_id": "sales", "schema": "sparql", "table": "things",
                 "query_template": "SELECT ?id WHERE { ?id a ?t }", "columns": [column]},
            ],
            "webhooks": [
                {"name": "notify", "url": "https://hooks.test/in?token=${env:IT_HOOK_TOKEN}",
                 "domain_id": "sales"}
            ],
        }
    )  # fmt: skip


async def _everything_stored(db: Database) -> dict[str, str]:
    """Every column of every control-plane row, as text, keyed by ``table.column``."""
    held: dict[str, str] = {}
    async with db.acquire() as conn:
        for table in metadata.sorted_tables:
            rows = (await conn.execute_core(select(table))).fetchall()
            for row in rows:
                for column, value in row._mapping.items():
                    if value is not None:
                        key = f"{table.name}.{column}"
                        held[key] = (
                            held.get(key, "")
                            + "\n"
                            + (
                                value.decode("utf-8", "replace")
                                if isinstance(value, bytes)
                                else str(value)
                            )
                        )
    return held


async def assert_no_value_is_stored(db: Database, tmp_path, schema: str = "main") -> None:
    """Load the config into ``db`` and fail naming every column that holds a value, and every
    file of the environment's repository projection that does (a secret committed to git cannot
    be taken back by a reload). ``schema`` is the org schema the projection reads (SQLite's
    database is ``main``). Shared by the PostgreSQL plane
    (tests/integration/test_config_load_stores_references_pg.py)."""
    import json

    spec = tmp_path / "petstore.json"
    spec.write_text(json.dumps(_SPEC))
    async with db.acquire() as conn:
        await apply_config(_config(str(spec)), conn)
    stored = await _everything_stored(db)
    assert stored["sources.id"].split() == ["pg", "petstore", "graph", "kg"]  # the load happened
    leaked = sorted(
        f"{where} holds the value of {name}"
        for where, text in stored.items()
        for name, value in SECRETS.items()
        if value in text
    )
    assert leaked == [], "\n".join(leaked)
    # ...and the reference is what is stored, for its use point to resolve.
    assert "${env:IT_PG_PASSWORD}" in stored["sources.password_ref"]
    assert "${env:IT_API_TOKEN}" in stored["api_sources.base_url"]

    from provisa.core.env_files import dump
    from provisa.core.env_project import project

    async with db.acquire() as conn:
        files = dump(await project(conn, schema))
    assert any("notify" in text for text in files.values())  # the webhook is projected
    in_git = sorted(
        f"{path} holds the value of {name}"
        for path, text in files.items()
        for name, value in SECRETS.items()
        if value in text
    )
    assert in_git == [], "\n".join(in_git)


async def test_after_a_config_load_no_control_plane_column_holds_a_credentials_value(db, tmp_path):
    await assert_no_value_is_stored(db, tmp_path)


def test_the_running_process_still_has_the_resolved_values(monkeypatch, tmp_path):
    """What is stored is the reference; what the process uses is its value, as before."""
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)
    config = _config(str(tmp_path / "unused.json"))
    pg = config.sources[0]
    assert (pg.password, pg.host) == (SECRETS["IT_PG_PASSWORD"], SECRETS["IT_PG_HOST"])
    written = config.written.sources[0]
    assert (written.password, written.host) == ("${env:IT_PG_PASSWORD}", "${env:IT_PG_HOST}")
    assert written.port == 5432 and written.id == "pg"


def test_a_reference_in_a_field_that_is_not_text_is_resolved_in_both_forms(monkeypatch):
    """A port has no text form to store: ``${env:PORT}`` there is its number, as written too."""
    from provisa.core.config_loader import parse_config_dict

    monkeypatch.setenv("IT_PORT", "6543")
    config = parse_config_dict(
        {
            "domains": [],
            "roles": [],
            "tables": [],
            "sources": [
                {"id": "pg", "type": "postgresql", "host": "h", "port": "${env:IT_PORT}",
                 "database": "d"}
            ],
        }
    )  # fmt: skip
    assert config.sources[0].port == 6543 and config.written.sources[0].port == 6543


@pytest.mark.parametrize(
    ("written", "sent_to"),
    [
        ("https://pets.test/${env:IT_API_TOKEN}/v1", "https://pets.test/api-tok-77e0d4/v1/pets"),
        ("https://pets.test/v1", "https://pets.test/v1/pets"),
    ],
)  # fmt: skip
def test_an_api_call_resolves_its_sources_address_at_the_call(monkeypatch, written, sent_to):
    from provisa.api_source.caller import prepare_call
    from provisa.api_source.models import ApiEndpoint

    monkeypatch.setenv("IT_API_TOKEN", SECRETS["IT_API_TOKEN"])
    endpoint = ApiEndpoint(source_id="petstore", path="/pets", table_name="listPets", columns=[])
    assert prepare_call(endpoint, {}, written).url == sent_to


def test_a_sparql_address_that_is_a_reference_is_stored_whole_and_called_resolved(monkeypatch):
    from provisa.api_source.caller import prepare_call
    from provisa.sparql.source import SparqlSourceConfig, build_api_source, build_endpoint

    monkeypatch.setenv("IT_SPARQL_URL", "https://kg.test/ds/sparql")
    cfg = SparqlSourceConfig(source_id="kg", endpoint_url="${env:IT_SPARQL_URL}")
    source, endpoint = build_api_source(cfg), build_endpoint(cfg, "things", "SELECT ?s", [])
    assert (source.base_url, endpoint.path) == ("${env:IT_SPARQL_URL}", "")
    assert prepare_call(endpoint, {}, source.base_url).url == "https://kg.test/ds/sparql"
    plain = SparqlSourceConfig(source_id="kg", endpoint_url="https://kg.test/ds/sparql")
    assert build_api_source(plain).base_url == "https://kg.test"
    assert build_endpoint(plain, "things", "SELECT ?s", []).path == "/ds/sparql"

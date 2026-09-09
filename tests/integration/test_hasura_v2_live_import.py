# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7a9c2d-8e1b-4d6f-a4c5-0b9e2d7f1a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hasura v2 import validated live (REQ-1680, REQ-1681, REQ-1682).

Hasura's own metadatautil sample (cli/internal/metadatautil/testdata/json/t1/metadata.json, a
snake_case Chinook subset) is imported through the admin import surface against a seeded Postgres
in the test stack, then queried through the data GraphQL endpoint as the sample's ``user`` role:
the select permission's session-variable filter holds row by row, its column list holds field by
field, the FK-declared relationships join, and the remote schema's landed table answers from the
public countries API.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hasura's own sample (graphql-engine cli/internal/metadatautil/testdata/json/t1/metadata.json),
# copied into the fixtures so the test carries its input.
SAMPLE = Path(__file__).resolve().parents[1] / "fixtures" / "hasura_v2_t1_metadata.json"
SEED = Path(__file__).resolve().parents[1] / "fixtures" / "hasura_v2_t1_seed.sql"
DB = "hasura_t1"
COUNTRIES_URL = "https://countries.trevorblades.com/graphql"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def seeded_pg(docker_postgres):
    asyncpg = pytest.importorskip("asyncpg")
    host, port = docker_postgres["host"], docker_postgres["port"]
    user = os.environ.get("PG_USER", "provisa")
    password = os.environ.get("PG_PASSWORD", "provisa")
    admin = await asyncpg.connect(
        host=host, port=port, user=user, password=password, database="provisa"
    )
    try:
        exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", DB)
        if not exists:
            await admin.execute(f'CREATE DATABASE "{DB}"')
    finally:
        await admin.close()
    conn = await asyncpg.connect(host=host, port=port, user=user, password=password, database=DB)
    try:
        await conn.execute(SEED.read_text())
    finally:
        await conn.close()
    return {"host": host, "port": port, "database": DB, "username": user, "password": password}


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(seeded_pg):
    os.environ.setdefault("PG_PASSWORD", "provisa")
    os.environ["RS_ENV"] = COUNTRIES_URL
    # The native engine lands the remote schema's rows into its materialization store; point the
    # store at the test stack's Postgres (tests/conftest.py computes the default before the isolated
    # stack's port is known).
    os.environ["PROVISA_MATERIALIZE_URL"] = (
        f"postgresql://{seeded_pg['username']}:{seeded_pg['password']}@{seeded_pg['host']}:"
        f"{seeded_pg['port']}/provisa"
    )
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", timeout=120) as c:
            yield c


async def _data(client, query, headers=None):
    resp = await client.post("/data/graphql", json={"query": query}, headers=headers or {})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _field(client, table: str, headers) -> str:
    body = await _data(client, "{ __schema { queryType { fields { name } } } }", headers)
    names = [f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]]
    wanted = table.replace("_", "").lower()
    matches = [n for n in names if n.split("__", 1)[-1].lower() == wanted]
    assert matches, f"{table} not exposed: {names}"
    return matches[0]


USER = {"x-provisa-role": "user", "x-provisa-session-user-id": "2"}
USER_NO_SESSION = {"x-provisa-role": "user"}


class TestLiveImport:
    async def test_import_applies(self, client, seeded_pg):
        if not SAMPLE.exists():
            pytest.fail(f"Hasura v2 sample not found at {SAMPLE}; set HASURA_V2_SAMPLE")
        preview = await client.post(
            "/admin/import/hasura/preview",
            json={
                "filename": "metadata.json",
                "content_b64": base64.b64encode(SAMPLE.read_bytes()).decode(),
                "flavor": "hasura_v2",
                "domain_map": {"public": "music", "countries": "geo"},
                "source_overrides": {"default": seeded_pg},
            },
        )
        assert preview.status_code == 200, preview.text
        body = preview.json()
        assert body["summary"]["tables"] == 10  # 7 Chinook + 3 landed remote root fields
        assert body["summary"]["relationships"] == 12
        assert not any(w["category"] == "relationships" for w in body["warnings"])
        # REQ-1683: the override reaches the seeded source, so every column is typed at preview.
        untyped = [w for w in body["warnings"] if w["category"] in ("sources", "tables")]
        assert not untyped, untyped
        assert "data_type: integer" in body["config_yaml"], body["config_yaml"][:2000]
        applied = await client.post(
            "/admin/import/hasura/apply",
            json={"config_yaml": body["config_yaml"], "replace": False},
        )
        assert applied.status_code == 200, applied.text

    async def test_session_variable_filter_holds_row_by_row(self, client):
        field = await _field(client, "albums", USER)
        body = await _data(client, f"{{ {field} {{ title artistId }} }}", USER)
        assert not body.get("errors"), body
        rows = body["data"][field]
        assert rows == [{"title": "Balls to the Wall", "artistId": 2}]

    async def test_hidden_column_is_absent_from_the_role_schema(self, client):
        field = await _field(client, "albums", USER)
        resp = await client.post(
            "/data/graphql", json={"query": f"{{ {field} {{ id }} }}"}, headers=USER
        )
        assert resp.status_code == 400, resp.text  # id is not in the user's select columns
        assert "Cannot query field 'id'" in resp.text

    async def test_unbound_session_variable_denies_every_row(self, client):
        field = await _field(client, "albums", USER_NO_SESSION)
        body = await _data(client, f"{{ {field} {{ title }} }}", USER_NO_SESSION)
        assert not body.get("errors"), body
        assert body["data"][field] == []

    async def test_fk_declared_relationship_joins(self, client):
        admin = {"x-provisa-role": "org_admin"}
        field = await _field(client, "albums", admin)
        body = await _data(client, f"{{ {field} {{ id title artist {{ name }} }} }}", admin)
        assert not body.get("errors"), body
        by_id = {r["id"]: r for r in body["data"][field]}
        assert by_id[2]["artist"]["name"] == "Accept"
        assert by_id[1]["artist"]["name"] == "AC/DC"

    async def test_landed_remote_schema_answers_from_the_public_api(self, client):
        field = await _field(client, "countries", USER)
        body = await _data(client, f"{{ {field} {{ code name }} }}", USER)
        assert not body.get("errors"), body
        rows = body["data"][field]
        # The role's default row cap (100) bounds the page; the landed replica answered.
        assert len(rows) == 100
        assert {"code": "AD", "name": "Andorra"} in rows

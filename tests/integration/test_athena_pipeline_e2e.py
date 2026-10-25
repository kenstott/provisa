# Copyright (c) 2026 Kenneth Stott
# Canary: c4a25306-b241-4f1e-b881-17db535eb0e2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A live Amazon Athena source end to end (REQ-1981): created and its tables registered through the
admin GraphQL, read through the settled pipeline with a filter and a limit, written through SQL.

An Iceberg table takes INSERT, UPDATE and DELETE; a Hive table takes INSERT only, and an UPDATE of
it is refused before anything is sent, naming the table and the operation. Each run makes its own
Glue database and S3 prefix (see ``test_athena_source_e2e``) and removes them whether it passes or
fails. It reads ATHENA_REGION, ATHENA_S3_BUCKET and the ATHENA_AWS_* key pair when set.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.helpers import registered_id, release_mutation
from tests.integration.connector_source_harness import (
    _gql,
    create_domain,
    query_semantic,
    rebuild_schemas,
)
from tests.integration.test_athena_source_e2e import _DB, _hints, _key_pair
from tests.integration.test_athena_source_e2e import driver  # noqa: F401 -- the seeded database

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_athena,
    pytest.mark.requires_warehouse,
    pytest.mark.asyncio(loop_scope="session"),
]

_SOURCE_ID = "athena-pipeline"
_DOMAIN = "athena_e2e"
_VISIBLE_TO = '["org_admin"]'


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def client():
    """An in-process app on the DuckDB engine, which reads Athena DIRECT or from a replica."""
    import provisa.api.app as app_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    prior_env = os.environ.get("PROVISA_ENGINE")
    os.environ["PROVISA_ENGINE"] = "duckdb"
    from tests.conftest import as_deployment_org

    # The engine is the deployment org's, set as its boot sets it (REQ-1266).
    with as_deployment_org():
        prior_engine = app_mod.state.federation_engine
        app_mod.state.federation_engine = EngineRuntime(build_engine(), app_mod.state)
        app_mod.state.federation_engine.bind_terminal()
    try:
        app = app_mod.create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=600) as c:
                yield c
    finally:
        with as_deployment_org():
            app_mod.state.federation_engine = prior_engine
        if prior_env is None:
            os.environ.pop("PROVISA_ENGINE", None)
        else:
            os.environ["PROVISA_ENGINE"] = prior_env


async def _register(client: AsyncClient, table: str, columns: list[str]) -> None:
    cols = ", ".join(f'{{ name: "{c}", visibleTo: {_VISIBLE_TO} }}' for c in columns)
    registered = await _gql(
        client,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", schemaName: "{_DB}",
                tableName: "{table}", alias: "{table}", columns: [{cols}]
            }}) {{ success message }}
        }}
        """,
        "registerTable",
    )
    await _gql(client, release_mutation(registered_id(registered["message"])), "setTableDraft")


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def registered(client, driver):  # noqa: F811 -- the seeded database
    """The source, its Hive ``pets`` and Iceberg ``notes`` registered, released and in the schema."""
    del driver
    key, secret = _key_pair()
    await create_domain(client, _DOMAIN)
    await _gql(
        client,
        f"""
        mutation {{
            createSource(input: {{
                id: "{_SOURCE_ID}", type: "athena", host: "", port: 0, database: "{_DB}",
                username: {json.dumps(key)}, password: {json.dumps(secret)},
                federationHintsJson: {json.dumps(json.dumps(_hints()))}
            }}) {{ success message }}
        }}
        """,
        "createSource",
    )
    await _register(client, "pets", ["id", "name", "kind"])
    await _register(client, "notes", ["id", "body"])
    await rebuild_schemas(client)


async def _sql(client: AsyncClient, sql: str) -> Any:
    resp = await client.post("/data/sql", json={"sql": sql, "role": "org_admin"})
    return resp.status_code, resp.json()


async def test_a_table_is_read_through_the_pipeline_with_a_filter_and_a_limit(client, registered):
    del registered
    rows = await query_semantic(
        client,
        f'SELECT "name" FROM "{_DOMAIN}"."pets" WHERE "kind" = \'dog\' ORDER BY "id" LIMIT 5',
    )
    assert [r["name"] for r in rows] == ["rex", "ada"]


async def test_an_iceberg_table_is_written_through_sql(client, registered):
    del registered
    for statement in (
        f'INSERT INTO "{_DOMAIN}"."notes" ("id", "body") VALUES (10, \'a\'), (11, \'b\')',
        f'UPDATE "{_DOMAIN}"."notes" SET "body" = \'z\' WHERE "id" = 11',
        f'DELETE FROM "{_DOMAIN}"."notes" WHERE "id" = 10',
    ):
        status, body = await _sql(client, statement)
        assert status == 200, body
    rows = await query_semantic(client, f'SELECT "id", "body" FROM "{_DOMAIN}"."notes"')
    assert [(r["id"], r["body"]) for r in rows] == [(11, "z")]


async def test_a_hive_tables_update_is_refused_by_name_before_anything_is_sent(client, registered):
    del registered
    status, body = await _sql(
        client, f'UPDATE "{_DOMAIN}"."pets" SET "name" = \'x\' WHERE "id" = 1'
    )
    assert status != 200
    said = json.dumps(body)
    assert "pets" in said and "UPDATE" in said, said
    rows = await query_semantic(client, f'SELECT "name" FROM "{_DOMAIN}"."pets" WHERE "id" = 1')
    assert [r["name"] for r in rows] == ["rex"]


async def test_a_hive_tables_merge_is_refused_by_name_before_anything_is_sent(client, registered):
    del registered
    status, body = await _sql(
        client,
        f'MERGE INTO "{_DOMAIN}"."pets" AS t USING (SELECT 1 AS "id") AS s ON t."id" = s."id" '
        "WHEN MATCHED THEN UPDATE SET \"name\" = 'x'",
    )
    assert status != 200
    said = json.dumps(body)
    assert "pets" in said and "MERGE" in said.upper(), said
    rows = await query_semantic(client, f'SELECT "name" FROM "{_DOMAIN}"."pets" WHERE "id" = 1')
    assert [r["name"] for r in rows] == ["rex"]


async def test_a_row_is_inserted_into_a_hive_table(client, registered):
    del registered
    status, body = await _sql(
        client,
        f'INSERT INTO "{_DOMAIN}"."pets" ("id", "name", "kind") VALUES (90, \'kit\', \'cat\')',
    )
    assert status == 200, body
    rows = await query_semantic(client, f'SELECT "name" FROM "{_DOMAIN}"."pets" WHERE "id" = 90')
    assert [r["name"] for r in rows] == ["kit"]

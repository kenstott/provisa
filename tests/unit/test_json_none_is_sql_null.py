# Copyright (c) 2026 Kenneth Stott
# Canary: baa25be6-760f-46e3-831e-f8fcca7e3da7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A JSON column's None is stored as SQL NULL, not the JSON literal null.

SQLAlchemy's JSON type encodes None as the JSON text ``null`` unless the column says
``none_as_null=True``. PostgreSQL's jsonb ``null`` reads back as Python None, so nothing noticed
there; SQLite returns the text ``'null'``, a truthy string. An OpenAPI table registered with no
paging therefore left api_endpoints.pagination = 'null', and the next schema build crashed reading
it (api_source.loader: ``PaginationConfig(**None)``) -- the Playwright core lane on a SQLite control
plane never started (ui-e2e-core run 37323808040). Every SQLite-backed install was exposed."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import JSON, insert, text

from provisa.core.models import Column, Table
from provisa.core.schema_org import domains, metadata, sources

BASE = "https://pets.test"
SPEC = {
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
async def control_plane(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    await init_schema(db, "", org_id="default")
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="petstore", type="openapi"))
    try:
        yield db
    finally:
        engine.dispose()


async def test_an_openapi_table_with_no_paging_is_stored_unpaged_and_loads(control_plane):
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import register_openapi_source
    from provisa.core.repositories import table as table_repo

    table = Table(
        source_id="petstore",
        domain_id="d",
        schema_name="openapi",
        table_name="listPets",
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
    )
    state = SimpleNamespace(
        openapi_specs={"petstore": {"spec": SPEC, "base_url": BASE, "auth_config": None}},
        api_endpoints={},
        api_sources={},
    )
    async with control_plane.acquire() as conn:
        await register_openapi_source(conn, "petstore", BASE)
        await table_repo.upsert(conn, table)
        assert await persist_openapi_endpoint(state, conn, table) is None

        stored = await conn.fetch(
            "SELECT pagination IS NULL AS unpaged, default_params IS NULL AS no_defaults "
            "FROM api_endpoints WHERE table_name = 'listPets'"
        )
        assert [tuple(r) for r in stored] == [(1, 1)]
        table_row = await conn.fetch(
            "SELECT pagination IS NULL FROM registered_tables WHERE table_name = 'listPets'"
        )
        assert [tuple(r) for r in table_row] == [(1,)]

        endpoints, _ = await load_api_sources(conn, {})
    assert endpoints["listPets"].pagination is None


def test_every_nullable_json_column_stores_none_as_sql_null():
    """Any JSON column a writer may pass None to: its None must be SQL NULL on every backend."""
    from provisa.core.schema_admin import metadata as admin_metadata

    offenders = [
        f"{t.name}.{c.name}"
        for md in (metadata, admin_metadata)
        for t in md.sorted_tables
        for c in t.columns
        if isinstance(c.type, JSON) and c.nullable and not c.type.none_as_null
    ]
    assert offenders == []


async def test_the_none_round_trips_as_null_through_an_update(control_plane):
    """conn.upsert updates an existing row by a plain UPDATE; that path must keep NULL too."""
    from provisa.core.schema_org import api_endpoints, api_sources

    row = {
        "source_id": "s",
        "path": "/p",
        "method": "GET",
        "table_name": "t",
        "columns": [],
        "ttl": 0,
        "pagination": None,
    }
    async with control_plane.acquire() as conn:
        await conn.execute_core(insert(api_sources).values(id="s", type="openapi", base_url=BASE))
        for _ in range(2):  # the first call INSERTs, the second UPDATEs
            await conn.upsert(api_endpoints, row, index_elements=["table_name"])
            got = await conn.fetch(text("SELECT pagination IS NULL FROM api_endpoints").text)
            assert [tuple(r) for r in got] == [(1,)]

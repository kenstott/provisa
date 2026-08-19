# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318/REQ-319: the OpenAPI response cache must materialize on a schema-less control plane.

SQLite has no schema concept: ``CREATE SCHEMA "default"`` is a syntax error and every
``"default"."table"`` reference resolves to nothing. Unguarded, cache_openapi_table raised at
its first DDL statement, config_loader logged the failure and moved on, and the first runtime
hydration surfaced it as ``no such table: default.find_pets_by_status`` on an unrelated query.
"""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.openapi.pg_cache import _is_fresh, _relation, cache_openapi_table

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}


@pytest.fixture
async def sqlite_conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    db = Database(engine, "test")
    async with db.acquire() as conn:
        yield conn
    await engine.dispose()


async def test_relation_is_bare_name_on_schemaless_backend(sqlite_conn):
    assert _relation(sqlite_conn, "default", "find_pets_by_status") == '"find_pets_by_status"'


async def test_cache_table_is_created_and_readable_on_sqlite(sqlite_conn):
    # A path-param endpoint: created empty for engine introspection, no HTTP fetch.
    rows = await cache_openapi_table(
        "http://unused.invalid",
        "/pet/{petId}",
        {},
        sqlite_conn,
        "default",
        "find_pets_by_status",
        RESPONSE_SCHEMA,
    )
    assert rows == 0
    # The table now exists, so the freshness probe answers instead of raising.
    assert (
        await _is_fresh(sqlite_conn, "default", "find_pets_by_status", "deadbeef", ttl=300) is False
    )

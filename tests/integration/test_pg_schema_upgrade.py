# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org schema created before this week's columns upgrades in place on PostgreSQL.

V1 ships no migrations: ``schema.sql`` re-runs at every org runtime build, so every column added
after a schema was first created must reach it either through a hand-written
``ADD COLUMN IF NOT EXISTS`` block or through the metadata reconciliation ``init_schema`` now runs
after the script. cloud-dev found the gap the hard way — its API crash-looped on
``column "body_encoding" does not exist`` — so this test strips a fresh org schema back to that
older shape and proves both paths restore it, on a real PostgreSQL.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.db import add_missing_columns, init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_ORG = "upgrade_probe"
_SCHEMA = f"org_{_ORG}"
_OLD_CHECK = "CHECK (type IN ('openapi', 'graphql_api', 'grpc_api'))"


async def _columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{_SCHEMA}' AND table_name = '{table}'"
    )
    return {r["column_name"] for r in rows}


@pytest_asyncio.fixture
async def old_shape(tenant_db):
    """A fresh org schema, then regressed to the pre-REQ-1668 shape."""
    await init_schema(tenant_db, _SCHEMA_SQL, org_id=_ORG)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN body_encoding")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN query_template")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN response_normalizer")
        await conn.execute("ALTER TABLE sources DROP COLUMN password_ref")
        await conn.execute("ALTER TABLE api_sources DROP CONSTRAINT api_sources_type_check")
        await conn.execute(
            f"ALTER TABLE api_sources ADD CONSTRAINT api_sources_type_check {_OLD_CHECK}"
        )
        assert "body_encoding" not in await _columns(conn, "api_endpoints")
    yield tenant_db
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


async def test_init_schema_upgrades_an_older_org_schema_in_place(old_shape):
    await init_schema(old_shape, _SCHEMA_SQL, org_id=_ORG)
    async with old_shape.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        cols = await _columns(conn, "api_endpoints")
        assert {"body_encoding", "query_template", "response_normalizer"} <= cols
        assert "password_ref" in await _columns(conn, "sources")
        # the widened type check admits the query-API kinds an older schema refused
        await conn.execute(
            "INSERT INTO api_sources (id, type, base_url, auth) "
            "VALUES ('probe_neo4j', 'neo4j', 'http://neo4j:7474', '{}')"
        )
        await conn.execute("DELETE FROM api_sources WHERE id = 'probe_neo4j'")


async def test_metadata_reconciliation_restores_a_column_schema_sql_never_alters(old_shape):
    """The reconciliation is what covers a column nobody wrote an ALTER block for: run it alone,
    scoped to the org schema, and the metadata's column comes back typed like schema.sql's."""
    async with old_shape.engine.begin() as sa_conn:
        from provisa.core import schema_org

        await sa_conn.run_sync(add_missing_columns, schema_org.metadata.sorted_tables, _SCHEMA)
    async with old_shape.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name, data_type, column_default FROM information_schema.columns "
            f"WHERE table_schema = '{_SCHEMA}' AND table_name = 'sources' "
            "AND column_name = 'password_ref'"
        )
        assert len(rows) == 1
        assert rows[0]["data_type"] == "text"
        assert rows[0]["column_default"] == "''::text"
        assert {"body_encoding", "query_template"} <= await _columns(conn, "api_endpoints")

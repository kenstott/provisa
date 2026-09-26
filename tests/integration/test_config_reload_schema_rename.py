# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table's ``schema:`` changing across a config reload must not leave the OLD row
orphaned in ``registered_tables`` alongside a NEW one (REQ-013/REQ-016).

table.py's upsert conflict key is ``(source_id, schema_name, table_name)`` — schema_name is part
of the row's identity, not an updatable column (table.py:209). ``_purge_removed_tables`` used to
only track ``table_name`` per source, so a schema change inserted a second row under the new
identity and never cleaned up the first: two rows for the same logical table, and the compiler
resolved to whichever one it found first (confirmed live on perf-bench — a ClickHouse source's
``schema:`` corrected from "public" to "default" left every subsequent query still compiling
against "public", well after the YAML, control-plane, and in-memory schema cache were all
already correct)."""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.config_loader import load_config, parse_config_dict

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(tenant_db, _init_schema):
    async with tenant_db.acquire() as conn:
        await conn.execute(
            "TRUNCATE rls_rules, relationships, relationship_candidates, table_columns, "
            "registered_tables, naming_rules, roles, domains, sources CASCADE"
        )
    yield


def _config(schema: str) -> dict:
    return {
        "domains": [{"id": "bench"}],
        "sources": [
            {
                "id": "ch1",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        ],
        "tables": [
            {
                "source_id": "ch1",
                "domain_id": "bench",
                "schema": schema,
                "table": "order_events",
                "columns": [{"name": "id", "data_type": "integer", "visible_to": ["admin"]}],
            }
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


class TestSchemaRenameReload:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_schema_change_leaves_exactly_one_row(self, tenant_db, graphql_client):
        # graphql_client (unused directly) is a session-scoped fixture that populates
        # state.admin_db — _upsert_sources's bound_to_request_org() asserts it's set even for a
        # plain-password source with no ${secret:...} reference. tenant_db alone doesn't provide
        # it; test_domain_policy_integration.py's tests carry this same implicit dependency.
        async with tenant_db.acquire() as conn:
            await load_config(parse_config_dict(_config("public")), conn)
            rows = await conn.fetch(
                "SELECT schema_name FROM registered_tables WHERE table_name = 'order_events'"
            )
            assert [r["schema_name"] for r in rows] == ["public"]

            # Config's schema: field changes — a real reload, not a first-time registration.
            await load_config(parse_config_dict(_config("default")), conn)
            rows = await conn.fetch(
                "SELECT schema_name FROM registered_tables WHERE table_name = 'order_events'"
            )
            assert [r["schema_name"] for r in rows] == ["default"], (
                "the stale (source_id, 'public', 'order_events') row must be purged, not left "
                "alongside the new (source_id, 'default', 'order_events') row"
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_schema_change_survives_replace_mode_too(self, tenant_db, graphql_client):
        async with tenant_db.acquire() as conn:
            await load_config(parse_config_dict(_config("public")), conn, replace=True)
            await load_config(parse_config_dict(_config("default")), conn, replace=True)
            rows = await conn.fetch(
                "SELECT schema_name FROM registered_tables WHERE table_name = 'order_events'"
            )
            assert [r["schema_name"] for r in rows] == ["default"]

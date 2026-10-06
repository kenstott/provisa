# Copyright (c) 2026 Kenneth Stott
# Canary: 08423a39-6fed-4cf4-8690-d5cb998ab108
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table's ``schema:`` changing in a configuration applied again (REQ-013/REQ-016,
REQ-1919, amended 2026-10-06).

table.py's upsert conflict key is ``(source_id, schema_name, table_name)`` — schema_name is part
of the row's identity. A configuration is a one-time seed and an explicit apply adds and updates
and removes nothing, and with no record of which rows a configuration made, an apply cannot tell
a table its file moved from a same-named table registered in another schema. A table the file
names under a corrected schema would stand beside the registered one under one published name in
one domain, so the apply is refused, naming both (superseding the 2026-10-03 re-key of a
config-origin row)."""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.config_loader import PublishedNameClash, apply_config, parse_config_dict
from tests.helpers import ALL_DATA_CAPABILITIES

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
        "roles": [{"id": "admin", "capabilities": ALL_DATA_CAPABILITIES, "domain_access": ["*"]}],
    }


class TestSchemaRenameReload:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_a_schema_change_is_refused_naming_both_tables(self, tenant_db, graphql_client):
        # graphql_client (unused directly) is a session-scoped fixture that populates
        # state.admin_db — _upsert_sources's bound_to_request_org() asserts it's set even for a
        # plain-password source with no ${secret:...} reference. tenant_db alone doesn't provide
        # it; test_domain_policy_integration.py's tests carry this same implicit dependency.
        async with tenant_db.acquire() as conn:
            await apply_config(parse_config_dict(_config("public")), conn)
            rows = await conn.fetch(
                "SELECT schema_name FROM registered_tables WHERE table_name = 'order_events'"
            )
            assert [r["schema_name"] for r in rows] == ["public"]

            # The config's schema: field changes, and the config is applied again. An apply
            # removes nothing, so the table would stand under two schemas with one name in one
            # domain: refused, naming both, and nothing changes.
            with pytest.raises(PublishedNameClash) as err:
                await apply_config(parse_config_dict(_config("default")), conn)
            assert "ch1.default.order_events" in str(err.value)
            assert "ch1.public.order_events" in str(err.value)
            rows = await conn.fetch(
                "SELECT schema_name FROM registered_tables WHERE table_name = 'order_events'"
            )
            assert [r["schema_name"] for r in rows] == ["public"]

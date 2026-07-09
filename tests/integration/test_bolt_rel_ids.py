# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for REQ-806: rel_ids durable integer IDs for graph relationships.

Uses real Postgres via docker-compose. Exercises register_rel_ids end-to-end:
upsert → composite_id row in rel_ids, BIGSERIAL id assigned, idempotent re-upsert
returns the same id.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_REL_IDS_DDL = """
CREATE TABLE IF NOT EXISTS rel_ids (
    id           BIGSERIAL PRIMARY KEY,
    composite_id TEXT UNIQUE NOT NULL,
    rel_type     TEXT NOT NULL,
    properties   JSONB NOT NULL DEFAULT '{}'
)
"""


@pytest_asyncio.fixture(scope="module", autouse=True)
async def _ensure_rel_ids_table(tenant_db):
    """Create rel_ids if the test-DB snapshot pre-dates REQ-806."""
    async with tenant_db.acquire() as conn:
        await conn.execute(_REL_IDS_DDL)
    yield


@pytest.mark.asyncio(loop_scope="session")
class TestRelIdsTable:
    async def test_rel_ids_table_exists(self, tenant_db):
        async with tenant_db.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'rel_ids' AND table_schema = 'public'"
            )
        assert count == 1

    async def test_rel_ids_has_bigserial_id(self, tenant_db):
        async with tenant_db.acquire() as conn:
            col = await conn.fetchrow(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = 'rel_ids' AND column_name = 'id'"
            )
        assert col is not None
        assert col["data_type"] == "bigint"

    async def test_rel_ids_composite_id_unique(self, tenant_db):
        async with tenant_db.acquire() as conn:
            constraints = await conn.fetch(
                "SELECT constraint_type FROM information_schema.table_constraints "
                "WHERE table_name = 'rel_ids' AND constraint_type = 'UNIQUE'"
            )
        assert len(constraints) >= 1


@pytest.mark.asyncio(loop_scope="session")
class TestRegisterRelIds:
    async def test_upsert_assigns_integer_id(self, tenant_db):
        """Registering an edge inserts a row with a BIGSERIAL integer id."""
        composite_id = "TEST_REL:unit-integ-1"
        async with tenant_db.acquire() as conn:
            await conn.execute("DELETE FROM rel_ids WHERE composite_id = $1", composite_id)

        rows = [
            {
                "r": {
                    "identity": composite_id,
                    "type": "TEST_REL",
                    "startNode": {"id": "unit"},
                    "endNode": {"id": "integ"},
                    "properties": {"weight": 1.0},
                }
            }
        ]
        from provisa.cypher.assembler import register_rel_ids

        await register_rel_ids(rows, tenant_db)

        async with tenant_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, composite_id, rel_type FROM rel_ids WHERE composite_id = $1",
                composite_id,
            )
        assert row is not None
        assert isinstance(row["id"], int)
        assert row["composite_id"] == composite_id
        assert row["rel_type"] == "TEST_REL"

    async def test_upsert_idempotent_returns_same_id(self, tenant_db):
        """Re-registering the same edge returns the same integer id (ON CONFLICT DO UPDATE)."""
        composite_id = "TEST_REL:idem-potent-1"
        async with tenant_db.acquire() as conn:
            await conn.execute("DELETE FROM rel_ids WHERE composite_id = $1", composite_id)

        edge_row = {
            "r": {
                "identity": composite_id,
                "type": "TEST_REL",
                "startNode": {"id": "idem"},
                "endNode": {"id": "potent"},
                "properties": {},
            }
        }
        from provisa.cypher.assembler import register_rel_ids

        rows1 = [dict(edge_row)]
        await register_rel_ids(rows1, tenant_db)

        rows2 = [dict(edge_row)]
        await register_rel_ids(rows2, tenant_db)

        async with tenant_db.acquire() as conn:
            all_rows = await conn.fetch(
                "SELECT id FROM rel_ids WHERE composite_id = $1", composite_id
            )
        assert len(all_rows) == 1
        assert isinstance(all_rows[0]["id"], int)

    async def test_register_replaces_composite_identity_with_integer(self, tenant_db):
        """After register_rel_ids, the edge dict's 'identity' field is an integer."""
        composite_id = "TEST_REL:replace-check-1"
        async with tenant_db.acquire() as conn:
            await conn.execute("DELETE FROM rel_ids WHERE composite_id = $1", composite_id)

        rows = [
            {
                "r": {
                    "identity": composite_id,
                    "type": "TEST_REL",
                    "startNode": {"id": "replace"},
                    "endNode": {"id": "check"},
                    "properties": {},
                }
            }
        ]
        from provisa.cypher.assembler import register_rel_ids

        await register_rel_ids(rows, tenant_db)
        identity = rows[0]["r"]["identity"]
        assert isinstance(identity, int), f"expected int, got {type(identity)}: {identity!r}"

    async def test_noop_when_no_edges(self, tenant_db):
        """register_rel_ids is a no-op when rows contain no edge dicts."""
        rows: list[dict] = [{"scalar": 42}]
        from provisa.cypher.assembler import register_rel_ids

        await register_rel_ids(rows, tenant_db)
        assert rows == [{"scalar": 42}]

    async def test_noop_when_pg_pool_is_none(self):
        """register_rel_ids is a no-op when tenant_db is None."""
        rows = [
            {
                "r": {
                    "identity": "TEST_REL:null-pool-1",
                    "type": "TEST_REL",
                    "startNode": {},
                    "endNode": {},
                    "properties": {},
                }
            }
        ]
        from provisa.cypher.assembler import register_rel_ids

        await register_rel_ids(rows, None)
        assert rows[0]["r"]["identity"] == "TEST_REL:null-pool-1"

    async def test_properties_stored_in_rel_ids(self, tenant_db):
        """Properties JSONB is stored in rel_ids on upsert."""
        composite_id = "TEST_REL:props-stored-1"
        async with tenant_db.acquire() as conn:
            await conn.execute("DELETE FROM rel_ids WHERE composite_id = $1", composite_id)

        rows = [
            {
                "r": {
                    "identity": composite_id,
                    "type": "TEST_REL",
                    "startNode": {"id": "props"},
                    "endNode": {"id": "stored"},
                    "properties": {"key": "value", "num": 7},
                }
            }
        ]
        from provisa.cypher.assembler import register_rel_ids

        await register_rel_ids(rows, tenant_db)

        async with tenant_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT properties FROM rel_ids WHERE composite_id = $1",
                composite_id,
            )
        assert row is not None
        # The Database shim's jsonb codec decodes the column to a dict on read.
        props = row["properties"]
        assert props.get("key") == "value"
        assert props.get("num") == 7

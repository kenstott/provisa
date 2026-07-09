# Copyright (c) 2026 Kenneth Stott
# Canary: 3f2b8d16-9c47-4a5e-b0d1-7e6a2c4f9b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-686: RLS filter_expr column encryption, proven end-to-end against live PG.

The RLS predicate is injected as SQL at every governance read, so it is sensitive
metadata. This proves the repository boundary: ``rls_repo.upsert`` stores ciphertext
(the predicate never appears in the BYTEA column) and ``rls_repo.list_all`` /
``list_for_role`` decrypt it back to SQL on read. A wrong master key cannot recover
it. Uses a throwaway schema so it never touches real config.
"""

from __future__ import annotations

import base64
import os

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import RLSRule
from provisa.core.repositories import rls as rls_repo
from provisa.encryption import configure_encryption, reset_encryption

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req686_rls"

_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {_SCHEMA};
CREATE TABLE IF NOT EXISTS {_SCHEMA}.rls_rules (
    id SERIAL PRIMARY KEY,
    table_id INTEGER,
    domain_id TEXT,
    role_id TEXT NOT NULL,
    filter_expr BYTEA NOT NULL,
    tenant_id UUID,  -- SaaS multi-tenancy column (matches schema.sql / schema_org metadata)
    UNIQUE (domain_id, role_id)
);
"""

_PREDICATE = "region = 'us-east' AND owner = current_setting('provisa.role')"


@pytest.fixture(autouse=True)
def _enc():
    reset_encryption()
    os.environ["PROVISA_ENCRYPTION_KEY"] = base64.b64encode(bytes(range(1, 33))).decode()
    configure_encryption("local")
    yield
    reset_encryption()


@pytest.fixture
async def db():
    engine = create_async_engine(_PG_URL, pool_pre_ping=True)
    database = Database(engine, name="req686rls", search_path=_SCHEMA)
    try:
        async with database.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
            await conn.execute(_DDL)
    except Exception as exc:  # noqa: BLE001 — skip cleanly if the live store is absent
        await engine.dispose()
        pytest.skip(f"live Postgres not reachable at {_PG_URL}: {exc}")
    yield database
    async with database.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    await engine.dispose()


async def test_filter_stored_ciphertext_and_decrypts_on_read(db):
    rule = RLSRule(domain_id="sales", role_id="analyst", filter=_PREDICATE)
    async with db.acquire() as conn:
        await rls_repo.upsert(conn, rule)

        raw = await conn.fetchrow(
            f"SELECT filter_expr FROM {_SCHEMA}.rls_rules WHERE role_id='analyst'"
        )
        stored = bytes(raw["filter_expr"])
        assert b"region = 'us-east'" not in stored  # ciphertext at rest

        loaded = await rls_repo.list_all(conn)
        assert [r["filter_expr"] for r in loaded] == [_PREDICATE]

        for_role = await rls_repo.list_for_role(conn, "analyst")
        assert for_role[0]["filter_expr"] == _PREDICATE


async def test_wrong_master_key_cannot_read(db):
    async with db.acquire() as conn:
        await rls_repo.upsert(
            conn, RLSRule(domain_id="sales", role_id="analyst", filter=_PREDICATE)
        )

    # Rotate to a different master key — the stored ciphertext must no longer decrypt.
    os.environ["PROVISA_ENCRYPTION_KEY"] = base64.b64encode(bytes(range(33, 65))).decode()
    configure_encryption("local")
    async with db.acquire() as conn:
        with pytest.raises(Exception):
            await rls_repo.list_all(conn)

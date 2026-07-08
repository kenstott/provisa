# Copyright (c) 2026 Kenneth Stott
# Canary: 3d5f7a9b-1c2e-4840-8b6d-0f2a4c6e8b1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-689: audit query-text encryption, proven end-to-end against the live store.

Writes an audit row through the real Database abstraction into the running
Postgres, verifies the stored ``query_text_enc`` column is CIPHERTEXT (not the
plaintext query), and that an authorised read decrypts it back. A NullEncryption
row confirms the dev/test passthrough. Uses a throwaway schema so it never touches
real audit data.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.encryption import NullEncryption
from provisa.encryption.providers import LocalKeychain
from provisa.encryption.envelope import EnvelopeEncryption
from provisa.audit.query_log import log_query, read_query_text

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req689_audit"

_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {_SCHEMA};
CREATE TABLE IF NOT EXISTS {_SCHEMA}.query_audit_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID,
    user_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    query_text_enc BYTEA,
    table_ids JSONB NOT NULL DEFAULT '[]',
    source TEXT NOT NULL,
    status_code INT NOT NULL,
    duration_ms INT NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


@pytest.fixture
async def db():
    engine = create_async_engine(_PG_URL, pool_pre_ping=True)
    database = Database(engine, name="req689", search_path=_SCHEMA)
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


_QUERY = "SELECT ssn, salary FROM hr.employees WHERE dept = 'exec'"


async def _log(db, encryption):
    await log_query(
        db,
        tenant_id=None,
        user_id="alice",
        role_id="admin",
        query_text=_QUERY,
        table_ids=["1"],
        source="graphql",
        status_code=200,
        duration_ms=12,
        encryption=encryption,
    )
    row = await db.fetchrow(
        f"SELECT id, query_text_enc FROM {_SCHEMA}.query_audit_log ORDER BY id DESC LIMIT 1"
    )
    return row


async def test_query_text_stored_encrypted_and_decrypts(db):
    enc = EnvelopeEncryption(LocalKeychain(os.urandom(32)))
    row = await _log(db, enc)
    stored = bytes(row["query_text_enc"])
    # Stored column is ciphertext — the plaintext query must not appear.
    assert _QUERY.encode() not in stored
    assert stored != _QUERY.encode()
    # Authorised read decrypts back to the original query text.
    assert await read_query_text(db, row["id"], enc) == _QUERY


async def test_wrong_key_cannot_read(db):
    row = await _log(db, EnvelopeEncryption(LocalKeychain(os.urandom(32))))
    other = EnvelopeEncryption(LocalKeychain(os.urandom(32)))
    with pytest.raises(Exception):
        await read_query_text(db, row["id"], other)


async def test_null_encryption_passthrough(db):
    # Dev/test provider stores plaintext bytes and reads them straight back.
    row = await _log(db, NullEncryption())
    assert bytes(row["query_text_enc"]) == _QUERY.encode()
    assert await read_query_text(db, row["id"], NullEncryption()) == _QUERY

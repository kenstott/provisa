# Copyright (c) 2026 Kenneth Stott
# Canary: 8c0e2a4b-6d1f-4073-9a5c-2e4b6d8f0a1c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-686: API-source auth encryption, proven end-to-end against the live store.

Mirrors the register-write / loader-read paths at the DB level: the encrypted
`api_sources.auth` column holds ciphertext (the plaintext token never appears),
and an authorised read decrypts it back to the original auth config. Uses a
throwaway schema so it never touches real config.
"""

from __future__ import annotations

import json
import os

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.encryption.envelope import EnvelopeEncryption
from provisa.encryption.providers import LocalKeychain
from provisa.encryption.service import NullEncryption

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req686_apiauth"

_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {_SCHEMA};
CREATE TABLE IF NOT EXISTS {_SCHEMA}.api_sources (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    base_url TEXT NOT NULL,
    spec_url TEXT,
    auth BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_AUTH = {"type": "bearer", "token": "super-secret-token-abc123"}


@pytest.fixture
async def db():
    engine = create_async_engine(_PG_URL, pool_pre_ping=True)
    database = Database(engine, name="req686", search_path=_SCHEMA)
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


async def _write(db, enc, auth):
    blob = enc.encrypt(json.dumps(auth).encode("utf-8")) if auth else None
    await db.execute(
        f"INSERT INTO {_SCHEMA}.api_sources (id, type, base_url, auth) VALUES ($1,$2,$3,$4)",
        "s1",
        "openapi",
        "https://api.example.com",
        blob,
    )
    row = await db.fetchrow(f"SELECT auth FROM {_SCHEMA}.api_sources WHERE id='s1'")
    return row["auth"]


async def test_auth_stored_encrypted_and_decrypts(db):
    enc = EnvelopeEncryption(LocalKeychain(os.urandom(32)))
    stored = bytes(await _write(db, enc, _AUTH))
    assert b"super-secret-token-abc123" not in stored  # ciphertext, not the token
    assert json.loads(enc.decrypt(stored).decode("utf-8")) == _AUTH


async def test_wrong_key_cannot_read(db):
    stored = bytes(await _write(db, EnvelopeEncryption(LocalKeychain(os.urandom(32))), _AUTH))
    with pytest.raises(Exception):
        EnvelopeEncryption(LocalKeychain(os.urandom(32))).decrypt(stored)


async def test_null_auth_stored_as_null(db):
    assert await _write(db, NullEncryption(), None) is None

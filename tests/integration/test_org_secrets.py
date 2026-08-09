# Copyright (c) 2026 Kenneth Stott
# Canary: 4e6d1a92-8b7c-4f5e-9a3d-2c8f6b1e0d4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1395: the per-org encrypted secrets table, against a schema built by the real
``init_schema``.

Mirrors ``test_org_settings_overrides.py`` — the write side is the admin surface, the read
side is whatever LLM call site resolves the org's Anthropic key, and both name the same table
in the same org schema.
"""

from __future__ import annotations

import base64
import os

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.org_secrets import read_org_api_keys, read_org_secret, write_org_secret
from provisa.encryption import configure_encryption, reset_encryption

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _enc():
    # A real provider — the default NullEncryption passthrough would let the
    # ciphertext-format assertion below pass even if encryption were silently dropped.
    reset_encryption()
    os.environ["PROVISA_ENCRYPTION_KEY"] = base64.b64encode(bytes(range(1, 33))).decode()
    configure_encryption("local")
    yield
    reset_encryption()

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1395"
_SCHEMA = f"org_{_ORG_ID}"

_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)


@pytest.fixture
async def tenant_db():
    engine = create_engine_from_url(_ASYNC_URL)
    db = Database(engine, name="tenant", search_path=_SCHEMA)
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        await init_schema(db, fh.read(), org_id=_ORG_ID)
    yield db
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    await engine.dispose()


async def test_a_provisioned_org_has_no_secret_by_default(tenant_db):
    # A new org must be able to read secrets from its first request rather than 500ing on an
    # UndefinedTable the first time an NL query runs.
    assert await read_org_secret(tenant_db, "anthropic_api_key") is None


async def test_a_secret_round_trips_and_is_encrypted_at_rest(tenant_db):
    await write_org_secret(
        tenant_db, "anthropic_api_key", "sk-ant-example-12345", updated_by="alice"
    )

    assert await read_org_secret(tenant_db, "anthropic_api_key") == "sk-ant-example-12345"

    # The raw column must never hold the plaintext value.
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT value_enc FROM org_secrets WHERE key = 'anthropic_api_key'")
        )
        row = result.fetchall()[0]
        assert b"sk-ant-example-12345" not in bytes(row[0])


async def test_a_second_write_replaces_and_reattributes(tenant_db):
    await write_org_secret(tenant_db, "anthropic_api_key", "sk-ant-first", updated_by="alice")
    await write_org_secret(tenant_db, "anthropic_api_key", "sk-ant-second", updated_by="bob")

    assert await read_org_secret(tenant_db, "anthropic_api_key") == "sk-ant-second"
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT updated_by FROM org_secrets WHERE key = 'anthropic_api_key'")
        )
        assert result.fetchall()[0][0] == "bob"


async def test_a_none_value_deletes_the_secret(tenant_db):
    # Clearing the key must revert to the deployment's ANTHROPIC_API_KEY env var — an absent row,
    # not a row holding a copy of anything.
    await write_org_secret(tenant_db, "anthropic_api_key", "sk-ant-example", updated_by="alice")

    await write_org_secret(tenant_db, "anthropic_api_key", None, updated_by="alice")

    assert await read_org_secret(tenant_db, "anthropic_api_key") is None


async def test_a_non_secret_key_is_refused(tenant_db):
    with pytest.raises(ValueError, match="not an org secret key"):
        await write_org_secret(tenant_db, "not_a_real_secret", "value", updated_by="alice")


async def test_a_simple_vendor_key_round_trips(tenant_db):
    # REQ-1398: not just anthropic — any simple-api-key vendor may be set.
    await write_org_secret(tenant_db, "openai_api_key", "sk-openai-example", updated_by="alice")

    assert await read_org_secret(tenant_db, "openai_api_key") == "sk-openai-example"


async def test_read_org_api_keys_returns_every_configured_vendor(tenant_db):
    assert await read_org_api_keys(tenant_db) == {}

    await write_org_secret(tenant_db, "anthropic_api_key", "sk-ant-example", updated_by="alice")
    await write_org_secret(tenant_db, "openai_api_key", "sk-openai-example", updated_by="alice")

    assert await read_org_api_keys(tenant_db) == {
        "anthropic": "sk-ant-example",
        "openai": "sk-openai-example",
    }

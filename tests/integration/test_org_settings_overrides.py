# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f4d18-53be-4c09-b1e6-9d3057ac8842
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1349: the per-org override table, against a schema built by the real ``init_schema``.

The unit tests cover the merge and the allow-list in isolation. What can only be shown here is that
the table an org's overrides live in actually exists in a provisioned org schema — ``schema.sql``
creates it per org and ``schema_org.py`` mirrors it — and that a write from the admin surface is
read back by the query path in the same schema.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.org_settings import (
    merge_org_overrides,
    read_org_overrides,
    write_org_overrides,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1349"
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


async def test_a_provisioned_org_has_the_overrides_table(tenant_db):
    # Provisioning runs schema.sql; a new org must be able to hold overrides from its first request
    # rather than 500ing on an UndefinedTable the first time an administrator opens AI Models.
    assert await read_org_overrides(tenant_db) == {}


async def test_an_override_round_trips(tenant_db):
    # The write side is the admin surface, the read side is the query path. Both name the same
    # table in the same org schema, so what an administrator sets is what the next query resolves.
    await write_org_overrides(
        tenant_db,
        {"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}},
        updated_by="alice",
    )

    overrides = await read_org_overrides(tenant_db)
    assert overrides == {"ai_models": {"sql_generation": {"vendor": "openai", "model": "gpt-4o"}}}
    resolved = merge_org_overrides(
        {"ai_models": {"sql_generation": "claude-opus-5", "table_selection": "gpt-4o-mini"}},
        overrides,
    )
    assert resolved["ai_models"]["sql_generation"] == {"vendor": "openai", "model": "gpt-4o"}


async def test_a_second_write_replaces_and_reattributes(tenant_db):
    await write_org_overrides(tenant_db, {"nl": {"rate_limit": 5}}, updated_by="alice")
    await write_org_overrides(tenant_db, {"nl": {"rate_limit": 9}}, updated_by="bob")

    assert (await read_org_overrides(tenant_db))["nl"] == {"rate_limit": 9}
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT updated_by FROM org_settings WHERE key = 'nl'")
        )
        assert result.fetchall()[0][0] == "bob"


async def test_a_none_value_deletes_the_override(tenant_db):
    # Reverting to the deployment's choice is the absence of a row, not a row holding a copy of the
    # deployment value — otherwise a later deployment-wide change would never reach the org.
    await write_org_overrides(tenant_db, {"nl": {"rate_limit": 5}}, updated_by="alice")

    await write_org_overrides(tenant_db, {"nl": None}, updated_by="alice")

    assert await read_org_overrides(tenant_db) == {}


async def test_a_non_overridable_key_is_refused_before_any_write(tenant_db):
    # The rejection must be atomic with respect to the batch: a request mixing an owned key with a
    # deployment-wide one must not half-apply.
    with pytest.raises(ValueError, match="not org-overridable"):
        await write_org_overrides(
            tenant_db,
            {"nl": {"rate_limit": 5}, "federation": {"engine": "trino"}},
            updated_by="alice",
        )

    assert await read_org_overrides(tenant_db) == {}

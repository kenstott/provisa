# Copyright (c) 2026 Kenneth Stott
# Canary: 6a8bc7a0-ab17-4eaf-b660-97f95220527f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Taking a role off its grants, then deleting it, on a PostgreSQL control plane (REQ-1918).

The scenarios of ``tests/unit/test_revoke_role_grants.py`` run here unchanged against an org
schema in PostgreSQL, where the grant lists are JSONB and the schema's foreign keys are
enforced. Lands on the TEST instance's PostgreSQL only, in a schema this module creates and
drops.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import provisa.api.app as appmod
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from tests.unit.test_revoke_role_grants import *  # noqa: F403 — the scenarios

pytestmark = [pytest.mark.integration]

_ORG_ID = "revokegrants"
_SCHEMA = f"org_{_ORG_ID}"
_URL = "postgresql+psycopg://provisa:provisa@{host}:{port}/provisa".format(
    host=os.environ.get("PG_HOST", "localhost"), port=os.environ.get("PG_PORT", "5432")
)


@pytest.fixture
async def plane(monkeypatch) -> Database:  # noqa: F811 — replaces the SQLite plane
    db = Database(create_engine_from_url(_URL), name="revoke-grants", search_path=_SCHEMA)
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    schema_sql = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"
    await init_schema(db, schema_sql.read_text(encoding="utf-8"), org_id=_ORG_ID)
    await seed(db)  # noqa: F405 — from the scenarios module
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    try:
        yield db
    finally:
        async with db.acquire() as conn:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
        await db.close()

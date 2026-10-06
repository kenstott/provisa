# Copyright (c) 2026 Kenneth Stott
# Canary: b2a64928-2477-4fa3-9295-829a6b2dc50f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model store's dependency guard on a PostgreSQL control plane (REQ-1918, REQ-1919).

The scenarios of ``tests/unit/test_model_store_guard.py`` — written against a SQLite control
plane — run here unchanged against an org schema in PostgreSQL, where the schema's foreign keys
are enforced and would cascade. The guard's answers, and the rows left after a removal, are the
same. Lands on the TEST instance's PostgreSQL only, in a schema this module creates and drops.
"""

# Requirements: REQ-1917, REQ-1918, REQ-1919

from __future__ import annotations

import os
from pathlib import Path

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from tests.unit.test_model_store_guard import *  # noqa: F403 — the scenarios themselves
from tests.unit.test_model_store_guard import Plane

pytestmark = [pytest.mark.integration]

_ORG_ID = "guardscenarios"
_SCHEMA = f"org_{_ORG_ID}"
_URL = "postgresql+psycopg://provisa:provisa@{host}:{port}/provisa".format(
    host=os.environ.get("PG_HOST", "localhost"), port=os.environ.get("PG_PORT", "5432")
)


@pytest.fixture
async def plane() -> Plane:  # noqa: F811 — replaces the SQLite plane the scenarios were given
    db = Database(create_engine_from_url(_URL), name="guard-scenarios", search_path=_SCHEMA)
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    schema_sql = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"
    await init_schema(db, schema_sql.read_text(encoding="utf-8"), org_id=_ORG_ID)
    p = Plane(db)
    await p.add("sources", id="pg", type="postgresql")
    for domain_id in ("sales", "finance"):
        await p.add("domains", id=domain_id)
    try:
        yield p
    finally:
        async with db.acquire() as conn:
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
        await db.close()

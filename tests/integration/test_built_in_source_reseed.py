# Copyright (c) 2026 Kenneth Stott
# Canary: eb7a02e6-06e3-403d-871a-93bc4aa72c12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The built-in source rows describe the deployment as it is now, not as it first booted.

``_seed_built_in_sources`` upserted ``provisa-admin``/``provisa-otel``/``__derived__`` with
``update_columns=[]``, so every column but ``description`` was written on the first boot and never
again. Re-pinning ``PROVISA_ENGINE`` from duckdb to trino therefore left the bootstrap org's
``__derived__.type`` and ``provisa-otel.dialect`` reading duckdb while an org created after the
re-pin read trino — one shared federation engine described two contradictory ways, observed on
cloud.provisa.dev.

A single-boot test cannot see this: the bug lives entirely in the ON CONFLICT branch. So each case
here seeds twice, changing the deployment between the two, and asserts the second boot's answer
wins — while a description the user edited in between does not.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import sources as sources_t

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def seeded_schema(docker_postgres, monkeypatch):
    """A throwaway org schema plus a `seed(engine_name)` that runs the real seeding against it.

    The org id is deliberately NOT ``state.org_id``: that is what makes ``_seed_built_in_sources``
    skip ``seed_org_registry_view``, which opens the admin plane this test does not stand up.
    """
    import os
    from pathlib import Path
    from types import SimpleNamespace

    from provisa.api.app import state
    from provisa.api.startup_seed import _seed_built_in_sources
    from provisa.core.database import Database, create_engine_from_url
    from provisa.audit.query_log import init_audit_schema
    from provisa.core.db import init_schema

    org_id = f"reseed{uuid.uuid4().hex[:8]}"
    schema = f"org_{org_id}"
    host = docker_postgres["host"]
    port = docker_postgres["port"]
    url = f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@{host}:{port}/provisa"

    engine = create_engine_from_url(url, pool_size=2)
    db = Database(engine, name="org", search_path=schema)
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(db, schema_sql, org_id=org_id)
    # The meta domain seeding registers query_audit_log, which lives in the audit schema rather
    # than schema.sql — the same pair build_org_runtime runs.
    await init_audit_schema(db, org_id=org_id)

    monkeypatch.setattr(state, "tenant_db", db, raising=False)

    async def seed(engine_name: str) -> None:
        monkeypatch.setattr(
            state, "federation_engine", SimpleNamespace(name=engine_name), raising=False
        )
        await _seed_built_in_sources(host, port, "provisa", "provisa", org_id=org_id)

    async def row(source_id: str) -> dict:
        async with db.acquire() as conn:
            result = await conn.execute_core(select(sources_t).where(sources_t.c.id == source_id))
            return dict(result.mappings().one())

    try:
        yield SimpleNamespace(seed=seed, row=row, db=db, schema=schema)
    finally:
        async with db.acquire() as conn:
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await engine.dispose()


async def test_derived_sentinel_follows_the_repinned_engine(seeded_schema):
    await seeded_schema.seed("duckdb")
    assert (await seeded_schema.row(DERIVED_SOURCE_ID))["type"] == "duckdb"

    await seeded_schema.seed("trino")
    assert (await seeded_schema.row(DERIVED_SOURCE_ID))["type"] == "trino"


async def test_otel_dialect_follows_the_repinned_engine(seeded_schema):
    await seeded_schema.seed("duckdb")
    assert (await seeded_schema.row("provisa-otel"))["dialect"] == "duckdb"

    await seeded_schema.seed("trino")
    assert (await seeded_schema.row("provisa-otel"))["dialect"] == "trino"


async def test_a_user_edited_description_survives_reseeding(seeded_schema):
    # The counterpart constraint. `description` is the one column here a person owns, so it stays
    # out of update_columns and the set_extra coalesce restores the seed text only when blank —
    # widening the fix to "update everything" would silently discard this on the next restart.
    await seeded_schema.seed("duckdb")
    async with seeded_schema.db.acquire() as conn:
        await conn.execute(
            f"UPDATE {seeded_schema.schema}.sources SET description = 'ours' "
            "WHERE id = 'provisa-otel'"
        )

    await seeded_schema.seed("trino")

    row = await seeded_schema.row("provisa-otel")
    assert row["description"] == "ours"
    assert row["dialect"] == "trino"

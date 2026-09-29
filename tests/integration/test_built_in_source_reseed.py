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
        # The otel Iceberg catalog is Trino's alone (EngineBackend.has_otel_catalog), and the ops
        # seed reads it to decide whether to register or remove the provisa-otel rows.
        monkeypatch.setattr(
            state,
            "federation_engine",
            SimpleNamespace(name=engine_name, has_otel_catalog=engine_name == "trino"),
            raising=False,
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


async def test_reseed_of_an_unchanged_view_does_not_drop_its_dependents(seeded_schema):
    """`deprecated_usage`/`pii_access` (``_OPS_REPORT_VIEWS``) both `FROM ops_table_usage`, so the
    old unconditional `DROP VIEW ops_table_usage CASCADE` also dropped (and took an
    AccessExclusiveLock across) both of them on EVERY boot, even one where nothing had changed
    since a prior warm-up. That multi-object CASCADE lock, taken unconditionally on every restart,
    is what a concurrent reader's AccessShareLock on one of the dependents (an already-ready peer
    `--workers N` process serving ordinary traffic — not itself part of REQ-1900's seed-phase
    advisory lock) can deadlock against; this is the live `asyncpg.exceptions.DeadlockDetectedError`
    seen inside `_seed_meta_domain`/`_seed_ops_domain` on an 8-worker cold boot right after a clean
    1-worker warm-up.

    `_seed_view`'s fix removes the mechanism, not just the symptom: `CREATE OR REPLACE VIEW` locks
    only `ops_table_usage` and is a true no-op when unchanged, so `deprecated_usage` is never
    touched — and therefore never dropped — by an ordinary reseed. Asserted here directly, without
    needing to reproduce Postgres's own lock-scheduling nondeterminism: the pre-fix
    `_adapt_view_ddl` path cascade-drops `deprecated_usage` outright (it stops existing at all),
    while `_seed_view` leaves its `oid` untouched (same object, never re-created).
    """
    from provisa.api._meta_views import _ops_table_usage_ddl
    from provisa.api.startup_seed import _adapt_view_ddl, _seed_view

    await seeded_schema.seed("trino")  # creates ops_table_usage + deprecated_usage.
    schema = seeded_schema.schema
    ddl = _ops_table_usage_ddl("postgresql")

    async def _dependent_oid() -> int | None:
        async with seeded_schema.db.acquire() as conn:
            return await conn.fetchval(f"SELECT to_regclass('{schema}.deprecated_usage')::oid")

    assert await _dependent_oid() is not None

    async with seeded_schema.db.acquire() as conn:
        await conn.execute(_adapt_view_ddl(ddl, "postgresql"))
    assert await _dependent_oid() is None, (
        "expected the pre-fix DROP...CASCADE path to actually cascade-drop deprecated_usage — "
        "otherwise this test isn't exercising the real bug"
    )

    await seeded_schema.seed("trino")  # repair: recreate deprecated_usage's registration.
    oid_before = await _dependent_oid()
    assert oid_before is not None

    async with seeded_schema.db.acquire() as conn:
        await _seed_view(conn, ddl, "postgresql")
    assert await _dependent_oid() == oid_before, (
        "_seed_view's CREATE OR REPLACE VIEW must never touch deprecated_usage at all when "
        "ops_table_usage's own shape is unchanged"
    )


async def test_concurrent_worker_reseed_does_not_error(seeded_schema):
    """Basic multi-worker sanity check: N concurrent `_seed_built_in_sources` passes (one per
    simulated `--workers N` process) against an already-warm schema must not raise. This does not
    by itself reproduce the CASCADE-vs-concurrent-reader deadlock covered by
    `test_reseed_of_an_unchanged_view_does_not_drop_its_dependents` above (Postgres's own lock
    scheduler decides whether two sessions' lock requests actually interleave, and REQ-1900's
    advisory lock already serializes worker-vs-worker access to this phase either way) — it exists
    to catch a plainer regression, like an exception thrown by re-running the seed concurrently at
    all.
    """
    import asyncio

    await seeded_schema.seed("trino")  # warm-up: nothing left to reconcile below.

    await asyncio.gather(*(seeded_schema.seed("trino") for _ in range(8)))


async def test_reseed_skips_drop_cascade_when_view_shape_is_unchanged(seeded_schema, monkeypatch):
    """The `_seed_view` fast path (`CREATE OR REPLACE VIEW`, no CASCADE) must be what actually
    runs on an ordinary reseed — not just that it happens not to raise. Asserts the DROP...CASCADE
    fallback (`_adapt_view_ddl`) is never invoked on a second, unchanged boot.
    """
    from provisa.api import startup_seed

    await seeded_schema.seed("trino")  # first boot creates every view.

    calls = []
    original = startup_seed._adapt_view_ddl

    def _tracking_adapt(ddl, dialect):
        calls.append(ddl)
        return original(ddl, dialect)

    monkeypatch.setattr(startup_seed, "_adapt_view_ddl", _tracking_adapt)
    await seeded_schema.seed("trino")  # second boot: shape is unchanged, no CASCADE needed.

    assert calls == []


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

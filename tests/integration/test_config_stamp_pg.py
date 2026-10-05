# Copyright (c) 2026 Kenneth Stott
# Canary: d92b4e07-5c18-4f63-8a2d-e7b0c1f96a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config stamp on a PostgreSQL control plane (REQ-1914).

The same rules ``tests/unit/test_config_stamp.py`` holds the SQLite control plane to, on the
control plane a multi-worker or multi-instance deployment runs on, plus what only PostgreSQL has:
one stamp per org schema in a shared database, a writer whose connection carries another
``search_path``, and a row rewritten with the values it already holds.

The test instance only: its own database on the session's Postgres, dropped at the end."""

# Requirements: REQ-1914

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa

from provisa.core import config_stamp, deployment_settings, schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema, org_schema
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import init_registry_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SCHEMA_SQL = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()


@pytest.fixture
def engine(docker_postgres):
    """An engine on a database of this test's own."""
    password = os.environ.get("PG_PASSWORD", "provisa")
    server = f"postgresql+psycopg://provisa:{password}@{docker_postgres['host']}:{docker_postgres['port']}"
    database = f"cfgstamp_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(f"{server}/provisa", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{database}"'))
    own = create_engine_from_url(f"{server}/{database}", pool_size=4)
    try:
        yield own
    finally:
        own.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
        admin.dispose()


async def _org(engine: sa.Engine, org_id: str) -> Database:
    db = Database(engine, name=f"org_{org_id}", search_path=org_schema(org_id))
    await init_schema(db, _SCHEMA_SQL, org_id=org_id)
    return db


async def _model(db: Database) -> int:
    return (await config_stamp.read(db))[config_stamp.MODEL]


def _roles(db: Database) -> sa.Table:
    return schema_org.roles.to_metadata(sa.MetaData(), schema=db.search_path)


async def test_insert_update_and_delete_each_advance_the_stamp(engine):
    db = await _org(engine, "acme")
    roles = _roles(db)
    seen = [await _model(db)]
    with engine.begin() as conn:
        conn.execute(sa.insert(roles).values(id="risk_reviewer", capabilities=[], origin="admin"))
    seen.append(await _model(db))
    with engine.begin() as conn:
        conn.execute(
            sa.update(roles).where(roles.c.id == "risk_reviewer").values(domain_access=["sales"])
        )
    seen.append(await _model(db))
    with engine.begin() as conn:
        conn.execute(sa.delete(roles).where(roles.c.id == "risk_reviewer"))
    seen.append(await _model(db))
    assert seen == sorted(set(seen)), seen


async def test_the_stamp_is_written_in_the_transaction_of_the_change(engine):
    db = await _org(engine, "acme")
    roles = _roles(db)
    before = await _model(db)
    conn = engine.connect()
    try:
        conn.execute(sa.insert(roles).values(id="risk_reviewer", capabilities=[], origin="admin"))
        inside = conn.execute(
            sa.text(f"SELECT stamp FROM \"{db.search_path}\".config_stamp WHERE kind = 'model'")
        ).scalar_one()
        assert inside > before
        assert await _model(db) == before  # no other session sees it before the commit
        conn.rollback()
    finally:
        conn.close()
    assert await _model(db) == before


async def test_a_row_rewritten_with_the_values_it_holds_is_not_a_change(engine):
    db = await _org(engine, "acme")
    roles = _roles(db)
    with engine.begin() as conn:
        conn.execute(
            sa.insert(roles).values(id="risk_reviewer", domain_access=["sales"], origin="admin")
        )
    before = await _model(db)
    with engine.begin() as conn:
        conn.execute(
            sa.update(roles).where(roles.c.id == "risk_reviewer").values(domain_access=["sales"])
        )
        conn.execute(
            sa.text(
                f"INSERT INTO \"{db.search_path}\".roles (id, origin) VALUES ('risk_reviewer', 'admin') "
                "ON CONFLICT DO NOTHING"
            )
        )
    assert await _model(db) == before


async def test_a_second_boot_of_the_same_schema_does_not_advance_the_stamp(engine):
    """Every worker of a launch runs the schema init; it must not look like a config change."""
    db = await _org(engine, "acme")
    before = await config_stamp.read(db)
    await init_schema(db, _SCHEMA_SQL, org_id="acme")
    assert await config_stamp.read(db) == before
    with engine.begin() as conn:
        conn.execute(
            sa.insert(_roles(db)).values(id="risk_reviewer", capabilities=[], origin="admin")
        )
    assert await _model(db) == before[config_stamp.MODEL] + 1  # one trigger per event, not two


async def test_each_org_schema_has_its_own_stamp(engine):
    acme = await _org(engine, "acme")
    globex = await _org(engine, "globex")
    before = await _model(globex)
    # The writer's connection carries NO org search_path: the trigger finds the stamp of the
    # schema the written table lives in.
    with engine.begin() as conn:
        conn.execute(
            sa.insert(_roles(acme)).values(id="risk_reviewer", capabilities=[], origin="admin")
        )
    assert await _model(globex) == before
    # ...and a writer whose connection is scoped to ANOTHER org still advances the right one.
    acme_before = await _model(acme)
    async with globex.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{acme.search_path}".naming_rules (pattern, replacement) VALUES ($1, $2)',
            "a",
            "b",
        )
    assert await _model(acme) > acme_before
    assert await _model(globex) == before


async def test_runtime_bookkeeping_does_not_advance_the_stamp(engine):
    db = await _org(engine, "acme")
    views = schema_org.materialized_views.to_metadata(sa.MetaData(), schema=db.search_path)
    with engine.begin() as conn:
        conn.execute(
            sa.insert(views).values(
                id="mv1",
                source_tables=["t"],
                target_catalog="c",
                target_schema="s",
                target_table="t",
            )
        )
    builds = schema_org.mv_build_state.to_metadata(sa.MetaData(), schema=db.search_path)
    before = await config_stamp.read(db)
    with engine.begin() as conn:
        # What a refresh writes: the region's build of the view (REQ-1922), not its definition.
        conn.execute(
            sa.insert(builds).values(
                mv_id="mv1",
                region="default",
                status="fresh",
                row_count=10,
                writer="w1",
                materialized_input_version="v2",
            )
        )
    assert await config_stamp.read(db) == before
    with engine.begin() as conn:
        conn.execute(sa.update(views).where(views.c.id == "mv1").values(enabled=False))
    assert await _model(db) > before[config_stamp.MODEL]


async def test_org_settings_move_the_settings_stamp_alone(engine):
    db = await _org(engine, "acme")
    settings = schema_org.org_settings.to_metadata(sa.MetaData(), schema=db.search_path)
    before = await config_stamp.read(db)
    with engine.begin() as conn:
        conn.execute(sa.insert(settings).values(key="cache", value={"default_ttl": 7}))
    after = await config_stamp.read(db)
    assert after[config_stamp.SETTINGS] > before[config_stamp.SETTINGS]
    assert after[config_stamp.MODEL] == before[config_stamp.MODEL]


async def test_the_platform_plane_stamps_a_stored_operator_setting(engine, monkeypatch):
    platform = Database(engine, name="platform")
    await init_registry_schema(platform, "acme")
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    deployment_settings.bind(platform)
    assert deployment_settings.get("limits.default_row_limit") is None
    loaded = deployment_settings.loaded_stamp()

    # Another worker stores a setting.
    with engine.begin() as conn:
        conn.execute(settings_table.insert().values(key="limits.default_row_limit", value="7"))
    stored = (await config_stamp.read(platform))[config_stamp.SETTINGS]
    assert loaded is not None and stored > loaded
    assert deployment_settings.get("limits.default_row_limit") is None  # no read per request

    deployment_settings.reload()
    assert deployment_settings.get("limits.default_row_limit") == 7
    assert deployment_settings.loaded_stamp() == stored

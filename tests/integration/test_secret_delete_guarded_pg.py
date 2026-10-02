# Copyright (c) 2026 Kenneth Stott
# Canary: eff904c9-0b72-4acf-8009-6b39e705c2df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org secret is deleted only when no stored value names it, on PostgreSQL (REQ-1918).

The scenarios of ``tests/unit/test_secret_delete_guarded.py`` — written against SQLite control
planes — run here unchanged against PostgreSQL: a platform plane in a schema of its own (so the
vault and its key record are this module's and nobody else's), and two environment schemas of
one org, where JSON values are ``jsonb``. Lands on the TEST instance's PostgreSQL only, in
schemas this module creates and drops.
"""

# Requirements: REQ-1918, REQ-1558

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import text

from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from tests.unit.test_secret_delete_guarded import *  # noqa: F403 — the scenarios themselves
from tests.unit.test_secret_delete_guarded import Planes, seed_orgs

pytestmark = [pytest.mark.integration]

_ADMIN_SCHEMA = "test_secretguard_admin"
_ENV_ORGS = {"prod": "secretguard", "dev": "secretguard_env_dev"}
_URL = "postgresql+psycopg://provisa:provisa@{host}:{port}/provisa".format(
    host=os.environ.get("PG_HOST", "localhost"), port=os.environ.get("PG_PORT", "5432")
)


def _drop_all(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        for org in _ENV_ORGS.values():
            conn.execute(text(f"DROP SCHEMA IF EXISTS org_{org} CASCADE"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS org_{org}_mv_cache CASCADE"))


@pytest.fixture
async def planes() -> Planes:  # noqa: F811 — replaces the SQLite planes the scenarios were given
    engine = create_engine_from_url(_URL)
    _drop_all(engine)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        schema_admin.metadata.create_all(conn)
    admin = Database(engine, name="admin", search_path=_ADMIN_SCHEMA)
    schema_sql = (
        Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"
    ).read_text(encoding="utf-8")
    environments: dict[str, Database] = {}
    for env, org in _ENV_ORGS.items():
        plane = Database(create_engine_from_url(_URL), name=env, search_path=f"org_{org}")
        await init_schema(plane, schema_sql, org_id=org)
        environments[env] = plane
    await seed_orgs(admin)
    try:
        yield Planes(admin, environments)
    finally:
        for plane in environments.values():
            await plane.close()
        _drop_all(engine)
        engine.dispose()

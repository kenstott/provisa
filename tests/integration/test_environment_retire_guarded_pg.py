# Copyright (c) 2026 Kenneth Stott
# Canary: 0ff00f9f-0d47-459a-8999-24419f8160ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment is retired only when nothing still refers to it, on PostgreSQL (REQ-1918).

The scenarios of ``tests/unit/test_environment_retire_guarded.py`` — written against a SQLite
platform plane — run here unchanged against a platform plane in a PostgreSQL schema of its own,
where the registry's foreign keys are enforced. Lands on the TEST instance's PostgreSQL only, in
a schema this module creates and drops.
"""

# Requirements: REQ-1918, REQ-1596, REQ-1542, REQ-1523

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from tests.unit.test_environment_retire_guarded import *  # noqa: F403 — the scenarios themselves
from tests.unit.test_environment_retire_guarded import seed

pytestmark = [pytest.mark.integration]

_ADMIN_SCHEMA = "test_envguard_admin"
_URL = "postgresql+psycopg://provisa:provisa@{host}:{port}/provisa".format(
    host=os.environ.get("PG_HOST", "localhost"), port=os.environ.get("PG_PORT", "5432")
)


@pytest.fixture
async def admin() -> Database:  # noqa: F811 — replaces the SQLite plane the scenarios were given
    engine = create_engine_from_url(_URL)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        schema_admin.metadata.create_all(conn)
    plane = Database(engine, name="admin", search_path=_ADMIN_SCHEMA)
    await seed(plane)
    try:
        yield plane
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        engine.dispose()

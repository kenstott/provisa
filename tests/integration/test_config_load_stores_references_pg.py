# Copyright (c) 2026 Kenneth Stott
# Canary: cfcddc18-7195-4e05-b7d1-f53d3a5b0eb0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The config-load guard on a PostgreSQL control plane: after a load, no column of any table
holds a credential's value (the same scan as tests/unit/test_config_load_stores_references.py,
which runs it on SQLite).

Everything here is the ``test`` instance: the integration stack's Postgres, in a throwaway
schema this module creates and drops."""

from __future__ import annotations

import contextlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text

from provisa.core import secrets_store
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from tests.unit.test_config_load_stores_references import SECRETS, assert_no_value_is_stored

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture
async def db(pg_dsn, monkeypatch):
    """The real PostgreSQL org schema (schema.sql), in an org of its own, dropped after."""
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    org = f"refs{uuid.uuid4().hex[:10]}"
    schema = f"org_{org}"
    engine = create_engine_from_url(pg_dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    plane = Database(engine, name="refs-pg", search_path=schema)
    await init_schema(plane, SCHEMA_SQL.read_text(encoding="utf-8"), org_id=org)
    try:
        yield plane, schema
    finally:
        with engine.begin() as c:
            c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}_mv_cache" CASCADE'))
        engine.dispose()


async def test_after_a_config_load_no_postgres_column_holds_a_credentials_value(db, tmp_path):
    plane, schema = db
    await assert_no_value_is_stored(plane, tmp_path, schema)

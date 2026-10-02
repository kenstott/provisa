# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4a6e21-8b35-4f07-a2c9-5e1f7b3d0c68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The schema reconcile on a control plane without schemas (SQLite).

``tests/integration/test_pg_schema_upgrade.py`` holds the PostgreSQL half: tables land in the org
schema, typed as the plane declares, one creator at a time under the schema lock. A SQLite plane
has one namespace and no advisory locks; the same start must create every metadata table there,
restore a table or a column that is missing, change nothing on a second start, and never name a
schema or a lock to SQLite."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import sqlalchemy as sa

from provisa.core import schema_org
from provisa.core.database import Database
from provisa.core.db import add_missing_columns, init_schema

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_ORG = "acme"


@pytest.fixture
def plane(tmp_path):
    engine = sa.create_engine(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org", search_path=f"org_{_ORG}")
    yield db
    engine.dispose()


def _tables(db) -> set[str]:
    return set(sa.inspect(db.engine).get_table_names())


def _columns(db, table: str) -> set[str]:
    return {c["name"] for c in sa.inspect(db.engine).get_columns(table)}


def test_a_start_creates_every_metadata_table_and_a_second_changes_nothing(plane):
    asyncio.run(init_schema(plane, _SCHEMA_SQL, org_id=_ORG))
    expected = {t.name for t in schema_org.metadata.sorted_tables}
    assert expected <= _tables(plane)
    before = {t: _columns(plane, t) for t in expected}

    asyncio.run(init_schema(plane, _SCHEMA_SQL, org_id=_ORG))
    assert {t: _columns(plane, t) for t in expected} == before


def test_a_missing_table_and_a_missing_column_are_restored(plane):
    asyncio.run(init_schema(plane, _SCHEMA_SQL, org_id=_ORG))
    with plane.engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE provisa_sources")
        conn.exec_driver_sql("ALTER TABLE source_catalog_cache DROP COLUMN comment")
    assert "provisa_sources" not in _tables(plane)
    assert "comment" not in _columns(plane, "source_catalog_cache")

    asyncio.run(init_schema(plane, _SCHEMA_SQL, org_id=_ORG))
    assert "provisa_sources" in _tables(plane)
    assert "comment" in _columns(plane, "source_catalog_cache")


def test_the_reconcile_sends_sqlite_no_schema_lock_and_no_search_path(plane):
    """The schema lock and the search_path are PostgreSQL's; SQLite has neither statement."""
    sent: list[str] = []

    @sa.event.listens_for(plane.engine, "before_cursor_execute")
    def _record(_conn, _cursor, statement, *_rest):
        sent.append(statement)

    with plane.engine.begin() as conn:
        add_missing_columns(conn, schema_org.metadata.sorted_tables)
    assert sent, "the reconcile ran no statement"
    assert not [s for s in sent if "advisory" in s.lower() or "search_path" in s.lower()]
    assert {t.name for t in schema_org.metadata.sorted_tables} <= _tables(plane)

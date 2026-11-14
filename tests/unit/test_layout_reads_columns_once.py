# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8f6d41-9c07-4e3a-b5d2-7e19a0c4f86d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org schema's reconciliation reads its live columns in one statement (REQ-2006).

Laying an org environment's schema out reconciles it against the metadata (add_missing_columns):
it read each table's columns in a reflection of its own, a statement per table, and a demo
sandbox's open lays three schemas out. It reads every table's columns at once, and still adds
exactly the column a schema lacks."""

# Requirements: REQ-2006

from __future__ import annotations

import pytest
from sqlalchemy import event, text

from provisa.core import schema_org
from provisa.core.db import add_missing_columns

pytestmark = pytest.mark.unit

_SCHEMA = "org_default"


def _columns(engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t"
            ),
            {"s": _SCHEMA, "t": table},
        )
        return {r[0] for r in rows}


def test_the_live_columns_are_read_in_one_statement(control_plane):
    engine = control_plane.engine()
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _seen(conn, cursor, statement, *args):
        statements.append(statement)

    with engine.begin() as conn:
        add_missing_columns(conn, schema_org.metadata.sorted_tables, _SCHEMA)

    # The lock, the search path, the tables, the columns: no statement per table.
    assert len(statements) <= 6, len(statements)


def test_a_column_the_schema_lacks_is_added(control_plane):
    engine = control_plane.engine()
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{_SCHEMA}".domains DROP COLUMN description'))
    assert "description" not in _columns(engine, "domains")

    with engine.begin() as conn:
        add_missing_columns(conn, schema_org.metadata.sorted_tables, _SCHEMA)

    assert "description" in _columns(engine, "domains")


def test_a_table_the_layout_creates_copies_the_metadata_once_per_process(
    control_plane, monkeypatch
):
    """A table schema.sql does not create is created from a JSONB copy of the metadata. The copy
    was made again for each such table, ~0.1 s of Python each, four per new org schema (REQ-2006);
    it is made once, and every schema laid out after creates from it."""
    from sqlalchemy.sql.schema import Table

    engine = control_plane.engine()
    copied: list[str] = []
    to_metadata = Table.to_metadata

    def _counted(self, *args, **kwargs):
        copied.append(self.name)
        return to_metadata(self, *args, **kwargs)

    monkeypatch.setattr(Table, "to_metadata", _counted)
    # Tables the schema lacks, as a new org schema lacks the ones schema.sql does not create.
    gone = ["domains", "sources"]
    for laid_out in range(2):
        with engine.begin() as conn:
            for name in gone:
                conn.execute(text(f'DROP TABLE "{_SCHEMA}"."{name}" CASCADE'))
        with engine.begin() as conn:
            add_missing_columns(conn, schema_org.metadata.sorted_tables, _SCHEMA)
        assert set(gone) <= _tables(engine), laid_out
        assert {"description"} <= _columns(engine, "domains")
    # At most the one copy of the whole metadata (none when an earlier test in this process
    # made it), never one per table created.
    assert len(copied) in (0, len(schema_org.metadata.tables)), len(copied)


def _tables(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = :s"),
            {"s": _SCHEMA},
        )
        return {r[0] for r in rows}

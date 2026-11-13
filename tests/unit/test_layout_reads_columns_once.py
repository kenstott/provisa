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

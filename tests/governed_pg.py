# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41c7e2-5a39-4f60-b1e8-2c7a90d3f615
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a governed statement returns, read on PostgreSQL (tests/embedded_pg.py).

A test of governance checks what the statement it governs answers, not the text of the governed
SQL: the shape governance builds may change (REQ-039 replaced per-reference rewrites with governed
relations), what the role reads may not. ``GovernedPG.answer`` creates the tables a test names, in
a transaction of their own, fills them, runs the governed statement and rolls everything back.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, LiteralString, cast

_TYPES = {
    bool: "boolean",
    int: "bigint",
    float: "double precision",
    str: "text",
    Decimal: "numeric",
    date: "date",
    datetime: "timestamp",
}


def _type(values: list[Any]) -> str:
    for value in values:
        if value is not None:
            return _TYPES.get(type(value), "text")
    return "text"


class Refused(Exception):
    """PostgreSQL refused the governed statement (a column or relation that is not there)."""


class GovernedPG:
    """A database of the test session's embedded PostgreSQL for governed statements."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def answer(self, governed: str, tables: dict[str, list[dict[str, Any]]]) -> list[tuple]:
        """The rows ``governed`` returns over ``tables`` (``"schema.table"`` or ``"table"`` -> its
        rows, each a dict of column -> value; every row of a table has the same columns).
        :class:`Refused` when PostgreSQL refuses the statement."""
        import psycopg
        from psycopg import sql

        from provisa.transpiler.transpile import transpile

        # As the engine is handed it: a mask's portable function lowered to PostgreSQL's.
        physical = transpile(governed, "postgres")
        with psycopg.connect(self.dsn) as conn:
            try:
                for name, rows in tables.items():
                    parts = name.split(".")
                    if len(parts) == 2:
                        conn.execute(
                            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                                sql.Identifier(parts[0])
                            )
                        )
                    columns = list(rows[0])
                    conn.execute(
                        sql.SQL("CREATE TABLE {} ({})").format(
                            sql.Identifier(*parts),
                            sql.SQL(", ").join(
                                sql.SQL("{} {}").format(
                                    sql.Identifier(c),
                                    sql.SQL(cast(LiteralString, _type([r[c] for r in rows]))),
                                )
                                for c in columns
                            ),
                        )
                    )
                    for row in rows:
                        conn.execute(
                            sql.SQL("INSERT INTO {} VALUES ({})").format(
                                sql.Identifier(*parts),
                                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                            ),
                            [row[c] for c in columns],
                        )
                try:
                    cur = conn.execute(cast(LiteralString, physical))
                except psycopg.Error as exc:
                    raise Refused(f"{type(exc).__name__}: {exc}") from exc
                return cur.fetchall() if cur.description else []
            finally:
                conn.rollback()


def governed_database(embedded_postgres: Any, name: str) -> GovernedPG:
    """A fresh database ``name`` on the session's embedded PostgreSQL."""
    import psycopg
    from psycopg import sql

    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    return GovernedPG(embedded_postgres.dsn(name))

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
        #: The output column names of the last statement answered.
        self.names: list[str] = []

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
                self.names = [d.name for d in cur.description or []]
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


def _value(column: str, kind: str, i: int) -> Any:
    if kind in ("integer", "int", "bigint"):
        return i
    if kind in ("numeric", "decimal"):
        return Decimal(i) + Decimal("0.5")
    if kind.startswith("timestamp"):
        return datetime(2026, i, 15, 10, 30)
    if kind == "date":
        return date(2026, i, 15)
    if kind == "boolean":
        return i % 2 == 1
    return f"{column} {i}@v{i}.x"


def sample_tables(
    *statements: str, types: dict[str, str] | None = None, rows: int = 3
) -> dict[str, list[dict[str, Any]]]:
    """Rows for every table ``statements`` read, each holding every column any of them names:
    text unless ``types`` (column -> integer, numeric, timestamp, date, boolean) says otherwise.
    The first rows take the constants the statements compare each column with, so a filter has
    rows to keep; the rest take values no constant names, so it has rows to drop."""
    import sqlglot
    import sqlglot.expressions as exp

    tables: set[str] = set()
    columns: set[str] = set()
    named: dict[str, list[Any]] = {}
    for statement in statements:
        tree = sqlglot.parse_one(statement, read="postgres")
        ctes = {c.alias for c in tree.find_all(exp.CTE)}
        for table in tree.find_all(exp.Table):
            if table.name and not (not table.db and table.name in ctes):
                tables.add(f"{table.db}.{table.name}" if table.db else table.name)
        columns |= {
            c.name for c in tree.find_all(exp.Column) if not isinstance(c.this, exp.Star) and c.name
        }
        for node in tree.find_all(exp.EQ, exp.NEQ, exp.In):
            column = node.this
            others = node.expressions if isinstance(node, exp.In) else [node.expression]
            if not isinstance(column, exp.Column):
                column, others = node.expression, [node.this]
            if not isinstance(column, exp.Column):
                continue
            for other in others:
                if isinstance(other, exp.Literal):
                    value = other.this if other.is_string else int(other.this)
                elif isinstance(other, exp.Boolean):
                    value = other.this
                else:
                    continue
                pool = named.setdefault(column.name, [])
                if value not in pool:
                    pool.append(value)
    kinds = types or {}
    count = max(rows, *(len(p) + 2 for p in named.values()), 0)

    def value(column: str, i: int) -> Any:
        pool = named.get(column, [])
        return pool[i - 1] if i <= len(pool) else _value(column, kinds.get(column, "text"), i)

    return {
        name: [{c: value(c, i) for c in sorted(columns)} for i in range(1, count + 1)]
        for name in sorted(tables)
    }


def _bound(statement: str) -> str:
    """``statement`` with each ``$n`` it binds read as the constant ``'p<n>'``."""
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(statement, read="postgres")
    for node in list(tree.find_all(exp.Placeholder, exp.Parameter)):
        node.replace(exp.Literal.string(f"p{node.name}"))
    return tree.sql(dialect="postgres")


def assert_same_answer(
    pg: GovernedPG,
    governed: str,
    expected: str,
    *,
    types: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> None:
    """``governed`` answers what ``expected`` -- the statement as a reference reading of the
    tables -- answers, over :func:`sample_tables` rows. ``params`` binds ``$n`` placeholders to
    literals in both."""
    for name, literal in (params or {}).items():
        governed, expected = governed.replace(name, literal), expected.replace(name, literal)
    # Any other bound value: a constant of its own, which the sample rows then hold.
    governed, expected = _bound(governed), _bound(expected)
    tables = sample_tables(governed, expected, types=types)
    want = pg.answer(expected, tables)
    names = pg.names
    assert want, f"the reference reads nothing: {expected}"
    assert sorted(map(repr, pg.answer(governed, tables))) == sorted(map(repr, want)), governed
    assert pg.names == names, f"{governed} names its columns {pg.names}, not {names}"

# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1d5e27-4c3f-4b96-a0e8-6f2c9b17d354
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""lastval() from a pgwire session's own inserts (REQ-2167, amended 2026-11-02).

Provisa answers lastval(), not the source: a source's connection is pooled, so its own lastval
would be some other session's. An INSERT that makes its source generate an identity or serial
value is written to report it -- on PostgreSQL by RETURNING the generated columns after the
client's own, stripped again before the client is answered; on MySQL by the connection's
LAST_INSERT_ID() (the driver's ``execute_reporting_generated``) -- and the session keeps the last
one. Until it holds one, the call is left in the statement, where it raises PostgreSQL's 55000.
"""

# Requirements: REQ-2167

from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

from provisa.executor.drivers.base import GeneratedColumn
from provisa.executor.result import QueryResult

#: The ways a driver reports a generated value (``DirectDriver.generated_keys``).
RETURNING, LAST_INSERT_ID = "returning", "last_insert_id"
#: The name each column the rewrite adds to a RETURNING list is returned under.
_ADDED = "provisa_generated_{}"
_LASTVAL = re.compile(r"\blastval\b", re.I)


@dataclass(frozen=True)
class GeneratedKey:
    """How a pgwire INSERT's plan reports the value it made its source generate.

    ``added``: the columns the rewrite appended to the statement's RETURNING, after the client's
    own; ``client_returns``: whether the client asked for rows. Which value is the last generated:
    for each VALUES row, ``rows`` holds the index (into ``added``) of the last column that row
    generated, None for a row that generates none; a statement whose rows are not listed
    generates the same columns on every row, the last of them ``every``."""

    by: str
    added: int = 0
    client_returns: bool = False
    rows: tuple[int | None, ...] | None = None
    every: int | None = None

    def taken(self, result: QueryResult) -> QueryResult:
        """``result`` as the client asked for it, carrying the last value generated."""
        if self.by != RETURNING:
            return result  # the connection reported it (execute_reporting_generated)
        kept = len(result.column_names) - self.added
        generated = None
        for i in range(len(result.rows) - 1, -1, -1):
            pick = self.every if self.rows is None else self.rows[i]
            if pick is not None:
                generated = int(result.rows[i][kept + pick])
                break
        if not self.client_returns:
            return dataclasses.replace(
                result,
                rows=[],
                column_names=[],
                column_types=None,
                column_type_oids=None,
                rowcount=len(result.rows),
                generated_key=generated,
            )
        return dataclasses.replace(
            result,
            rows=[row[:kept] for row in result.rows],
            column_names=result.column_names[:kept],
            column_types=None if result.column_types is None else result.column_types[:kept],
            column_type_oids=(
                None if result.column_type_oids is None else result.column_type_oids[:kept]
            ),
            generated_key=generated,
        )


def _name(identifier: exp.Expr) -> str:
    """An identifier as the catalog holds it: as written when quoted, else folded to lower case."""
    return identifier.name if identifier.args.get("quoted") else identifier.name.lower()


def _insert(sql: str, dialect: str) -> exp.Insert | None:
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except ParseError:
        return None
    return tree if isinstance(tree, exp.Insert) else None


def inserted_table(sql: str, dialect: str) -> tuple[str | None, str] | None:
    """The (schema, table) an INSERT writes, None for any other statement."""
    insert = _insert(sql, dialect)
    if insert is None:
        return None
    target = insert.this
    table = target.this if isinstance(target, exp.Schema) else target
    if not isinstance(table, exp.Table):
        return None
    schema = table.args.get("db")
    return (_name(schema) if schema else None), _name(table.this)


def _is_default(value: exp.Expr) -> bool:
    return isinstance(value, exp.Var) and value.name.upper() == "DEFAULT"


def _generating(
    insert: exp.Insert, columns: tuple[GeneratedColumn, ...]
) -> tuple[tuple[int | None, ...] | None, int | None] | None:
    """Which of ``columns`` the INSERT's rows leave to the source: (rows, every) as
    :class:`GeneratedKey` holds them, None when no row generates a value. A row generates a
    column it gives no value or DEFAULT; the source fills them in the table's column order, so a
    row's last value is its last such column."""
    target = insert.this
    listed = [_name(i) for i in target.expressions] if isinstance(target, exp.Schema) else None

    def place(column: GeneratedColumn) -> int | None:
        """Where a row gives the column its value, None when the column list leaves it out."""
        if listed is None:
            return column.position - 1
        return listed.index(column.name) if column.name in listed else None

    def last_of(given: Callable[[int], bool]) -> int | None:
        last = None
        for i, column in enumerate(columns):
            at = place(column)
            if at is None or not given(at):
                last = i
        return last

    if insert.args.get("default"):  # DEFAULT VALUES
        return None, len(columns) - 1
    source = insert.expression
    if isinstance(source, exp.Values):
        rows = tuple(
            last_of(
                lambda at, values=list(row.expressions) if isinstance(row, exp.Tuple) else [row]: (
                    at < len(values) and not _is_default(values[at])
                )
            )
            for row in source.expressions
        )
        return (rows, None) if any(r is not None for r in rows) else None
    # A query fills every place of its column list; without one, a select list of plain items
    # fills its first places only, and any other query (a star, a set operation) every place.
    width = len(listed) if listed is not None else None
    plain = isinstance(source, exp.Select) and not any(
        isinstance(e, exp.Star) for e in source.expressions
    )
    if listed is None and plain:
        width = len(source.expressions)
    every = last_of(lambda at: width is None or at < width)
    return (None, every) if every is not None else None


def returning_rewrite(
    sql: str, columns: tuple[GeneratedColumn, ...]
) -> tuple[str, GeneratedKey] | None:
    """A PostgreSQL INSERT written to return the ``columns`` it makes the source generate, after
    what the client's own RETURNING asks for, and how its result is read; None when no row
    generates one.

    An INSERT with ON CONFLICT is not rewritten: the source draws a value for a row it then skips
    or updates, which nothing it returns shows, so the session's value is left as it was."""
    insert = _insert(sql, "postgres")
    if insert is None or insert.args.get("conflict") is not None:
        return None
    generating = _generating(insert, columns)
    if generating is None:
        return None
    rows, every = generating
    added = [
        exp.alias_(exp.column(c.name, quoted=True), _ADDED.format(i + 1), quoted=True)
        for i, c in enumerate(columns)
    ]
    returning = insert.args.get("returning")
    if returning is None:
        insert.set("returning", exp.Returning(expressions=added))
    else:
        returning.set("expressions", [*returning.expressions, *added])
    key = GeneratedKey(
        RETURNING,
        added=len(added),
        client_returns=returning is not None,
        rows=rows,
        every=every,
    )
    return insert.sql(dialect="postgres"), key


def answered_lastval(sql: str, value: int) -> str | None:
    """``sql`` with each lastval() call answered by ``value``, the session's last generated
    value; None when it calls none. A call that is a whole select item keeps the column name
    PostgreSQL gives it."""
    if _LASTVAL.search(sql) is None:
        return None
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return None
    calls = [
        n for n in tree.find_all(exp.Anonymous) if n.name.lower() == "lastval" and not n.expressions
    ]
    if not calls:
        return None
    for call in calls:
        node = call.parent if isinstance(call.parent, exp.Dot) else call
        answer: exp.Expr = exp.cast(exp.Literal.number(value), "BIGINT")
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            answer = exp.alias_(answer, "lastval", quoted=True)
        node.replace(answer)
    return tree.sql(dialect="postgres")

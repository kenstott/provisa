# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1c4b96-2f8e-4a53-9b0d-e6a3f5c1287d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one write route of a Google Workspace or Microsoft 365 source (REQ-1994).

A statement that writes ``events``, ``event_attendees`` or ``tasks`` reaches this module on the
DIRECT terminal as every other source's write does, already governed: the role's right to write
the table and its row rules are in the statement. It is carried out against the provider's API,
as the account each row names:

- INSERT: each row of literal values is created (an event in its calendar, an attendee on its
  event, a task in its list);
- UPDATE and DELETE: the rows the statement names by their write key
  (``canonical_mail.WRITE_KEYS``, or a series' occurrence by ``series_id`` and
  ``original_start_at``) are found at the provider as they stand now, and each is changed or
  removed only where the statement's whole WHERE -- the role's row rules with it -- holds for
  it. The predicate is evaluated over the row exactly as the tables give it.

A write sets only the columns ``canonical_mail.WRITABLE_COLUMNS`` names that the provider keeps;
any other is refused by name. Every other statement form (a SELECT as the rows to insert, ON
CONFLICT, RETURNING, a join) is refused at admission (``executor.writable``).

The written table's replica is read again after the write (``api.data.table_written``), so a read
after the write sees it.
"""

# Requirements: REQ-1994
from __future__ import annotations

import datetime as dt
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import sqlglot
from sqlglot import exp

from provisa.core import canonical_mail as cm
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class WriteRefused(ValueError):
    """A write this route does not carry out, and why."""


@dataclass
class Found:
    """A row a write names, as the tables give it, and what the provider's writer needs to
    change or remove it."""

    row: dict[str, Any]
    handle: Any


def as_instant(value: Any) -> dt.datetime | None:
    """A timestamp a statement gave, in UTC; a time stated without a zone is UTC."""
    if value is None or isinstance(value, dt.datetime):
        moment = value
    else:
        text = str(value).strip().replace(" ", "T", 1).replace("Z", "+00:00")
        try:
            moment = dt.datetime.fromisoformat(text)
        except ValueError:
            raise WriteRefused(f"{value!r} is not a timestamp") from None
    if moment is None:
        return None
    return (moment if moment.tzinfo else moment.replace(tzinfo=dt.UTC)).astimezone(dt.UTC)


def as_date(value: Any) -> dt.date | None:
    if value is None or (isinstance(value, dt.date) and not isinstance(value, dt.datetime)):
        return value
    try:
        return dt.date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        raise WriteRefused(f"{value!r} is not a date") from None


def rfc3339(moment: dt.datetime | None) -> str | None:
    return None if moment is None else moment.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


#: Each writable table's writer methods: find the rows a statement names, insert, update, delete.
_METHODS: dict[str, tuple[str, str, str, str]] = {
    "events": ("find_events", "insert_event", "update_event", "delete_event"),
    "event_attendees": ("find_attendees", "insert_attendee", "update_attendee", "delete_attendee"),
    "tasks": ("find_tasks", "insert_task", "update_task", "delete_task"),
}

#: Makes the writer of one account: (account, the source's reading) -> an async context holding
#: the writer. Built by each provider's loader module (``writer_for``).
WriterFor = Callable[[str], Any]


def _literal(node: exp.Expression, params: list | None) -> Any:
    """The value a VALUES entry or a SET right-hand side states: a literal, NULL, a boolean, or
    a statement parameter. Anything computed is refused."""
    if isinstance(node, exp.Parameter | exp.Placeholder):
        if params is None:
            raise WriteRefused("the statement names a parameter it was not given")
        index = node.this.this if isinstance(node, exp.Parameter) else node.name
        try:
            return params[int(index) - 1]
        except (ValueError, IndexError):
            raise WriteRefused(
                f"the statement names parameter {node.sql()} it was not given"
            ) from None
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return -_number(node.this)
    if isinstance(node, exp.Cast):
        return _literal(node.this, params)
    if isinstance(node, exp.Literal):
        return node.this if node.is_string else _number(node)
    raise WriteRefused(
        f"{node.sql(dialect='postgres')} is not a value: a write to a mail source sets values, "
        "not expressions"
    )


def _number(node: exp.Literal) -> int | float:
    text = node.this
    return float(text) if any(c in text for c in ".eE") else int(text)


def _bound(where: exp.Expression, params: list | None) -> exp.Expression:
    """``where`` with each parameter replaced by its value and no column qualified by a table:
    the predicate as it is evaluated over one row."""

    def bind(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Parameter | exp.Placeholder):
            return cast(exp.Expression, exp.convert(_literal(node, params)))
        if isinstance(node, exp.Column) and node.table:
            return exp.column(node.name)
        return node

    return where.copy().transform(bind)


def _pins(where: exp.Expression) -> dict[str, Any]:
    """The columns the predicate pins to one value by a top-level ``column = value``."""
    pins: dict[str, Any] = {}
    for part in where.flatten() if isinstance(where, exp.And) else [where]:
        if not isinstance(part, exp.EQ):
            continue
        left, right = part.this, part.expression
        if isinstance(right, exp.Column) and not isinstance(left, exp.Column):
            left, right = right, left
        if isinstance(left, exp.Column) and not isinstance(right, exp.Column):
            try:
                pins[left.name] = _literal(right, None)
            except WriteRefused:
                continue
    return pins


_DUCK_TYPES = {
    "text": "VARCHAR",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "timestamptz": "TIMESTAMPTZ",
    "json": "JSON",
}


def holds(where: exp.Expression, table: str, row: dict[str, Any]) -> bool:
    """Whether ``where`` (bound, unqualified) holds for ``row`` of ``table``."""
    from provisa.core import engine_memory

    columns = cm.ir_columns(table)
    with engine_memory.connect() as conn:
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute(
            "CREATE TABLE r ("
            + ", ".join(f'"{name}" {_DUCK_TYPES[kind]}' for name, kind in columns)
            + ")"
        )
        values = [
            json.dumps(row.get(name), default=str) if kind == "json" else row.get(name)
            for name, kind in columns
        ]
        conn.execute(f"INSERT INTO r VALUES ({', '.join('?' for _ in columns)})", values)
        predicate = where.sql(dialect="duckdb")
        answer = conn.execute(f"SELECT count(*) FROM r WHERE {predicate}").fetchone()
    return answer is not None and answer[0] == 1


class MailWriteDriver(DirectDriver):
    """The DIRECT driver of a mail source's writes. ``writer_for(account)`` opens the writer of
    one account (an async context manager); ``accounts`` answers the source's accounts now;
    ``provider`` is ``canonical_mail.GOOGLE`` or ``MICROSOFT``."""

    def __init__(
        self,
        provider: str,
        accounts: Callable[[], Awaitable[tuple[str, ...]]],
        writer_for: WriterFor,
    ) -> None:
        self._provider = provider
        self._accounts = accounts
        self._writer_for = writer_for
        self._connected = False

    async def connect(self, host, port, database, user, password, min_pool=1, max_pool=5) -> None:
        self._connected = True

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def close(self) -> None:
        self._connected = False

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        statement = sqlglot.parse_one(sql, read="postgres")
        if isinstance(statement, exp.Insert):
            count = await self._insert(statement, params)
        elif isinstance(statement, exp.Update):
            count = await self._change(statement, params, delete=False)
        elif isinstance(statement, exp.Delete):
            count = await self._change(statement, params, delete=True)
        else:
            raise WriteRefused("a mail source takes INSERT, UPDATE and DELETE only")
        return QueryResult(rows=[], column_names=[], rowcount=count)

    # -- what a statement writes ---------------------------------------------------------------

    def _table(self, target: exp.Expression) -> str:
        table = target.this if isinstance(target, exp.Schema) else target
        name = table.name if isinstance(table, exp.Table) else ""
        if name not in _METHODS:
            raise WriteRefused(
                f"{name or target.sql()} takes no writes; a mail source writes "
                f"{', '.join(_METHODS)}"
            )
        return name

    def _settable(self, table: str, columns: list[str]) -> None:
        populated = {c.name for c in cm.TABLES[table].columns if c.populated_by(self._provider)}
        writable = cm.WRITABLE_COLUMNS[table] & populated
        refused = [c for c in columns if c not in writable and c != "provider"]
        if refused:
            raise WriteRefused(
                f"{table}.{', '.join(refused)} is not written by a write to a "
                f"{self._provider} source; it writes {', '.join(sorted(writable))}"
            )

    async def _account(self, stated: Any) -> str:
        accounts = tuple(a.lower() for a in await self._accounts())
        if stated is None:
            if len(accounts) != 1:
                raise WriteRefused(
                    "this source reads more than one account: name the account the write is for"
                )
            return accounts[0]
        if str(stated).lower() not in accounts:
            raise WriteRefused(f"{stated} is not an account of this source")
        return str(stated).lower()

    def _provider_stated(self, value: Any) -> None:
        if value is not None and value != self._provider:
            raise WriteRefused(f"provider {value!r}: this source is {self._provider}")

    async def _insert(self, statement: exp.Insert, params: list | None) -> int:
        target = statement.this
        table = self._table(target)
        if not isinstance(target, exp.Schema) or not target.expressions:
            raise WriteRefused(f"an INSERT into {table} names the columns it sets")
        columns = [c.name for c in target.expressions]
        self._settable(table, columns)
        source = statement.expression
        if not isinstance(source, exp.Values):
            raise WriteRefused(f"an INSERT into {table} states its rows as VALUES")
        _find, insert, _update, _delete = _METHODS[table]
        count = 0
        for tuple_ in source.expressions:
            values = dict(zip(columns, (_literal(v, params) for v in tuple_.expressions)))
            self._provider_stated(values.pop("provider", None))
            account = await self._account(values.pop("account", None))
            async with self._writer_for(account) as writer:
                await getattr(writer, insert)(values)
            count += 1
        return count

    async def _change(
        self, statement: exp.Update | exp.Delete, params: list | None, *, delete: bool
    ) -> int:
        table = self._table(statement.this)
        where_clause = statement.args.get("where")
        if where_clause is None:
            raise WriteRefused(f"a write to {table} names the rows it changes in its WHERE")
        where = _bound(where_clause.this, params)
        values: dict[str, Any] = {}
        if not delete:
            for assignment in statement.expressions:
                values[assignment.this.name] = _literal(assignment.expression, params)
            self._settable(table, list(values))
            self._provider_stated(values.pop("provider", None))
            if "account" in values:
                raise WriteRefused("a row is not moved to another account by a write")
        pins = _pins(where)
        self._provider_stated(pins.get("provider"))
        account = await self._account(pins.get("account"))
        find, _insert, update, remove = _METHODS[table]
        count = 0
        async with self._writer_for(account) as writer:
            for found in await getattr(writer, find)(pins):
                if not holds(where, table, found.row):
                    continue
                if delete:
                    await getattr(writer, remove)(found.handle)
                else:
                    await getattr(writer, update)(found.handle, values)
                count += 1
        return count

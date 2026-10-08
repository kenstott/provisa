# Copyright (c) 2026 Kenneth Stott
# Canary: bf17ffc0-d05f-48d8-a98d-8d20617e60f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's own temporary tables (REQ-615, REQ-1926, REQ-1942).

The one exception to "nothing is defined through a query protocol": a session may create, write,
read and drop temporary tables of its own. They are not model objects, no other session sees
them, they end with the session, and a write into one is not a mutation -- it never reaches a
source or the environment's data.

A temporary table is held in the serving engine's store, in a schema of the session's own, so a
statement reads it beside governed tables through the one pipeline. This module is the session's
registry and the reading of a statement: what it does to a temporary table, as a governed SELECT
giving the rows it takes (run through the pipeline as the session's role, like any read) and what
is then done with them. :mod:`provisa.pgwire.temp_exec` does it.
"""

# Requirements: REQ-615, REQ-1926, REQ-1942

from __future__ import annotations

import uuid
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

from provisa.compiler.definitions import (
    _LEADING_COMMENTS_RE,
    _OPENS_TEMPORARY_TABLE_RE,
    NotAvailableHere,
)


@dataclass
class TempTable:
    name: str
    columns: list[tuple[str, str]]  # (name, type as declared or inferred)


@dataclass
class TempSession:
    """One session's temporary tables. ``id`` names its schema in the store."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    tables: dict[str, TempTable] = field(default_factory=dict)
    # Whether its store schema exists: created with its first table, dropped when it ends.
    stored: bool = False
    # Advanced each time a table of it is created or dropped: what a statement kept for the
    # session was governed against.
    generation: int = 0


_SESSION: ContextVar[TempSession | None] = ContextVar("temp_session", default=None)


def bind(session: TempSession) -> Token[TempSession | None]:
    """Make ``session`` the one the current context's statements belong to."""
    return _SESSION.set(session)


def unbind(token: Token[TempSession | None]) -> None:
    _SESSION.reset(token)


def current() -> TempSession | None:
    return _SESSION.get()


def names() -> frozenset[str]:
    """The temporary tables the current session holds."""
    session = _SESSION.get()
    return frozenset(session.tables) if session is not None else frozenset()


def slot_key() -> list[tuple[str, str]]:
    """What sets a statement governed in the current session apart from the same words governed
    anywhere else: nothing while the session holds no temporary table, else the session and the
    state of its tables."""
    session = _SESSION.get()
    if session is None or not session.tables:
        return []
    return [("provisa.temp_session", f"{session.id}:{session.generation}")]


def reads(tree: Any) -> bool:
    """Whether ``tree`` names one of the session's temporary tables."""
    held = names()
    return bool(held) and any(not t.db and t.name in held for t in tree.find_all(exp.Table))


@dataclass(frozen=True)
class Action:
    """What a statement does to a temporary table.

    ``select``: the governed SELECT whose rows it takes -- a CREATE ... AS SELECT's, an INSERT's
    source -- run through the pipeline as any read; a statement that takes none reads nothing.
    ``rewrite``: for an UPDATE or a DELETE, the SELECT over the table itself giving its rows
    afterwards, the table named ``{table}``.
    """

    kind: str  # create | insert | rewrite | drop
    name: str
    select: str
    columns: list[tuple[str, str]] | None = None  # declared (create) or named (insert)
    rewrite: str | None = None


#: A statement that takes no rows of its own still passes the pipeline as a read of nothing.
_NOTHING = "SELECT 1 AS n WHERE FALSE"


def _target(node: Any, what: str) -> tuple[str, list[Any]]:
    """The table a statement names and the column list it gives, refusing a qualified name."""
    table, given = (
        (node.this, list(node.expressions)) if isinstance(node, exp.Schema) else (node, [])
    )
    if not isinstance(table, exp.Table) or table.db or table.catalog:
        raise NotAvailableHere(
            f"{what}: a temporary table is named by its own name, with no schema"
        )
    return table.name, given


def _is_temporary(tree: exp.Create) -> bool:
    properties = tree.args.get("properties")
    return properties is not None and any(
        isinstance(p, exp.TemporaryProperty) for p in properties.expressions
    )


def action_of(sql: str) -> Action | None:
    """What ``sql`` does to a temporary table of the current session, or None when it does
    nothing to one -- then it is any other statement, and a definition among them is refused
    (provisa.compiler.definitions)."""
    session = _SESSION.get()
    held = names()
    # Told by its opening words before anything is parsed: the pipeline parses a statement once,
    # and every statement passes here.
    opening = _LEADING_COMMENTS_RE.sub("", sql, count=1)
    verb = opening.split(None, 1)[0].upper() if opening.strip() else ""
    if verb == "CREATE":
        if not _OPENS_TEMPORARY_TABLE_RE.match(opening):
            return None
    elif verb not in ("INSERT", "UPDATE", "DELETE", "DROP") or not held:
        return None
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return None  # the pipeline's own parse says what is wrong with it
    if isinstance(tree, exp.Create):
        if str(tree.args.get("kind", "")).upper() != "TABLE" or not _is_temporary(tree):
            return None
        if session is None:
            raise NotAvailableHere(
                "CREATE TEMPORARY TABLE needs a session to belong to: send it on a connection, "
                "or in one request with the statements that use it"
            )
        name, given = _target(tree.this, "CREATE TEMPORARY TABLE")
        if name in held:
            raise ValueError(f"temporary table {name!r} already exists in this session")
        if tree.expression is not None:
            if given:
                raise NotAvailableHere(
                    "CREATE TEMPORARY TABLE ... AS takes its columns from the SELECT"
                )
            return Action("create", name, tree.expression.sql(dialect="postgres"))
        columns = [
            (c.name, c.args["kind"].sql(dialect="postgres"))
            for c in given
            if isinstance(c, exp.ColumnDef) and c.args.get("kind") is not None
        ]
        if not columns or len(columns) != len(given):
            raise NotAvailableHere(
                f"CREATE TEMPORARY TABLE {name}: give each column a name and a type, or AS SELECT"
            )
        return Action("create", name, _NOTHING, columns)
    if isinstance(tree, exp.Drop):
        if str(tree.args.get("kind", "")).upper() != "TABLE":
            return None
        dropped = [t for t in tree.find_all(exp.Table)]
        if len(dropped) == 1 and not dropped[0].db and dropped[0].name in held:
            return Action("drop", dropped[0].name, _NOTHING)
        return None
    if isinstance(tree, exp.Insert):
        target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        if not isinstance(target, exp.Table) or target.db or target.name not in held:
            return None
        name, given = _target(tree.this, "INSERT")
        source = tree.expression
        if source is None or tree.args.get("returning") is not None:
            raise NotAvailableHere(f"INSERT INTO {name}: give VALUES or a SELECT, no RETURNING")
        table = session.tables[name] if session is not None else None
        assert table is not None  # ``name in held`` says the session holds it
        named = [c.name for c in given] or [c for c, _ in table.columns]
        unknown = [c for c in named if c not in {col for col, _ in table.columns}]
        if unknown:
            raise ValueError(f"temporary table {name!r} has no column {unknown[0]!r}")
        if isinstance(source, exp.Values):
            aliases = ", ".join(f'"{c}"' for c in named)
            select = f"SELECT * FROM ({source.sql(dialect='postgres')}) AS v({aliases})"
        else:
            select = source.sql(dialect="postgres")
        return Action("insert", name, select, [(c, "") for c in named])
    if isinstance(tree, (exp.Update, exp.Delete)):
        target = tree.this
        if not isinstance(target, exp.Table) or target.db or target.name not in held:
            return None
        name = target.name
        if tree.args.get("returning") is not None or tree.args.get("from") is not None:
            raise NotAvailableHere(
                f"{tree.key.upper()} of temporary table {name}: no FROM and no RETURNING"
            )
        assert session is not None
        where = tree.args.get("where")
        condition = where.this.sql(dialect="postgres") if where is not None else "TRUE"
        if isinstance(tree, exp.Delete):
            rewrite = f"SELECT * FROM {{table}} WHERE NOT COALESCE(({condition}), FALSE)"
        else:
            assigned = {
                e.this.name: e.expression.sql(dialect="postgres")
                for e in tree.expressions
                if isinstance(e, exp.EQ) and isinstance(e.this, exp.Column)
            }
            if len(assigned) != len(tree.expressions):
                raise NotAvailableHere(f"UPDATE {name}: each assignment is column = expression")
            columns = [c for c, _ in session.tables[name].columns]
            unknown = [c for c in assigned if c not in columns]
            if unknown:
                raise ValueError(f"temporary table {name!r} has no column {unknown[0]!r}")
            parts = [
                f'CASE WHEN COALESCE(({condition}), FALSE) THEN ({assigned[c]}) ELSE "{c}" END '
                f'AS "{c}"'
                if c in assigned
                else f'"{c}"'
                for c in columns
            ]
            rewrite = f"SELECT {', '.join(parts)} FROM {{table}}"
        return Action("rewrite", name, _NOTHING, rewrite=rewrite)
    return None

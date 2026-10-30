# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8e2c61-0f37-4b9d-b6d4-c29e71f3a058
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's U and D row rules on the model store (REQ-2188, REQ-1919).

Semantic objects are models too. The rule an UPDATE or DELETE of a model table is held to is the
acting role's U or D rule on that table, resolved as a data table's are (inheritance, domain and
tag reach, by the runtime build). It is applied at the one place every repository statement passes
through, :meth:`provisa.core.database.Connection._execute_core`, beside the tenant guard, so no
route can miss it: GraphQL, REST and MCP all write through the same repositories.

A rule only narrows. It gives no right: the role's R or RW on the model is the ceiling, decided
above the store by the right the route already gates on. A role with no rule for a right is not
narrowed.

A statement is refused whole: when a row it matches is outside the rule, it is refused by name and
nothing is written, never rewritten to change only the rows that pass. A statement that matches no
row is no refusal: there is no row to narrow, and the UPDATE half of an upsert of a new object
matches none (RW includes create).

WHO IS NOT NARROWED. Only a person is narrowed. A statement with no acting role (a boot, the
scheduler, a background worker) has no one to hold to a rule. A config load has no acting person
either, even when an admin's request starts it, and neither does a rebuild, a deployment, an
undo or anything else that writes the model as a whole (:func:`provisa.core.model_change.committed_by_caller`):
those run :func:`unnarrowed`.
"""

# Requirements: REQ-2188, REQ-1919

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlalchemy import Delete, Update, literal_column

from provisa.core import model_change
from provisa.core.request_context import current_acting_role
from provisa.core.session_terms import resolve_session_terms

#: A statement's right: what an UPDATE or a DELETE does to a row that exists.
_RIGHTS: tuple[tuple[type, str, str], ...] = ((Update, "U", "UPDATE"), (Delete, "D", "DELETE"))

#: A backend's name in sqlglot, where it differs from the connection's.
_SQLGLOT_DIALECT = {"postgresql": "postgres", "mssql": "tsql", "singlestoredb": "singlestore"}


class ModelRowRefused(PermissionError):
    """A write of a semantic object outside the acting role's U or D rule."""

    def __init__(self, verb: str, role: str, table: str, right: str, why: str) -> None:
        super().__init__(f"{verb} of {table}: role {role!r}'s {right} rule {why}")
        self.role = role
        self.table = table
        self.right = right


@dataclass(frozen=True)
class ModelRule:
    """What holds a role's writes of one model table: the predicate (in the model's column names),
    the session values it reads, and the ``*_meta`` view the model is registered through, which a
    column the table does not hold is read from."""

    predicate: str
    session_vars: dict[str, str] = field(default_factory=dict)
    view: str | None = None


#: (acting role, table, right) -> its rule. The application registers it; a process with no
#: application (tooling, a bare control plane) has no runtime build, so no rule applies.
_Provider = Callable[[str, str, str], ModelRule | None]
_provider: _Provider | None = None


def register_rule_provider(fn: _Provider | None) -> None:
    global _provider
    _provider = fn


_unnarrowed: ContextVar[bool] = ContextVar("provisa_model_unnarrowed", default=False)


@contextmanager
def unnarrowed() -> Iterator[None]:
    """A body that writes the model with no person acting, whoever's request started it."""
    token = _unnarrowed.set(True)
    try:
        yield
    finally:
        _unnarrowed.reset(token)


@dataclass(frozen=True)
class Narrowing:
    """One statement held to a rule."""

    verb: str
    role: str
    table: str
    right: str
    predicate: Any


def _column_names(tree: exp.Expression) -> set[str]:
    return {c.name for c in tree.find_all(exp.Column)}


def _rendered(tree: exp.Expression, dialect: str) -> str:
    return tree.sql(dialect=_SQLGLOT_DIALECT.get(dialect, dialect))


def _predicate(
    table: Any, rule: ModelRule, dialect: str, refuse: Callable[[str], Exception]
) -> Any:
    """The rule as a criterion on ``table``: over its own columns where it holds them all; else,
    a column only the model's view holds, as the rows of the view it admits."""
    parsed = sqlglot.parse_one(rule.predicate, read="postgres")
    tree = resolve_session_terms(parsed, rule.session_vars)  # pyright: ignore[reportArgumentType]  # sqlglot stub types parse_one as Expr
    outside = _column_names(tree) - set(table.c.keys())
    if not outside:
        # literal_column, not text(): a literal in a rule may hold a colon (a meta-role's id),
        # which text() would take for a bind parameter.
        return literal_column(f"({_rendered(tree, dialect)})")
    key = list(table.primary_key.columns)
    if rule.view is None or len(key) != 1:
        raise refuse(f"reads {sorted(outside)}, which {table.name} does not hold")
    column = key[0].name
    through_view = sqlglot.parse_one(
        f'"{column}" IN (SELECT "{column}" FROM "{rule.view}" WHERE {tree.sql(dialect="postgres")})',
        read="postgres",
    )
    return literal_column(f"({_rendered(through_view, dialect)})")  # pyright: ignore[reportArgumentType]  # sqlglot stub types parse_one as Expr


def narrowing_of(stmt: Any, dialect: str) -> Narrowing | None:
    """The rule ``stmt`` is held to, or None when it is held to none: not an UPDATE or DELETE, no
    acting person, or a role with no rule for the right on that table."""
    if _provider is None or _unnarrowed.get():
        return None
    match = next(((right, verb) for kind, right, verb in _RIGHTS if isinstance(stmt, kind)), None)
    role = current_acting_role.get()
    if match is None or role is None or model_change.writes_whole_model():
        return None
    right, verb = match
    table = stmt.table
    rule = _provider(role, str(getattr(table, "name", "")), right)
    if rule is None:
        return None

    def refuse(why: str) -> ModelRowRefused:
        return ModelRowRefused(verb, role, table.name, right, why)

    return Narrowing(verb, role, table.name, right, _predicate(table, rule, dialect, refuse))


def refused(narrowing: Narrowing) -> ModelRowRefused:
    """The refusal of a statement that matched a row the rule does not admit."""
    return ModelRowRefused(
        narrowing.verb,
        narrowing.role,
        narrowing.table,
        narrowing.right,
        "does not admit a row this write touches; nothing was written",
    )

# Copyright (c) 2026 Kenneth Stott
# Canary: 769bb0d4-51be-4aa9-aef6-023f13c75bd9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column transform in a common table expression's select list: a staged, selective step
(REQ-2131, amended 2026-10-27, "A TRANSFORM IN A CTE IS A STAGED, SELECTIVE STEP" and "EACH CTE IS
CLASSIFIED BEFORE IT IS STAGED").

A CTE holding a transform runs on its own: its rows are read, governed as the statement's whole
read was, enriched by the transform (:func:`provisa.codesource.columns_fold.apply`) and landed in a
session temporary table, and the statement reads that table where it named the CTE, so the rest of
it -- filters, joins and grouping on the computed columns -- runs in the engine as ordinary SQL
(:func:`stage`).

Before it is staged, each such CTE is classified (:func:`classify`): what restricts its rows in the
statement -- a filter, a join, both or neither -- and, for each restricting condition, when its
values are known: in advance (literals, bound parameters, session variables), from another relation
(join keys), or only once the transform has run (it names a computed column). A condition known in
advance is pushed into the CTE's read; a join's other side is read first and the CTE's read is
restricted to the keys it yields; a condition on a computed column stays where it is. Each rewrite
is made only where it leaves the answer unchanged: never across an outer join, an aggregate over
the CTE or a LIMIT on it, nor into a CTE whose own read groups, limits or windows its rows. With
nothing known in advance and no join to run first, the CTE runs as written.

A statement that holds no transform in a CTE is not touched here (:func:`stage` returns None).
"""

# Requirements: REQ-2131

from __future__ import annotations

import uuid
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Literal

import sqlglot
import sqlglot.expressions as exp

#: When a restricting condition's values are known.
Known = Literal["advance", "join", "computed"]
#: What restricts a CTE's rows in the statement.
Restricted = Literal["filter", "join", "both", "neither"]


@dataclass(frozen=True)
class Condition:
    """A condition of the statement on the CTE, and when its values are known."""

    sql: str
    known: Known


@dataclass(frozen=True)
class JoinKey:
    """The CTE's inner (or EXISTS) join to another relation on one of its input columns: the CTE's
    column, the other relation as the statement reads it and the name it reads it by, its key, and
    the statement's conditions on that relation alone."""

    column: str
    relation: str
    other: str
    key: str
    conditions: tuple[str, ...] = ()


@dataclass(frozen=True)
class Classified:
    """What restricts a CTE's rows, each restricting condition with when its values are known, the
    join a key read can restrict it by, and whether any rewrite may be made at all."""

    cte: str
    restricted: Restricted
    conditions: tuple[Condition, ...]
    join: JoinKey | None
    rewritable: bool

    @property
    def pushed(self) -> tuple[str, ...]:
        """The conditions pushed into the CTE's read: those known in advance."""
        if not self.rewritable:
            return ()
        return tuple(c.sql for c in self.conditions if c.known == "advance")


def _conjuncts(where: exp.Expr | None) -> list[exp.Expr]:
    if where is None:
        return []
    return list(where.flatten()) if isinstance(where, exp.And) else [where]


def _relations(tree: exp.Select) -> list[exp.Expr]:
    """The relations the select reads, its FROM's and each join's."""
    found = []
    origin = tree.args.get("from_")
    if origin is not None:
        found.append(origin.this)
    found += [j.this for j in tree.args.get("joins") or []]
    return found


def _named(relation: exp.Expr) -> str:
    return relation.alias_or_name


def _own_columns(node: exp.Expr) -> list[exp.Column]:
    """The columns ``node`` names outside any subquery of its own."""
    return [
        c for c in node.find_all(exp.Column) if c.find_ancestor(exp.Subquery, exp.Exists) is None
    ]


def _belongs(column: exp.Column, alias: str, only: bool) -> bool:
    """Whether ``column`` is the CTE's: qualified by its alias, or unqualified where the CTE is the
    only relation read."""
    return column.table == alias or (not column.table and only)


def _aggregated(tree: exp.Select) -> bool:
    return bool(
        tree.args.get("group")
        or tree.args.get("having")
        or any(e.find(exp.AggFunc) for e in tree.expressions)
    )


def _limited(tree: exp.Select) -> bool:
    return bool(tree.args.get("limit") or tree.args.get("offset") or tree.args.get("fetch"))


def _outer_joined(tree: exp.Select) -> bool:
    return any(j.side for j in tree.args.get("joins") or [])


def _key_of(eq: exp.Expr, alias: str, computed: set[str]) -> tuple[str, str, str] | None:
    """``(the CTE's input column, the other relation's name, its key)`` of an equality between a
    column of the CTE and one of another relation."""
    if not isinstance(eq, exp.EQ):
        return None
    left, right = eq.this, eq.expression
    if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
        return None
    for mine, theirs in ((left, right), (right, left)):
        if mine.table == alias and theirs.table not in ("", alias) and mine.name not in computed:
            return mine.name, theirs.table, theirs.sql(dialect="postgres")
    return None


def _on_alone(conjuncts: list[exp.Expr], other: str) -> tuple[str, ...]:
    """The conjuncts that read only the relation ``other``: what restricts it alone."""
    return tuple(
        c.sql(dialect="postgres")
        for c in conjuncts
        if (cols := _own_columns(c))
        and all(col.table == other for col in cols)
        and not c.find(exp.Subquery, exp.Exists)
    )


def _exists_key(conjunct: exp.Expr, alias: str, computed: set[str]) -> JoinKey | None:
    """The join an ``EXISTS (SELECT ... FROM r WHERE r.k = cte.col [AND conditions on r])`` is."""
    if not isinstance(conjunct, exp.Exists) or not isinstance(conjunct.this, exp.Select):
        return None
    inner = conjunct.this
    relations = _relations(inner)
    if len(relations) != 1 or not isinstance(relations[0], exp.Table):
        return None
    other = _named(relations[0])
    key, rest = None, []
    for c in _conjuncts(inner.args["where"].this if inner.args.get("where") else None):
        found = _key_of(c, alias, computed)
        if found is not None and found[1] == other and key is None:
            key = found
            continue
        if any(col.table != other for col in c.find_all(exp.Column)):
            return None  # it reads the CTE otherwise than by the key
        rest.append(c.sql(dialect="postgres"))
    if key is None:
        return None
    return JoinKey(key[0], relations[0].sql(dialect="postgres"), other, key[2], tuple(rest))


def classify(tree: exp.Select, cte: str, computed: set[str]) -> Classified:
    """How the statement ``tree`` restricts the rows of its CTE ``cte``, whose select list computes
    ``computed`` (the transforms' output columns): each condition on it -- a WHERE conjunct, an
    inner join's ON, an EXISTS -- with when its values are known, the join a key read can restrict
    it by, and whether a rewrite may be made at all."""
    relations = _relations(tree)
    reading = [r for r in relations if isinstance(r, exp.Table) and r.name == cte and not r.db]
    only = len(relations) == 1
    alias = _named(reading[0]) if len(reading) == 1 else cte
    tables = (
        {_named(r): r for r in relations if isinstance(r, exp.Table) and r is not reading[0]}
        if reading
        else {}
    )
    where = _conjuncts(tree.args["where"].this if tree.args.get("where") else None)
    conditions: list[Condition] = []
    joins: list[JoinKey] = []

    def known_of(node: exp.Expr) -> Known | None:
        """When ``node``'s values are known, if it restricts the CTE at all."""
        own = _own_columns(node)
        mine = [c for c in own if _belongs(c, alias, only)]
        if not mine:
            return None
        if any(c.name in computed for c in mine):
            return "computed"
        if len(mine) < len(own) or node.find(exp.Subquery, exp.Exists):
            return "join"  # its values come from another relation
        return "advance"

    for join in tree.args.get("joins") or []:
        on = _conjuncts(join.args.get("on"))
        for c in on:
            known = known_of(c)
            if known is None:
                continue
            conditions.append(Condition(c.sql(dialect="postgres"), known))
            key = _key_of(c, alias, computed) if known == "join" and not join.side else None
            if key is not None and key[1] in tables and key[1] not in _cte_names(tree):
                joins.append(
                    JoinKey(
                        key[0],
                        tables[key[1]].sql(dialect="postgres"),
                        key[1],
                        key[2],
                        _on_alone([*on, *where], key[1]),
                    )
                )
    for c in where:
        exists = _exists_key(c, alias, computed)
        if exists is not None:
            conditions.append(Condition(c.sql(dialect="postgres"), "join"))
            joins.append(exists)
            continue
        known = known_of(c)
        if known is not None:
            conditions.append(Condition(c.sql(dialect="postgres"), known))
    filtered = any(c.known != "join" for c in conditions)
    restricted: Restricted = (
        "both" if filtered and joins else "filter" if filtered else "join" if joins else "neither"
    )
    rewritable = (
        len(reading) == 1
        and not _outer_joined(tree)
        and not _aggregated(tree)
        and not _limited(tree)
    )
    return Classified(
        cte=cte,
        restricted=restricted,
        conditions=tuple(conditions),
        join=joins[0] if rewritable and len(joins) == 1 else None,
        rewritable=rewritable,
    )


def _cte_names(tree: exp.Select) -> set[str]:
    with_ = tree.args.get("with_")
    return {c.alias for c in with_.expressions} if with_ is not None else set()


# -- staging ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Stage:
    """A CTE holding a transform, run on its own and landed in the session table ``table``.

    ``body`` is its read: the CTE's statement (the CTEs before it that it reads kept with it), each
    condition known in advance pushed in. With a ``key_read`` -- the other side of its join, read
    first -- its read is restricted to the keys that yields (:meth:`restricted`)."""

    cte: str
    table: str
    body: str
    key_read: str | None
    key_column: str | None
    classified: Classified

    def restricted(self, keys: list) -> str:
        """``body`` restricted to the rows whose key column is one of ``keys``."""
        from provisa.compiler.sql_literals import sql_literal

        tree = sqlglot.parse_one(self.body, read="postgres")
        assert isinstance(tree, exp.Select) and self.key_column is not None
        condition = (
            sqlglot.parse_one(
                f"{self.key_column} IN ({', '.join(sql_literal(k, 'postgres') for k in keys)})",
                read="postgres",
            )
            if keys
            else exp.false()
        )
        return tree.where(condition, copy=False).sql(dialect="postgres")


@dataclass(frozen=True)
class Staged:
    """A statement whose CTEs holding a transform are staged: each stage in the CTEs' order, and
    the statement as it then runs, each staged CTE read from its table."""

    stages: tuple[Stage, ...]
    outer: str


def _calls_in(select: exp.Select, by_name: Mapping[str, str]) -> list[exp.Anonymous]:
    return [
        item
        for item in select.expressions
        if isinstance(item, exp.Anonymous) and item.name.lower() in by_name
    ]


def _pushable_body(body: exp.Select) -> bool:
    """Whether a condition can be added to the CTE's own read without changing which rows it
    gives: it neither groups, limits, de-duplicates nor windows them."""
    return not (
        _aggregated(body)
        or _limited(body)
        or body.args.get("distinct")
        or any(e.find(exp.Window) for e in body.expressions)
    )


def _body_expression(body: exp.Select, column: str) -> exp.Expr | None:
    """The expression of the CTE's select list its output ``column`` is."""
    for item in body.expressions:
        if item.alias_or_name == column:
            return item.this if isinstance(item, exp.Alias) else item
    return None


def _in_body(condition: str, alias: str, body: exp.Select) -> exp.Expr | None:
    """``condition`` (on the CTE's columns, as the statement reads them) as a condition of the CTE's
    own read, each column replaced by the expression it is there; None where one is not there."""
    tree = sqlglot.parse_one(condition, read="postgres")
    for column in list(tree.find_all(exp.Column)):
        if column.table not in ("", alias):
            return None
        expression = _body_expression(body, column.name)
        if expression is None or expression.find(exp.AggFunc, exp.Window):
            return None
        column.replace(expression.copy())
    return tree


def _rename(tree: exp.Expr, tables: Mapping[str, str]) -> None:
    """Each read of a staged CTE in ``tree`` is a read of its table, under the name it was read
    by."""
    for table in list(tree.find_all(exp.Table)):
        if table.db or table.name not in tables:
            continue
        alias = table.alias or table.name
        table.set("this", exp.to_identifier(tables[table.name]))
        table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))


def _holding(tree: exp.Expr, by_name: Mapping[str, str]) -> dict[str, list[exp.Anonymous]]:
    """Each CTE of the statement ``tree`` holding a column transform as an item of its select list,
    with those calls."""
    if not isinstance(tree, exp.Select) or tree.args.get("with_") is None:
        return {}
    return {
        c.alias: calls
        for c in tree.args["with_"].expressions
        if isinstance(c.this, exp.Select) and (calls := _calls_in(c.this, by_name))
    }


def holds(sql: str, transforms: Collection[str]) -> bool:
    """Whether a CTE of ``sql`` holds one of ``transforms`` (their names) as an item of its select
    list: the statement is run staged (:func:`stage`)."""
    from provisa.codesource.columns_fold import mentions

    if not mentions(sql, transforms):
        return False
    by_name = {name.lower(): name for name in transforms}
    return bool(_holding(sqlglot.parse_one(sql, read="postgres"), by_name))


def stage(
    sql: str,
    transforms: Mapping[str, dict],
    admit: Callable[[dict, str], None],
) -> Staged | None:
    """The staged form of the governed statement ``sql`` when a CTE of it holds a registered column
    transform (``transforms``, by name) as an item of its select list, else None: the statement
    runs as it is. ``admit(command, name)`` refuses a call the role may not make."""
    from provisa.codesource.columns_fold import mentions

    if not mentions(sql, transforms):
        return None
    tree = sqlglot.parse_one(sql, read="postgres")
    by_name = {name.lower(): name for name in transforms}
    holding = _holding(tree, by_name)
    if not holding:
        return None
    assert isinstance(tree, exp.Select)  # _holding finds none in any other statement
    ctes = list(tree.args["with_"].expressions)
    tables = {name: f"__provisa_stage_{uuid.uuid4().hex[:12]}" for name in holding}
    stages: list[Stage] = []
    earlier: list[exp.CTE] = []
    for cte in ctes:
        if cte.alias not in holding:
            earlier.append(cte)
            continue
        body = cte.this.copy()
        computed: set[str] = set()
        for call in holding[cte.alias]:
            command = transforms[by_name[call.name.lower()]]
            admit(command, by_name[call.name.lower()])
            computed |= {c["name"] for c in command["output_columns"]}
        classified = classify(tree, cte.alias, computed)
        reading = [r for r in _relations(tree) if isinstance(r, exp.Table) and r.name == cte.alias]
        alias = _named(reading[0]) if reading else cte.alias
        key_read = key_column = None
        if classified.rewritable and _pushable_body(body) and not cte.args.get("columns"):
            for condition in classified.pushed:
                pushed = _in_body(condition, alias, body)
                if pushed is not None:
                    body.where(pushed, copy=False)
            join = classified.join
            if join is not None:
                key = _body_expression(body, join.column)
                if key is not None and not key.find(exp.AggFunc, exp.Window):
                    key_column = key.sql(dialect="postgres")
                    where = f" WHERE {' AND '.join(join.conditions)}" if join.conditions else ""
                    key_read = f"SELECT DISTINCT {join.key} FROM {join.relation}{where}"
        _rename(body, tables)
        read = body
        if earlier:
            kept = [e.copy() for e in earlier]
            for e in kept:
                _rename(e, tables)
            read = body.copy()
            read.set("with_", exp.With(expressions=kept))
        stages.append(
            Stage(
                cte=cte.alias,
                table=tables[cte.alias],
                body=read.sql(dialect="postgres"),
                key_read=key_read,
                key_column=key_column,
                classified=classified,
            )
        )
    outer = tree.copy()
    left = [c for c in outer.args["with_"].expressions if c.alias not in holding]
    outer.set("with_", exp.With(expressions=left) if left else None)
    _rename(outer, tables)
    return Staged(tuple(stages), outer.sql(dialect="postgres"))

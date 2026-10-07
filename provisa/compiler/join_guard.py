# Copyright (c) 2026 Kenneth Stott
# Canary: b4432b97-dda6-46bf-8ed8-e3fde9b57b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationships govern how tables are related, however it is written (REQ-603).

Two registered tables are RELATED whenever rows of one are matched or filtered against rows of
the other by comparing their columns: a join of any spelling (ON, USING, NATURAL, a comma or a
CROSS JOIN with the comparison in WHERE), through a CTE or a derived table, ``x IN`` / ``NOT IN
(SELECT ...)``, EXISTS / NOT EXISTS, a correlated scalar subquery, LATERAL. Every such pairing of
columns must be a registered relationship between those two tables on those columns, in either
direction. Matching by anything else -- an inequality, an expression, no condition at all -- is
refused.

Not relating, so needing no relationship: the branches of a UNION / INTERSECT / EXCEPT, and a
subquery that refers to nothing outside itself (a column compared with another table's
aggregate).

The rule is stated over table INSTANCES -- each time a statement reads a registered table, so a
self-join needs a registered self-relationship like any other pair -- and every column reference
is traced to the instance it comes from, through CTE, derived-table and subquery boundaries. A
column that is an expression over a base column is not that column: an equality on it is not a
column equality. A registered view is the registered table it is; the statement is checked as
written, before any view is expanded, so a view's internals -- governed when it was registered
-- are not examined again, and a relationship to a view is registered on the view.
"""

# Requirements: REQ-603

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Any

import sqlglot.expressions as exp
from sqlglot.optimizer.scope import Scope, build_scope


@dataclass(frozen=True)
class _Base:
    """A column of one instance of a registered table."""

    instance: int  # id() of the exp.Table node reading it
    table_id: int
    column: str


#: Why two tables are related outside the registered relationships.
UNREGISTERED = "unregistered"  # a column equality that is no registered relationship
NOT_EQUALITY = "not_equality"  # matched by something other than a column equality
NO_CONDITION = "no_condition"  # combined with nothing relating them


@dataclass(frozen=True)
class Unrelated:
    """Two tables a statement relates outside the registered relationships, and how
    (``reason``); the columns of the equality relating them, for an UNREGISTERED one."""

    left_table: int
    right_table: int
    reason: str
    left_column: str | None = None
    right_column: str | None = None


def _selects(scope: Scope) -> list[exp.Expr]:
    """What ``scope``'s query projects."""
    query: Any = scope.expression
    # A LATERAL wraps its query; a table function (UNNEST, a row generator) projects no column
    # of any table.
    while not isinstance(query, exp.Query):
        query = query.args.get("this")
        if not isinstance(query, exp.Expr):
            return []
    return query.selects


class _Resolver:
    def __init__(self, resolve_table_id: Any, columns_of: Any) -> None:
        self._table_id = resolve_table_id
        self._columns_of = columns_of
        self._instances: dict[int, list[frozenset[int]]] = {}
        self.table_of: dict[int, int] = {}  # instance -> table id

    def _base(self, table: exp.Table) -> _Base | None:
        table_id = self._table_id(table)
        if table_id is None:
            return None
        self.table_of[id(table)] = table_id
        return _Base(id(table), table_id, "")

    def alternatives(self, scope: Scope) -> list[frozenset[int]]:
        """The sets of instances ``scope`` combines: one set, or one per choice of branch where it
        reads a UNION (the branches of a union are alternatives, never combined with each other)."""
        key = id(scope)
        if key in self._instances:
            return self._instances[key]
        self._instances[key] = [frozenset()]  # a recursive CTE reads itself: nothing more
        if scope.union_scopes:
            found = [alt for branch in scope.union_scopes for alt in self.alternatives(branch)]
        else:
            per_source: list[list[frozenset[int]]] = []
            for _name, (_node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table):
                    base = self._base(source)
                    per_source.append([frozenset({base.instance})] if base else [frozenset()])
                elif isinstance(source, Scope):
                    per_source.append(self.alternatives(source))
            found = [frozenset().union(*choice) for choice in product(*per_source)]
        self._instances[key] = found
        return found

    def _has_column(self, source: Any, name: str) -> bool:
        if isinstance(source, exp.Table):
            table_id = self._table_id(source)
            return table_id is not None and name in self._columns_of(table_id)
        if isinstance(source, Scope):
            branches = source.union_scopes or [source]
            return any(
                any(s.alias_or_name == name or s.is_star for s in _selects(b)) for b in branches
            )
        return False

    def resolve(
        self, scope: Scope, column: exp.Column, seen: frozenset = frozenset()
    ) -> set[_Base]:
        """The base columns ``column`` of ``scope`` names; empty for a computed value, a column of
        no registered table, and a name nothing in reach holds."""
        marker = (id(scope), column.table, column.name)
        if marker in seen:
            return set()
        seen = seen | {marker}
        if column.table:
            # A CTE the statement names without reading it in this FROM still relates its rows
            # to the rows around the reference: it is resolved as the CTE it names.
            sources = [scope.sources.get(column.table) or scope.cte_sources.get(column.table)]
        else:
            sources = [
                source
                for _node, source in scope.selected_sources.values()
                if self._has_column(source, column.name)
            ]
        sources = [s for s in sources if s is not None]
        if not sources:
            # Not of this scope: a column of the statement around it (a correlated reference).
            return self.resolve(scope.parent, column, seen) if scope.parent is not None else set()
        out: set[_Base] = set()
        for source in sources:
            if isinstance(source, exp.Table):
                base = self._base(source)
                if base is not None:
                    out.add(_Base(base.instance, base.table_id, column.name))
            elif isinstance(source, Scope):
                for branch in source.union_scopes or [source]:
                    out |= self._projection(branch, column.name, seen)
        return out

    def touched(self, scope: Scope, column: exp.Column, seen: frozenset = frozenset()) -> set[int]:
        """Every instance ``column`` of ``scope`` draws on: the one it names, or, where it names a
        value a CTE or a derived table computes, each instance that value is computed from."""
        marker = (id(scope), column.table, column.name)
        if marker in seen:
            return set()
        seen = seen | {marker}
        plain = self.resolve(scope, column)
        if plain:
            return {b.instance for b in plain}
        if column.table:
            sources = [scope.sources.get(column.table) or scope.cte_sources.get(column.table)]
        else:
            sources = [
                source
                for _node, source in scope.selected_sources.values()
                if self._has_column(source, column.name)
            ]
        sources = [s for s in sources if s is not None]
        if not sources:
            return self.touched(scope.parent, column, seen) if scope.parent is not None else set()
        out: set[int] = set()
        for source in sources:
            if not isinstance(source, Scope):
                continue
            for branch in source.union_scopes or [source]:
                for select in _selects(branch):
                    if select.alias_or_name != column.name:
                        continue
                    for inner in select.find_all(exp.Column):
                        out |= self.touched(branch, inner, seen)
        return out

    def _projection(self, scope: Scope, name: str, seen: frozenset) -> set[_Base]:
        if scope.union_scopes:
            return set().union(*(self._projection(b, name, seen) for b in scope.union_scopes))
        out: set[_Base] = set()
        for select in _selects(scope):
            inner = select.unalias()
            if select.is_star:
                table = inner.table if isinstance(inner, exp.Column) else ""
                out |= self.resolve(scope, exp.column(name, table=table or None), seen)
            elif select.alias_or_name == name and isinstance(inner, exp.Column):
                out |= self.resolve(scope, inner, seen)
        return out


def unrelated_tables(
    tree: exp.Expr,
    *,
    resolve_table_id: Any,
    columns_of: Any,
    registered: set[tuple[int, int, str, str]],
    exempt_table: Any,
    same_remote_source: Any,
) -> list[Unrelated]:
    """The pairs of registered tables ``tree`` combines outside the ``registered`` relationships
    -- (table id, table id, column, column), both directions. ``exempt_table(table_id)``: a table
    any table may be read beside (the meta and ops domains). ``same_remote_source(a, b)``: two
    tables of one remote source, whose own model relates them, with no relationship registered
    between them here."""
    queries = [tree] if isinstance(tree, exp.Query) else []
    if not queries:
        queries = [q for q in tree.find_all(exp.Query) if q.find_ancestor(exp.Query) is None]
    found: list[Unrelated] = []
    for query in queries:
        root = build_scope(query)
        if root is not None:
            found += _unrelated_in(
                root, resolve_table_id, columns_of, registered, exempt_table, same_remote_source
            )
    return found


def _conjuncts(condition: exp.Expr | None) -> list[exp.Expr]:
    """``condition`` as the conditions it ANDs together."""
    if condition is None:
        return []
    node = condition.unnest()
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _unrelated_in(
    root: Scope,
    resolve_table_id: Any,
    columns_of: Any,
    registered: set[tuple[int, int, str, str]],
    exempt_table: Any,
    same_remote_source: Any,
) -> list[Unrelated]:
    resolver = _Resolver(resolve_table_id, columns_of)
    scopes = list(root.traverse())
    by_query = {id(s.expression): s for s in scopes}
    edges: set[frozenset[int]] = set()  # registered pairings, by instance pair
    groups: list[frozenset[int]] = []
    found: list[Unrelated] = []
    table_of = resolver.table_of

    def refuse(item: Unrelated) -> None:
        if item not in found:
            found.append(item)

    def exempt(instance: int) -> bool:
        return exempt_table(table_of[instance])

    def pair(left: set[_Base], right: set[_Base]) -> None:
        """A pairing of columns the statement makes: registered, or refused."""
        for a in left:
            for b in right:
                if a.instance == b.instance or exempt(a.instance) or exempt(b.instance):
                    continue
                if (a.table_id, b.table_id, a.column, b.column) in registered or same_remote_source(
                    a.table_id, b.table_id
                ):
                    edges.add(frozenset({a.instance, b.instance}))
                else:
                    x, y = sorted((a, b), key=lambda c: (c.table_id, c.column))
                    refuse(Unrelated(x.table_id, y.table_id, UNREGISTERED, x.column, y.column))

    def other_matching(instances: set[int]) -> None:
        """Instances matched by something other than a column equality."""
        held = sorted(i for i in instances if not exempt(i))
        tables = sorted({table_of[i] for i in held})
        if len(held) >= 2:
            refuse(Unrelated(tables[0], tables[-1], NOT_EQUALITY))

    for scope in scopes:
        own = resolver.alternatives(scope)
        groups += own
        reach = frozenset().union(*own)
        mine = {id(c) for c in scope.columns}
        query = scope.expression
        while not isinstance(query, (exp.Select, exp.SetOperation)):
            query = query.args.get("this")
            if not isinstance(query, exp.Expr):
                break
        conditions: list[exp.Expr] = []
        if isinstance(query, exp.Select):
            for join in query.args.get("joins") or []:
                conditions += _conjuncts(join.args.get("on"))
                named = [u.name for u in join.args.get("using") or []]
                joined = join.this
                source = scope.sources.get(joined.alias_or_name)
                if join.args.get("method") == "NATURAL" and source is not None:
                    named = sorted(
                        self_columns(resolver, source) & others_columns(resolver, scope, source)
                    )
                for name in named:
                    # USING (c) and NATURAL pair the joined table's c with every other c in reach.
                    inside = resolver.resolve(scope, exp.column(name, table=joined.alias_or_name))
                    around = resolver.resolve(scope, exp.column(name)) - inside
                    pair(inside, around)
            where = query.args.get("where")
            conditions += _conjuncts(where.this if where is not None else None)
            having = query.args.get("having")
            # A HAVING condition over aggregates compares totals, not rows of two tables; one
            # with no aggregate in it compares group keys, and is held to the rule.
            conditions += [
                c
                for c in _conjuncts(having.this if having is not None else None)
                if c.find(exp.AggFunc) is None
            ]
        for condition in conditions:
            tested = condition.this.unnest() if isinstance(condition, exp.Not) else condition
            if isinstance(tested, exp.In) and tested.args.get("query") is not None:
                inner = by_query.get(id(tested.args["query"].this))
                if inner is not None:
                    left = tested.this.unnest()
                    given: set[_Base] = set()
                    drawn: set[int] = set()
                    for branch in inner.union_scopes or [inner]:
                        selects = _selects(branch)
                        only = selects[0].unalias() if len(selects) == 1 else None
                        if isinstance(only, exp.Column):
                            given |= resolver.resolve(branch, only)
                        for alt in resolver.alternatives(branch):
                            drawn |= alt
                    mine_plain = (
                        resolver.resolve(scope, left) if isinstance(left, exp.Column) else set()
                    )
                    touching: set[int] = set()
                    for column in left.find_all(exp.Column):
                        touching |= resolver.touched(scope, column)
                    # x IN (SELECT y ...) matches the rows around it with the rows selected.
                    for outer_alt, inner_alt in product(own, resolver.alternatives(inner)):
                        if touching & outer_alt:
                            groups.append(frozenset(touching & outer_alt) | inner_alt)
                    if mine_plain and given:
                        pair(mine_plain, given)
                    elif touching and drawn:
                        other_matching(touching | drawn)
                    continue
            columns = [c for c in condition.find_all(exp.Column) if id(c) in mine]
            touching = set()
            for column in columns:
                touching |= resolver.touched(scope, column)
            if len({i for i in touching if not exempt(i)}) < 2:
                continue  # a condition on one table relates it to no other
            if isinstance(condition, exp.EQ):
                left_side, right_side = condition.left.unnest(), condition.right.unnest()
                if isinstance(left_side, exp.Column) and isinstance(right_side, exp.Column):
                    left_bases = resolver.resolve(scope, left_side)
                    right_bases = resolver.resolve(scope, right_side)
                    if left_bases and right_bases:
                        pair(left_bases, right_bases)
                        continue
            other_matching(touching)
        # A column of the statement around it: a correlated subquery is combined with the
        # instance it reaches out to.
        for column in scope.columns:
            for instance in resolver.touched(scope, column):
                if instance in reach:
                    continue  # one of its own, whichever branch of a union it reads it from
                for alt in own:
                    if alt:
                        groups.append(alt | {instance})

    # Tables a statement combines must be connected by its registered pairings: one combined
    # with nothing relating it to the rest is a product of the two.
    for group in groups:
        members = sorted(i for i in group if i in table_of and not exempt(i))
        if len(members) < 2:
            continue
        component = {i: i for i in members}

        def top(i: int, component: dict[int, int] = component) -> int:
            while component[i] != i:
                i = component[i]
            return i

        for edge in edges:
            a, b = tuple(edge)
            if a in component and b in component:
                component[top(a)] = top(b)
        first = top(members[0])
        apart = next((m for m in members[1:] if top(m) != first), None)
        if apart is None:
            continue
        tables = sorted((table_of[members[0]], table_of[apart]))
        # Already refused for how it relates them: said once, as that.
        if not any({f.left_table, f.right_table} == set(tables) for f in found):
            refuse(Unrelated(tables[0], tables[1], NO_CONDITION))
    return found


def self_columns(resolver: _Resolver, source: Any) -> set[str]:
    """The column names ``source`` (a table or a derived scope) offers."""
    if isinstance(source, exp.Table):
        table_id = resolver._table_id(source)
        return set(resolver._columns_of(table_id)) if table_id is not None else set()
    if isinstance(source, Scope):
        return {s.alias_or_name for b in (source.union_scopes or [source]) for s in _selects(b)} - {
            "*"
        }
    return set()


def others_columns(resolver: _Resolver, scope: Scope, source: Any) -> set[str]:
    """The column names every other source of ``scope`` offers."""
    out: set[str] = set()
    for _node, other in scope.selected_sources.values():
        if other is not source:
            out |= self_columns(resolver, other)
    return out

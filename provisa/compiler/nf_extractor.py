# Copyright (c) 2026 Kenneth Stott
# Canary: 7e37b8c8-4be2-4dbe-8f10-41a1fa44cc69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extract _nf_* native param conditions from SQL WHERE clauses.

Native param columns use the _nf_ prefix (e.g. _nf_id).
In SQL and Cypher queries they appear as ordinary WHERE conditions; this module
strips them out before the engine execution and returns them as api_args for the
REST call Phase 1.

Handles both literal values and $N positional params (postgres dialect).
After extraction, remaining $N references are renumbered contiguously.
"""

# Requirements: REQ-009, REQ-264, REQ-301

from __future__ import annotations

import functools
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.expressions as exp

_NF_PREFIX = "_nf_"
_PARAM_RE = re.compile(r"\$(\d+)")


def _flatten_and(expr: exp.Expr) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(expr, exp.And):
        return _flatten_and(expr.left) + _flatten_and(expr.right)
    return [expr]


def _rebuild_and(conditions: list[exp.Expr]) -> exp.Expr | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = exp.And(this=result, expression=c)
    return result


_NOT_LITERAL = object()


def _literal(val_expr: exp.Expr) -> Any:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A literal's value -- a string, a number, a negated number -- or ``_NOT_LITERAL``."""
    if isinstance(val_expr, exp.Neg) and isinstance(val_expr.this, exp.Literal):
        inner = _literal(val_expr.this)
        return -inner if isinstance(inner, (int, float)) else _NOT_LITERAL
    if isinstance(val_expr, exp.Literal):
        if val_expr.is_string:
            return val_expr.this
        raw = val_expr.this
        return float(raw) if "." in raw or "e" in raw.lower() else int(raw)
    return _NOT_LITERAL


def _resolve_value(val_expr: exp.Expr, params: list[Any]) -> tuple[Any, int | None]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Return (value, param_index_1based) from a value expression.

    param_index is set when the value came from a $N placeholder; None for literals.
    """
    literal = _literal(val_expr)
    if literal is not _NOT_LITERAL:
        return literal, None
    if isinstance(val_expr, exp.Array):
        # REQ-1980: an array literal (a query vector, ARRAY[0.1, -0.2]) is a list of its items.
        items = [_literal(item) for item in val_expr.expressions]
        if all(item is not _NOT_LITERAL for item in items):
            return items, None
        return None, None
    if isinstance(val_expr, exp.Parameter):
        try:
            idx = int(str(val_expr.this))
            value = params[idx - 1] if 0 < idx <= len(params) else None
            return value, idx
        except (ValueError, TypeError):
            pass
    return None, None


#: The suffix a read of a parameter table is renamed with when a statement reads that table under
#: several sets of arguments (:func:`bind_parameters`).
_READ_SUFFIX = re.compile(r"__nf\d+$")


def read_base(table_name: str) -> str:
    """The registered table a read named by :func:`bind_parameters` reads."""
    return _READ_SUFFIX.sub("", table_name)


def _own_sources(select: exp.Select) -> dict[str, exp.Table]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The tables ``select`` itself reads in its FROM and JOINs, by the name a column qualifies
    them with (their alias, else their name): a governed relation (REQ-039) as the table it
    reads, under the same alias."""
    from provisa.compiler.governed_relation import base_table

    found: dict[str, exp.Table] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    from_ = select.args.get("from") or select.args.get("from_")
    candidates = [from_.this] if from_ is not None else []
    candidates += [join.this for join in select.args.get("joins") or []]
    for node in candidates:
        table = base_table(node)
        if table is not None and table.name:
            found[node.alias_or_name] = table
    return found


def _drop_unread_parameters(ast: exp.Expr, parameters: Collection[str]) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Take each parameter column out of the governed relations (REQ-039) that expose it, once
    nothing outside the relation reads it: a parameter is an argument of the read, not a column
    of the rows it returns, so the engine's read holds no such column. Whether any was taken."""
    from provisa.compiler.governed_relation import relations

    names = {p.lower() for p in parameters}
    if not names:
        return False
    found = relations(ast)
    # What the statement reads: a column a governed relation's own body names is the relation's.
    outside = {
        c.name.lower()
        for c in ast.find_all(exp.Column)
        if not any(_within(c, r.relation) for r in found)
    }
    dropped = False
    for relation in found:
        level = relation.relation
        # Down its chain too (the faked, kept and member projections, REQ-1494/1942/1620), each
        # of which passes the parameter up (stage2._pass_up_chain).
        while isinstance(level, exp.Subquery) and isinstance(level.this, exp.Select):
            for projection in list(level.this.expressions):
                name = projection.alias_or_name.lower()
                if name in names and name not in outside:
                    projection.pop()
                    dropped = True
            level = level.this.args["from_"].this
    return dropped


def _within(node: exp.Expr, ancestor: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    parent = node.parent
    while parent is not None:
        if parent is ancestor:
            return True
        parent = parent.parent
    return False


def _expose_landed(ast: exp.Expr, column: exp.Column, target: str) -> None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A frozen parameter's condition reads ``target``, the landed column its value filters
    (REQ-2031): the governed relation the condition reads (REQ-039) exposes it under that name,
    in the parameter's place, unless it already does."""
    from provisa.compiler.governed_relation import relations

    for relation in relations(ast):
        if column.table and column.table != relation.alias:
            continue
        body = relation.relation.this
        exposed = {p.alias_or_name for p in body.expressions}
        for projection in list(body.expressions):
            if projection.alias_or_name.lower() != column.name.lower():
                continue
            if target in exposed:
                continue
            value = projection.unalias()
            if isinstance(value, exp.Column):
                value = exp.column(target, table=value.table, quoted=True)
            projection.replace(exp.alias_(value, target, quoted=True))


@dataclass(frozen=True)
class JoinKey:
    """The read that gives a parameter its values, one call for each distinct value (REQ-1986):
    the column a join's equality names, read from its table as the statement reads it (under
    that table's own conditions), or the rows of an ``IN (SELECT ...)``. ``query`` is that read
    as a governed statement of one column, ``key``; ``label`` names it; ``listed`` is whether it
    lists the values outright (IN), so nothing of the condition is left to apply."""

    query: str
    label: str
    listed: bool = False

    def __str__(self) -> str:
        return self.label


@dataclass
class _Read:
    """One read of a parameter table in one SELECT, and what the SELECT's conditions assign its
    parameters: ``sets``, one per call, each parameter's value or the :class:`JoinKey` giving it."""

    table: exp.Table  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    sets: list[dict[str, Any]]


@dataclass(frozen=True)
class Bound:
    """A statement with its tables' parameters bound (:func:`bind_parameters`).

    ``sql`` and ``params``: the statement and its bound values with the assignments taken out.
    ``nf_args``: the arguments of the statement's one parameterized read, by bare parameter name,
    when it has one read under one set of arguments. ``reads``: otherwise each call the statement
    makes, named for itself (``<table>__nf<k>``), as (the table it reads, its arguments)."""

    sql: str
    params: list[Any]
    nf_args: dict[str, Any]
    reads: dict[str, tuple[str, dict[str, Any]]]


def _bare(name: str) -> str:
    return name[len(_NF_PREFIX) :]


def _unwrap(node: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    while isinstance(node, (exp.Paren, exp.Cast)):
        node = node.this
    return node


def _form(cond: exp.Expr) -> str:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """What a condition that assigns nothing is, as the refusal names it."""
    if isinstance(cond, exp.Not):
        inner = cond.this
        while isinstance(inner, exp.Paren):
            inner = inner.this
        if isinstance(inner, exp.In):
            return "NOT IN"
        if isinstance(inner, exp.Is):
            return "IS NOT NULL"
        if isinstance(inner, exp.EQ):
            return "NOT (=)"
        return "NOT"
    if isinstance(cond, exp.NEQ):
        return "<>"
    if isinstance(cond, exp.Is):
        return "IS NOT NULL" if cond.args.get("negate") else "IS NULL"
    for kind, name in (
        (exp.GTE, ">="),
        (exp.GT, ">"),
        (exp.LTE, "<="),
        (exp.LT, "<"),
        (exp.Between, "BETWEEN"),
        (exp.ILike, "ILIKE"),
        (exp.Like, "LIKE"),
        (exp.SimilarTo, "SIMILAR TO"),
        (exp.RegexpLike, "~"),
        (exp.In, "IN (SELECT ...)"),
        (exp.EQ, "= (an expression)"),
    ):
        if isinstance(cond, kind):
            return name
    return cond.key.upper()


class _Binder:
    """The one step that binds a statement's parameters (REQ-1986): it reads each SELECT's WHERE
    and JOIN conditions, gives each read of a parameter table its argument sets, and refuses
    every condition on an opaque parameter that assigns it nothing."""

    def __init__(
        self,
        ast: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        params: list[Any],
        known: set[str],
        real: Mapping[str, str | None],
        types: Mapping[str, str],
        cap: int | None,
    ) -> None:
        from provisa.compiler.governed_relation import relations

        self.ast = ast
        self.params = params
        self.known = known
        self.real = real
        self.types = {k.lower(): v for k, v in types.items()}
        self.cap = cap
        self.relations = {id(r.table): r for r in relations(ast)}
        self.reads: list[_Read] = []
        self.changed = False
        # The conditions that only assign values: taken out of the statement once bound.
        self.assigning: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for select in ast.find_all(exp.Select):
            self._bind_select(select)

    # -- reading -------------------------------------------------------------------------------

    def _source(self, column: exp.Column, select: exp.Select) -> exp.Table | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The table ``column`` is read from: one of ``select``'s own, else one of a SELECT
        around it (a correlated condition)."""
        scope: exp.Expr | None = select  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        while scope is not None:
            if isinstance(scope, exp.Select):
                sources = _own_sources(scope)
                if column.table:
                    if column.table in sources:
                        return sources[column.table]
                else:
                    exposing = [
                        t
                        for t in sources.values()
                        if id(t) in self.relations
                        and any(
                            k.lower() == column.name.lower() for k in self.relations[id(t)].columns
                        )
                    ]
                    if len(exposing) == 1:
                        return exposing[0]
                    if len(sources) == 1:
                        return next(iter(sources.values()))
            scope = scope.parent
        return None

    def _parameter(self, node: exp.Expr, select: exp.Select) -> tuple[exp.Table, str] | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``node`` as a parameter of one of ``select``'s own reads: (its table, its name)."""
        if not isinstance(node, exp.Column) or node.name.lower() not in self.known:
            return None
        if node.find_ancestor(exp.Select) is not select:
            return None
        table = self._source(node, select)
        if table is None or all(table is not t for t in _own_sources(select).values()):
            return None
        return table, node.name

    def _parameters_in(self, cond: exp.Expr, select: exp.Select) -> list[tuple[exp.Table, str]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        found = [self._parameter(c, select) for c in cond.find_all(exp.Column)]
        return [f for f in found if f is not None]

    def _values(self, node: exp.Expr) -> list[Any] | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The values ``node`` lists (``ANY(ARRAY[...])``, ``ANY($1)``), or None."""
        inner = _unwrap(node.this)
        if isinstance(inner, exp.Array):
            items = [self._value(item) for item in inner.expressions]
            return None if any(i is _NOT_LITERAL for i in items) else items
        if isinstance(inner, exp.Parameter):
            value = self._value(inner)
            return list(value) if isinstance(value, (list, tuple)) else None
        return None

    def _value(self, node: exp.Expr) -> Any:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The value ``node`` states -- a literal, an array of literals, a bound ``$N`` (None when
        it is not bound yet) -- or ``_NOT_LITERAL``."""
        node = _unwrap(node)
        if isinstance(node, exp.Boolean):
            return bool(node.this)
        if not isinstance(node, (exp.Literal, exp.Neg, exp.Array, exp.Parameter)):
            return _NOT_LITERAL
        value, index = _resolve_value(node, self.params)
        return _NOT_LITERAL if value is None and index is None else value

    def _refuse(self, cond: exp.Expr, select: exp.Select) -> list[dict]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """A condition that assigns nothing: refused naming its first opaque parameter. When
        every parameter in it is a real column of its table, its meaning is known: it is a filter
        on that column."""
        from provisa.core.parameter_binding import ParameterByEqualityOnly

        for _table, name in self._parameters_in(cond, select):
            if name.lower() not in self.real:
                raise ParameterByEqualityOnly(name, _form(cond))
        for column in list(cond.find_all(exp.Column)):
            if self._parameter(column, select) is None:
                continue
            target = self.real[column.name.lower()]
            if target is not None:
                _expose_landed(self.ast, column, target)
                column.set("this", exp.to_identifier(target, quoted=True))
                self.changed = True
        return [{}]

    def _branches(self, cond: exp.Expr, select: exp.Select) -> list[dict]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """What ``cond`` assigns, as its branches: each a set of (read, parameter) -> value. A
        condition that names no parameter is one branch assigning nothing."""
        from provisa.core.parameter_binding import ParameterUnbound

        while isinstance(cond, exp.Paren):
            cond = cond.this
        if not self._parameters_in(cond, select):
            return [{}]
        if isinstance(cond, exp.And):
            return _product(self._branches(cond.left, select), self._branches(cond.right, select))
        if isinstance(cond, exp.Or):
            branches = self._branches(cond.left, select) + self._branches(cond.right, select)
            for key in {k for b in branches for k in b}:
                if any(key not in b for b in branches):
                    raise ParameterUnbound(key[2])
            return branches
        if isinstance(cond, exp.EQ):
            for side, other in ((cond.left, cond.right), (cond.right, cond.left)):
                parameter = self._parameter(side, select)
                if parameter is None:
                    continue
                key = (id(parameter[0]), parameter[1].lower(), parameter[1])
                if isinstance(_unwrap(other), exp.Any):
                    values = self._values(_unwrap(other))
                    if values is not None:
                        return [{key: self._typed(parameter[1], v)} for v in values]
                    break
                value = self._value(other)
                if value is not _NOT_LITERAL:
                    return [{key: self._typed(parameter[1], value)}]
                join = self._join_key(other, parameter[0], select)
                if join is not None:
                    return [{key: join}]
                break
            return self._refuse(cond, select)
        if isinstance(cond, exp.In) and not cond.args.get("query"):
            parameter = self._parameter(cond.this, select)
            values = [self._value(v) for v in cond.expressions]
            if parameter is not None and all(v is not _NOT_LITERAL for v in values):
                key = (id(parameter[0]), parameter[1].lower(), parameter[1])
                return [{key: self._typed(parameter[1], v)} for v in values]
        if isinstance(cond, exp.In) and cond.args.get("query") is not None:
            parameter = self._parameter(cond.this, select)
            if parameter is not None:
                key = (id(parameter[0]), parameter[1].lower(), parameter[1])
                return [{key: self._listed_key(cond.args["query"], parameter[1])}]
        return self._refuse(cond, select)

    def _typed(self, parameter: str, value: Any) -> Any:
        """``value``, assigned to ``parameter``: refused before any call when the parameter's
        registered type does not take it (``parameter_binding.check_value``)."""
        from provisa.core.parameter_binding import check_value

        check_value(parameter, self.types.get(parameter.lower()), value)
        return value

    def _inlined(self, node: exp.Expr) -> exp.Expr | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``node`` with each bound ``$N`` as its value; None when one is not bound yet."""
        node = node.copy()
        for bound in list(node.find_all(exp.Parameter)):
            index = int(str(bound.this)) if str(bound.this).isdigit() else 0
            if not 0 < index <= len(self.params):
                return None
            bound.replace(exp.convert(self.params[index - 1]))
        return node

    def _listed_key(self, query: exp.Expr, parameter: str) -> JoinKey:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``parameter IN (SELECT ...)``: the values the subquery lists, a read of its own. One
        that reads a column of the statement around it lists nothing on its own and is refused."""
        from provisa.core.parameter_binding import ParameterByEqualityOnly

        body = query.this if isinstance(query, exp.Subquery) else query
        own = {t.alias_or_name for t in body.find_all(exp.Table)} | {
            q.alias for q in body.find_all(exp.Subquery) if q.alias
        }
        if any(c.table and c.table not in own for c in body.find_all(exp.Column)):
            raise ParameterByEqualityOnly(parameter, "IN (a correlated subquery)")
        inlined = self._inlined(body)
        if inlined is None or len(getattr(body, "expressions", [])) != 1:
            raise ParameterByEqualityOnly(parameter, "IN (SELECT ...)")
        keys = exp.select(
            exp.alias_(exp.column("key", table="_k", quoted=True), "key", quoted=True)
        )
        keys = keys.distinct().from_(
            exp.Subquery(
                this=inlined,
                alias=exp.TableAlias(
                    this=exp.to_identifier("_k", quoted=True),
                    columns=[exp.to_identifier("key", quoted=True)],
                ),
            )
        )
        return JoinKey(keys.sql(dialect="postgres"), "IN (SELECT ...)", listed=True)

    def _owner(self, column: exp.Column, select: exp.Select) -> exp.Select | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The SELECT whose FROM holds the table ``column`` is read from (``_source``)."""
        table = self._source(column, select)
        scope: exp.Expr | None = select  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        while scope is not None:
            if isinstance(scope, exp.Select) and any(
                t is table for t in _own_sources(scope).values()
            ):
                return scope
            scope = scope.parent
        return None

    def _join_key(
        self,
        other: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        table: exp.Table,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        select: exp.Select,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> JoinKey | None:
        """The column of another registered table an equality joins a parameter of ``table`` on,
        as the read of its distinct values: from that table as the statement reads it, under the
        conditions the statement puts on that table alone -- its WHERE, its inner joins' ON, and
        the ON of the join to ``table`` -- so the calls made are the ones the join can use."""
        other = _unwrap(other)
        if not isinstance(other, exp.Column) or other.name.lower() in self.known:
            return None
        source = self._source(other, select)
        owner = self._owner(other, select)
        if source is None or owner is None or source is table or id(source) not in self.relations:
            return None
        relation = self.relations[id(source)].relation
        alias = relation.alias
        keys = (
            exp.select(
                exp.alias_(exp.column(other.name, table=alias, quoted=True), "key", quoted=True)
            )
            .distinct()
            .from_(relation.copy())
        )
        for cond in self._own_conditions(source, owner, table if owner is select else None):
            keys = keys.where(cond)
        return JoinKey(keys.sql(dialect="postgres"), f"{source.name}.{other.name}")

    def _own_conditions(
        self,
        source: exp.Table,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        owner: exp.Select,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joined: exp.Table | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The conditions ``owner`` puts on ``source`` alone, each safe to apply before the join:
        a WHERE conjunct, an inner join's ON conjunct, and an ON conjunct of the join to
        ``joined`` (the parameterized table), which only decides which rows that join matches."""
        from provisa.compiler.governed_relation import base_table

        holders: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        where = owner.args.get("where")
        if where is not None:
            holders.append(where.this)
        for join in owner.args.get("joins") or []:
            on = join.args.get("on")
            inner = not join.side and (join.kind or "").upper() in ("", "INNER", "CROSS")
            if on is not None and (
                inner or (joined is not None and base_table(join.this) is joined)
            ):
                holders.append(on)
        kept: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for cond in [c for holder in holders for c in _flatten_and(holder)]:
            columns = list(cond.find_all(exp.Column))
            if (
                not columns
                or cond.find(exp.Select) is not None
                or any(c.name.lower() in self.known for c in columns)
                or any(self._source(c, owner) is not source for c in columns)
            ):
                continue
            inlined = self._inlined(cond)
            if inlined is not None:
                kept.append(inlined)
        return kept

    def _bind_select(self, select: exp.Select) -> None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        holders = [select.args.get("where"), *(select.args.get("joins") or [])]
        conjuncts: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for holder in holders:
            cond = (
                None
                if holder is None
                else holder.args.get("this" if isinstance(holder, exp.Where) else "on")
            )
            if cond is not None:
                conjuncts += _flatten_and(cond)
        per_read: dict[int, tuple[exp.Table, list[list[dict]]]] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for key, join in self._read_joins(select):
            table = next(t for t in _own_sources(select).values() if id(t) == key[0])
            per_read.setdefault(key[0], (table, []))[1].append([{key[2]: join}])
        for cond in conjuncts:
            branches = self._branches(cond, select)
            if not any(branches):
                continue
            if _only_values(cond, branches):
                self.assigning.append(cond)
            for table_id in {k[0] for b in branches for k in b}:
                table = next(t for t in _own_sources(select).values() if id(t) == table_id)
                own = _distinct(
                    [{k[2]: v for k, v in b.items() if k[0] == table_id} for b in branches]
                )
                per_read.setdefault(table_id, (table, []))[1].append(own)
        for table, lists in per_read.values():
            sets: list[dict] = [{}]
            for own in lists:
                sets = _distinct(_product(sets, own))
            self._cap(table.name, len(sets))
            self.reads.append(_Read(table, sets))

    def _read_joins(self, select: exp.Select) -> list[tuple[tuple[int, str, str], JoinKey]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """A parameter ``select`` keys a relationship field's grouped read by
        (``sql_selection.rel_read``, #362): the read is LEFT JOINed on ``r._keyN = t0.k`` and
        ``select`` projects the parameter as ``_keyN``. That equality is the join's: the
        parameter takes the distinct values of ``t0.k`` under ``t0``'s own conditions, as the
        correlated equality the read replaced gave it. Each as ((read, parameter key), join)."""
        from provisa.compiler.sql_rewrite import keyed_select, read_keys
        from provisa.core.parameter_binding import ParameterByEqualityOnly

        found: list[tuple[tuple[int, str, str], JoinKey]] = []
        node: exp.Expr | None = select.parent  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        while node is not None:
            if isinstance(node, exp.Join) and isinstance(node.this, exp.Subquery):
                read = node.this.this
                for read_key, outer in read_keys(node) or []:
                    keyed = (
                        keyed_select(read, read_key.name) if isinstance(read, exp.Select) else None
                    )
                    if keyed is None or keyed[0] is not select:
                        continue
                    parameter = self._parameter(_unwrap(keyed[1]), select)
                    if parameter is None:
                        continue
                    table, name = parameter
                    join_key = self._join_key(outer, table, select)
                    if join_key is None:
                        if name.lower() not in self.real:
                            raise ParameterByEqualityOnly(name, "= (an expression)")
                        # A real column: the join filters it, as any condition on one does.
                        self._refuse(keyed[1], select)
                        continue
                    found.append(((id(table), name.lower(), name), join_key))
            node = node.parent
        return found

    def _cap(self, table: str, calls: int) -> None:
        from provisa.core.parameter_binding import ParameterCallsOverCap

        if self.cap is not None and calls > self.cap:
            raise ParameterCallsOverCap(table, calls, self.cap)

    # -- writing -------------------------------------------------------------------------------

    @property
    def joins(self) -> set[JoinKey]:
        return {v for r in self.reads for s in r.sets for v in s.values() if isinstance(v, JoinKey)}

    def _expanded(self, read: _Read, keys: Mapping[JoinKey, list[Any]]) -> list[dict]:
        """``read``'s argument sets with each join's distinct values in its key's place."""
        sets: list[dict] = []
        for one in read.sets:
            expanded: list[dict] = [{}]
            for name, value in one.items():
                values = _distinct_values(keys[value]) if isinstance(value, JoinKey) else [value]
                expanded = [{**e, name: v} for e in expanded for v in values]
                self._cap(read.table.name, len(expanded))
            sets += expanded
        sets = _distinct(sets)
        self._cap(read.table.name, len(sets))
        return sets

    def apply(self, keys: Mapping[JoinKey, list[Any]]) -> tuple[dict, dict]:
        """Rewrite the statement for its bound reads; returns (nf_args, reads) as :class:`Bound`
        holds them."""
        for cond in self.assigning:
            _take_out(cond)
        _drop_unread_parameters(self.ast, self.known)
        bound = [(r, self._expanded(r, keys)) for r in self.reads]
        if len(bound) == 1 and len(bound[0][1]) == 1 and not self.joins:
            read, [one] = bound[0]
            self._put(read.table, [(None, one)])
            return {_bare(k): v for k, v in one.items()}, {}
        named: dict[str, tuple[str, dict[str, Any]]] = {}
        for read, sets in bound:
            base = read.table.name
            arms: list[tuple[str | None, dict]] = []
            for one in sets:
                name = f"{base}__nf{len(named) + 1}"
                named[name] = (base, {_bare(k): v for k, v in one.items()})
                arms.append((name, one))
            self._put(read.table, arms)
        return {}, named

    def stamp_unasked(self) -> bool:
        """A read the statement assigns nothing that reads an opaque parameter's column: the
        column answers NULL, since nothing was asked -- stated here, whatever the landing holds.
        Whether any read was so stamped."""
        bound = {id(r.table) for r in self.reads}
        stamped = False
        for relation in self.relations.values():
            table = relation.table
            body = table.find_ancestor(exp.Select)
            if id(table) in bound or body is None:
                continue
            alias = table.alias_or_name
            if any(
                c.name.lower() in self.known
                and c.name.lower() not in self.real
                and c.table in ("", alias)
                for c in body.find_all(exp.Column)
            ):
                self._put(table, [(None, {})])
                stamped = True
        return stamped

    def _put(self, table: exp.Table, arms: list[tuple[str | None, dict]]) -> None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Put ``table``'s calls in its place: each arm the rows of one call (named for itself,
        or the table as it stands), carrying the value asked of each parameter the governed
        relation reads, and the arms unioned. A table that stands outside a governed relation
        holds no parameter column to carry: its one call takes its place as it is."""
        body = table.find_ancestor(exp.Select)
        alias = table.alias_or_name
        asked = (
            []
            if id(table) not in self.relations or body is None
            else _unique(
                c.name
                for c in body.find_all(exp.Column)
                if c.name.lower() in self.known and c.table in ("", alias)
            )
        )
        if not asked and len(arms) == 1:
            if arms[0][0] is not None:
                if not table.alias:
                    # Its columns are qualified by its name: it keeps that name as its alias.
                    table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
                table.set("this", exp.to_identifier(arms[0][0], quoted=table.this.quoted))
            return
        assert body is not None
        columns = (
            [exp.Star()]
            if id(table) not in self.relations
            else [
                exp.column(name, quoted=True)
                for name in _unique(
                    c.name
                    for c in body.find_all(exp.Column)
                    if c.name.lower() not in self.known
                    and c.table in ("", alias)
                    and c.find_ancestor(exp.Select) is body
                )
            ]
        )
        selects: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for name, one in arms:
            source = table.copy()
            source.set("alias", None)
            if name is not None:
                source.set("this", exp.to_identifier(name, quoted=table.this.quoted))
            given = {k.lower(): v for k, v in one.items()}
            stamps = [
                exp.alias_(self._stamp(p, given.get(p.lower())), p, quoted=True) for p in asked
            ]
            selects.append(exp.select(*[c.copy() for c in columns], *stamps).from_(source))
        if not selects:
            # A join that gives no value makes no call: the table holds no row.
            empty = [exp.alias_(exp.Null(), c.name, quoted=True) for c in columns]
            empty += [exp.alias_(self._stamp(p, None), p, quoted=True) for p in asked]
            selects = [exp.select(*empty).where(exp.false())]
        union = selects[0]
        for arm in selects[1:]:
            union = exp.union(union, arm, distinct=False)
        table.replace(
            exp.Subquery(
                this=union, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True))
            )
        )

    def _stamp(self, parameter: str, value: Any) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The value asked of ``parameter``, as the column of the rows it was asked for."""
        from provisa.core.ir_types import to_physical

        if value is None:
            literal: exp.Expr = exp.Null()  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        elif isinstance(value, bool):
            literal = exp.Boolean(this=value)
        elif isinstance(value, (int, float)):
            literal = exp.Literal.number(value)
        elif isinstance(value, (list, tuple)):
            literal = exp.Array(expressions=[exp.convert(v) for v in value])
        else:
            literal = exp.Literal.string(str(value))
        ir_type = self.types.get(parameter.lower())
        if ir_type is None:
            return literal
        if isinstance(value, str) and _is_vector(ir_type):
            # REQ-430: a search's vector query is also given a text, embedded by the search's
            # model; the column repeats the text asked, which is no vector to cast to.
            return literal
        return exp.cast(literal, to_physical(ir_type, "postgresql"))


def _is_vector(ir_type: str) -> bool:
    """Whether ``ir_type`` declares a vector: an array, in any spelling, or a vector."""
    from provisa.core.ir_types import base_type

    spelled = base_type(ir_type).strip().lower()
    return spelled.startswith(("array(", "array<", "vector")) or spelled.endswith("[]")


def _same(a: Any, b: Any) -> bool:
    return isinstance(a, JoinKey) == isinstance(b, JoinKey) and a == b


def _product(left: list[dict], right: list[dict]) -> list[dict]:
    """Every branch of ``left`` with every branch of ``right``; one parameter given two
    different values on one branch is a conflict, refused."""
    from provisa.core.parameter_binding import ParameterConflict

    out: list[dict] = []
    for a in left:
        for b in right:
            for k in a.keys() & b.keys():
                if not _same(a[k], b[k]):
                    name = k[2] if isinstance(k, tuple) else k
                    raise ParameterConflict(name, a[k], b[k])
            out.append({**a, **b})
    return out


def _distinct(sets: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for one in sets:
        mark = repr(sorted(((str(k), repr(v)) for k, v in one.items())))
        if mark not in seen:
            seen.add(mark)
            out.append(one)
    return out


def _distinct_values(values: list[Any]) -> list[Any]:
    """A join's keys, each once; a NULL key joins nothing and asks nothing."""
    seen: set[str] = set()
    out: list[Any] = []
    for v in values:
        if v is not None and repr(v) not in seen:
            seen.add(repr(v))
            out.append(v)
    return out


def _unique(names: Any) -> list[str]:
    return list(dict.fromkeys(names))


def _only_values(cond: exp.Expr, branches: list[dict]) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether ``cond`` is one equality or IN that only assigns values: nothing of it is left to
    apply once its values are given."""
    while isinstance(cond, exp.Paren):
        cond = cond.this
    return isinstance(cond, (exp.EQ, exp.In)) and not any(
        isinstance(v, JoinKey) and not v.listed for b in branches for v in b.values()
    )


def _take_out(cond: exp.Expr) -> None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Take an assignment out of the conjunction that holds it: its other side stands in its
    place, and a WHERE left with nothing goes; a JOIN left with nothing joins every row."""
    node = cond
    while isinstance(node.parent, exp.Paren):
        node = node.parent
    parent = node.parent
    if isinstance(parent, exp.And):
        kept = parent.right if parent.left is node else parent.left
        parent.replace(kept)
    elif isinstance(parent, exp.Where):
        parent.pop()
    elif isinstance(parent, exp.Join):
        parent.set("on", exp.true())


def bind_parameters(
    sql: str,
    params: list[Any],
    parameters: Collection[str] | Mapping[str, str],
    frozen: Mapping[str, str | None] | None = None,
    *,
    real: Collection[str] | Mapping[str, str | None] = (),
    cap: int | None = None,
) -> "_Binding":  # REQ-1986, REQ-301, REQ-2031
    """Read what a statement (postgres dialect) assigns its tables' parameters.

    ``parameters`` are the parameter columns of the statement's context
    (``CompilationContext.native_filter_columns``), each with its IR type where known: only a
    condition on one of them binds. A ``_nf_`` column that is no parameter there -- a generated
    table's parameter in a Test (synthetic) environment, an ordinary column of its copy
    (REQ-1942) -- stays a filter.

    ``frozen``: the parameters of a table kept for good, each with the landed column its value
    filters (``materialization.frozen_parameter_filters``, REQ-2031). Its rows are landed, never
    fetched under an argument, so a condition on one is an ordinary filter on that column; one
    with no such column is refused by name.

    ``real``: the parameters that are real columns of their tables (a partition key, a column
    value), each with that column's name where the table holds it. Equality and IN assign them as
    any parameter; any other condition on one is a filter on the real column.

    ``cap``: the most calls one statement makes to one table
    (``parameter_binding.CALLS_SETTING``).

    The rules (REQ-1986): equality assigns a parameter its value for the call, IN (and
    ``= ANY``) one call for each value, a join's equality on the parameter one call for each
    distinct value of the other table's column; OR assigns on each branch, and a parameter one
    branch leaves without a value is refused; two different values on one branch are refused;
    any other condition on an opaque parameter is refused naming it. The assignments are taken
    out of the statement -- the rows a call returns are not filtered by them again -- and the
    parameter's column, where the statement reads it, repeats the value that was asked.

    Returns a binding: ``joins`` are the columns whose distinct values it needs, and
    ``bound(keys)`` the statement with every call in place (:class:`Bound`)."""
    return _Binding(sql, params, parameters, frozen, real, cap)


class _Binding:
    def __init__(
        self,
        sql: str,
        params: list[Any],
        parameters: Collection[str] | Mapping[str, str],
        frozen: Mapping[str, str | None] | None,
        real: Collection[str] | Mapping[str, str | None],
        cap: int | None,
    ) -> None:
        self._sql = sql
        self._params = params
        self._binder: _Binder | None = None
        self._changed = False
        # A statement that names no ``_nf_`` column has nothing to bind: a parameter's column
        # name starts with the prefix, and that name is in the text.
        if _NF_PREFIX not in sql:
            return
        # Parse failure must fail loud: returning the input would skip the binding.
        ast = sqlglot.parse_one(sql, dialect="postgres")
        landed = {k.lower(): v for k, v in (frozen or {}).items()}
        for column in [
            c
            for holder in [*ast.find_all(exp.Where), *ast.find_all(exp.Join)]
            for c in holder.find_all(exp.Column)
        ]:
            if column.name.lower() not in landed:
                continue
            target = landed[column.name.lower()]
            if target is None:
                raise ValueError(
                    f"{column.name} is a parameter of a table read from its landed rows, and they"
                    f" hold no column {column.name[len(_NF_PREFIX) :]!r} for it to filter"
                )
            _expose_landed(ast, column, target)
            column.set("this", exp.to_identifier(target, quoted=True))
            self._changed = True
        types = parameters if isinstance(parameters, Mapping) else {}
        known = {p.lower() for p in parameters if p.startswith(_NF_PREFIX)} - set(landed)
        self._landed = set(landed)
        targets = real if isinstance(real, Mapping) else dict.fromkeys(real)
        self._binder = _Binder(
            ast, params, known, {k.lower(): v for k, v in targets.items()}, types, cap
        )

    @property
    def joins(self) -> set[JoinKey]:
        return set() if self._binder is None else self._binder.joins

    @property
    def assigned(self) -> set[str]:
        """The parameters the statement assigns, by bare name: by a value or by a join."""
        if self._binder is None:
            return set()
        return {_bare(name) for read in self._binder.reads for one in read.sets for name in one}

    def bound(self, keys: Mapping[JoinKey, list[Any]] | None = None) -> Bound:
        binder = self._binder
        if binder is None:
            return Bound(self._sql, self._params, {}, {})
        if not binder.reads:
            dropped = _drop_unread_parameters(binder.ast, binder.known | self._landed)
            stamped = binder.stamp_unasked()
            changed = self._changed or binder.changed or dropped or stamped
            sql = binder.ast.sql(dialect="postgres") if changed else self._sql
            return Bound(sql, self._params, {}, {})
        nf_args, reads = binder.apply(keys or {})
        _drop_unread_parameters(binder.ast, self._landed)
        binder.stamp_unasked()
        # A bound value an assignment held went with it, unless the statement reads it elsewhere.
        left = {
            int(str(p.this)) for p in binder.ast.find_all(exp.Parameter) if str(p.this).isdigit()
        }
        gone = {i for i in range(1, len(self._params) + 1) if i not in left}
        clean_sql = binder.ast.sql(dialect="postgres")
        if not gone:
            return Bound(clean_sql, self._params, nf_args, reads)
        clean_params = [v for i, v in enumerate(self._params, 1) if i not in gone]
        renumber = {old: new for new, old in enumerate(sorted(left), 1)}
        clean_sql = _PARAM_RE.sub(
            lambda m: f"${renumber.get(int(m.group(1)), int(m.group(1)))}", clean_sql
        )
        return Bound(clean_sql, clean_params, nf_args, reads)


def extract_nf_args(
    sql: str,
    params: list[Any],
    parameters: Collection[str] | Mapping[str, str],
    frozen: Mapping[str, str | None] | None = None,
) -> tuple[str, list[Any], dict[str, Any]]:  # REQ-301, REQ-1986, REQ-2031
    """:func:`bind_parameters` for a statement that makes one call: (its SQL, its bound values,
    its arguments by bare parameter name). A statement that makes several, or joins on a
    parameter, is not one call and is refused here: its caller binds it whole."""
    binding = bind_parameters(sql, params, parameters, frozen)
    if binding.joins:
        raise ValueError("a join on a parameter is bound by the statement's pipeline")
    bound = binding.bound()
    if bound.reads:
        raise ValueError(
            "a statement that makes several calls is bound by the statement's pipeline"
        )
    return bound.sql, bound.params, bound.nf_args


@functools.lru_cache(maxsize=4096)
def _table_names(sql: str) -> tuple[str, ...]:
    ast = sqlglot.parse_one(sql, dialect="postgres")
    return tuple(tbl.name for tbl in ast.find_all(exp.Table) if tbl.name)


def find_api_table_names(sql: str) -> list[str]:  # REQ-599
    """Return table names referenced in FROM/JOIN clauses of a SQL string. A function of the text
    alone, asked on every execution of a statement (``would_materialize_optimize``), so the parse
    is kept per text."""
    return list(_table_names(sql))


def left_join_table_names(sql: str) -> set[str]:  # REQ-599
    """Return table names that appear in LEFT JOIN clauses."""
    ast = sqlglot.parse_one(sql, dialect="postgres")
    names: set[str] = set()
    for join in ast.find_all(exp.Join):
        if join.side and join.side.upper() == "LEFT":
            for tbl in join.find_all(exp.Table):
                if tbl.name:
                    names.add(tbl.name)
    return names


def drop_joined_table(sql: str, table_name: str) -> str:  # REQ-264
    """Remove any JOIN for *table_name* (any join type) and NULL-out its SELECT-list columns."""
    # Parse failure must fail loud: returning input skips the REQ-264 governance transform.
    tree = sqlglot.parse_one(sql, dialect="postgres")

    for select in tree.find_all(exp.Select):
        joins = select.args.get("joins") or []
        pruned_aliases: set[str] = set()

        kept: list[exp.Join] = []
        for join in joins:
            has_target = any(tbl.name == table_name for tbl in join.find_all(exp.Table))
            if not has_target:
                kept.append(join)
                continue
            for tbl in join.find_all(exp.Table):
                alias = tbl.alias or tbl.name
                if alias:
                    pruned_aliases.add(alias)

        if len(kept) == len(joins):
            continue

        select.set("joins", kept)

        if pruned_aliases:
            new_exprs = []
            for expr in select.expressions:
                col = expr.this if isinstance(expr, exp.Alias) else expr
                tbl_ref = col.table if isinstance(col, exp.Column) else ""
                if tbl_ref in pruned_aliases:
                    alias = (
                        expr.alias
                        if isinstance(expr, exp.Alias)
                        else (col.name if isinstance(col, exp.Column) else "")
                    )
                    new_exprs.append(exp.alias_(exp.null(), alias) if alias else expr)
                else:
                    new_exprs.append(expr)
            select.set("expressions", new_exprs)

    return tree.sql(dialect="postgres")


# Keep old name as alias for callers that imported it before the rename.
drop_left_join_table = drop_joined_table


def _union_arms(union: exp.Union) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The arms of a chain of unions of one kind (``a UNION ALL b UNION ALL c``), left to right.
    A nested union with its own ORDER BY or LIMIT is one arm."""
    left = union.this
    if (
        isinstance(left, exp.Union)
        and type(left) is type(union)
        and left.args.get("distinct") == union.args.get("distinct")
        and not any(left.args.get(k) for k in ("order", "limit", "offset", "with_"))
    ):
        return [*_union_arms(left), union.expression]
    return [left, union.expression]


def _reads(node: exp.Expression, cte_name: str) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether *node* reads the CTE named *cte_name* (an unqualified table of that name)."""
    return any(not t.db and t.name == cte_name for t in node.find_all(exp.Table))


def _self_referencing(cte: exp.CTE) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether *cte* is recursive: its body reads itself."""
    return _reads(cte.this, cte.alias)


def _keep_column_names(arm: exp.Expression, names_from: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """*arm*, now a union's first arm, naming its columns as the dropped first arm did: the
    union's columns (and a CTE's, which the recursive term reads by name) take their names from
    its first arm."""
    if not isinstance(arm, exp.Select) or not isinstance(names_from, exp.Select):
        return arm
    named, exprs = names_from.expressions, arm.expressions
    if len(named) != len(exprs) or any(isinstance(e, exp.Star) for e in [*named, *exprs]):
        return arm
    renamed = []
    for name_expr, expr in zip(named, exprs):
        name = name_expr.alias_or_name if isinstance(name_expr, (exp.Alias, exp.Column)) else ""
        if name and expr.alias_or_name != name:
            expr = exp.alias_(expr.this if isinstance(expr, exp.Alias) else expr, name)
        renamed.append(expr)
    return arm.select(*renamed, append=False)


def drop_union_branches_for_table(sql: str, table_name: str) -> str:  # REQ-599, REQ-1661
    """Remove every UNION branch whose FROM clause references *table_name*.

    Works at any nesting depth (including inside CTEs).  Used when a GQL-remote
    table with unsatisfied required_args cannot be dropped as a JOIN. A union whose every arm
    references the table is left as it is, and so is a recursive CTE whose every anchor (an arm
    that does not read the CTE itself) does: every row of it comes through the table, so the
    table stays in the statement and ``apply_dropped_tables`` refuses the read naming it (#312).
    A recursive arm is dropped like any branch; the walk keeps the rows that never read the table.
    """
    # Parse failure must fail loud: returning input skips the union-branch governance transform.
    tree = sqlglot.parse_one(sql, dialect="postgres")

    def _has_from_table(select: exp.Select) -> bool:  # pyright: ignore[reportPrivateImportUsage]
        return any(t.name == table_name for t in select.find_all(exp.Table))

    root = tree
    modified = False

    # Process nested unions before the unions that contain them: `_has_from_table`
    # recurses into subqueries, so a branch that wraps a nested UNION (e.g. a
    # relationship-type union nested inside an outer Cypher UNION ALL branch)
    # would otherwise look like a whole-branch match on *table_name* the moment
    # *any* inner arm references it, dropping the entire outer branch instead of
    # just the one inner arm. Innermost-first ensures the inner arm is pruned
    # first, so the outer branch no longer references the table by the time it
    # is checked.
    def _depth(node: exp.Expression) -> int:
        depth = 0
        parent = node.parent
        while parent is not None:
            depth += 1
            parent = parent.parent
        return depth

    # One chain (a UNION ALL b UNION ALL c) at a time, from its outermost union, so every arm of
    # it is weighed together: a recursive CTE's anchors are only known across the whole chain.
    chains = [
        u
        for u in tree.find_all(exp.Union)
        if not (
            isinstance(u.parent, exp.Union)
            and u.parent.this is u
            and all(arm is not u for arm in _union_arms(u.parent))
        )
    ]
    for union in sorted(chains, key=_depth, reverse=True):
        arms = _union_arms(union)
        matched = [isinstance(arm, exp.Select) and _has_from_table(arm) for arm in arms]
        if not any(matched) or all(matched):
            continue
        cte = union.parent
        if isinstance(cte, exp.CTE) and _self_referencing(cte):
            anchors = [m for arm, m in zip(arms, matched) if not _reads(arm, cte.alias)]
            if all(anchors):
                continue
        kept = [arm for arm, m in zip(arms, matched) if not m]
        if matched[0]:
            kept[0] = _keep_column_names(kept[0], arms[0])
        replacement: exp.Expression = kept[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for arm in kept[1:]:
            replacement = type(union)(
                this=replacement, expression=arm, distinct=union.args.get("distinct")
            )
        # The chain's own WITH, ORDER BY and LIMIT belong to the statement, not to an arm: an arm
        # that is parenthesized or has its own is read as a subquery to carry them.
        carried = {
            key: union.args[key]
            for key in ("with_", "order", "limit", "offset")
            if union.args.get(key) is not None
        }
        if carried and (
            not isinstance(replacement, (exp.Select, exp.Union))
            or any(replacement.args.get(key) is not None for key in carried)
        ):
            replacement = exp.subquery(replacement, "_u").select("*")
        for key, value in carried.items():
            replacement.set(key, value)
        modified = True
        if union.parent is None:
            root = replacement
        else:
            union.replace(replacement)

    if not modified:
        return sql
    for with_ in root.find_all(exp.With):
        if with_.args.get("recursive") and not any(_self_referencing(c) for c in with_.expressions):
            with_.set("recursive", False)
    return root.sql(dialect="postgres")


def apply_dropped_tables(sql: str, dropped: dict[str, list[str]]) -> str:  # REQ-599
    """Drop UNION branches for each table in *dropped*; raise the table's own reason for any
    that survive.

    ``dropped`` maps a table the query cannot address to the required parameters (required_args /
    path params) it is given no value for; an unreachable remote or a failed fetch is never here -- it fails the query
    (REQ-1661, amended 2026-09-30). A table referenced inside a UNION
    (a relationship join, a multi-label sweep) is simply excluded from the sweep — the ordinary
    REQ-848/REQ-941 behavior. A table that is the SOLE FROM target has no branch to drop, would
    otherwise reach the engine unqualified, and surfaces as an opaque native catalog error
    instead of the caller-facing reason recorded here — so it raises loud instead (matches the
    pgwire ``_optimize_and_route`` no-op detection, generalized to every settled-pipeline caller).
    """
    from provisa.core.parameter_binding import ParameterRequired

    for table_name, missing in dropped.items():
        sql = drop_union_branches_for_table(sql, table_name)
        if table_name in find_api_table_names(sql):
            raise ParameterRequired(table_name, missing)
    return sql


def where_referenced_tables(sql: str) -> set[str]:  # REQ-599
    """Return table names (or aliases) that appear in WHERE predicates (not JOIN ON conditions)."""
    ast = sqlglot.parse_one(sql, dialect="postgres")
    names: set[str] = set()
    for where in ast.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            tbl = col.table
            if tbl:
                names.add(tbl)
    return names


def name_parameters_by_column(tree: exp.Expr, ctx: Any) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # REQ-1986
    """Rename, IN PLACE, each reference to a table's parameter by another of the names it is bound
    under (its own name, its alias: ``CompilationContext.parameter_names``) to its parameter
    column, before the statement is governed and bound -- the one step a SQL statement's
    parameter names go through, so the binder reads every parameter by its column. A renamed
    parameter selected bare keeps the name it was given as its output name. An unqualified name
    is renamed only where exactly one source of its scope can hold it. True when one was
    renamed."""
    names_by_id: Mapping[int, Mapping[str, str]] = ctx.parameter_names
    other = {n for m in names_by_id.values() for n, column in m.items() if n != column.lower()}
    if not other or not any(c.name.lower() in other for c in tree.find_all(exp.Column)):
        return False
    from sqlglot.optimizer.scope import traverse_scope

    from provisa.compiler.naming import table_id_of
    from provisa.compiler.stage2 import context_table_map

    table_map = context_table_map(ctx)
    held: dict[int, set[str]] = {}
    for (table_id, physical), published in ctx.physical_to_sql.items():
        held.setdefault(table_id, set()).update((physical.lower(), published.lower()))
    changed = False
    for scope in traverse_scope(tree):
        parameterized: dict[str, Mapping[str, str]] = {}
        others: list[set[str] | None] = []
        for name, source in scope.sources.items():
            table_id = table_id_of(source, table_map) if isinstance(source, exp.Table) else None
            if table_id is not None and table_id in names_by_id:
                parameterized[name] = names_by_id[table_id]
            elif table_id is not None:
                others.append(held.get(table_id, set()))
            else:
                # A derived table, a CTE, a function: it may hold any name.
                others.append(None)
        if not parameterized:
            continue
        renamed: dict[int, str] = {}
        for col in scope.columns:
            key = col.name.lower()
            if key not in other:
                continue
            if col.table:
                names = parameterized.get(col.table)
                column = None if names is None else names.get(key)
            else:
                owners = [m[key] for m in parameterized.values() if key in m]
                clear = all(o is not None and key not in o for o in others)
                column = owners[0] if len(owners) == 1 and clear else None
            if column is None:
                continue
            renamed[id(col)] = col.name
            col.set("this", exp.to_identifier(column, quoted=True))
            changed = True
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        for projection in list(select.expressions):
            if isinstance(projection, exp.Column) and id(projection) in renamed:
                projection.replace(exp.alias_(projection.copy(), renamed[id(projection)]))
    return changed

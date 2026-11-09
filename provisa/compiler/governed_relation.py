# Copyright (c) 2026 Kenneth Stott
# Canary: 3c6f0b52-9e1d-4a87-b4f3-61d2e7a8c915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a governed statement through its governed relations (REQ-039).

Stage 2 puts the role's governed relation in the place of every registered table a statement
reads: a derived table, under the table's alias, whose SELECT reads that one table and exposes
only the columns the role is served, masked, with the row rule applied
(``stage2._governed_relation``). The governed statement passes between stages as text, so the
passes after governance -- result typing, row-level bounds, ``_nf_`` extraction, materialized-view
rewrite -- recognise a governed relation by its shape, here, and read its table through it.

The shape: a ``Subquery`` with an alias, whose body is a plain SELECT (no join, grouping,
DISTINCT, window, ordering or limit) of named projections from either the table itself under the
same alias, or another derived table of the same shape under the same alias (the faked and
member projections of the chain stage 2 builds). A derived table a role writes in that shape is
read the same way: it is only ever read here, after stage 2 governed what is inside it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

import sqlglot.expressions as exp

#: What a governed relation's SELECT may not carry: each changes which rows or values it holds.
_NOT_A_RELATION = (
    "joins",
    "laterals",
    "group",
    "having",
    "distinct",
    "order",
    "limit",
    "offset",
    "qualify",
    "windows",
    "with_",
    "kind",
    "pivots",
)


def _folded(identifier: exp.Expr | None) -> str:
    if not isinstance(identifier, exp.Identifier):
        return ""
    return identifier.name if identifier.quoted else identifier.name.lower()


def _alias(node: exp.Expr) -> str:
    alias = node.args.get("alias")
    return _folded(alias.this) if isinstance(alias, exp.TableAlias) else ""


@dataclass
class ReadThrough:
    """A governed relation read as its table.

    ``table`` is the registered table it reads, ``alias`` the name the statement reads it by,
    ``columns`` each exposed column's value over ``table`` (resolved through the chain, so a
    column served as it is reads as a bare column of ``table``), and ``predicates`` the row rule
    each level applies, over ``table``'s columns."""

    relation: exp.Subquery
    table: exp.Table
    alias: str
    columns: dict[str, exp.Expr] = field(default_factory=dict)
    predicates: list[exp.Expr] = field(default_factory=list)

    def column(self, name: str) -> str | None:
        """The physical column of ``table`` the relation exposes as ``name`` unchanged -- served as
        it is, neither masked nor computed. None when it exposes no such column, or a value that is
        not the column's own."""
        value = self.columns.get(name)
        if value is None:
            value = next((v for k, v in self.columns.items() if k.lower() == name.lower()), None)
        if isinstance(value, exp.Column) and not isinstance(value.this, exp.Star):
            return value.name
        return None

    def over_table(self, predicate: exp.Expr, *, sole: bool) -> exp.Expr:
        """``predicate``, read where the relation stands, as a predicate over its table: each
        column of the relation (qualified by its alias, or unqualified when it is the one relation
        its SELECT reads) as the table's column it serves as it is, under the same alias. A column
        it serves masked or computed reads as NULL, so no bound is taken from it -- a mask's value
        is not the column's."""

        def swap(n: exp.Expr) -> exp.Expr:
            if not isinstance(n, exp.Column) or isinstance(n.this, exp.Star):
                return n
            if n.table != self.alias and not (sole and not n.table):
                return n
            physical = self.column(n.name)
            if physical is None:
                return exp.Null()
            return exp.column(physical, table=self.alias)

        return predicate.copy().transform(swap)


def _level(select: exp.Select, alias: str) -> tuple[dict[str, exp.Expr], exp.Expr | None] | None:
    """One level of the chain: its projections by exposed name, and its FROM item. None when the
    SELECT is not a plain projection of one relation under ``alias``."""
    if any(select.args.get(k) for k in _NOT_A_RELATION):
        return None
    source = select.args.get("from_")
    if source is None or not isinstance(source.this, (exp.Table, exp.Subquery)):
        return None
    if _alias(source.this) != alias and not (
        isinstance(source.this, exp.Table) and not _alias(source.this) and source.this.name == alias
    ):
        return None
    projections: dict[str, exp.Expr] = {}
    for projection in select.expressions:
        if isinstance(projection, exp.Alias):
            projections[projection.alias] = projection.this
        elif isinstance(projection, exp.Column) and not isinstance(projection.this, exp.Star):
            projections[projection.name] = projection
        else:
            return None
    return projections, source.this


def read_through(node: exp.Expr | None) -> ReadThrough | None:
    """``node`` read as the registered table its governed relation reads, or None when ``node``
    is not a derived table of that shape."""
    if not isinstance(node, exp.Subquery) or not isinstance(node.this, exp.Select):
        return None
    alias = _alias(node)
    if not alias:
        return None
    levels: list[tuple[dict[str, exp.Expr], exp.Select]] = []
    current: exp.Expr | None = node
    while isinstance(current, exp.Subquery):
        if not isinstance(current.this, exp.Select):
            return None
        level = _level(current.this, alias)
        if level is None:
            return None
        projections, source = level
        levels.append((projections, current.this))
        current = source
    if not isinstance(current, exp.Table) or not current.name:
        return None
    # Innermost first: each level's values and row rule over the base table, an inner level's
    # bare column replaced by the value the level below exposes under its name.
    below: dict[str, exp.Expr] | None = None
    predicates: list[exp.Expr] = []
    for projections, select in reversed(levels):
        where = select.args.get("where")
        if where is not None:
            predicates.append(
                where.this.copy() if below is None else _over(where.this, alias, below)
            )
        below = (
            {name: value.copy() for name, value in projections.items()}
            if below is None
            else {name: _over(value, alias, below) for name, value in projections.items()}
        )
    return ReadThrough(node, current, alias, below or {}, predicates)


def _over(value: exp.Expr, alias: str, below: dict[str, exp.Expr]) -> exp.Expr:
    """``value`` with each column of the level below replaced by what that level exposes."""

    def swap(n: exp.Expr) -> exp.Expr:
        if (
            isinstance(n, exp.Column)
            and not isinstance(n.this, exp.Star)
            and (not n.table or n.table == alias)
            and n.name in below
        ):
            return below[n.name].copy()
        return n

    return value.copy().transform(swap)


def base_table(node: exp.Expr | None) -> exp.Table | None:
    """The table a FROM or JOIN item reads: the item itself, or the table its governed relation
    reads. None for any other item."""
    if isinstance(node, exp.Table):
        return node
    relation = read_through(node)
    return None if relation is None else relation.table


def relation_of(table: exp.Table) -> ReadThrough | None:
    """The outermost governed relation that reads ``table`` as its base, or None when ``table``
    stands in the statement as itself."""
    found: ReadThrough | None = None
    node: exp.Expr | None = table.parent
    while node is not None:
        if isinstance(node, exp.Subquery):
            rt = read_through(node)
            if rt is None or rt.table is not table:
                break
            found = rt
        elif not isinstance(node, (exp.From, exp.Select)):
            break
        node = node.parent
    return found


def relations(tree: exp.Expr) -> list[ReadThrough]:
    """Every governed relation in ``tree``, outermost of each chain only."""
    out: list[ReadThrough] = []
    for table in tree.find_all(exp.Table):
        rt = relation_of(table)
        if rt is not None:
            out.append(rt)
    return out


# -- the outputs a statement reads ------------------------------------------------------------


def read_outputs(tree: exp.Expr, relation: ReadThrough) -> set[str] | None:
    """The outputs of ``relation`` that ``tree`` reads outside it, by the names it exposes; None
    when it reads them all -- a ``*``, the alias's ``.*``, a whole-row reference or a NATURAL
    join. A name the statement may read unqualified is counted as read."""
    return _read_names(tree, relation.relation, relation.alias, relation.columns)


class _Reads:
    """What a statement's columns and joins name, tallied in one scan: a statement of many
    governed relations (an unlabelled Cypher MATCH has one per table) is asked of each, and a scan
    of the whole statement per relation spent the request's time budget in this pass."""

    def __init__(self, scope: exp.Expr) -> None:
        self.columns: dict[tuple[str, str], int] = {}
        self.stars: dict[str, int] = {}
        self.using: dict[str, int] = {}
        self.method_joins = 0
        self.add(scope, 1)

    def add(self, scope: exp.Expr, sign: int) -> None:
        """Count every read under ``scope``, or take them back with a ``sign`` of -1."""
        for node in scope.walk():
            if isinstance(node, exp.Column):
                qualifier = _folded(node.args.get("table")).lower()
                if isinstance(node.this, exp.Star):
                    self.stars[qualifier] = self.stars.get(qualifier, 0) + sign
                else:
                    key = (qualifier, node.name.lower())
                    self.columns[key] = self.columns.get(key, 0) + sign
            elif isinstance(node, exp.Join):
                if node.args.get("method"):
                    self.method_joins += sign
                for ident in node.args.get("using") or []:
                    name = ident.name.lower()
                    self.using[name] = self.using.get(name, 0) + sign


def _read_names(
    tree: exp.Expr,
    relation: exp.Subquery,
    alias: str,
    names: Iterable[str],
    reads: _Reads | None = None,
) -> set[str] | None:
    """The ``names`` of ``relation``'s outputs that ``tree`` reads outside it (see
    :func:`read_outputs`); None when it reads them all. ``reads`` is ``tree``'s tally, when the
    caller keeps one across its relations."""
    exposed: dict[str, list[str]] = {}
    for name in names:
        exposed.setdefault(name.lower(), []).append(name)
    tally = reads if reads is not None else _Reads(tree)
    if tally.method_joins > 0:
        return None
    holder = relation.parent
    outer = holder.parent if isinstance(holder, (exp.From, exp.Join)) else None
    if isinstance(outer, exp.Select) and any(isinstance(p, exp.Star) for p in outer.expressions):
        return None
    inside = _Reads(relation)
    own = alias.lower()

    def outside(qualifier: str, name: str) -> int:
        key = (qualifier, name)
        return tally.columns.get(key, 0) - inside.columns.get(key, 0)

    for qualifier in ("", own):
        if tally.stars.get(qualifier, 0) - inside.stars.get(qualifier, 0) > 0:
            return None
    if own not in exposed and outside("", own) > 0:
        return None  # a whole-row reference: SELECT t0 FROM ...
    read: set[str] = set()
    for name, spelled in exposed.items():
        if outside("", name) > 0 or outside(own, name) > 0 or tally.using.get(name, 0) > 0:
            read.update(spelled)
    return read


def keep_read_outputs(tree: exp.Expr, relation: ReadThrough) -> None:
    """:func:`keep_read` for ``relation``."""
    keep_read(tree, relation.relation, relation.alias)


def keep_read(
    tree: exp.Expr, relation: exp.Subquery, alias: str, reads: _Reads | None = None
) -> None:
    """Drop, IN PLACE, the outputs of the governed relation ``relation`` (read under ``alias``)
    that ``tree`` never reads, and then, level by level down its chain (the faked, kept and member
    projections, REQ-1494/1942/1620), each output the level above no longer reads: an engine
    reads, and a live source is asked for, only the columns the statement and the relation's own
    rules use, as for a table read as itself. A SELECT keeps its first output when none is read
    (``count(*)``): it has at least one. ``reads`` is ``tree``'s tally, kept true here, for a
    caller pruning several relations of one statement (:func:`keep_each_read`)."""
    body = relation.this
    if not isinstance(body, exp.Select):
        return
    read = _read_names(tree, relation, alias, [p.alias_or_name for p in body.expressions], reads)
    if reads is not None:
        reads.add(relation, -1)
    _keep(body, read)
    if reads is not None:
        reads.add(relation, 1)


def keep_each_read(tree: exp.Expr, relations: list[tuple[exp.Subquery, str]]) -> None:
    """:func:`keep_read` for each ``(relation, alias)`` of ``tree``, on one tally of it."""
    reads = _Reads(tree)
    for relation, alias in relations:
        keep_read(tree, relation, alias, reads)


#: What a SELECT whose outputs may be dropped does not carry: each reads its outputs as a whole
#: (a DISTINCT's rows, a grouping's or an aggregate's, an output named by an ordering).
_READS_ITS_OUTPUTS = ("distinct", "group", "having", "qualify", "order", "windows", "with_")


def _keep(query: exp.Expr, read: set[str] | None) -> None:
    """Keep only the outputs of ``query`` named in ``read`` (all of them when None), then prune
    each derived table it reads to the outputs it still reads. A UNION ALL keeps the same
    positions in every branch."""
    wanted = None if read is None else {name.lower() for name in read}
    if isinstance(query, exp.Union):
        branches = _branches(query)
        if (
            wanted is not None
            and query.args.get("distinct") is not True
            and all(_droppable(b) for b in branches)
        ):
            first = branches[0].expressions
            positions = [i for i, p in enumerate(first) if p.alias_or_name.lower() in wanted]
            positions = positions or [0]
            if all(len(b.expressions) == len(first) for b in branches):
                for branch in branches:
                    branch.set("expressions", [branch.expressions[i] for i in positions])
        for branch in branches:
            _keep_sources(branch)
        return
    if not isinstance(query, exp.Select):
        return
    if wanted is not None and _droppable(query):
        kept = [p for p in query.expressions if p.alias_or_name.lower() in wanted]
        query.set("expressions", kept or query.expressions[:1])
    _keep_sources(query)


def _branches(union: exp.Union) -> list[exp.Select]:
    out: list[exp.Select] = []
    for side in (union.this, union.expression):
        if isinstance(side, exp.Union) and side.args.get("distinct") is not True:
            out += _branches(side)
        elif isinstance(side, exp.Select):
            out.append(side)
        else:
            return []
    return out


def _droppable(select: exp.Select) -> bool:
    """Whether an output of ``select`` may be dropped without changing its rows or other values."""
    if any(select.args.get(k) for k in _READS_ITS_OUTPUTS):
        return False
    for projection in select.expressions:
        if isinstance(projection, exp.Star) or (
            isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
        ):
            return False
        for agg in projection.find_all(exp.AggFunc):
            if agg.find_ancestor(exp.Window) is None:
                return False
    return True


def _keep_sources(select: exp.Select) -> None:
    """Prune each derived table ``select`` reads in its FROM to the outputs it reads."""
    if select.args.get("joins"):
        return
    source = select.args.get("from_")
    item = None if source is None else source.this
    if not isinstance(item, exp.Subquery) or not isinstance(item.this, (exp.Select, exp.Union)):
        return
    names = _output_names(item.this)
    read = None if names is None else _read_names(select, item, _alias(item), names)
    _keep(item.this, read)


def _output_names(query: exp.Expr) -> list[str] | None:
    if isinstance(query, exp.Union):
        select = next(iter(_branches(query)), None)
    else:
        select = query
    if not isinstance(select, exp.Select):
        return None
    return [p.alias_or_name for p in select.expressions]


# -- a lone governed relation, inlined ----------------------------------------------------------


def _served(relation: ReadThrough, column: exp.Column) -> exp.Expr | None:
    """The value ``relation`` exposes under ``column``'s name, read as PostgreSQL resolves the
    name (an unquoted name folds to lower case). None when it exposes no such column."""
    value = relation.columns.get(column.name)
    if value is None and not column.this.quoted:
        value = relation.columns.get(column.name.lower())
    return value


def _inside(node: exp.Expr, ancestor: exp.Expr) -> bool:
    parent = node.parent
    while parent is not None:
        if parent is ancestor:
            return True
        parent = parent.parent
    return False


def _positional(column: exp.Column) -> bool:
    """Whether ``column`` stands as a GROUP BY, ORDER BY or DISTINCT ON item itself, where
    PostgreSQL reads a constant as an output column's position."""
    parent = column.parent
    if isinstance(parent, exp.Tuple) and isinstance(parent.parent, exp.Distinct):
        return True
    return isinstance(parent, (exp.Group, exp.Ordered))


def _inlined(select: exp.Select) -> bool:
    """``select`` reading its lone governed relation's table in its place; False, ``select``
    untouched, when it reads anything else or names what the relation does not expose."""
    source = select.args.get("from_")
    relation = read_through(source.this if source is not None else None)
    if relation is None or select.args.get("joins") or select.args.get("laterals"):
        return False
    # A nested statement resolves names in a scope of its own: the step stays out of it.
    if any(
        s is not select and not _inside(s, relation.relation)
        for s in select.find_all(exp.Select, exp.SetOperation)
    ):
        return False
    outputs = {p.alias_or_name for p in select.expressions}
    stars: list[exp.Expr] = []
    swaps: list[tuple[exp.Column, exp.Expr]] = []
    for column in select.find_all(exp.Column):
        if _inside(column, relation.relation):
            continue
        if column.args.get("db") or column.args.get("catalog"):
            return False
        if column.table and _folded(column.args.get("table")) != relation.alias:
            continue  # a column of an enclosing statement's relation
        if isinstance(column.this, exp.Star):
            if column.parent is not select:
                return False
            stars.append(column)
            continue
        ordered = column.parent
        if (
            not column.table
            and isinstance(ordered, exp.Ordered)
            and isinstance(ordered.parent, exp.Order)
            and column.name in outputs
        ):
            continue  # an ORDER BY name is the output column it names first
        value = _served(relation, column)
        if value is None:
            return False
        if _positional(column) and value.find(exp.Column) is None:
            return False  # a constant there names an output column by number, or none
        swaps.append((column, value))
    stars += [p for p in select.expressions if isinstance(p, exp.Star)]

    for column, value in swaps:
        if column.parent is select:
            same = isinstance(value, exp.Column) and value.name == column.name
            column.replace(
                value.copy() if same else exp.Alias(this=value.copy(), alias=column.this.copy())
            )
        else:
            column.replace(value.copy())
    for star in stars:
        index = select.expressions.index(star)
        served = [
            exp.Alias(this=v.copy(), alias=exp.to_identifier(n, quoted=True))
            for n, v in relation.columns.items()
        ]
        select.set(
            "expressions", select.expressions[:index] + served + select.expressions[index + 1 :]
        )
    where = select.args.get("where")
    held = [p.copy() for p in relation.predicates] + ([where.this] if where is not None else [])
    if held:
        select.set("where", exp.Where(this=exp.and_(*held)))
    relation.relation.replace(relation.table.copy())
    return True


def inline(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each SELECT whose one relation is a governed relation reading the relation's
    table in its place: each exposed column as the value it is served as (a masked or computed
    one stays its expression), ``*`` as the served columns, the relation's row rules ANDed into the
    SELECT's WHERE. It answers what ``tree`` answers -- the same rows, columns and masks -- for a
    translator into a query language with no derived table (REQ-039, REQ-2000). A SELECT that
    also reads another relation, holds a nested statement, or names a column its relation does
    not expose is left as it stands. ``tree`` itself is not changed."""
    out = tree.copy()
    for select in list(out.find_all(exp.Select)):
        if select.root() is out:
            _inlined(select)
    return out

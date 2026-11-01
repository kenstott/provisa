# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e93ad-5f28-4b06-9d41-e2a86b0f7c35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which relations a catalog statement is about (REQ-2190): PostgreSQL pushes a client's
``c.relname = 'orders'`` into no view, and joining on oids it would name and type every column of
the model for one table. When the statement says, by its own conditions, that every catalog row it
can use belongs to tables of certain names, its settings tell the views so
(:func:`~provisa.transpiler.metadata_settings.asked_sql`) and they read those tables alone.

This is a proof, so anything not proved leaves the statement whole, and slower. A read is pinned by
a literal name, or by an equality on a relation's oid with a read already pinned. A condition counts
where it binds every row: a conjunct of WHERE or of an inner join's ON; and an outer join's ON only
for the side it joins. A statement with one read of the model unpinned, or a function of a catalog
asked about anything but a pinned read's row, is not pinned at all. What a statement gets is
unchanged: its rows come from views as they stand, under the same row scope, on every statement."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.metadata_functions import FORMS
from provisa.transpiler.metadata_views import READ_WHOLE

_META = "meta"
_CLASS = "catalog_pg_class"
#: Each relation's column holding the oid of the table a row is about. Equal columns are one table.
_TABLE_OID = {
    _CLASS: "oid",
    "catalog_pg_attribute": "attrelid",
    "catalog_pg_index": "indrelid",
    "catalog_pg_constraint": "conrelid",
    "catalog_pg_attrdef": "adrelid",
    "catalog_pg_description": "objoid",
}
#: Each relation's column holding the oid of an index: pg_class's row of that index, which goes by
#: its own table's name. It reaches pg_class and goes no further.
_INDEX_OID = {"catalog_pg_index": "indexrelid", "catalog_pg_constraint": "conindid"}
#: What a function's argument may read beside those: a key's own oid, of a key of a pinned table.
_KEY_OID = {"catalog_pg_constraint": "oid"}


def _bare(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _conjuncts(condition: exp.Expr | None) -> list[exp.Expr]:
    if condition is None:
        return []
    node = _bare(condition)
    if isinstance(node, exp.And):
        return [*_conjuncts(node.left), *_conjuncts(node.right)]
    return [node]


def _sources(select: exp.Select) -> dict[str, exp.Expr]:
    """What ``select`` reads, by the name its columns qualify it by."""
    from_ = select.args.get("from_")
    found = [from_.this] if from_ is not None else []
    found += [join.this for join in select.args.get("joins") or ()]
    return {node.alias_or_name.lower(): node for node in found}


def _enclosing(node: exp.Expr) -> exp.Select | None:
    return node.find_ancestor(exp.Select)


def _read_of(column: exp.Expr) -> tuple[exp.Table, str] | None:
    """The read of a relation ``column`` is a column of, and its name there: through a derived
    table's plain projection, as pgjdbc reads ``ci.oid`` as ``tmp.CI_OID``. None for anything else
    or any other column."""
    column = _bare(column)
    if not isinstance(column, exp.Column):
        return None
    name, qualifier = column.name.lower(), column.table.lower()
    select = _enclosing(column)
    while select is not None:
        sources = _sources(select)
        source = sources.get(qualifier) if qualifier else None
        if not qualifier and len(sources) == 1:
            source = next(iter(sources.values()))
        if isinstance(source, exp.Table):
            return source, name
        if isinstance(source, exp.Subquery) and isinstance(source.this, exp.Select):
            for projected in source.this.selects:
                if projected.alias_or_name.lower() == name:
                    return _read_of(projected.unalias())
            return None
        if source is not None:
            return None
        select = _enclosing(select)
    return None


class _NotPinned(Exception):
    """A form of FROM this does not read."""


def _bindings(select: exp.Select) -> list[tuple[exp.Expr, exp.Expr | None]]:
    """Each conjunct binding the rows of ``select``, and the one read an outer join's binds."""
    where = select.args.get("where")
    bound: list[tuple[exp.Expr, exp.Expr | None]] = [
        (c, None) for c in _conjuncts(where.this if where is not None else None)
    ]
    for join in select.args.get("joins") or ():
        if join.args.get("using"):
            raise _NotPinned
        side = (join.side or "").upper()
        if side in ("RIGHT", "FULL"):
            raise _NotPinned
        bound += [(c, join.this if side else None) for c in _conjuncts(join.args.get("on"))]
    return bound


def _literal_names(condition: exp.Expr) -> tuple[exp.Column, list[str]] | None:
    """The relname column ``condition`` compares with literal names, and the names."""
    if isinstance(condition, exp.EQ):
        for column, value in (
            (condition.left, condition.right),
            (condition.right, condition.left),
        ):
            if (
                isinstance(column, exp.Column)
                and isinstance(value, exp.Literal)
                and value.is_string
            ):
                return (column, [value.this]) if column.name.lower() == "relname" else None
    if isinstance(condition, exp.In) and isinstance(condition.this, exp.Column):
        values = condition.expressions
        if (
            values
            and condition.this.name.lower() == "relname"
            and all(isinstance(v, exp.Literal) and v.is_string for v in values)
        ):
            return condition.this, [v.this for v in values]
    return None


def _relation(read: exp.Table) -> str:
    return read.name.lower()


def _names_of_literals(
    bound: list[tuple[exp.Expr, exp.Expr | None]], pinned: set[int], names: set[str]
) -> None:
    for condition, joined in bound:
        found = _literal_names(condition)
        if found is None:
            continue
        read = _read_of(found[0])
        if (
            read is None
            or _relation(read[0]) != _CLASS
            or (joined is not None and joined is not read[0])
        ):
            continue
        pinned.add(id(read[0]))
        names.update(found[1])


def _follow(
    bound: list[tuple[exp.Expr, exp.Expr | None]], pinned: set[int], indexes: set[int]
) -> bool:
    """Each read an equality binds to a pinned one's table; True if that pinned any."""
    grew = False
    for condition, joined in bound:
        if not isinstance(condition, exp.EQ):
            continue
        first, second = _read_of(condition.left), _read_of(condition.right)
        if first is None or second is None:
            continue
        for (given, given_column), (other, other_column) in ((first, second), (second, first)):
            if id(given) not in pinned or id(other) in pinned or id(given) in indexes:
                continue
            if joined is not None and joined is not other:
                continue
            to_table = _TABLE_OID.get(_relation(other)) == other_column
            if _TABLE_OID.get(_relation(given)) == given_column and to_table:
                pinned.add(id(other))
                grew = True
            elif _INDEX_OID.get(_relation(given)) == given_column and _relation(other) == _CLASS:
                pinned.add(id(other))
                indexes.add(id(other))
                grew = True
    return grew


def _kind_condition(
    relation_read: tuple[exp.Table, str], other: tuple[exp.Table, str]
) -> exp.Expr | None:
    """What an equality between pg_class's oid and ``other`` says of the pg_class row: no other
    relation lists an index as a table, and an index oid is an index's."""
    read, column = relation_read
    if _relation(read) != _CLASS or column != "oid":
        return None
    kind = exp.column("relkind", table=read.alias_or_name)
    index = exp.Literal.string("i")
    relation = _relation(other[0])
    if relation != _CLASS and _TABLE_OID.get(relation) == other[1]:
        return exp.NEQ(this=kind, expression=index)
    if _INDEX_OID.get(relation) == other[1]:
        return exp.EQ(this=kind, expression=index)
    return None


def narrow_kinds(tree: exp.Expr) -> None:
    """Add to ``tree`` what its joins say of a pg_class read's relkind, which a view's arms are told
    apart by: a read joined on a table's oid is spared every key of the model. Only a condition
    binding the read's rows, in the read's own SELECT, says so."""
    for select in list(tree.find_all(exp.Select)):
        try:
            bound = _bindings(select)
        except _NotPinned:
            continue
        own = {id(source) for source in _sources(select).values()}
        for condition, joined in bound:
            if joined is not None or not isinstance(condition, exp.EQ):
                continue
            first, second = _read_of(condition.left), _read_of(condition.right)
            if first is None or second is None:
                continue
            for read, other in ((first, second), (second, first)):
                kind = _kind_condition(read, other) if id(read[0]) in own else None
                if kind is not None:
                    select.where(kind, copy=False)


def _reads_catalog(name: str, arity: int) -> bool:
    form = FORMS.get((name, arity))
    return form is not None and (bool(form.facts) or "meta." in form.sql)


def _asks_of_pinned(tree: exp.Expr, pinned: set[int]) -> bool:
    """Whether every function of a catalog asks about a row of a pinned read."""
    for call in tree.find_all(exp.Anonymous):
        args = call.expressions
        if not _reads_catalog(call.name.lower(), len(args)):
            continue
        read = _read_of(args[0])
        if read is None or id(read[0]) not in pinned:
            return False
        relation, column = _relation(read[0]), read[1]
        if column not in (
            _TABLE_OID.get(relation),
            _INDEX_OID.get(relation),
            _KEY_OID.get(relation),
        ):
            return False
    return True


def asked_relations(tree: exp.Expr) -> frozenset[str] | None:
    """The names of the relations ``tree`` (addressed to meta, its functions not yet written in) is
    about, or None for a statement that must read the model whole."""
    reads = [
        table
        for table in tree.find_all(exp.Table)
        if (table.db or "").lower() == _META and table.name.lower() not in READ_WHOLE
    ]
    if not reads:
        return None
    try:
        bound = [b for select in tree.find_all(exp.Select) for b in _bindings(select)]
    except _NotPinned:
        return None
    pinned: set[int] = set()
    indexes: set[int] = set()
    names: set[str] = set()
    _names_of_literals(bound, pinned, names)
    while _follow(bound, pinned, indexes):
        pass
    if not names or any(id(read) not in pinned for read in reads):
        return None
    return frozenset(names) if _asks_of_pinned(tree, pinned) else None

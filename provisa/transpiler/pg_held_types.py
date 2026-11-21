# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f4c81-3b9e-4a06-b1d5-90e6c2a7f134
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's types no other engine has (REQ-2171, REQ-2190): a statement with no table goes to
the engine, and a value of one of these is held there as the type below. pgwire names the column by
the statement's cast (provisa.pgwire.cast_types); the engine has only the value."""

# Requirements: REQ-2171, REQ-2190

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.scope import Scope, traverse_scope

#: Each engine type, and PostgreSQL's types whose values it holds.
HELD_AS: dict[str, tuple[str, ...]] = {
    "VARCHAR": (
        "pg_lsn", "pg_snapshot", "txid_snapshot", "aclitem", "macaddr", "macaddr8", "cidr",
        "tsvector", "pg_mcv_list", "pg_ndistinct", "pg_dependencies", "pg_node_tree",
        "box", "circle", "line", "lseg", "path", "polygon", "point", "xml", "varbit", "cstring",
    ),
    "UBIGINT": ("xid8",),
    "BIGINT": (
        "regcollation", "regconfig", "regdictionary", "regoper", "regoperator", "regprocedure",
        "regrole",
    ),
}  # fmt: skip
#: What a client casts to that the engine reads as text or a number, and only the catalog named.
_ON_THE_ENGINE = {
    **{name: held for held, names in HELD_AS.items() for name in names},
    "name": "VARCHAR",
    "tid": "VARCHAR",
    "oid": "BIGINT",
    "oidvector": "VARCHAR",
    "int2vector": "VARCHAR",
    "regproc": "BIGINT",
    "xid": "BIGINT",
    "cid": "BIGINT",
}
ENGINE_NAMES = frozenset(_ON_THE_ENGINE)
_ARRAYS = frozenset("_" + name for name in _ON_THE_ENGINE)
#: PostgreSQL's array type names of what every engine has, as a form spells them.
_OWN_ARRAYS = {"_text": "VARCHAR[]"}


def mentioned(sql: str) -> bool:
    """Whether a cast in ``sql`` may name one of the types, before the statement is parsed."""
    lowered = sql.lower()
    return any(name in lowered for name in (*ENGINE_NAMES, *_OWN_ARRAYS))


def _written(data_type: exp.DataType) -> str:
    return data_type.sql(dialect="postgres").lower().removeprefix("pg_catalog.").strip('"')


def _is_array(data_type: exp.DataType) -> bool:
    written = _written(data_type)
    return written in _ARRAYS or written.removesuffix("[]") in ENGINE_NAMES and written[-1] == "]"


def _is_tid(node: exp.Expr, scopes: dict[int, Scope]) -> bool:
    """Whether ``node`` is a tid: a cast to one, or a column a derived table, a CTE or VALUES
    gives one in."""
    if isinstance(node, (exp.Paren, exp.Alias)):
        return _is_tid(node.this, scopes)
    if isinstance(node, exp.Cast):
        return _written(node.to) == "tid"
    if not isinstance(node, exp.Column):
        return False
    scope = scopes.get(id(node.find_ancestor(exp.Select)))
    if scope is None:
        return False
    sources = scope.sources
    named = [sources[node.table]] if node.table in sources else list(sources.values())
    name = node.name.lower()
    for source in named:
        query = source.expression if isinstance(source, Scope) else None
        if isinstance(query, exp.Values) and name in query.alias_column_names:
            at = query.alias_column_names.index(name)
            if _is_tid(query.expressions[0].expressions[at], scopes):
                return True
        elif isinstance(query, exp.Query):
            given = [p for p in query.selects if p.alias_or_name.lower() == name]
            if given and _is_tid(given[0], scopes):
                return True
    return False


#: A tid's place in PostgreSQL's order of tids, by its block and then its offset, from its text.
_TID_ORDER = (
    "CAST(split_part(btrim(CAST(_v AS TEXT), '()'), ',', 1) AS BIGINT) * 65536 "
    "+ CAST(split_part(btrim(CAST(_v AS TEXT), '()'), ',', 2) AS BIGINT)"
)


def _tids_ordered(tree: exp.Expr) -> exp.Expr:
    """``tree`` with max and min of a tid as PostgreSQL orders tids (block, then offset): the
    engine holds a tid as its text, whose order is another."""
    aggregates = [
        node for node in tree.find_all(exp.Max, exp.Min) if not isinstance(node.parent, exp.Window)
    ]
    if not aggregates:
        return tree
    try:
        scopes = {id(scope.expression): scope for scope in traverse_scope(tree)}
    except OptimizeError:
        return tree  # no scopes to follow: only a cast says a tid
    for node in aggregates:
        if not _is_tid(node.this, scopes):
            continue
        key = sqlglot.parse_one(_TID_ORDER, read="postgres").transform(
            lambda n, v=node.this: v.copy() if isinstance(n, exp.Column) and n.name == "_v" else n
        )
        kind = exp.ArgMax if isinstance(node, exp.Max) else exp.ArgMin
        node.replace(kind(this=node.this.copy(), expression=key))
    return tree


def held(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each cast to one of the types a cast to what the engine holds it as, and
    each max and min of a tid in PostgreSQL's order of tids."""
    tree = _tids_ordered(tree)

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal) and _is_array(node.to):
            # PostgreSQL's array text, as the engine reads one.
            node.set("this", exp.Literal.string(node.this.this.replace("{", "[").replace("}", "]")))
            return node
        if not isinstance(node, exp.DataType):
            return node
        written = _written(node)
        if written in _OWN_ARRAYS:
            return exp.DataType.build(_OWN_ARRAYS[written], dialect="duckdb")
        if written in _ARRAYS:
            return exp.DataType.build(f"{_ON_THE_ENGINE[written[1:]]}[]", dialect="duckdb")
        element = written.removesuffix("[]")
        if element not in _ON_THE_ENGINE:
            return node
        suffix = "[]" if written != element else ""
        return exp.DataType.build(f"{_ON_THE_ENGINE[element]}{suffix}", dialect="duckdb")

    return tree.transform(rewrite, copy=False)

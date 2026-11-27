# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9a14-2b6d-4f58-9e1a-d4b07c5f3e82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JSON a compiled statement builds, on ClickHouse (#362): a relationship field's json_object
and json_agg (compiler/sql_selection.rel_read) and a list field's empty default, written as the
JSON text PostgreSQL's would parse to. ClickHouse has neither function: sqlglot writes
JSON_OBJECT('k': v) in a syntax ClickHouse does not parse, and the polyfill grid refuses a value
whose type the statement does not say (polyfill.json_built), which no compiled column does.

A value goes into JSON by its type, read where it runs (toTypeName):

- JSON the statement built (an object, a list, a grouped read's value): its own text;
- an instant: PostgreSQL's text of a timestamp -- a T between date and time, the fraction's
  trailing zeros dropped (ClickHouse holds a timestamp and a timestamptz alike, DateTime64);
- any other value: ClickHouse's toJSONString, with a decimal's scale kept
  (``output_format_decimal_trailing_zeros``, set on the statement), as PostgreSQL's numeric keeps it;
- NULL: null.

json_agg keeps a NULL value as null and answers NULL of no rows, as PostgreSQL's does. Only a
statement Provisa compiled is read so (``generated``): a statement a client writes says its types
or is refused by the grid."""

# Requirements: REQ-2236

from __future__ import annotations

import json

import sqlglot
import sqlglot.expressions as exp

_CH = "clickhouse"
#: The setting that keeps a decimal's scale in toJSONString, as PostgreSQL's numeric text does.
_DECIMAL_SCALE = "output_format_decimal_trailing_zeros"


def _form(template: str, **holes: exp.Expr) -> exp.Expr:
    """``template`` in ClickHouse's SQL with each ``_name`` hole filled by ``holes[name]``."""
    tree = sqlglot.parse_one(template, read=_CH)

    def fill(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name.startswith("_"):
            return holes[node.name[1:]].copy()
        return node

    return tree.transform(fill)


#: A value of any type, as its JSON text (not NULL: the caller says null).
_TYPED = (
    "multiIf(match(toTypeName(_v), 'DateTime'), "
    "concat('\"', replaceRegexpOne(replaceOne(toString(_v), ' ', 'T'), "
    "'(\\\\.[0-9]*[1-9])0+$|\\\\.0+$', '\\\\1'), '\"'), "
    "toJSONString(_v))"
)


def _is_read_value(node: exp.Expr) -> bool:
    """The JSON column of a relationship's grouped read (sql_selection.rel_read), or of the
    numbered rows a capped list's read collects."""
    from provisa.compiler.sql_selection import READ_ROWS, READ_VALUE

    return (
        isinstance(node, exp.Column)
        and node.name == READ_VALUE
        and (node.table.endswith("_read") or node.table == READ_ROWS)
    )


def _is_built(node: exp.Expr, built: set[int]) -> bool:
    """Whether ``node`` is JSON text this pass wrote or a grouped read holds."""
    node = node.unnest()
    if id(node) in built or _is_read_value(node):
        return True
    if isinstance(node, exp.Coalesce):
        return all(_is_built(e, built) for e in [node.this, *node.expressions])
    if isinstance(node, exp.Case):
        values = [i.args["true"] for i in node.args.get("ifs") or []]
        default = node.args.get("default")
        return all(_is_built(v, built) for v in values) and (
            default is None or _is_built(default, built)
        )
    return isinstance(node, exp.Literal) and node.is_string and node.this == "[]"


def _member(value: exp.Expr, built: set[int]) -> exp.Expr:
    """``value`` as its JSON text inside an object or a list: never NULL, null for one."""
    if isinstance(value, exp.Null):
        return exp.Literal.string("null")
    if isinstance(value, exp.Literal):
        return exp.Literal.string(value.this if not value.is_string else json.dumps(value.this))
    if isinstance(value, exp.Boolean):
        return exp.Literal.string("true" if value.this else "false")
    text = value if _is_built(value, built) else _form(_TYPED, v=value)
    return exp.Coalesce(this=text, expressions=[exp.Literal.string("null")])


def _object(node: exp.JSONObject, built: set[int]) -> exp.Expr:
    parts: list[exp.Expr] = []
    for i, pair in enumerate(node.expressions):
        if not isinstance(pair, exp.JSONKeyValue) or not isinstance(pair.this, exp.Literal):
            raise ValueError(f"a JSON object's member is no key and value: {pair.sql()}")
        lead = "{" if i == 0 else ","
        parts.append(exp.Literal.string(f"{lead}{json.dumps(pair.this.this)}:"))
        parts.append(_member(pair.expression, built))
    parts.append(exp.Literal.string("}" if parts else "{}"))
    return exp.Anonymous(this="concat", expressions=parts)


def _aggregated(node: exp.Expr) -> exp.Expr | None:
    """The value json_agg collects, or None for any other node."""
    if isinstance(node, exp.JSONArrayAgg):
        return node.this
    if (
        isinstance(node, exp.Anonymous)
        and node.name.lower() in ("json_agg", "jsonb_agg")
        and len(node.expressions) == 1
    ):
        return node.expressions[0]
    return None


def _list(value: exp.Expr, built: set[int], kept: exp.Expr | None = None) -> exp.Expr:
    """json_agg(value), or json_agg(value) FILTER (WHERE kept): the list cap's (REQ-005). A value
    ``ORDER BY`` one key (an OData expansion's order) is listed in that key's order."""
    order = value if isinstance(value, exp.Order) else None
    if order is not None:
        if len(order.expressions) != 1:
            raise ValueError(f"a JSON list in the order of several keys: {order.sql()}")
        (key,) = order.expressions
        value = exp.Tuple(expressions=[_member(order.this, built), key.this.copy()])
    else:
        value = _member(value, built)
    if kept is None:
        members: exp.Expr = exp.Anonymous(this="groupArray", expressions=[value])
    else:
        members = exp.Anonymous(this="groupArrayIf", expressions=[value, kept.copy()])
    if order is not None:
        sort = "arrayReverseSort" if order.expressions[0].args.get("desc") else "arraySort"
        members = _form(f"arrayMap(x -> x.1, {sort}(x -> x.2, _m))", m=members)
    return _form(
        "if(empty(_m), NULL, concat('[', arrayStringConcat(_m, ','), ']'))",
        m=members,
    )


def _is_json_list_default(node: exp.Expr) -> bool:
    """CAST('[]' AS JSON): a list field's empty default (sql_selection.EMPTY_JSON_LIST)."""
    return (
        isinstance(node, exp.Cast)
        and node.to.is_type(exp.DataType.Type.JSON)
        and isinstance(node.this, exp.Literal)
        and node.this.this == "[]"
    )


def _written(node: exp.Expr, built: set[int]) -> exp.Expr | None:
    """``node`` in ClickHouse's JSON text, or None for a node this pass does not write. An
    aggregate under a FILTER is written with its filter, not alone."""
    if _is_json_list_default(node):
        return exp.Literal.string("[]")
    if isinstance(node, exp.JSONObject):
        return _object(node, built)
    if isinstance(node, exp.Filter):
        collected = _aggregated(node.this)
        if collected is None:
            return None
        return _list(collected, built, node.expression.this)
    collected = _aggregated(node)
    if collected is None or isinstance(node.parent, exp.Filter):
        return None
    return _list(collected, built)


def relationship_json(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each JSON object and json_agg it builds, and each list field's empty default
    (CAST('[]' AS JSON)), as ClickHouse's JSON text. Inner first, so a value already written is
    told by ``built`` and embedded as the JSON it is."""
    built: set[int] = set()
    nodes = list(
        tree.find_all(exp.JSONObject, exp.JSONArrayAgg, exp.Anonymous, exp.Cast, exp.Filter)
    )
    nodes.sort(key=lambda n: -n.depth)
    for node in nodes:
        written = _written(node, built)
        if written is None:
            continue
        built.add(id(written))
        if node is tree:
            tree = written
        else:
            node.replace(written)
    if built and isinstance(tree, exp.Select):
        settings = list(tree.args.get("settings") or [])
        settings.append(exp.EQ(this=exp.column(_DECIMAL_SCALE), expression=exp.Literal.number(1)))
        tree.set("settings", settings)
    return tree

# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c1e08-9a4d-4b6e-8c1f-2d3a6b5e9c74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Predicate + projection pushdown translation for the airport service (REQ-1106).

The DuckDB airport extension delivers pushdown through the ``endpoints`` DoAction
``parameters`` map (verified against airport-go v0.2.1 flight/doaction_metadata.go):

  * ``column_ids``  — list[uint] of column indices into the table's FULL Arrow
    schema (the value ``2**64-1`` denotes the rowid pseudo-column, which the
    governed catalog does not advertise, so it is dropped).
  * ``json_filters`` — a JSON string holding DuckDB's serialized bound-expression
    tree: ``{"filters": [expr, ...], "column_binding_names_by_index": [name, ...]}``.
    The top-level ``filters`` are implicitly AND-ed. Expression shapes mirror
    airport-go filter/parse.go + filter/duckdb.go.

Both are translated here into a projected column list and a semantic WHERE clause,
which the server folds into the DoGet ticket. The governed pipeline then applies
them at the SOURCE (they become part of the semantic SQL that ``_govern_and_route``
compiles), with RLS/masking layered on top — so the source filters, not just the
DuckDB client.

Translation is deliberately conservative: any expression this module cannot render
is dropped from the pushed-down predicate (returns ``None`` for that branch). That
is safe because the airport protocol treats pushdown as a hint and DuckDB re-applies
the full predicate client-side; a dropped branch only forgoes an optimization, it
never changes results. An unrenderable branch is NEVER emitted as a partial/incorrect
SQL fragment.
"""

from __future__ import annotations

import json
from typing import Any

# airport-go: column_ids uses all-bits-set uint64 for the rowid pseudo-column.
_ROWID_SENTINEL = 2**64 - 1

_COMPARE_OPS = {
    "COMPARE_EQUAL": "=",
    "COMPARE_NOTEQUAL": "<>",
    "COMPARE_LESSTHAN": "<",
    "COMPARE_GREATERTHAN": ">",
    "COMPARE_LESSTHANOREQUALTO": "<=",
    "COMPARE_GREATERTHANOREQUALTO": ">=",
    "COMPARE_DISTINCT_FROM": "IS DISTINCT FROM",
    "COMPARE_NOT_DISTINCT_FROM": "IS NOT DISTINCT FROM",
}

# DuckDB operator-function names → SQL operator (filter/duckdb.go encodeOperatorFunction).
_OP_FUNCS = {
    "~~": "LIKE",
    "!~~": "NOT LIKE",
    "~~*": "ILIKE",
    "!~~*": "NOT ILIKE",
}


def resolve_projection(column_ids: list[Any] | None, full_columns: list[str]) -> list[str] | None:
    """Map airport ``column_ids`` (indices into the full schema) to column names.

    Returns the projected column names in request order, or ``None`` when no
    projection is requested (all columns). The rowid sentinel is dropped — the
    governed catalog exposes no rowid pseudo-column.
    """
    if not column_ids:
        return None
    out: list[str] = []
    for raw in column_ids:
        idx = int(raw)
        if idx == _ROWID_SENTINEL:
            continue
        if 0 <= idx < len(full_columns):
            out.append(full_columns[idx])
    # An empty projection (e.g. COUNT(*) selecting only rowid) still needs one column
    # for a valid SELECT; fall back to all columns rather than emit ``SELECT  FROM``.
    return out or None


def translate_filters(json_filters: str | bytes | None, columns: list[str]) -> str | None:
    """Translate DuckDB ``json_filters`` into a semantic SQL WHERE body (no ``WHERE``).

    ``columns`` resolves column-ref binding indices when the payload omits
    ``column_binding_names_by_index``. Returns ``None`` when nothing renderable.
    """
    if not json_filters:
        return None
    if isinstance(json_filters, bytes):
        json_filters = json_filters.decode("utf-8")
    try:
        payload = json.loads(json_filters)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    bindings = payload.get("column_binding_names_by_index") or columns
    filters = payload.get("filters")
    if not isinstance(filters, list):
        return None
    parts: list[str] = []
    for expr in filters:
        rendered = _encode(expr, bindings)
        if rendered:
            parts.append(rendered)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return "(" + ") AND (".join(parts) + ")"


def _encode(expr: Any, bindings: list[str]) -> str | None:
    if not isinstance(expr, dict):
        return None
    cls = expr.get("expression_class")
    if cls == "BOUND_COMPARISON":
        return _encode_comparison(expr, bindings)
    if cls == "BOUND_CONJUNCTION":
        return _encode_conjunction(expr, bindings)
    if cls == "BOUND_CONSTANT":
        return _encode_constant(expr)
    if cls == "BOUND_COLUMN_REF":
        return _encode_column_ref(expr, bindings)
    if cls == "BOUND_OPERATOR":
        return _encode_operator(expr, bindings)
    if cls == "BOUND_BETWEEN":
        return _encode_between(expr, bindings)
    if cls == "BOUND_FUNCTION":
        return _encode_function(expr, bindings)
    return None  # unsupported → drop (DuckDB re-applies client-side)


def _encode_comparison(expr: dict, bindings: list[str]) -> str | None:
    left = _encode(expr.get("left"), bindings)
    right = _encode(expr.get("right"), bindings)
    if left is None or right is None:
        return None
    op = _COMPARE_OPS.get(expr.get("type", ""))
    if op is None:
        return None
    return f"{left} {op} {right}"


def _encode_conjunction(expr: dict, bindings: list[str]) -> str | None:
    children = expr.get("children")
    if not isinstance(children, list):
        return None
    rendered = [c for c in (_encode(ch, bindings) for ch in children) if c]
    is_or = expr.get("type") == "CONJUNCTION_OR"
    # For OR, a dropped child would broaden the predicate incorrectly — bail entirely.
    if is_or and len(rendered) != len(children):
        return None
    if not rendered:
        return None
    if len(rendered) == 1:
        return rendered[0]
    joiner = " OR " if is_or else " AND "
    return "(" + joiner.join(rendered) + ")"


def _encode_operator(expr: dict, bindings: list[str]) -> str | None:
    children = expr.get("children")
    if not isinstance(children, list) or not children:
        return None
    otype = expr.get("type")
    first = _encode(children[0], bindings)
    if first is None:
        return None
    if otype == "OPERATOR_IS_NULL":
        return f"{first} IS NULL"
    if otype == "OPERATOR_IS_NOT_NULL":
        return f"{first} IS NOT NULL"
    if otype == "OPERATOR_NOT":
        return f"NOT ({first})"
    if otype in ("COMPARE_IN", "COMPARE_NOT_IN"):
        values = [_encode(c, bindings) for c in children[1:]]
        if not values or any(v is None for v in values):
            return None
        op = "NOT IN" if otype == "COMPARE_NOT_IN" else "IN"
        return f"{first} {op} ({', '.join(v for v in values if v)})"
    return None


def _encode_between(expr: dict, bindings: list[str]) -> str | None:
    inp = _encode(expr.get("input"), bindings)
    lower = _encode(expr.get("lower"), bindings)
    upper = _encode(expr.get("upper"), bindings)
    if inp is None or lower is None or upper is None:
        return None
    return f"{inp} BETWEEN {lower} AND {upper}"


def _encode_function(expr: dict, bindings: list[str]) -> str | None:
    name = expr.get("name")
    children = expr.get("children")
    if not isinstance(children, list):
        return None
    args = [_encode(c, bindings) for c in children]
    if any(a is None for a in args):
        return None
    if name in _OP_FUNCS and len(args) == 2:
        return f"{args[0]} {_OP_FUNCS[name]} {args[1]}"
    return None  # arbitrary functions are not safely pushable to a semantic layer


def _encode_column_ref(expr: dict, bindings: list[str]) -> str | None:
    binding = expr.get("binding") or {}
    idx = binding.get("column_index")
    if not isinstance(idx, int) or idx < 0 or idx >= len(bindings):
        return None
    return _quote_ident(bindings[idx])


def _encode_constant(expr: dict) -> str | None:
    value = expr.get("value")
    # DuckDB constants: {"is_null": bool, "value": <scalar>, "type": {...}}.
    if isinstance(value, dict):
        if value.get("is_null"):
            return "NULL"
        value = value.get("value")
    return _sql_literal(value)


def _sql_literal(value: Any) -> str | None:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return _quote_literal(value)
    return None


def _quote_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

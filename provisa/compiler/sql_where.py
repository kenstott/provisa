# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WHERE/ORDER BY compilation and field-argument helpers for sql_gen.

Leaf layer: turns GraphQL field args into SQL WHERE/ORDER BY fragments and
extracts limit/offset/lateral-force flags. No dependency on sql_gen or
sql_selection.
"""

from __future__ import annotations

import fnmatch as _fnmatch
import re as _re

from graphql import (
    BooleanValueNode,
    EnumValueNode,
    FieldNode,
    FloatValueNode,
    IntValueNode,
    ListValueNode,
    ObjectValueNode,
    StringValueNode,
    VariableNode,
)

from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_types import (
    CompilationContext,
)
from provisa.compiler.sql_rewrite import (
    _q,
)


_VIRTUAL_COLS = frozenset({"_name_", "_domain_"})


# --- Build CompilationContext from SchemaInput ---


def _extract_value(
    node: object,  # object-ok: truly-any GraphQL AST value node
    variables: dict | None,
) -> object:  # object-ok: truly-any payload — GraphQL AST value nodes and Python primitives unified
    """Extract a Python value from a GraphQL AST value node."""
    if isinstance(node, StringValueNode):
        return node.value
    if isinstance(node, IntValueNode):
        return int(node.value)
    if isinstance(node, FloatValueNode):
        return float(node.value)
    if isinstance(node, BooleanValueNode):
        return node.value
    if isinstance(node, EnumValueNode):
        return node.value
    if isinstance(node, ListValueNode):
        return [_extract_value(v, variables) for v in node.values]
    if isinstance(node, ObjectValueNode):
        return {f.name.value: _extract_value(f.value, variables) for f in node.fields}
    if isinstance(node, VariableNode):
        var_name = node.name.value
        if variables and var_name in variables:
            return variables[var_name]
        raise ValueError(f"Variable ${var_name} not provided")
    raise ValueError(f"Unsupported value node type: {type(node).__name__}")


# --- WHERE clause compilation ---


_ISO_DATE_RE = _re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _timestamp_literal_or_param(
    val: object,  # object-ok: truly-any payload — caller may pass str, int, float, bool, None
    collector,
) -> str:  # object-ok: truly-any payload — caller may pass str, int, float, bool, None
    """Return a TIMESTAMP literal if val is an ISO date, otherwise a parameter.

    Accepts: 2000-01-01, 2000-01-01T00:00:00, 2000-01-01 00:00:00,
             2000-01-01T00:00:00Z, 2000-01-01T00:00:00+05:30
    With timezone → TIMESTAMP '...' WITH TIME ZONE
    Without → TIMESTAMP '...'
    """
    if isinstance(val, str) and _ISO_DATE_RE.match(val):
        normalized = val.replace("T", " ")
        # Check for timezone suffix
        tz_match = _re.search(r"(Z|[+-]\d{2}:?\d{2})$", normalized)
        if tz_match:
            tz = tz_match.group(1)
            base = normalized[: tz_match.start()].strip()
            if tz == "Z":
                tz = "UTC"
            return f"TIMESTAMP '{base} {tz}'"
        return f"TIMESTAMP '{normalized}'"
    return collector.add(val)


def _compile_virtual_col_filter(
    vv: str,
    filter_obj: dict,
) -> list[str]:
    """Compile filter predicates for a virtual column against its compile-time value."""
    parts: list[str] = []
    for op, val in filter_obj.items():
        if op == "eq":
            parts.append("TRUE" if vv == str(val) else "FALSE")
        elif op == "neq":
            parts.append("TRUE" if vv != str(val) else "FALSE")
        elif op == "gt":
            parts.append("TRUE" if vv > str(val) else "FALSE")
        elif op == "gte":
            parts.append("TRUE" if vv >= str(val) else "FALSE")
        elif op == "lt":
            parts.append("TRUE" if vv < str(val) else "FALSE")
        elif op == "lte":
            parts.append("TRUE" if vv <= str(val) else "FALSE")
        elif op == "in":
            parts.append("TRUE" if vv in [str(v) for v in val] else "FALSE")
        elif op == "like":
            pattern = str(val).replace("%", "*").replace("_", "?")
            parts.append("TRUE" if _fnmatch.fnmatch(vv, pattern) else "FALSE")
        elif op == "is_null":
            parts.append("FALSE")
    return parts


def _compile_column_filter(
    col: str,
    filter_obj: dict,
    collector: ParamCollector,
) -> list[str]:
    """Compile filter predicates for a physical column expression."""
    parts: list[str] = []
    for op, val in filter_obj.items():
        if op == "eq":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} = {rhs}")
        elif op == "neq":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} != {rhs}")
        elif op == "gt":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} > {rhs}")
        elif op == "gte":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} >= {rhs}")
        elif op == "lt":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} < {rhs}")
        elif op == "lte":
            rhs = _timestamp_literal_or_param(val, collector)
            parts.append(f"{col} <= {rhs}")
        elif op == "in":
            placeholders = [collector.add(v) for v in val]
            parts.append(f"{col} IN ({', '.join(placeholders)})")
        elif op == "like":
            placeholder = collector.add(val)
            parts.append(f"{col} LIKE {placeholder}")
        elif op == "is_null":
            parts.append(f"{col} IS NULL" if val else f"{col} IS NOT NULL")
    return parts


def _compile_where(
    where_obj: dict,
    collector: ParamCollector,
    alias: str | None,
    virtual_vals: dict[str, str] | None = None,
    table_id: int | None = None,
    exposed_to_physical: dict | None = None,
) -> str:
    """Compile a where input object to a SQL WHERE clause fragment."""
    parts: list[str] = []
    _e2p = exposed_to_physical or {}

    for key, value in where_obj.items():
        if key == "_and":
            sub_parts = [
                _compile_where(sub, collector, alias, virtual_vals, table_id, exposed_to_physical)
                for sub in value
            ]
            parts.append(f"({' AND '.join(sub_parts)})")
            continue
        if key == "_or":
            sub_parts = [
                _compile_where(sub, collector, alias, virtual_vals, table_id, exposed_to_physical)
                for sub in value
            ]
            parts.append(f"({' OR '.join(sub_parts)})")
            continue

        # Virtual column: resolve at compile time — no physical column exists
        if key in _VIRTUAL_COLS and virtual_vals is not None:
            vv = virtual_vals.get(key, "")
            parts.extend(_compile_virtual_col_filter(vv, value))
            continue

        # Column filter: map GQL name → physical name, then quote
        phys_key = _e2p.get((table_id, key), key) if table_id is not None else key
        col = _q(phys_key) if alias is None else f"{_q(alias)}.{_q(phys_key)}"
        parts.extend(_compile_column_filter(col, value, collector))

    return " AND ".join(parts) if parts else "TRUE"


# --- ORDER BY compilation ---


_DIRECTION_SQL = {
    "asc": "ASC",
    "desc": "DESC",
    "asc_nulls_first": "ASC NULLS FIRST",
    "asc_nulls_last": "ASC NULLS LAST",
    "desc_nulls_first": "DESC NULLS FIRST",
    "desc_nulls_last": "DESC NULLS LAST",
}


def _compile_order_by(
    order_by_list: list[dict],
    alias: str | None,
    table_id: int | None = None,
    exposed_to_physical: dict | None = None,
) -> str:
    """Compile order_by input list to SQL ORDER BY clause.

    Hasura v2 format: each item is {column_name: direction} where direction
    is one of: asc, desc, asc_nulls_first, asc_nulls_last, desc_nulls_first,
    desc_nulls_last.
    """
    parts: list[str] = []
    _e2p = exposed_to_physical or {}
    for item in order_by_list:
        for col_name, direction in item.items():
            if isinstance(direction, dict):
                # Nested relationship order-by: { rel_field: { col: dir } }
                # Flatten one level — qualify with the relationship field name as alias.
                for nested_col, nested_dir in direction.items():
                    sql_dir = _DIRECTION_SQL.get(nested_dir)
                    if sql_dir is None:
                        raise ValueError(f"Unknown order direction: {nested_dir!r}")
                    parts.append(f"{_q(col_name)}.{_q(nested_col)} {sql_dir}")
                continue
            sql_dir = _DIRECTION_SQL.get(direction)
            if sql_dir is None:
                raise ValueError(f"Unknown order direction: {direction!r}")
            phys_col = (
                _e2p.get((table_id, col_name), col_name) if table_id is not None else col_name
            )
            col = _q(phys_col) if alias is None else f"{_q(alias)}.{_q(phys_col)}"
            parts.append(f"{col} {sql_dir}")
    return ", ".join(parts)


def _has_joins(field_node: FieldNode, ctx: CompilationContext, type_name: str) -> bool:
    """Check if any selected field is a relationship (requires JOIN)."""
    if not field_node.selection_set:
        return False
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode):
            if (type_name, sel.name.value) in ctx.joins:
                return True
    return False


# Args that require LATERAL JOIN — limit is handled inside ARRAY_AGG subquery
_LATERAL_FORCE_ARGS = frozenset({"where", "order_by", "offset", "distinct_on"})


def _has_lateral_force_args(field_node: FieldNode) -> bool:
    return any(arg.name.value in _LATERAL_FORCE_ARGS for arg in field_node.arguments)


def _explicit_limit(field_node: FieldNode, variables: dict | None) -> int | None:
    for arg in field_node.arguments:
        if arg.name.value == "limit":
            val = _extract_value(arg.value, variables)
            if isinstance(val, int) and not isinstance(val, bool) and val > 0:
                return val
    return None


def _extract_non_negative_int(
    value: object,  # object-ok: truly-any payload — validated via isinstance checks inside body
    name: str,
) -> int:  # object-ok: truly-any payload — validated via isinstance checks inside body
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _field_args(field_node: FieldNode, variables: dict | None) -> dict:
    return {arg.name.value: _extract_value(arg.value, variables) for arg in field_node.arguments}

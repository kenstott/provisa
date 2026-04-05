# Copyright (c) 2025 Kenneth Stott
# Canary: 10338ccf-ccb2-4aed-8382-2a035eac3565
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Convert Hasura boolean expressions to SQL WHERE clauses."""

from __future__ import annotations

from typing import Any

# Hasura operator -> SQL operator
_OPERATORS: dict[str, str] = {
    "_eq": "=",
    "_neq": "!=",
    "_gt": ">",
    "_lt": "<",
    "_gte": ">=",
    "_lte": "<=",
    "_like": "LIKE",
    "_nlike": "NOT LIKE",
    "_ilike": "ILIKE",
    "_nilike": "NOT ILIKE",
    "_in": "IN",
    "_nin": "NOT IN",
    "_is_null": "IS NULL",
}


def _format_value(val: Any) -> str:
    """Format a value for SQL embedding."""
    if isinstance(val, str):
        if val.startswith("x-hasura-") or val.startswith("X-Hasura-"):
            return f"${{{val}}}"
        return f"'{val}'"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, list):
        items = ", ".join(_format_value(v) for v in val)
        return f"({items})"
    if val is None:
        return "NULL"
    return str(val)


def _convert_session_var(val: Any) -> str:
    """Convert Hasura session variable references."""
    if isinstance(val, dict) and len(val) == 1:
        key = next(iter(val))
        if key == "x-hasura-user-id" or key == "X-Hasura-User-Id":
            return "${x-hasura-user-id}"
        if key.startswith("x-hasura-") or key.startswith("X-Hasura-"):
            return f"${{{key}}}"
    return ""


def bool_expr_to_sql(expr: dict[str, Any], table_alias: str = "") -> str:
    """Convert a Hasura boolean expression to a SQL WHERE clause.

    Args:
        expr: Hasura boolean expression dict.
        table_alias: Optional table alias prefix for column references.

    Returns:
        SQL WHERE clause string (without the WHERE keyword).
    """
    if not expr:
        return "TRUE"
    return _convert_node(expr, table_alias)


def _col_ref(col: str, table_alias: str) -> str:
    if table_alias:
        return f"{table_alias}.{col}"
    return col


def _convert_node(node: dict[str, Any], table_alias: str) -> str:
    parts: list[str] = []

    for key, value in node.items():
        if key == "_and":
            sub = [_convert_node(item, table_alias) for item in value]
            parts.append("(" + " AND ".join(sub) + ")")
        elif key == "_or":
            sub = [_convert_node(item, table_alias) for item in value]
            parts.append("(" + " OR ".join(sub) + ")")
        elif key == "_not":
            inner = _convert_node(value, table_alias)
            parts.append(f"NOT ({inner})")
        elif key == "_exists":
            tbl = value.get("_table", {})
            schema = tbl.get("schema", "public")
            table = tbl.get("name", "unknown")
            where = _convert_node(value.get("_where", {}), "")
            parts.append(f"EXISTS (SELECT 1 FROM {schema}.{table} WHERE {where})")
        elif key.startswith("_"):
            # Unknown operator at top level — skip
            continue
        else:
            # Column name -> operator dict
            col = _col_ref(key, table_alias)
            if isinstance(value, dict):
                for op, operand in value.items():
                    session_ref = _convert_session_var(operand)
                    if op == "_is_null":
                        if operand:
                            parts.append(f"{col} IS NULL")
                        else:
                            parts.append(f"{col} IS NOT NULL")
                    elif op in _OPERATORS:
                        sql_op = _OPERATORS[op]
                        if session_ref:
                            parts.append(f"{col} {sql_op} {session_ref}")
                        else:
                            parts.append(f"{col} {sql_op} {_format_value(operand)}")
                    else:
                        parts.append(f"{col} /* unsupported op: {op} */")

    if not parts:
        return "TRUE"
    if len(parts) == 1:
        return parts[0]
    return " AND ".join(parts)

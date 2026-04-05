# Copyright (c) 2026 Kenneth Stott
# Canary: a61ef49c-f529-4a5e-b11d-2b1a7c7a8a1f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cursor pagination helpers for Relay-style connections (REQ-218).

Cursor = base64(json([sort_key_values])). Decoding extracts sort key values
to compile WHERE clauses for keyset pagination.
"""

from __future__ import annotations

import base64
import json


def encode_cursor(sort_values: list) -> str:
    """Encode sort key values into an opaque cursor string.

    Args:
        sort_values: List of column values forming the sort key.

    Returns:
        Base64-encoded JSON string.
    """
    payload = json.dumps(sort_values, default=str)
    return base64.b64encode(payload.encode()).decode()


def decode_cursor(cursor: str) -> list:
    """Decode an opaque cursor string back to sort key values.

    Args:
        cursor: Base64-encoded cursor from encode_cursor.

    Returns:
        List of sort key values.

    Raises:
        ValueError: If cursor is malformed.
    """
    try:
        payload = base64.b64decode(cursor.encode()).decode()
        return json.loads(payload)
    except Exception as exc:
        raise ValueError(f"Invalid cursor: {cursor!r}") from exc


def cursor_where_clause(
    sort_columns: list[str],
    cursor_values: list,
    direction: str,
    collector: object,
    alias: str | None,
) -> str:
    """Build a WHERE clause fragment for cursor-based keyset pagination.

    For forward pagination (after): row > cursor → (col1, col2) > ($1, $2)
    For backward pagination (before): row < cursor → (col1, col2) < ($1, $2)

    Args:
        sort_columns: Physical column names in sort order.
        cursor_values: Decoded cursor values (one per sort column).
        direction: "forward" or "backward".
        collector: ParamCollector for positional parameters.
        alias: Table alias (e.g. "t0") or None.

    Returns:
        SQL WHERE clause fragment (without WHERE keyword).
    """
    if len(sort_columns) != len(cursor_values):
        raise ValueError(
            f"Cursor has {len(cursor_values)} values but sort key has "
            f"{len(sort_columns)} columns"
        )

    def _q(name: str) -> str:
        return f'"{name}"'

    def _col_ref(col: str) -> str:
        if alias is None:
            return _q(col)
        return f'{_q(alias)}.{_q(col)}'

    op = ">" if direction == "forward" else "<"

    if len(sort_columns) == 1:
        param = collector.add(cursor_values[0])
        return f"{_col_ref(sort_columns[0])} {op} {param}"

    # Tuple comparison for multi-column sort keys
    col_tuple = ", ".join(_col_ref(c) for c in sort_columns)
    param_tuple = ", ".join(collector.add(v) for v in cursor_values)
    return f"({col_tuple}) {op} ({param_tuple})"


def apply_cursor_pagination(
    args: dict,
    sort_columns: list[str],
    collector: object,
    alias: str | None,
) -> tuple[str | None, int | None, bool]:
    """Process cursor pagination args and return WHERE fragment + limit + reverse flag.

    Args:
        args: Parsed GraphQL arguments dict.
        sort_columns: Column names used for ordering (from order_by or default [id]).
        collector: ParamCollector for positional parameters.
        alias: Table alias or None.

    Returns:
        (where_fragment or None, effective_limit or None, is_backward)
        where_fragment: SQL cursor WHERE clause or None.
        effective_limit: first+1 or last+1 for has-more detection, or None.
        is_backward: True if paginating backward (last/before).
    """
    first = args.get("first")
    after = args.get("after")
    last = args.get("last")
    before = args.get("before")

    if first is not None and last is not None:
        raise ValueError("Cannot use both 'first' and 'last' in the same query")

    where_fragment = None
    effective_limit = None
    is_backward = False

    if first is not None:
        effective_limit = int(first) + 1  # fetch one extra for hasNextPage
        if after is not None:
            cursor_values = decode_cursor(after)
            where_fragment = cursor_where_clause(
                sort_columns, cursor_values, "forward", collector, alias,
            )

    elif last is not None:
        is_backward = True
        effective_limit = int(last) + 1  # fetch one extra for hasPreviousPage
        if before is not None:
            cursor_values = decode_cursor(before)
            where_fragment = cursor_where_clause(
                sort_columns, cursor_values, "backward", collector, alias,
            )

    return where_fragment, effective_limit, is_backward


def extract_sort_columns(args: dict) -> list[str]:
    """Extract sort column names from order_by args, defaulting to ["id"]."""
    if "order_by" not in args:
        return ["id"]
    order_by_val = args["order_by"]
    if isinstance(order_by_val, dict):
        order_by_val = [order_by_val]
    cols = []
    for item in order_by_val:
        for col_name in item:
            cols.append(col_name)
    return cols if cols else ["id"]


def reverse_order(order_sql: str) -> str:
    """Reverse ASC/DESC in an ORDER BY clause for backward pagination."""
    import re as _re_local

    result = order_sql
    # Replace compound patterns first (longer matches)
    replacements = [
        ("ASC NULLS FIRST", "__DESC_NULLS_LAST__"),
        ("ASC NULLS LAST", "__DESC_NULLS_FIRST__"),
        ("DESC NULLS FIRST", "__ASC_NULLS_LAST__"),
        ("DESC NULLS LAST", "__ASC_NULLS_FIRST__"),
    ]
    for old, new in replacements:
        result = result.replace(old, new)
    # Replace remaining plain ASC/DESC with word-boundary awareness
    result = _re_local.sub(r'\bASC\b', '__PLACEHOLDER_DESC__', result)
    result = _re_local.sub(r'\bDESC\b', 'ASC', result)
    result = result.replace('__PLACEHOLDER_DESC__', 'DESC')
    # Restore compound patterns
    result = result.replace("__DESC_NULLS_LAST__", "DESC NULLS LAST")
    result = result.replace("__DESC_NULLS_FIRST__", "DESC NULLS FIRST")
    result = result.replace("__ASC_NULLS_LAST__", "ASC NULLS LAST")
    result = result.replace("__ASC_NULLS_FIRST__", "ASC NULLS FIRST")
    return result

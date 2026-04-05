# Copyright (c) 2025 Kenneth Stott
# Canary: c88a327d-c038-4dec-a4c5-f292b2675f6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reconstruct nested GraphQL JSON from flat JOIN result rows (REQ-047).

many-to-one  -> single nested object
one-to-many  -> array of nested objects
Null propagation for nullable relationships.
"""

from __future__ import annotations

from decimal import Decimal

from provisa.compiler.cursor import encode_cursor
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery


def _convert_value(val: object) -> object:
    """Convert database types to JSON-safe Python types."""
    if isinstance(val, Decimal):
        f = float(val)
        if f == int(f) and "." not in str(val):
            return int(f)
        return f
    if hasattr(val, 'isoformat'):
        return val.isoformat()
    return val


def serialize_rows(
    rows: list[tuple],
    columns: list[ColumnRef],
    root_field: str,
) -> dict:
    """Serialize flat SQL rows into nested GraphQL JSON.

    Args:
        rows: Result rows from SQL execution (tuples).
        columns: Column references from compilation (maps positions to fields).
        root_field: The GraphQL root field name (e.g., "orders").

    Returns:
        {"data": {root_field: [...]}}
    """
    # Group columns by nesting path
    root_cols: list[tuple[int, ColumnRef]] = []
    nested_groups: dict[str, list[tuple[int, ColumnRef]]] = {}

    for i, col in enumerate(columns):
        if col.nested_in is None:
            root_cols.append((i, col))
        else:
            nested_groups.setdefault(col.nested_in, []).append((i, col))

    result_rows: list[dict] = []

    for row in rows:
        obj: dict = {}

        # Root-level fields
        for idx, col in root_cols:
            obj[col.field_name] = _convert_value(row[idx])

        # Nested relationship fields — support dotted paths for deep nesting
        for nest_path, nest_cols in nested_groups.items():
            all_none = all(row[idx] is None for idx, _ in nest_cols)
            parts = nest_path.split(".")
            # Walk down the object tree, creating intermediate dicts as needed
            target = obj
            for part in parts[:-1]:
                if part not in target or target[part] is None:
                    target[part] = {}
                target = target[part]
            leaf = parts[-1]
            if all_none:
                target[leaf] = None
            else:
                nested_obj: dict = {}
                for idx, col in nest_cols:
                    nested_obj[col.field_name] = _convert_value(row[idx])
                target[leaf] = nested_obj

        result_rows.append(obj)

    return {"data": {root_field: result_rows}}


def serialize_aggregate(
    agg_rows: list[tuple],
    agg_columns: list[ColumnRef],
    nodes_rows: list[tuple] | None,
    nodes_columns: list[ColumnRef] | None,
    root_field: str,
    agg_alias: str = "aggregate",
) -> dict:
    """Serialize aggregate query result (with optional nodes) into GraphQL JSON.

    Column nested_in paths use agg_alias as prefix (default "aggregate"), e.g.:
      "aggregate"      → aggregate.count
      "aggregate.sum"  → aggregate.sum.amount

    Returns:
        {"data": {root_field: {agg_alias: {...}, "nodes": [...]}}}
        "nodes" key is present only when nodes_rows is not None.
    """
    # Build aggregate inner object from first (only) row.
    # Strip the leading agg_alias prefix from each nested_in path, then
    # reconstruct the sub-structure (sum, avg, min, max).
    agg_inner: dict = {}
    if agg_rows:
        row = agg_rows[0]
        for i, col in enumerate(agg_columns):
            path = col.nested_in or agg_alias
            # Strip agg_alias prefix
            if path == agg_alias:
                sub_path: list[str] = []
            elif path.startswith(f"{agg_alias}."):
                sub_path = path[len(f"{agg_alias}."):].split(".")
            else:
                sub_path = path.split(".")

            target = agg_inner
            for part in sub_path:
                if part not in target or not isinstance(target[part], dict):
                    target[part] = {}
                target = target[part]
            target[col.field_name] = _convert_value(row[i])

    payload: dict = {agg_alias: agg_inner}

    if nodes_rows is not None and nodes_columns is not None:
        node_list: list[dict] = []
        for row in nodes_rows:
            node: dict = {}
            for i, col in enumerate(nodes_columns):
                node[col.field_name] = _convert_value(row[i])
            node_list.append(node)
        payload["nodes"] = node_list

    return {"data": {root_field: payload}}


def serialize_connection(
    rows: list[tuple],
    compiled: CompiledQuery,
) -> dict:
    """Serialize flat SQL rows into Relay-style connection format."""
    columns = compiled.columns
    sort_columns = compiled.sort_columns
    page_size = compiled.page_size
    is_backward = compiled.is_backward

    has_more = False
    if page_size is not None and len(rows) > page_size:
        has_more = True
        rows = rows[:page_size]

    if is_backward:
        rows = list(reversed(rows))

    col_index = {col.field_name: i for i, col in enumerate(columns)}

    edges = []
    for row in rows:
        node: dict = {}
        for i, col in enumerate(columns):
            node[col.field_name] = _convert_value(row[i])

        cursor_vals = []
        for sc in sort_columns:
            idx = col_index.get(sc)
            if idx is not None:
                cursor_vals.append(_convert_value(row[idx]))
        cursor = encode_cursor(cursor_vals)
        edges.append({"cursor": cursor, "node": node})

    if not is_backward:
        has_next = has_more
        has_prev = compiled.has_cursor
    else:
        has_next = compiled.has_cursor
        has_prev = has_more

    page_info = {
        "hasNextPage": has_next,
        "hasPreviousPage": has_prev,
        "startCursor": edges[0]["cursor"] if edges else None,
        "endCursor": edges[-1]["cursor"] if edges else None,
    }

    return {"data": {compiled.root_field: {"edges": edges, "pageInfo": page_info}}}

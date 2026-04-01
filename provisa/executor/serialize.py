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

from provisa.compiler.sql_gen import ColumnRef


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
    # Group columns by nesting
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

        # Nested relationship fields
        for nest_name, nest_cols in nested_groups.items():
            # Check if all nested values are None (null relationship)
            all_none = all(row[idx] is None for idx, _ in nest_cols)
            if all_none:
                obj[nest_name] = None
            else:
                nested_obj: dict = {}
                for idx, col in nest_cols:
                    nested_obj[col.field_name] = _convert_value(row[idx])
                obj[nest_name] = nested_obj

        result_rows.append(obj)

    return {"data": {root_field: result_rows}}

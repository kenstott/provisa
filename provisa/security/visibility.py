# Copyright (c) 2026 Kenneth Stott
# Canary: 9ed5037b-7264-42ef-b398-079e06dcea86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Schema visibility enforcement (REQ-039).

Unauthorized tables/columns do not appear in the GraphQL SDL.
This module formalizes what schema_gen already does and adds validation.
"""

from __future__ import annotations


def visible_tables(tables: list[dict], role: dict) -> list[dict]:
    """Filter tables to those visible to the role based on domain access."""
    accessible = set(role["domain_access"])
    all_access = "*" in accessible

    result = []
    for table in tables:
        if not all_access and table["domain_id"] not in accessible:
            continue
        # Filter columns by visibility
        visible_cols = [
            c for c in table["columns"]
            if role["id"] in c["visible_to"]
        ]
        if not visible_cols:
            continue
        result.append({**table, "columns": visible_cols})
    return result


def is_column_visible(
    table: dict,
    column_name: str,
    role_id: str,
) -> bool:
    """Check if a specific column is visible to a role."""
    for col in table.get("columns", []):
        if col["column_name"] == column_name:
            return role_id in col["visible_to"]
    return False


def visible_column_names(table: dict, role_id: str) -> set[str]:
    """Get the set of column names visible to a role for a table."""
    return {
        col["column_name"]
        for col in table.get("columns", [])
        if role_id in col["visible_to"]
    }

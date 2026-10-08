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

from provisa.security.rights import column_served, reaches_domain

# Requirements: REQ-001, REQ-038, REQ-039, REQ-042, REQ-263, REQ-363


def visible_tables(tables: list[dict], role: dict) -> list[dict]:  # REQ-039, REQ-042, REQ-363
    """The tables ``role`` is served, each with the columns it is served — by the one rule the
    schema build and SQL governance read (:func:`provisa.security.rights.column_served`)."""
    result = []
    for table in tables:
        reaches = reaches_domain(role["domain_access"], table["domain_id"])
        visible_cols = [
            c
            for c in table["columns"]
            if column_served(role, table["domain_id"], c, reaches=reaches)
        ]
        if not visible_cols:
            continue
        result.append({**table, "columns": visible_cols})
    return result


def is_column_visible(  # REQ-039, REQ-263
    table: dict,
    column_name: str,
    role_id: str,
) -> bool:
    """Whether a column's own grant names ``role_id`` (its ``visible_to``, literally)."""
    for col in table.get("columns", []):
        if col["column_name"] == column_name:
            return role_id in col["visible_to"]
    return False


def visible_column_names(table: dict, role_id: str) -> set[str]:  # REQ-039, REQ-263
    """The columns whose own grant names ``role_id`` (their ``visible_to``, literally)."""
    return {col["column_name"] for col in table.get("columns", []) if role_id in col["visible_to"]}

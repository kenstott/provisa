# Copyright (c) 2026 Kenneth Stott
# Canary: d50b6b33-25ce-413b-aafa-4da7dea9e634
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Inline expansion of non-materialized views at query time.

Replaces table references to view tables with their defining SQL as subqueries.
"""

from __future__ import annotations

import re

from provisa.compiler.sql_gen import CompiledQuery


def expand_view_refs(sql: str, view_sql_map: dict[str, str]) -> str:
    """Replace view-table references in a SQL string with inline subqueries.

    For each view table in view_sql_map, replaces a catalog- or schema-qualified
    reference  "catalog"."schema"."view_table" [alias]  with  (view_sql) [alias].
    Shared by the GraphQL/SQL path (via expand_views) and the Cypher path.
    """
    if not view_sql_map:
        return sql
    for view_table, view_sql in view_sql_map.items():
        # Catalog-qualified: "catalog"."schema"."view_table" "alias"
        sql = re.sub(
            rf'"[^"]+"\."[^"]+"\."({re.escape(view_table)})"(\s+"t\d+")?',
            lambda m: f"({view_sql}){m.group(2) or ''}",
            sql,
        )
        # Schema-qualified: "schema"."view_table" "alias"
        sql = re.sub(
            rf'"[^"]+"\."({re.escape(view_table)})"(\s+"t\d+")?',
            lambda m: f"({view_sql}){m.group(2) or ''}",
            sql,
        )
    return sql


def expand_views(
    compiled: CompiledQuery,
    view_sql_map: dict[str, str],
) -> CompiledQuery:
    """Replace view table references in compiled SQL with inline subqueries.

    For each view table in view_sql_map, replaces:
        FROM "schema"."view_table" "alias"
    with:
        FROM (view_sql) "alias"

    Also handles catalog-qualified references:
        FROM "catalog"."schema"."view_table" "alias"
    """
    if not view_sql_map:
        return compiled

    sql = expand_view_refs(compiled.sql, view_sql_map)

    if sql == compiled.sql:
        return compiled

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=compiled.sources,
    )
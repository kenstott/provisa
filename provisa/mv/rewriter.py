# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL rewriter for materialized view optimization (REQ-082, REQ-083).

After compilation, inspects FROM/JOIN clauses and rewrites to use MV target
tables when a matching fresh MV is found. Full-match only for V1.
"""

from __future__ import annotations

import logging
import re

from provisa.compiler.sql_gen import CompiledQuery
from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)


def _extract_tables_from_sql(sql: str) -> list[tuple[str, str | None]]:
    """Extract (table_name, alias) pairs from FROM/JOIN clauses.

    Handles patterns like:
        FROM "schema"."table" "t0"
        LEFT JOIN "schema"."table" "t1" ON ...
    """
    # Match: "schema"."table" "alias" or "schema"."table"
    pattern = r'"[^"]+"\."([^"]+)"(?:\s+"(t\d+)")?'
    matches = re.findall(pattern, sql)
    return [(table, alias or None) for table, alias in matches]


def _extract_join_info(sql: str) -> list[dict]:
    """Extract JOIN information from SQL.

    Returns list of {left_table, left_column, right_table, right_column, join_type}.
    """
    # Match: LEFT JOIN "schema"."table" "alias" ON "alias1"."col1" = "alias2"."col2"
    # Also handle CAST(...) expressions around join columns
    join_pattern = (
        r'(LEFT|INNER|RIGHT)?\s*JOIN\s+"[^"]+"\."([^"]+)"\s+"(t\d+)"\s+'
        r'ON\s+(?:CAST\()?\"(t\d+)\"\.\"([^"]+)\"(?:\s+AS\s+\w+\))?\s*=\s*'
        r'(?:CAST\()?\"(t\d+)\"\.\"([^"]+)\"'
    )
    matches = re.findall(join_pattern, sql, re.IGNORECASE)
    result = []
    for join_type, right_table, right_alias, left_alias, left_col, ra2, right_col in matches:
        result.append({
            "join_type": (join_type or "LEFT").lower(),
            "right_table": right_table,
            "right_alias": right_alias,
            "left_alias": left_alias,
            "left_column": left_col,
            "right_column": right_col,
        })
    return result


def _find_root_table(sql: str) -> tuple[str, str, str] | None:
    """Extract (schema, table_name, alias) from the FROM clause."""
    match = re.search(
        r'FROM\s+"([^"]+)"\."([^"]+)"(?:\s+"(t\d+)")?',
        sql, re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2), match.group(3) or ""
    return None


def rewrite_if_mv_match(
    compiled: CompiledQuery,
    fresh_mvs: list[MVDefinition],
) -> CompiledQuery:
    """Attempt to rewrite SQL to use a materialized view.

    Checks if the query's FROM/JOIN pattern matches any fresh MV.
    On match: rewrites FROM to MV target table (removes JOINs).
    On no match or stale MV: returns original SQL unchanged.

    V1: full-match only (all JOINs must be covered by a single MV).
    """
    if not fresh_mvs:
        return compiled

    root_info = _find_root_table(compiled.sql)
    if not root_info:
        return compiled

    _, root_table, _ = root_info
    joins = _extract_join_info(compiled.sql)

    # For each fresh MV, check if it covers the query's join pattern
    for mv in fresh_mvs:
        if not mv.join_pattern or not mv.is_fresh:
            continue

        jp = mv.join_pattern

        # Single JOIN match
        if len(joins) == 1:
            join = joins[0]
            # Check if the MV covers this exact join
            tables_match = (
                (root_table == jp.left_table and join["right_table"] == jp.right_table)
                or (root_table == jp.right_table and join["right_table"] == jp.left_table)
            )
            if not tables_match:
                continue

            # Columns must match (order doesn't matter for equality join)
            cols_match = (
                {join["left_column"], join["right_column"]}
                == {jp.left_column, jp.right_column}
            )
            if not cols_match:
                continue

            # Match found — rewrite SQL
            log.info(
                "MV %s matches query join %s↔%s, rewriting",
                mv.id, root_table, join["right_table"],
            )
            return _rewrite_to_mv(compiled, mv, joins)

    return compiled


def _rewrite_to_mv(
    compiled: CompiledQuery,
    mv: MVDefinition,
    joins: list[dict],
) -> CompiledQuery:
    """Rewrite compiled SQL to read from MV target table instead of source tables.

    Removes JOINs, replaces FROM with MV target table, adjusts column refs.
    All table-aliased column refs ("t0"."col", "t1"."col") become just "col"
    since there's only one table now.
    """
    sql = compiled.sql

    # Remove all JOIN clauses: LEFT JOIN "schema"."table" "alias" ON "x"."col" = "y"."col"
    # Handle both plain and CAST(...) join conditions
    sql = re.sub(
        r'\s+(?:LEFT|INNER|RIGHT)?\s*JOIN\s+"[^"]+"\."[^"]+"\s+"t\d+"\s+ON\s+'
        r'(?:CAST\([^)]+\)|"[^"]+"\."[^"]+")\s*=\s*(?:CAST\([^)]+\)|"[^"]+"\."[^"]+")',
        '',
        sql,
        flags=re.IGNORECASE,
    )

    # Replace FROM clause with MV target table
    mv_ref = f'"{mv.target_catalog}"."{mv.target_schema}"."{mv.target_table}"'
    sql = re.sub(
        r'FROM\s+"[^"]+"\."[^"]+"\s*(?:"t0")?',
        f'FROM {mv_ref}',
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    # Remove table aliases from column references: "t0"."col" → "col"
    sql = re.sub(r'"t\d+"\.("(?:[^"]+)")', r'\1', sql)

    # Update sources to reflect MV target source
    new_sources = {mv.target_catalog}

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=new_sources,
    )

# Copyright (c) 2026 Kenneth Stott
# Canary: 6babf68a-a16d-4c2d-a24f-3960beb27088
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL rewriter for materialized view optimization (REQ-082, REQ-083).

After compilation, inspects FROM/JOIN clauses and rewrites to use MV target
tables when a matching fresh MV is found. Supports full and partial matching.
"""

from __future__ import annotations

import logging
import re

from provisa.compiler.sql_gen import CompiledQuery
from provisa.mv.models import MVDefinition
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


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

    Returns list of {left_table, left_column, right_table, right_column, join_type,
                     right_alias, left_alias, full_match (the full JOIN clause text)}.
    """
    # Match: LEFT JOIN "schema"."table" "alias" ON "alias1"."col1" = "alias2"."col2"
    # Also handle CAST(...) expressions around join columns
    join_pattern = (
        r'((?:LEFT|INNER|RIGHT)?\s*JOIN\s+"[^"]+"\."([^"]+)"\s+"(t\d+)"\s+'
        r'ON\s+(?:CAST\()?\"(t\d+)\"\.\"([^"]+)\"(?:\s+AS\s+\w+\))?\s*=\s*'
        r'(?:CAST\()?\"(t\d+)\"\.\"([^"]+)\"(?:\s+AS\s+\w+\))?)'
    )
    matches = re.findall(join_pattern, sql, re.IGNORECASE)
    result = []
    for full_clause, right_table, right_alias, left_alias, left_col, ra2, right_col in matches:
        join_type_match = re.match(r'(LEFT|INNER|RIGHT)', full_clause, re.IGNORECASE)
        result.append({
            "join_type": (join_type_match.group(1) if join_type_match else "LEFT").lower(),
            "right_table": right_table,
            "right_alias": right_alias,
            "left_alias": left_alias,
            "left_column": left_col,
            "right_column": right_col,
            "full_clause": full_clause,
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


def _match_join_to_mv(
    root_table: str,
    join: dict,
    mv: MVDefinition,
) -> bool:
    """Check if a single JOIN matches an MV's join pattern."""
    jp = mv.join_pattern
    if not jp:
        return False

    tables_match = (
        (root_table == jp.left_table and join["right_table"] == jp.right_table)
        or (root_table == jp.right_table and join["right_table"] == jp.left_table)
    )
    if not tables_match:
        return False

    cols_match = (
        {join["left_column"], join["right_column"]}
        == {jp.left_column, jp.right_column}
    )
    return cols_match


def rewrite_if_mv_match(
    compiled: CompiledQuery,
    fresh_mvs: list[MVDefinition],
) -> CompiledQuery:
    """Attempt to rewrite SQL to use a materialized view.

    Checks if the query's FROM/JOIN pattern matches any fresh MV.
    Supports both full match (all JOINs covered) and partial match
    (MV covers a subset of JOINs, remaining JOINs preserved) (REQ-083).
    """
    with _tracer.start_as_current_span("mv.rewrite") as span:
        span.set_attribute("mv.candidates", len(fresh_mvs))

        if not fresh_mvs:
            span.set_attribute("mv.hit", False)
            return compiled

        root_info = _find_root_table(compiled.sql)
        if not root_info:
            span.set_attribute("mv.hit", False)
            return compiled

        _, root_table, _ = root_info
        joins = _extract_join_info(compiled.sql)

        if not joins:
            span.set_attribute("mv.hit", False)
            return compiled

        # For each fresh MV, check if it covers any of the query's join patterns
        for mv in fresh_mvs:
            if not mv.join_pattern or not mv.is_fresh:
                continue

            # Find which joins this MV covers
            matched_indices = []
            for i, join in enumerate(joins):
                if _match_join_to_mv(root_table, join, mv):
                    matched_indices.append(i)

            if not matched_indices:
                continue

            if len(matched_indices) == len(joins):
                # Full match — rewrite entirely to MV
                log.info("MV %s fully matches query joins, rewriting", mv.id)
                span.set_attribute("mv.hit", True)
                span.set_attribute("mv.id", str(mv.id))
                span.set_attribute("mv.match_type", "full")
                return _rewrite_to_mv(compiled, mv, joins)
            else:
                # Partial match (REQ-083) — rewrite covered portion, keep rest
                log.info(
                    "MV %s partially matches query (%d/%d joins), rewriting",
                    mv.id, len(matched_indices), len(joins),
                )
                span.set_attribute("mv.hit", True)
                span.set_attribute("mv.id", str(mv.id))
                span.set_attribute("mv.match_type", "partial")
                return _partial_rewrite_to_mv(compiled, mv, joins, matched_indices)

        span.set_attribute("mv.hit", False)
        return compiled


def _rewrite_to_mv(
    compiled: CompiledQuery,
    mv: MVDefinition,
    joins: list[dict],
) -> CompiledQuery:
    """Rewrite compiled SQL to read from MV target table instead of source tables.

    Removes JOINs, replaces FROM with MV target table, adjusts column refs.
    Left-table columns ("t0"."col") become just "col".
    Right-table columns ("t1"."col") become "right_table__col" to match the
    MV column naming convention from refresh.py.
    """
    sql = compiled.sql

    # Build alias → table name mapping from the SQL
    alias_to_table: dict[str, str] = {}
    from_match = re.search(
        r'FROM\s+"[^"]+"\."([^"]+)"\s+"(t\d+)"', sql, re.IGNORECASE,
    )
    if from_match:
        alias_to_table[from_match.group(2)] = from_match.group(1)
    for join in joins:
        alias_to_table[join["right_alias"]] = join["right_table"]

    # Determine which aliases are right-side (joined) tables
    right_aliases = {j["right_alias"] for j in joins}

    # Remove all JOIN clauses
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

    # Rewrite column references:
    # Right-table: "t1"."col" → "right_table__col"
    # Left-table:  "t0"."col" → "col"
    def _rewrite_col_ref(m: re.Match) -> str:
        alias = m.group(1)
        col = m.group(2)
        if alias in right_aliases:
            table_name = alias_to_table.get(alias, "")
            return f'"{table_name}__{col}"'
        return f'"{col}"'

    sql = re.sub(r'"(t\d+)"\."([^"]+)"', _rewrite_col_ref, sql)

    new_sources = {mv.target_catalog}

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=new_sources,
    )


def _partial_rewrite_to_mv(
    compiled: CompiledQuery,
    mv: MVDefinition,
    joins: list[dict],
    matched_indices: list[int],
) -> CompiledQuery:
    """Partially rewrite SQL: replace MV-covered JOINs, keep the rest (REQ-083).

    The MV covers the root table + some joined tables. After rewrite:
    - FROM becomes the MV target table (aliased as "t0")
    - Covered JOINs are removed
    - Covered right-table column refs become MV column names ("right_table__col")
    - Uncovered JOINs remain, with their ON clauses adjusted for covered aliases
    - Root-table (t0) column refs stay as "t0"."col" since we alias the MV as t0
    """
    sql = compiled.sql
    matched_set = set(matched_indices)

    # Build alias → table name mapping
    alias_to_table: dict[str, str] = {}
    from_match = re.search(
        r'FROM\s+"[^"]+"\."([^"]+)"\s+"(t\d+)"', sql, re.IGNORECASE,
    )
    if from_match:
        alias_to_table[from_match.group(2)] = from_match.group(1)
    for join in joins:
        alias_to_table[join["right_alias"]] = join["right_table"]

    # Identify covered and uncovered aliases
    covered_right_aliases = {joins[i]["right_alias"] for i in matched_indices}

    # Remove only the matched JOIN clauses (in reverse order to preserve positions)
    for i in sorted(matched_indices, reverse=True):
        join = joins[i]
        # Escape the full clause for regex
        escaped = re.escape(join["full_clause"])
        sql = re.sub(r'\s+' + escaped, '', sql, count=1)

    # Replace FROM clause with MV target table, keeping "t0" alias
    mv_ref = f'"{mv.target_catalog}"."{mv.target_schema}"."{mv.target_table}"'
    sql = re.sub(
        r'FROM\s+"[^"]+"\."[^"]+"\s*"t0"',
        f'FROM {mv_ref} "t0"',
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    # Rewrite only covered right-table column references:
    # Covered right: "t1"."col" → "t0"."customers__col"
    # Root (t0) and uncovered aliases: unchanged
    def _rewrite_col_ref(m: re.Match) -> str:
        alias = m.group(1)
        col = m.group(2)
        if alias in covered_right_aliases:
            table_name = alias_to_table.get(alias, "")
            return f'"t0"."{table_name}__{col}"'
        return m.group(0)  # unchanged

    sql = re.sub(r'"(t\d+)"\."([^"]+)"', _rewrite_col_ref, sql)

    new_sources = compiled.sources | {mv.target_catalog}

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=new_sources,
    )
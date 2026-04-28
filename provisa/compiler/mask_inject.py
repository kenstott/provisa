# Copyright (c) 2026 Kenneth Stott
# Canary: e07027a2-1bdb-4f34-a9eb-2c124971ab05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inject column-level masking into compiled SQL (REQ-087).

Applied after RLS injection, before sampling. Rewrites the SELECT projection
to replace masked columns with their mask expression, preserving column aliases
so serialization still works.
"""

from __future__ import annotations

import re

from provisa.compiler.sql_gen import ColumnRef, CompiledQuery, CompilationContext
from provisa.security.masking import MaskingRule, build_mask_expression
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)


# Key: (table_id, role_id) → {column_name: (MaskingRule, data_type)}
MaskingRules = dict[tuple[int, str], dict[str, tuple[MaskingRule, str]]]


def inject_masking(
    compiled: CompiledQuery,
    ctx: CompilationContext,
    masking_rules: MaskingRules,
    role_id: str,
) -> CompiledQuery:
    """Inject masking expressions into the SELECT projection of compiled SQL.

    For each column in the SELECT that has a masking rule for the current role,
    the column reference is replaced with the mask expression (preserving alias).

    Args:
        compiled: The compiled query with SQL and column refs.
        ctx: Compilation context for table metadata lookup.
        masking_rules: Keyed by (table_id, role_id).
        role_id: The requesting role.

    Returns:
        New CompiledQuery with masked SELECT projection.
    """
    with _tracer.start_as_current_span("masking.inject") as span:
        root_table = ctx.tables.get(compiled.root_field)
        if not root_table:
            span.set_attribute("masking.columns_masked", 0)
            return compiled

        # Collect replacements: (original_column_ref_str, mask_expression)
        replacements: list[tuple[str, str]] = []

        for col_ref in compiled.columns:
            # Determine which table this column belongs to
            if col_ref.nested_in is not None:
                # Joined table — walk the join chain for dotted paths
                parts = col_ref.nested_in.split(".")
                current_type = root_table.type_name
                join_meta = None
                for part in parts:
                    join_key = (current_type, part)
                    join_meta = ctx.joins.get(join_key)
                    if not join_meta:
                        break
                    current_type = join_meta.target.type_name
                if not join_meta:
                    continue
                table_id = join_meta.target.table_id
            else:
                table_id = root_table.table_id

            # Check for masking rule
            rules_for_table = masking_rules.get((table_id, role_id))
            if not rules_for_table:
                continue
            rule_entry = rules_for_table.get(col_ref.column)
            if not rule_entry:
                continue

            rule, data_type = rule_entry

            # Build the original column reference as it appears in SQL
            if col_ref.alias:
                original = f'"{col_ref.alias}"."{col_ref.column}"'
            else:
                original = f'"{col_ref.column}"'

            # Build the mask expression
            mask_expr = build_mask_expression(rule, original, data_type)

            # The replacement preserves the column alias for serialization
            replacement = f'{mask_expr} AS "{col_ref.column}"'
            replacements.append((original, replacement))

        span.set_attribute("masking.columns_masked", len(replacements))
        if not replacements:
            return compiled

        # Apply replacements to the SELECT clause only
        sql = compiled.sql
        select_end = _find_select_end(sql)
        select_part = sql[:select_end]
        rest_part = sql[select_end:]

        for original, replacement in replacements:
            select_part = select_part.replace(original, replacement, 1)

        return CompiledQuery(
            sql=select_part + rest_part,
            params=compiled.params,
            root_field=compiled.root_field,
            columns=compiled.columns,
            sources=compiled.sources,
        )


def _find_select_end(sql: str) -> int:
    """Find the end of the SELECT clause (start of FROM).

    Returns the index of the FROM keyword.
    """
    match = re.search(r'\bFROM\b', sql, re.IGNORECASE)
    if match:
        return match.start()
    return len(sql)

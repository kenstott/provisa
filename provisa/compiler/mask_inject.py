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

# Requirements: REQ-040, REQ-263, REQ-264

from __future__ import annotations

import sqlglot
from sqlglot import exp

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext
from provisa.security.masking import MaskingRule, build_mask_expression
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)


# Key: (table_id, role_id) → {column_name: (MaskingRule, data_type)}
MaskingRules = dict[tuple[int, str], dict[str, tuple[MaskingRule, str]]]


def inject_masking(  # REQ-040, REQ-263, REQ-264
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
        # canonical_field is the pre-alias schema field; root_field may be a client alias
        # absent from ctx.tables — using it alone would skip masking on aliased roots.
        root_table = ctx.tables.get(compiled.canonical_field or compiled.root_field)
        if not root_table:
            span.set_attribute("masking.columns_masked", 0)
            return compiled

        # Which (table_alias, column) refs carry a masking rule for this role, with their rule.
        # Keyed the way a projection Column exposes itself: table alias (or None) + column name.
        mask_map: dict[tuple[str | None, str], tuple[MaskingRule, str]] = {}

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

            mask_map[(col_ref.alias, col_ref.column)] = rule_entry

        if not mask_map:
            span.set_attribute("masking.columns_masked", 0)
            return compiled

        # Rewrite the OUTER projection on the AST: only genuine top-level output columns are
        # candidates, so a scalar subquery in the SELECT list can never shift the masking scope
        # (its inner FROM is structurally distinct, not a text boundary).
        tree = sqlglot.parse_one(compiled.sql, read="postgres")
        select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
        if select is None:
            raise ValueError("inject_masking: query has no SELECT to mask")

        masked = 0
        new_projection: list = []  # mixed Alias/Column nodes; sqlglot stub types are inexact
        for proj in select.expressions:
            col_node = proj.this if isinstance(proj, exp.Alias) else proj
            if isinstance(col_node, exp.Column):
                key = (col_node.table or None, col_node.name)
                entry = mask_map.get(key)
                if entry:
                    rule, data_type = entry
                    mask_sql = build_mask_expression(
                        rule, col_node.sql(dialect="postgres"), data_type
                    )
                    out_alias = proj.alias if isinstance(proj, exp.Alias) else col_node.name
                    new_projection.append(
                        exp.alias_(
                            sqlglot.parse_one(mask_sql, read="postgres"), out_alias, quoted=True
                        )
                    )
                    masked += 1
                    continue
            new_projection.append(proj)

        span.set_attribute("masking.columns_masked", masked)
        if masked == 0:
            return compiled  # nothing matched — leave the SQL byte-identical

        select.set("expressions", new_projection)
        sql = tree.sql(dialect="postgres")

        from provisa.observability.stage_trace import trace_stage

        trace_stage("govern.mask", sql)
        return CompiledQuery(
            sql=sql,
            params=compiled.params,
            root_field=compiled.root_field,
            columns=compiled.columns,
            sources=compiled.sources,
        )


def _find_select_end(sql: str) -> int:
    """Index of the top-level ``FROM`` keyword — the end of the SELECT projection.

    Scans at parenthesis depth 0 and outside string/identifier quotes, so a scalar subquery in the
    projection (``SELECT (SELECT .. FROM ..) AS c, "ssn" FROM t``) does not move the boundary onto the
    inner FROM. Retained as a test/analysis utility (REQ-740/744); ``inject_masking`` rewrites the
    projection structurally on the AST and no longer needs it. Returns ``len(sql)`` when there is no FROM.
    """
    depth = 0
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":  # single-quoted string literal — skip to its close
            i += 1
            while i < n and sql[i] != "'":
                i += 2 if sql[i] == "\\" else 1
            i += 1
            continue
        if ch == '"':  # double-quoted identifier — skip to its close
            i += 1
            while i < n and sql[i] != '"':
                i += 1
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif (
            depth == 0
            and ch in "Ff"
            and sql[i : i + 4].upper() == "FROM"
            and (i == 0 or not (sql[i - 1].isalnum() or sql[i - 1] == "_"))
            and (i + 4 >= n or not (sql[i + 4].isalnum() or sql[i + 4] == "_"))
        ):
            return i
        i += 1
    return n

# Copyright (c) 2026 Kenneth Stott
# Canary: 6b4f2d8a-1c3e-4a9b-7f5d-9e2c4a6b8f1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stage 3 SQLGlot AST rewrite: graph variable → CAST(ROW(...) AS JSON).

Detects node/edge/path variable references in the SELECT list and wraps their
projected columns as CAST(ROW(...) AS JSON). Scalar property projections are
not modified.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.cypher.translator import GraphVarKind
from provisa.cypher.label_map import CypherLabelMap


def apply_graph_rewrites(
    sql_ast: exp.Select,
    graph_vars: dict[str, GraphVarKind],
    label_map: CypherLabelMap,
) -> exp.Select:
    """Rewrite graph variable columns in SELECT to CAST(ROW(...) AS JSON).

    Modifies and returns the sql_ast. Scalar columns are untouched.
    """
    if not graph_vars:
        return sql_ast

    # Build alias → NodeMapping from the FROM/JOIN clauses
    alias_to_node = _extract_alias_mappings(sql_ast, label_map)

    new_expressions: list[exp.Expression] = []

    for sel_expr in sql_ast.expressions:
        alias_name = _get_alias(sel_expr)
        inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
        table_ref = _get_table_ref(inner)
        col_name = _get_col_name(inner)

        # Determine graph variable name
        graph_var = alias_name if alias_name in graph_vars else (col_name if col_name in graph_vars else None)

        if graph_var is None and table_ref and table_ref in graph_vars:
            graph_var = table_ref

        if graph_var is not None:
            kind = graph_vars[graph_var]
            # EDGE, PATH, and PASSTHROUGH expressions are pre-built; pass through.
            if kind in (GraphVarKind.EDGE, GraphVarKind.PATH, GraphVarKind.PASSTHROUGH):
                new_expressions.append(sel_expr)
                continue
            # Find node meta: prefer alias_to_node lookup, then direct search
            node_meta = alias_to_node.get(graph_var) or _find_node_meta(graph_var, table_ref, label_map)
            out_alias = alias_name or graph_var
            tbl = table_ref or graph_var
            if node_meta is not None:
                rewritten = _build_row_cast(tbl, node_meta)
            else:
                rewritten = _build_domain_json(tbl)
            new_expressions.append(exp.alias_(rewritten, out_alias))
        else:
            new_expressions.append(sel_expr)

    return sql_ast.select(*new_expressions, append=False)


def _build_row_cast(tbl: str, node_meta: object) -> exp.Expression:
    """Build JSON_OBJECT('id', ..., 'label', ..., props...) for a graph variable."""
    from provisa.cypher.label_map import NodeMapping
    nm: NodeMapping = node_meta  # type: ignore[assignment]

    id_col = exp.Column(
        this=exp.Identifier(this=nm.id_column, quoted=True),
        table=exp.Identifier(this=tbl),
    )
    kv: list[exp.Expression] = [
        exp.JSONKeyValue(this=exp.Literal.string("id"), expression=id_col),
        exp.JSONKeyValue(this=exp.Literal.string("label"), expression=exp.Literal.string(nm.label)),
    ]
    reserved = {nm.id_column, "label"}
    for col_name in nm.properties.values():
        if col_name in reserved:
            continue
        kv.append(exp.JSONKeyValue(
            this=exp.Literal.string(col_name),
            expression=exp.Column(
                this=exp.Identifier(this=col_name, quoted=True),
                table=exp.Identifier(this=tbl),
            ),
        ))
    return exp.JSONObject(expressions=kv)


def _build_domain_json(var: str) -> exp.Expression:
    """Build JSON_OBJECT for a domain-union node (subquery with __id and __label)."""
    return exp.JSONObject(expressions=[
        exp.JSONKeyValue(
            this=exp.Literal.string("id"),
            expression=exp.Column(
                this=exp.Identifier(this="__id", quoted=True),
                table=exp.Identifier(this=var),
            ),
        ),
        exp.JSONKeyValue(
            this=exp.Literal.string("label"),
            expression=exp.Column(
                this=exp.Identifier(this="__label", quoted=True),
                table=exp.Identifier(this=var),
            ),
        ),
    ])


def _extract_alias_mappings(sql_ast: exp.Select, label_map: CypherLabelMap) -> dict[str, object]:
    """Walk FROM/JOIN clauses and map SQL alias → NodeMapping."""
    from provisa.cypher.label_map import NodeMapping
    alias_map: dict[str, NodeMapping] = {}

    for tbl in sql_ast.find_all(exp.Table):
        alias = tbl.alias  # populated by SQLGlot from AS clause
        table_name = tbl.name
        if alias and table_name:
            for nm in label_map.nodes.values():
                if nm.table_name == table_name:
                    alias_map[alias] = nm
                    break

    return alias_map


def _find_node_meta(var_name: str, table_ref: str | None, label_map: CypherLabelMap) -> object:
    """Fallback lookup: match by label name or table name."""
    for label, nm in label_map.nodes.items():
        if label.lower() == var_name.lower():
            return nm
        if table_ref and nm.table_name == table_ref:
            return nm
    return None


def _get_alias(expr: exp.Expression) -> str | None:
    if isinstance(expr, exp.Alias):
        return expr.alias
    return None


def _get_table_ref(expr: exp.Expression) -> str | None:
    """Extract table/alias name from a Column expression."""
    if isinstance(expr, exp.Column):
        tbl = expr.table
        if tbl:
            return tbl.name if hasattr(tbl, "name") else str(tbl)
    return None


def _get_col_name(expr: exp.Expression) -> str | None:
    """Extract the bare column/identifier name."""
    if isinstance(expr, exp.Column):
        return expr.name
    if isinstance(expr, exp.Identifier):
        return expr.name
    return None

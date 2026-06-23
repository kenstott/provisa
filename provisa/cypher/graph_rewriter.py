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

from collections.abc import Mapping
from typing import cast

import sqlglot.expressions as exp

from provisa.cypher.translator import GraphVarKind
from provisa.cypher.label_map import CypherLabelMap, NodeMapping


def apply_graph_rewrites(
    sql_ast: exp.Select | exp.Union,
    graph_vars: dict[str, GraphVarKind],
    label_map: CypherLabelMap,
) -> exp.Select | exp.Union:
    """Rewrite graph variable columns in SELECT to CAST(ROW(...) AS JSON).

    Modifies and returns the sql_ast. Scalar columns are untouched.
    Handles exp.Union by recursing into each arm.
    """
    if not graph_vars:
        return sql_ast

    if isinstance(sql_ast, exp.Union):
        left = apply_graph_rewrites(sql_ast.this, graph_vars, label_map)
        right = apply_graph_rewrites(sql_ast.expression, graph_vars, label_map)
        result = sql_ast.copy()
        result.set("this", left)
        result.set("expression", right)
        return result

    # Recurse into FROM subqueries that wrap a Union or Select (e.g. SELECT * FROM (UNION) AS
    # _union produced by _apply_order_limit when LIMIT is applied to a variable-length path).
    # Without this, a.*  /  b.*  inside the inner branches are never rewritten to JSON_OBJECT.
    for _subq in sql_ast.find_all(exp.Subquery):
        _inner = _subq.this
        if isinstance(_inner, (exp.Union, exp.Select)):
            _inner_rw = apply_graph_rewrites(_inner, graph_vars, label_map)
            if _inner_rw is not _inner:
                _subq.set("this", _inner_rw)

    # Build alias → NodeMapping from the FROM/JOIN clauses
    alias_to_node = _extract_alias_mappings(sql_ast, label_map)

    new_expressions: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    for sel_expr in sql_ast.expressions:
        alias_name = _get_alias(sel_expr)
        inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
        table_ref = _get_table_ref(inner)
        col_name = _get_col_name(inner)

        # Determine graph variable name
        graph_var = (
            alias_name
            if alias_name in graph_vars
            else (col_name if col_name in graph_vars else None)
        )

        if graph_var is None and table_ref and table_ref in graph_vars:
            graph_var = table_ref

        if graph_var is not None:
            kind = graph_vars[graph_var]
            # EDGE, PATH, and PASSTHROUGH expressions are pre-built; pass through.
            if kind in (GraphVarKind.EDGE, GraphVarKind.PATH, GraphVarKind.PASSTHROUGH):
                new_expressions.append(sel_expr)
                continue
            # NODE expressions already containing pre-built JSON (from _rewrite_graph_fns for
            # startNode/endNode/nodes) pass through — they must not be rebuilt from raw columns.
            # JSON_OBJECT parses to exp.JSONObject; JSON_ARRAY parses to exp.Anonymous("JSON_ARRAY").
            if kind == GraphVarKind.NODE and isinstance(inner, (exp.JSONObject, exp.JSONArray)):  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                new_expressions.append(sel_expr)
                continue
            if (  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                kind == GraphVarKind.NODE
                and isinstance(inner, exp.Anonymous)
                and inner.this.upper() == "JSON_ARRAY"
            ):
                new_expressions.append(sel_expr)
                continue
            # Find node meta: prefer alias_to_node lookup, then direct search
            node_meta = alias_to_node.get(graph_var) or _find_node_meta(
                graph_var, table_ref, label_map
            )
            out_alias = alias_name or graph_var
            tbl = table_ref or graph_var
            if node_meta is not None:
                rewritten = _build_row_cast(tbl, node_meta)
            else:
                domain_props = _extract_domain_props_from_union(sql_ast, tbl)
                rewritten = _build_domain_json(tbl, domain_props)
            new_expressions.append(cast(exp.Expression, exp.alias_(rewritten, out_alias)))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        else:
            new_expressions.append(cast(exp.Expression, sel_expr))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    return sql_ast.select(*new_expressions, append=False)  # type: ignore[return-value]


def _build_row_cast(tbl: str, node_meta: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build CASE WHEN id IS NULL THEN NULL ELSE JSON_OBJECT(...) END for a graph variable.

    The CASE guard ensures OPTIONAL JOINs with no match produce NULL rather than
    a JSON object whose 'id' is null but 'label' is a non-null constant.
    """
    nm = node_meta

    id_col_check = exp.Column(
        this=exp.Identifier(this=nm.id_column, quoted=True),
        table=exp.Identifier(this=tbl),
    )
    raw_id_col = exp.Column(
        this=exp.Identifier(this=nm.id_column, quoted=True),
        table=exp.Identifier(this=tbl),
    )
    # Compound id matches domain-union __id format: "Label|rawPk"
    compound_id = exp.DPipe(
        this=exp.DPipe(
            this=exp.Literal.string(nm.label),
            expression=exp.Literal.string("|"),
        ),
        expression=exp.Cast(this=raw_id_col, to=exp.DataType.build("VARCHAR")),
    )
    kv: list[exp.Expression] = [  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        exp.JSONKeyValue(this=exp.Literal.string("id"), expression=compound_id),
        exp.JSONKeyValue(this=exp.Literal.string("label"), expression=exp.Literal.string(nm.label)),
    ]
    reserved = {"id", "label"}
    for prop_key, col_name in nm.properties.items():
        if col_name in reserved:
            continue
        col_expr = exp.Column(
            this=exp.Identifier(this=col_name, quoted=True),
            table=exp.Identifier(this=tbl),
        )
        kv.append(exp.JSONKeyValue(this=exp.Literal.string(prop_key), expression=col_expr))
    json_obj = exp.JSONObject(expressions=kv)
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=id_col_check, expression=exp.null()), true=exp.null())],
        default=json_obj,
    )


def _build_domain_json(var: str, props: list[str] | None = None) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build JSON_OBJECT for a domain-union node (subquery with __id, __label, props)."""
    kv: list[exp.Expression] = [  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
    ]
    _reserved = {"id", "label"}
    for prop in props or []:
        if prop in _reserved:
            continue
        kv.append(
            exp.JSONKeyValue(
                this=exp.Literal.string(prop),
                expression=exp.Column(
                    this=exp.Identifier(this=prop, quoted=True),
                    table=exp.Identifier(this=var),
                ),
            )
        )
    return exp.JSONObject(expressions=kv)


def _extract_domain_props_from_union(sql_ast: exp.Select, var_alias: str) -> list[str]:
    """Extract property column aliases from the domain union subquery aliased as var_alias.

    Walks FROM/JOIN subqueries looking for one aliased as var_alias, then reads
    the first SELECT branch's column aliases (excluding __id and __label).
    """
    for node in sql_ast.find_all(exp.Subquery):
        parent = node.parent
        # Alias may be stored directly in the Subquery (after .from_() normalization)
        # or in a wrapping Alias parent node — check both.
        node_alias = node.alias or (parent.alias if isinstance(parent, exp.Alias) else None)
        if node_alias == var_alias:
            union_body = node.this
            # Drill into UNION ALL to get the first SELECT branch
            first_select = union_body
            while isinstance(first_select, (exp.Union,)):
                first_select = first_select.this
            if isinstance(first_select, exp.Select):
                props: list[str] = []
                for expr in first_select.expressions:
                    alias = expr.alias if hasattr(expr, "alias") else None
                    if alias and alias not in ("__id", "__label"):
                        props.append(alias)
                return props
    return []


def _extract_alias_mappings(
    sql_ast: exp.Select, label_map: CypherLabelMap
) -> Mapping[str, NodeMapping]:
    """Walk FROM/JOIN clauses and map SQL alias → NodeMapping.

    Handles both direct table refs (FROM table AS c) and subquery wrappers
    ((SELECT *, phys AS alias FROM table) AS c) produced by _node_table_expr.
    """
    alias_map: dict[str, NodeMapping] = {}

    for tbl in sql_ast.find_all(exp.Table):
        alias = tbl.alias
        table_name = tbl.name
        if alias and table_name:
            for nm in label_map.nodes.values():
                phys = nm.physical_table_name or nm.table_name
                if phys == table_name or nm.table_name == table_name:
                    alias_map[alias] = nm
                    break

    # Subquery-wrapped tables: (SELECT *, "phys" AS sql_alias FROM phys_table) AS c
    # Skip domain-union subqueries (UNION ALL of multiple tables) — they must be handled
    # by _build_domain_json, not _build_row_cast with a single table's NodeMapping.
    for subq in sql_ast.find_all(exp.Subquery):
        sq_alias = subq.alias
        if not sq_alias or sq_alias in alias_map:
            continue
        if isinstance(subq.this, exp.Union):
            continue
        inner_table = subq.find(exp.Table)
        if inner_table:
            table_name = inner_table.name
            for nm in label_map.nodes.values():
                phys = nm.physical_table_name or nm.table_name
                if phys == table_name or nm.table_name == table_name:
                    alias_map[sq_alias] = nm
                    break

    return alias_map


def _find_node_meta(
    var_name: str, table_ref: str | None, label_map: CypherLabelMap
) -> NodeMapping | None:
    """Fallback lookup: match by label name or table name."""
    for label, nm in label_map.nodes.items():
        if label.lower() == var_name.lower():
            return nm
        if table_ref:
            phys = nm.physical_table_name or nm.table_name
            if phys == table_ref or nm.table_name == table_ref:
                return nm
    return None


def _get_alias(expr: exp.Expression) -> str | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(expr, exp.Alias):
        return expr.alias
    return None


def _get_table_ref(expr: exp.Expression) -> str | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Extract table/alias name from a Column expression."""
    if isinstance(expr, exp.Column):
        tbl = expr.table
        if tbl:
            return tbl.name if isinstance(tbl, exp.Expression) else str(tbl)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return None


def _get_col_name(expr: exp.Expression) -> str | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Extract the bare column/identifier name."""
    if isinstance(expr, exp.Column):
        return expr.name
    if isinstance(expr, exp.Identifier):
        return expr.name
    return None

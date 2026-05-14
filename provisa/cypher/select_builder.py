# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c4a7e-1b5d-4e9a-3c6f-7d0b2e4a8f1c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SelectBuilderMixin — SELECT clause and path object construction.

Extracted from translator.py to stay under 1000 lines.
Mixed into _Translator; relies on _var_table, _graph_vars, _path_vars,
_shortestpath_hops_col, and _parse_expr.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.parser import ReturnClause


def _is_bare_variable(text: str) -> bool:
    import re

    return bool(re.match(r"^[A-Za-z_]\w*$", text))


class SelectBuilderMixin:
    """Mixin for _Translator: builds SELECT expressions and path objects."""

    _var_table: dict
    _graph_vars: dict
    _path_vars: dict
    _shortestpath_hops_col: exp.Expression | None
    _lm: CypherLabelMap
    _rel_var_types: dict
    _rel_var_endpoints: dict

    def _build_edge_object(
        self,
        rel_type: str,
        src_alias: str,
        src_nm: NodeMapping,
        tgt_alias: str,
        tgt_nm: NodeMapping,
    ) -> exp.Expression:
        """Emit a JSON edge object for RETURN r.

        Neo4j-compatible format:
        JSON_OBJECT(
            'identity', CAST(src.id AS VARCHAR) || '-' || CAST(tgt.id AS VARCHAR),
            'start', src.id,
            'end', tgt.id,
            'type', 'REL_TYPE',
            'properties', JSON_OBJECT(),
            'startNode', JSON_OBJECT('id', src.id, 'label', 'SrcLabel', 'properties', JSON_OBJECT()),
            'endNode', JSON_OBJECT('id', tgt.id, 'label', 'TgtLabel', 'properties', JSON_OBJECT()))
        startNode/endNode are Provisa extensions for graph visualization.
        """
        src_id_col = exp.Column(
            this=exp.Identifier(this=src_nm.id_column, quoted=True),
            table=exp.Identifier(this=src_alias),
        )
        tgt_id_col = exp.Column(
            this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
            table=exp.Identifier(this=tgt_alias),
        )
        # identity: rel_type || ':' || CAST(src.id AS VARCHAR) || '-' || CAST(tgt.id AS VARCHAR)
        # Include rel_type so edges of different types between same node pair get distinct identities.
        identity = exp.DPipe(
            this=exp.DPipe(
                this=exp.DPipe(
                    this=exp.DPipe(
                        this=exp.Literal.string(rel_type),
                        expression=exp.Literal.string(":"),
                    ),
                    expression=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
                ),
                expression=exp.Literal.string("-"),
            ),
            expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
        )
        empty_props = exp.Anonymous(this="JSON_OBJECT", expressions=[])

        def _node_props_expr(alias: str, nm: NodeMapping) -> exp.Expression:
            """Build JSON_OBJECT(...) for all node properties, or empty if none defined."""
            if not nm.properties:
                return empty_props
            exprs = []
            for prop_name, col_name in nm.properties.items():
                exprs.append(exp.Literal.string(prop_name))
                exprs.append(
                    exp.Column(
                        this=exp.Identifier(this=col_name, quoted=True),
                        table=exp.Identifier(this=alias),
                    )
                )
            return exp.Anonymous(this="JSON_OBJECT", expressions=exprs)

        start_node = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("id"),
                exp.Column(
                    this=exp.Identifier(this=src_nm.id_column, quoted=True),
                    table=exp.Identifier(this=src_alias),
                ),
                exp.Literal.string("label"),
                exp.Literal.string(src_nm.label),
                exp.Literal.string("properties"),
                _node_props_expr(src_alias, src_nm),
            ],
        )
        end_node = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("id"),
                exp.Column(
                    this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                    table=exp.Identifier(this=tgt_alias),
                ),
                exp.Literal.string("label"),
                exp.Literal.string(tgt_nm.label),
                exp.Literal.string("properties"),
                _node_props_expr(tgt_alias, tgt_nm),
            ],
        )
        return exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("identity"),
                identity,
                exp.Literal.string("start"),
                src_id_col,
                exp.Literal.string("end"),
                tgt_id_col,
                exp.Literal.string("type"),
                exp.Literal.string(rel_type),
                exp.Literal.string("properties"),
                empty_props,
                exp.Literal.string("startNode"),
                start_node,
                exp.Literal.string("endNode"),
                end_node,
            ],
        )

    def _build_path_object(self, path_var: str) -> exp.Expression:
        """Emit a JSON path object for RETURN p.

        Flat-JOIN paths: full {nodes:[...], edges:[...]} JSON via _build_path_json.
        Recursive CTE paths: JSON_OBJECT('start', src.id, 'end', tgt.id, 'length', hops)
        """
        src_var, tgt_var, is_recursive = self._path_vars[path_var]
        path_step_info = getattr(self, "_path_steps", {}).get(path_var)
        if path_step_info is not None:
            step_nodes, step_edges = path_step_info
            return self._build_path_json(step_nodes, step_edges)
        # Recursive CTE or fallback
        src_nm: NodeMapping | None = self._var_table.get(src_var, (src_var, None))[1]
        tgt_nm: NodeMapping | None = self._var_table.get(tgt_var, (tgt_var, None))[1]
        src_id_col_name = src_nm.id_column if src_nm else "id"
        tgt_id_col_name = tgt_nm.id_column if tgt_nm else "id"
        src_id = exp.Column(
            this=exp.Identifier(this=src_id_col_name, quoted=True),
            table=exp.Identifier(this=src_var),
        )
        if is_recursive and self._shortestpath_hops_col is not None:
            tgt_id = exp.Column(
                this=exp.Identifier(this="cur_id"),
                table=exp.Identifier(this="_t"),
            )
            length_val: exp.Expression = self._shortestpath_hops_col
        else:
            tgt_id = exp.Column(
                this=exp.Identifier(this=tgt_id_col_name, quoted=True),
                table=exp.Identifier(this=tgt_var),
            )
            length_val = exp.Literal.number(1)
        return exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("start"),
                src_id,
                exp.Literal.string("end"),
                tgt_id,
                exp.Literal.string("length"),
                length_val,
            ],
        )

    def _build_path_json(
        self,
        step_nodes: list,
        step_edges: list,
    ) -> exp.Expression:
        """Build JSON_OBJECT('nodes', JSON_ARRAY(...), 'edges', JSON_ARRAY(...)) for a flat-JOIN path."""

        def _node_obj(alias: str, nm: "NodeMapping") -> exp.Expression:
            props_exprs: list[exp.Expression] = []
            for prop_name, col_name in nm.properties.items():
                props_exprs.append(exp.Literal.string(prop_name))
                props_exprs.append(
                    exp.Column(
                        this=exp.Identifier(this=col_name, quoted=True),
                        table=exp.Identifier(this=alias),
                    )
                )
            props = exp.Anonymous(this="JSON_OBJECT", expressions=props_exprs)
            id_col = exp.Column(
                this=exp.Identifier(this=nm.id_column, quoted=True),
                table=exp.Identifier(this=alias),
            )
            return exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    exp.Cast(this=id_col, to=exp.DataType.build("VARCHAR")),
                    exp.Literal.string("label"),
                    exp.Literal.string(nm.label),
                    exp.Literal.string("properties"),
                    props,
                ],
            )

        def _edge_obj(
            rel_type: str,
            src_alias: str,
            src_nm: "NodeMapping",
            tgt_alias: str,
            tgt_nm: "NodeMapping",
            is_reversed: bool = False,
        ) -> exp.Expression:
            src_id_col = exp.Column(
                this=exp.Identifier(this=src_nm.id_column, quoted=True),
                table=exp.Identifier(this=src_alias),
            )
            tgt_id_col = exp.Column(
                this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                table=exp.Identifier(this=tgt_alias),
            )
            src_id_cast = exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR"))
            tgt_id_cast = exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR"))
            # When the edge is traversed in reverse of its canonical direction,
            # use canonical order (canonical_src-canonical_tgt) for identity so it
            # matches the identity produced by show-children (which always uses canonical direction).
            if is_reversed:
                identity_first, identity_second = tgt_id_cast, src_id_cast
            else:
                identity_first, identity_second = src_id_cast, tgt_id_cast
            identity = exp.DPipe(
                this=exp.DPipe(
                    this=exp.DPipe(
                        this=exp.DPipe(
                            this=exp.Literal.string(rel_type),
                            expression=exp.Literal.string(":"),
                        ),
                        expression=identity_first,
                    ),
                    expression=exp.Literal.string("-"),
                ),
                expression=identity_second,
            )
            return exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("identity"),
                    identity,
                    exp.Literal.string("type"),
                    exp.Literal.string(rel_type),
                    exp.Literal.string("start"),
                    src_id_cast,
                    exp.Literal.string("end"),
                    tgt_id_cast,
                    exp.Literal.string("startNode"),
                    _node_obj(src_alias, src_nm),
                    exp.Literal.string("endNode"),
                    _node_obj(tgt_alias, tgt_nm),
                    exp.Literal.string("properties"),
                    exp.Anonymous(this="JSON_OBJECT", expressions=[]),
                ],
            )

        nodes_array = exp.Anonymous(
            this="JSON_ARRAY",
            expressions=[_node_obj(alias, nm) for alias, nm in step_nodes],
        )
        edges_array = exp.Anonymous(
            this="JSON_ARRAY",
            expressions=[
                _edge_obj(rt, sa, snm, ta, tnm, rev) for rt, sa, snm, ta, tnm, rev in step_edges
            ],
        )
        return exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("nodes"),
                nodes_array,
                exp.Literal.string("edges"),
                edges_array,
                exp.Literal.string("length"),
                exp.Literal.number(len(step_edges)),
            ],
        )

    def _build_select(self, return_clause: ReturnClause) -> list[exp.Expression]:
        from provisa.cypher.translator import GraphVarKind  # avoid circular at module level

        exprs: list[exp.Expression] = []
        for item in return_clause.items:
            expr_text = item.expression.strip()
            alias = item.alias

            # Passthrough variable: pre-built JSON from all-rels union subquery
            passthrough_vars = getattr(self, "_passthrough_vars", set())
            all_rels_alias = getattr(self, "_all_rels_alias", "_all_rels")
            if _is_bare_variable(expr_text) and expr_text in passthrough_vars:
                # Edge var keeps EDGE kind; node vars get PASSTHROUGH so rewriter skips them
                rel_var_types = getattr(self, "_rel_var_types", {})
                if expr_text in rel_var_types:
                    self._graph_vars[alias or expr_text] = GraphVarKind.EDGE
                else:
                    self._graph_vars[alias or expr_text] = GraphVarKind.PASSTHROUGH
                col = exp.Column(
                    this=exp.Identifier(this=expr_text, quoted=True),
                    table=exp.Identifier(this=all_rels_alias),
                )
                out = alias or expr_text
                exprs.append(exp.alias_(col, out))
                continue

            # Path variable: RETURN p where p = shortestPath(...)
            if _is_bare_variable(expr_text) and expr_text in self._path_vars:
                self._graph_vars[alias or expr_text] = GraphVarKind.PATH
                path_expr = self._build_path_object(expr_text)
                if alias:
                    exprs.append(exp.alias_(path_expr, alias))
                else:
                    exprs.append(exp.alias_(path_expr, expr_text))
                continue

            # Bare relationship variable: RETURN r
            if (
                _is_bare_variable(expr_text)
                and hasattr(self, "_rel_var_types")
                and expr_text in self._rel_var_types
            ):
                self._graph_vars[alias or expr_text] = GraphVarKind.EDGE
                endpoints = getattr(self, "_rel_var_endpoints", {}).get(expr_text)
                if endpoints:
                    src_alias, src_nm, tgt_alias, tgt_nm = endpoints
                    rel_type = self._rel_var_types[expr_text]
                    edge_expr = self._build_edge_object(
                        rel_type, src_alias, src_nm, tgt_alias, tgt_nm
                    )
                else:
                    edge_expr = exp.Null()
                out = alias or expr_text
                exprs.append(exp.alias_(edge_expr, out))
                continue

            # Bare node variable: RETURN n
            if _is_bare_variable(expr_text) and expr_text in self._var_table:
                var_info = self._var_table[expr_text]
                if var_info[1] is not None:
                    self._graph_vars[alias or expr_text] = GraphVarKind.NODE
                    table_alias = var_info[0]
                    tbl_col = exp.Column(
                        this=exp.Star(),
                        table=exp.Identifier(this=table_alias),
                    )
                    if alias:
                        exprs.append(exp.alias_(tbl_col, alias))
                    else:
                        exprs.append(tbl_col)
                    continue
                # Domain-only node (var_info[1] is None): select all from the subquery alias
                if hasattr(self, "_domain_nodes") and expr_text in self._domain_nodes:
                    self._graph_vars[alias or expr_text] = GraphVarKind.NODE
                    tbl_col = exp.Column(
                        this=exp.Star(),
                        table=exp.Identifier(this=expr_text),
                    )
                    if alias:
                        exprs.append(exp.alias_(tbl_col, alias))
                    else:
                        exprs.append(tbl_col)
                    continue

            # Property access or expression
            parsed = self._parse_expr(expr_text)
            if alias:
                exprs.append(exp.alias_(parsed, alias))
            else:
                # For var.prop without an explicit alias, use the canonical Cypher
                # property name from NodeMapping (the naming authority) so the SQL
                # column name never leaks into results.
                import re as _re

                _prop_m = _re.match(r"^([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)$", expr_text.strip())
                _cypher_alias: str | None = None
                if _prop_m:
                    _var, _prop = _prop_m.group(1), _prop_m.group(2)
                    _info = self._var_table.get(_var)
                    if _info and _info[1] and _prop in _info[1].properties:
                        _cypher_alias = _prop
                if _cypher_alias:
                    exprs.append(exp.alias_(parsed, _cypher_alias))
                else:
                    exprs.append(parsed)

        return exprs

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

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.parser import ReturnClause

# Requirements: REQ-345, REQ-347, REQ-349, REQ-350, REQ-351

if TYPE_CHECKING:
    from provisa.cypher.translator_types import GraphVarKind


def _is_bare_variable(text: str) -> bool:
    import re

    return bool(re.match(r"^[A-Za-z_]\w*$", text))


class SelectBuilderMixin:  # REQ-345, REQ-349, REQ-350, REQ-351
    """Mixin for _Translator: builds SELECT expressions and path objects."""

    _var_table: dict
    _graph_vars: dict
    _path_vars: dict
    _shortestpath_hops_col: exp.Expression | None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _lm: CypherLabelMap
    _rel_var_types: dict
    _rel_var_endpoints: dict
    _domain_nodes: dict[str, str]
    _varlen_rel_vars: dict  # varlen rel variable → outer path variable

    def _parse_expr(self, text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        raise NotImplementedError(text)

    def _build_edge_object(
        self,
        rel_type: str,
        src_alias: str,
        src_nm: NodeMapping,
        tgt_alias: str,
        tgt_nm: NodeMapping,
        is_reversed: bool = False,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
        When is_reversed=True the pattern traverses the canonical edge backward; identity uses
        canonical (src→tgt) order so it matches imputed-edge identities (always canonical).
        startNode/endNode remain in pattern order for display.
        """
        src_id_col = exp.Column(
            this=exp.Identifier(this=src_nm.id_column, quoted=True),
            table=exp.Identifier(this=src_alias),
        )
        tgt_id_col = exp.Column(
            this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
            table=exp.Identifier(this=tgt_alias),
        )
        # identity: rel_type || ':' || CAST(canonical_src.id AS VARCHAR) || '-' || CAST(canonical_tgt.id AS VARCHAR)
        # When traversed backward, swap src/tgt so identity matches canonical (imputed) direction.
        # Include rel_type so edges of different types between same node pair get distinct identities.
        identity_first = exp.Cast(
            this=tgt_id_col if is_reversed else src_id_col, to=exp.DataType.build("VARCHAR")
        )
        identity_second = exp.Cast(
            this=src_id_col if is_reversed else tgt_id_col, to=exp.DataType.build("VARCHAR")
        )
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
        empty_props = exp.Anonymous(this="JSON_OBJECT", expressions=[])

        def _node_props_expr(alias: str, nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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

        src_compound_id = exp.DPipe(
            this=exp.DPipe(
                this=exp.Literal.string(src_nm.label),
                expression=exp.Literal.string("|"),
            ),
            expression=exp.Cast(
                this=exp.Column(
                    this=exp.Identifier(this=src_nm.id_column, quoted=True),
                    table=exp.Identifier(this=src_alias),
                ),
                to=exp.DataType.build("VARCHAR"),
            ),
        )
        tgt_compound_id = exp.DPipe(
            this=exp.DPipe(
                this=exp.Literal.string(tgt_nm.label),
                expression=exp.Literal.string("|"),
            ),
            expression=exp.Cast(
                this=exp.Column(
                    this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                    table=exp.Identifier(this=tgt_alias),
                ),
                to=exp.DataType.build("VARCHAR"),
            ),
        )
        start_node = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("id"),
                src_compound_id,
                exp.Literal.string("label"),
                exp.Literal.string(src_nm.label),
                exp.Literal.string("tableLabel"),
                exp.Literal.string(src_nm.table_label),
                exp.Literal.string("properties"),
                _node_props_expr(src_alias, src_nm),
            ],
        )
        end_node = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("id"),
                tgt_compound_id,
                exp.Literal.string("label"),
                exp.Literal.string(tgt_nm.label),
                exp.Literal.string("tableLabel"),
                exp.Literal.string(tgt_nm.table_label),
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

    def _build_path_object(self, path_var: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Emit a JSON path object for RETURN p.

        Flat-JOIN paths: full {nodes:[...], edges:[...]} JSON via _build_path_json.
        Recursive CTE paths: JSON_OBJECT('start', src.id, 'end', tgt.id, 'length', hops)
        """
        src_var, tgt_var, is_recursive = self._path_vars[path_var]
        path_step_info = getattr(self, "_path_steps", {}).get(path_var)
        if path_step_info is not None:
            step_nodes, step_edges = path_step_info
            return self._build_path_json(step_nodes, step_edges)
        # All-rels union path (anonymous or unlabeled-rel): build from _all_rels subquery columns.
        all_rels_alias = getattr(self, "_all_rels_alias", None)
        all_rels_src_col = getattr(self, "_all_rels_src_col", None)
        all_rels_rel_col = getattr(self, "_all_rels_rel_col", None)
        all_rels_tgt_col = getattr(self, "_all_rels_tgt_col", None)
        if (
            (not src_var or not tgt_var)
            and all_rels_alias
            and all_rels_src_col
            and all_rels_tgt_col
            and all_rels_rel_col
        ):
            n_expr = exp.Column(
                this=exp.Identifier(this=all_rels_src_col),
                table=exp.Identifier(this=all_rels_alias),
            )
            m_expr = exp.Column(
                this=exp.Identifier(this=all_rels_tgt_col),
                table=exp.Identifier(this=all_rels_alias),
            )
            r_expr = exp.Column(
                this=exp.Identifier(this=all_rels_rel_col),
                table=exp.Identifier(this=all_rels_alias),
            )
            return exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("nodes"),
                    exp.Anonymous(this="JSON_ARRAY", expressions=[n_expr, m_expr]),
                    exp.Literal.string("edges"),
                    exp.Anonymous(this="JSON_ARRAY", expressions=[r_expr]),
                ],
            )
        # Recursive CTE or fallback
        src_nm: NodeMapping | None = self._var_table.get(src_var, (src_var, None))[1]
        tgt_nm: NodeMapping | None = self._var_table.get(tgt_var, (tgt_var, None))[1]
        src_id_col_name = src_nm.id_column if src_nm else "id"
        tgt_id_col_name = tgt_nm.id_column if tgt_nm else "id"
        src_id: exp.Expression = (
            exp.Column(
                this=exp.Identifier(this=src_id_col_name, quoted=True),
                table=exp.Identifier(this=src_var),
            )
            if src_var
            else exp.Identifier(this=src_id_col_name, quoted=True)
        )
        if is_recursive and self._shortestpath_hops_col is not None:
            tgt_id: exp.Expression = exp.Column(
                this=exp.Identifier(this="cur_id"),
                table=exp.Identifier(this="_t"),
            )
            length_val: exp.Expression = self._shortestpath_hops_col  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        else:
            tgt_id = (
                exp.Column(
                    this=exp.Identifier(this=tgt_id_col_name, quoted=True),
                    table=exp.Identifier(this=tgt_var),
                )
                if tgt_var
                else exp.Identifier(this=tgt_id_col_name, quoted=True)
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
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build JSON_OBJECT('nodes', JSON_ARRAY(...), 'edges', JSON_ARRAY(...)) for a flat-JOIN path."""

        def _node_obj(alias: str, nm: "NodeMapping") -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
            # Compound ID "Label|raw_id" so _walk_for_nodes can find and register it.
            compound_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Literal.string(nm.label),
                    expression=exp.Literal.string("|"),
                ),
                expression=exp.Cast(this=id_col, to=exp.DataType.build("VARCHAR")),
            )
            return exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    compound_id,
                    exp.Literal.string("label"),
                    exp.Literal.string(nm.label),
                    exp.Literal.string("tableLabel"),
                    exp.Literal.string(nm.table_label),
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
        ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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

        if not step_nodes and step_edges:
            seen: set[str] = set()
            step_nodes = []
            for rt, sa, snm, ta, tnm, rev in step_edges:
                if sa not in seen:
                    step_nodes.append((sa, snm))
                    seen.add(sa)
                if ta not in seen:
                    step_nodes.append((ta, tnm))
                    seen.add(ta)
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

    def _select_passthrough_var(
        self,
        expr_text: str,
        alias: str | None,
        graph_var_kind_edge: GraphVarKind,
        graph_var_kind_passthrough: GraphVarKind,
    ) -> exp.Expr:
        """Emit SELECT expression for a passthrough variable from _all_rels subquery."""
        all_rels_alias = getattr(self, "_all_rels_alias", "_all_rels")
        rel_var_types = getattr(self, "_rel_var_types", {})
        if expr_text in rel_var_types:
            self._graph_vars[alias or expr_text] = graph_var_kind_edge
        else:
            self._graph_vars[alias or expr_text] = graph_var_kind_passthrough
        col = exp.Column(
            this=exp.Identifier(this=expr_text, quoted=True),
            table=exp.Identifier(this=all_rels_alias),
        )
        return exp.alias_(col, alias or expr_text)

    def _select_path_var(
        self,
        expr_text: str,
        alias: str | None,
        graph_var_kind_path: GraphVarKind,
    ) -> exp.Expr:
        """Emit SELECT expression for a path variable."""
        self._graph_vars[alias or expr_text] = graph_var_kind_path
        path_expr = self._build_path_object(expr_text)
        return exp.alias_(path_expr, alias or expr_text)

    def _select_rel_var(
        self,
        expr_text: str,
        alias: str | None,
        graph_var_kind_edge: GraphVarKind,
    ) -> exp.Expr:
        """Emit SELECT expression for a bare relationship variable."""
        self._graph_vars[alias or expr_text] = graph_var_kind_edge
        endpoints = getattr(self, "_rel_var_endpoints", {}).get(expr_text)
        if endpoints:
            src_alias, src_nm, tgt_alias, tgt_nm, is_reversed = endpoints
            rel_type = self._rel_var_types[expr_text]
            edge_expr: exp.Expression = self._build_edge_object(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                rel_type, src_alias, src_nm, tgt_alias, tgt_nm, is_reversed
            )
        else:
            edge_expr = exp.Null()
        return exp.alias_(edge_expr, alias or expr_text)

    def _select_node_var(
        self,
        expr_text: str,
        alias: str | None,
        graph_var_kind_node: GraphVarKind,
    ) -> exp.Expr | None:
        """Emit SELECT expression for a bare node variable. Returns None if not handled."""
        var_info = self._var_table[expr_text]
        if var_info[1] is not None:
            self._graph_vars[alias or expr_text] = graph_var_kind_node
            table_alias = var_info[0]
            tbl_col = exp.Column(
                this=exp.Star(),
                table=exp.Identifier(this=table_alias),
            )
            if alias:
                return exp.alias_(tbl_col, alias)
            return tbl_col
        if hasattr(self, "_domain_nodes") and expr_text in self._domain_nodes:
            self._graph_vars[alias or expr_text] = graph_var_kind_node
            tbl_col = exp.Column(
                this=exp.Star(),
                table=exp.Identifier(this=expr_text),
            )
            if alias:
                return exp.alias_(tbl_col, alias)
            return tbl_col
        return None

    def _select_prop_expr(self, expr_text: str, alias: str | None) -> exp.Expr:
        """Emit SELECT expression for a property access or arbitrary expression."""
        import re as _re

        parsed = self._parse_expr(expr_text)
        if alias:
            return exp.alias_(parsed, alias)
        _prop_m = _re.match(r"^([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)$", expr_text.strip())
        _cypher_alias: str | None = None
        if _prop_m:
            _var, _prop = _prop_m.group(1), _prop_m.group(2)
            _info = self._var_table.get(_var)
            if _info and _info[1] and _prop in _info[1].properties:
                _cypher_alias = _prop
        if _cypher_alias:
            return exp.alias_(parsed, _cypher_alias)
        return parsed

    def _select_varlen_rel_var(
        self,
        expr_text: str,
        alias: str | None,
        graph_var_kind_edge: "GraphVarKind",
    ) -> exp.Expr:
        """Emit SELECT expression for a variable-length relationship variable (e.g. c from [c*..5]).

        Flat-join paths: JSON_ARRAY of edge objects from the resolved schema path steps.
        Recursive CTE paths: NULL (intermediate edges are not projected by the recursive CTE).
        """
        path_var = self._varlen_rel_vars[expr_text]
        path_step_info = getattr(self, "_path_steps", {}).get(path_var)
        self._graph_vars[alias or expr_text] = graph_var_kind_edge
        if path_step_info is not None:
            _, step_edges = path_step_info
            edges_array = exp.Anonymous(
                this="JSON_ARRAY",
                expressions=[
                    self._build_edge_object(rt, sa, snm, ta, tnm, rev)
                    for rt, sa, snm, ta, tnm, rev in step_edges
                ],
            )
            return exp.alias_(edges_array, alias or expr_text)
        return exp.alias_(exp.Null(), alias or expr_text)

    def _build_node_object_expr(self, alias: str, nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
                exp.Literal.string("tableLabel"),
                exp.Literal.string(nm.table_label),
                exp.Literal.string("properties"),
                props,
            ],
        )

    def _build_select(self, return_clause: ReturnClause) -> list[exp.Expr]:
        from provisa.cypher.translator_types import GraphVarKind  # avoid circular at module level

        exprs: list[exp.Expr] = []
        passthrough_vars = getattr(self, "_passthrough_vars", set())

        for item in return_clause.items:
            expr_text = item.expression.strip()
            alias = item.alias
            is_bare = _is_bare_variable(expr_text)

            # Passthrough variable: pre-built JSON from all-rels union subquery
            if is_bare and expr_text in passthrough_vars:
                exprs.append(
                    self._select_passthrough_var(
                        expr_text, alias, GraphVarKind.EDGE, GraphVarKind.PASSTHROUGH
                    )
                )
                continue

            # Varlen relationship variable: RETURN c where c is from [c*..5]
            varlen_rel_vars = getattr(self, "_varlen_rel_vars", {})
            if is_bare and expr_text in varlen_rel_vars:
                exprs.append(self._select_varlen_rel_var(expr_text, alias, GraphVarKind.EDGE))
                continue

            # Path variable: RETURN p where p = shortestPath(...)
            if is_bare and expr_text in self._path_vars:
                exprs.append(self._select_path_var(expr_text, alias, GraphVarKind.PATH))
                continue

            # Bare relationship variable: RETURN r
            if is_bare and hasattr(self, "_rel_var_types") and expr_text in self._rel_var_types:
                exprs.append(self._select_rel_var(expr_text, alias, GraphVarKind.EDGE))
                continue

            # Bare node variable: RETURN n
            if is_bare and expr_text in self._var_table:
                node_expr = self._select_node_var(expr_text, alias, GraphVarKind.NODE)
                if node_expr is not None:
                    exprs.append(node_expr)
                    continue

            # Graph function expression: register alias kind for assembler deserialization
            import re as _re

            for _pat, _kind in (
                (r"^relationship(?:s)?\s*\(", GraphVarKind.EDGE),
                (r"^nodes\s*\(", GraphVarKind.NODE),
                (r"^startNode\s*\(", GraphVarKind.NODE),
                (r"^endNode\s*\(", GraphVarKind.NODE),
            ):
                if _re.match(_pat, expr_text, _re.IGNORECASE) and alias:
                    self._graph_vars[alias.lower()] = _kind
                    break

            # Property access or expression
            exprs.append(self._select_prop_expr(expr_text, alias))

        return exprs

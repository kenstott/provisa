# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate CypherAST to a SQLGlot SELECT AST.

Stage 1 of the Cypher pipeline:
  CypherAST + CypherLabelMap + params → (exp.Select, ordered param list)

Mapping:
  MATCH (n:Label)        → FROM schema.table AS n
  -[:REL]->              → JOIN … ON n.col = m.col
  OPTIONAL MATCH         → LEFT JOIN
  WHERE                  → WHERE
  RETURN                 → SELECT
  ORDER BY / SKIP / LIMIT → SQL equivalents
  WITH                   → CTE / subquery
"""

# Requirements: REQ-345, REQ-346, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-394, REQ-397, REQ-409

from __future__ import annotations

import re
from typing import Any

import sqlglot.expressions as exp
import sqlglot

from provisa.cypher.parser import (
    CypherAST,
    MatchClause,
    MatchStep,
    NodePattern,
    ReturnItem,
    UnwindClause,
    WhereClause,
    WithClause,
    OrderItem,
)
from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.path_functions import PathFunctionsMixin
from provisa.cypher.select_builder import SelectBuilderMixin
from provisa.cypher.correlated_call import CorrelatedCallMixin
from provisa.cypher.map_projection import rewrite_bare_map_literals
from provisa.cypher.group_by import GroupByMixin
from provisa.cypher.translator_helpers import (
    _fold_where_into_optional_joins,
    _is_bare_variable,
    _node_table_expr,
    _optional_vars,
    _rewrite_cypher_fn_node,
    _rewrite_property_access,
    _safe_alias,
)
from provisa.cypher.translator_types import (
    CypherTranslateError,
    GraphVarKind,
)
from provisa.cypher.translator_rel import _RelJoinMixin
from provisa.cypher.translator_union import _UnionMixin


def cypher_to_sql(  # REQ-345, REQ-347, REQ-352
    ast: CypherAST,
    label_map: CypherLabelMap,
    params: dict[str, Any],
) -> tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]:
    """Translate CypherAST to SQLGlot Select.

    Returns (sql_ast, ordered_param_names, graph_vars).
    """
    translator = _Translator(ast, label_map, params)
    return translator.translate()


def cypher_calls_to_sql_list(  # REQ-571
    ast: CypherAST,
    label_map: CypherLabelMap,
    params: dict[str, Any],
) -> list[tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]]:
    """Translate each CALL {} subquery independently.

    Returns one (sql_ast, ordered_params, graph_vars) tuple per CALL {} block.
    """
    results = []
    for call_sq in ast.call_subqueries:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(call_sq.body, label_map, params)
        results.append((sql_ast, ordered_params, graph_vars))
    return results


class _Translator(  # REQ-345, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-394, REQ-409
    PathFunctionsMixin,
    SelectBuilderMixin,
    CorrelatedCallMixin,
    GroupByMixin,
    _RelJoinMixin,
    _UnionMixin,
):
    def __init__(
        self,
        ast: CypherAST,
        label_map: CypherLabelMap,
        params: dict[str, Any],
    ) -> None:
        self._ast = ast
        self._lm = label_map
        self._params = params
        # variable_name → (alias, NodeMapping | None)
        self._var_table: dict[str, tuple[str, NodeMapping | None]] = {}
        # graph vars in the RETURN clause
        self._graph_vars: dict[str, GraphVarKind] = {}
        self._param_order: list[str] = []
        self._param_seen: set[str] = set()
        self._cte_sources: set[str] = set()
        # var → domain_name for nodes resolved via a domain label only
        self._domain_nodes: dict[str, str] = {}
        # extra (from, joins, path_step_overrides) branches from multi-path shortestPath/allShortestPaths
        self._extra_path_branches: list[tuple[exp.Expression | None, list[dict], dict]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # WITH RECURSIVE CTEs for self-referential variable-length paths
        self._recursive_ctes: list[tuple[str, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # Set by PathFunctionsMixin when a recursive shortestPath is emitted
        self._shortestpath_hops_col: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        self._shortestpath_is_all: bool = False
        # Counter for unique UNNEST alias names across the translation
        self._unwind_count: int = 0
        # UNWIND variables whose source array contains MAP elements (e.g. collect({...}))
        self._map_unwind_vars: set[str] = set()
        # Variables whose value is a MAP array (from collect({...}) or similar)
        self._map_array_vars: set[str] = set()
        # path_var → (src_var, tgt_var, is_recursive) for RETURN p support
        self._path_vars: dict[str, tuple[str, str, bool]] = {}
        # path_var → (step_nodes, step_edges) for flat-JOIN paths
        self._path_steps: dict[str, tuple[list, list]] = {}
        # vars from outer scope bound via CALL { WITH x ... } — skip as FROM source
        self._lateral_bound: set[str] = set()
        # ON conditions from lateral-bound first-node relationships → added as WHERE
        self._lateral_conditions: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        # CALL subquery return variable → lateral alias (e.g. "d_list" → "_call0")
        self._call_var_to_lateral: dict[str, str] = {}
        # relationship variable → resolved rel_type string (for type(r) resolution)
        self._rel_var_types: dict[str, str] = {}
        # relationship variable → (src_alias, src_nm, tgt_alias, tgt_nm, is_reversed)
        self._rel_var_endpoints: dict[str, tuple[str, "NodeMapping", str, "NodeMapping", bool]] = {}
        # id(RelPattern) → (rel_type, src_alias, src_nm, tgt_alias, tgt_nm, is_reversed) for
        # every resolved rel step, including anonymous/unnamed ones, so flat-JOIN path
        # building can reconstruct full {nodes, edges} even when endpoints have no variable.
        self._rel_step_endpoints: dict[int, tuple] = {}
        # vars that are pre-built JSON from an all-rels union subquery
        self._passthrough_vars: set[str] = set()
        # relationship variables whose value is JSON_OBJECT({id,type,startNode,endNode})
        self._all_rels_rel_vars: set[str] = set()
        # node variables whose value is JSON_OBJECT({id,label,tableLabel,properties:{...}})
        self._all_rels_node_vars: set[str] = set()
        # alias of the all-rels union subquery (when built)
        self._all_rels_alias: str | None = None
        # column names in the all-rels subquery for src node, rel, tgt node JSON objects
        self._all_rels_src_col: str | None = None
        self._all_rels_rel_col: str | None = None
        self._all_rels_tgt_col: str | None = None
        # varlen rel variable → outer path variable (e.g. c from [c*..5] → r from MATCH r = ...)
        self._varlen_rel_vars: dict[str, str] = {}

    def _build_cte_segment(
        self,
        n: int,
        match_steps: list,
        unwinds: list,
        with_clause: WithClause,
    ) -> tuple[str, exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build one CTE (from, joins, select, where, group-by) for a WITH segment."""
        all_matches = [m for step in match_steps for m in step.matches]
        stage_where: WhereClause | None = None
        for step in match_steps:
            if step.where is not None:
                stage_where = step.where
                break
        if all_matches:
            from_clause, joins = self._build_from_joins(all_matches)
            if unwinds:
                _, uw_joins = self._build_unwind_joins(unwinds, has_from=True)
                joins.extend(uw_joins)
        elif unwinds:
            from_clause, joins = self._build_unwind_joins(unwinds, has_from=False)
            assert from_clause is not None
        else:
            raise CypherTranslateError("Pipeline segment has no data source")

        select_exprs = self._build_with_select_items(with_clause.items)
        where_expr = self._build_where(stage_where)
        if where_expr and stage_where is not None:
            joins, where_expr = _fold_where_into_optional_joins(
                where_expr, _optional_vars(all_matches), stage_where.expression, joins
            )

        stage_query = exp.select(*select_exprs).from_(from_clause)
        for join in joins:
            stage_query = stage_query.join(
                join["table"], on=join["on"], join_type=join["join_type"]
            )
        if where_expr:
            stage_query = stage_query.where(where_expr)
        with_group_exprs = self._build_group_by_for_with(with_clause.items)
        if with_group_exprs:
            stage_query = stage_query.group_by(*with_group_exprs)
        if with_clause.where is not None:
            with_where_expr = self._build_where(with_clause.where)
            if with_where_expr:
                stage_query = (
                    exp.select(exp.Star())
                    .from_(exp.alias_(exp.Subquery(this=stage_query), alias="_inner"))
                    .where(with_where_expr)
                )

        cte_name = f"_w{n}"
        return cte_name, stage_query

    def _build_final_from(
        self,
        final_match_steps: list,
        final_unwinds: list,
        cte_defs: list[tuple[str, exp.Expression]],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> tuple[exp.Expression, list[dict], list[MatchClause], "WhereClause | None"]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM/joins for the final segment.

        Returns (from_clause, joins, all_matches, stage_where).
        """
        all_matches = [m for step in final_match_steps for m in step.matches]
        stage_where: WhereClause | None = None
        for step in final_match_steps:
            if step.where is not None:
                stage_where = step.where
                break

        if not all_matches and not final_unwinds and cte_defs:
            last_cte_name = cte_defs[-1][0]
            from_clause: exp.Expression = exp.Table(this=exp.Identifier(this=last_cte_name))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            joins: list[dict] = []
        elif not all_matches and final_unwinds:
            if cte_defs:
                last_cte_name = cte_defs[-1][0]
                from_clause = exp.Table(this=exp.Identifier(this=last_cte_name))
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(uw_joins)
            else:
                uw_from, uw_joins = self._build_unwind_joins(final_unwinds, has_from=False)
                assert uw_from is not None
                from_clause = uw_from
                joins = list(uw_joins)
        elif all_matches:
            from_clause, joins = self._build_from_joins(all_matches)
            if final_unwinds:
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(joins) + uw_joins
        else:
            raise CypherTranslateError("Query has no data source")

        return from_clause, joins, all_matches, stage_where

    def _apply_where_and_fold(
        self,
        stage_where: "WhereClause | None",
        all_matches: list[MatchClause],
        joins: list[dict],
    ) -> tuple["exp.Expression | None", list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build WHERE and fold optional-join conditions.

        Also updates self._extra_path_branches with folded WHERE.
        """
        where_expr = self._build_where(stage_where)
        if where_expr and stage_where is not None:
            opt_vars = _optional_vars(all_matches)
            joins, where_expr = _fold_where_into_optional_joins(
                where_expr, opt_vars, stage_where.expression, joins
            )
            raw_where = self._build_where(stage_where)
            if raw_where is not None:
                self._extra_path_branches = [
                    (
                        bf,
                        _fold_where_into_optional_joins(
                            raw_where, opt_vars, stage_where.expression, bj
                        )[0],
                        bps,
                    )
                    for bf, bj, bps in self._extra_path_branches
                ]
        return where_expr, joins

    def _build_main_query(
        self,
        from_clause: "exp.Expression",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
        where_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        select_exprs: list["exp.Expr"],
    ) -> "exp.Select":
        """Assemble the main SELECT query from components."""
        query = exp.select(*select_exprs).from_(from_clause)
        if self._ast.return_clause and self._ast.return_clause.distinct:
            query = query.distinct()
        for join in joins:
            query = query.join(join["table"], on=join["on"], join_type=join["join_type"])
        if where_expr:
            query = query.where(where_expr)
        for lat_cond in self._lateral_conditions:
            query = query.where(lat_cond)
        group_exprs = (
            self._build_group_by(self._ast.return_clause) if self._ast.return_clause else []
        )
        if group_exprs:
            query = query.group_by(*group_exprs)
        return query

    def _apply_extra_branches(
        self,
        result: "exp.Select | exp.Union",
        select_exprs: list["exp.Expr"],
        where_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "exp.Select | exp.Union":
        """Apply UNION ALL extra branches from multi-path shortestPath/allShortestPaths."""
        for extra_from, extra_joins, extra_path_steps_map in self._extra_path_branches:
            branch_select_exprs = []
            for s_expr in select_exprs:
                alias_name = getattr(s_expr, "alias", None)
                if alias_name and alias_name in extra_path_steps_map:
                    sn, se = extra_path_steps_map[alias_name]
                    new_path = self._build_path_json(sn, se)
                    branch_select_exprs.append(exp.alias_(new_path, alias_name))
                else:
                    branch_select_exprs.append(s_expr)
            assert extra_from is not None
            branch = exp.select(*branch_select_exprs).from_(extra_from)
            if self._ast.return_clause and self._ast.return_clause.distinct:
                branch = branch.distinct()
            for j in extra_joins:
                branch = branch.join(j["table"], on=j["on"], join_type=j["join_type"])
            if where_expr:
                branch = branch.where(where_expr)
            result = exp.Union(this=result, expression=branch, distinct=False)
        return result

    def _fold_union_parts(self, result: "exp.Select | exp.Union") -> "exp.Select | exp.Union":
        """Fold UNION / UNION ALL parts into result."""
        for i, (sub_ast, is_all) in enumerate(self._ast.union_parts):
            sub_sql, sub_params, sub_graph_vars = cypher_to_sql(sub_ast, self._lm, self._params)
            for p in sub_params:
                if p not in self._param_seen:
                    self._param_order.append(p)
                    self._param_seen.add(p)
            self._graph_vars.update(sub_graph_vars)
            # If the sub-branch has a per-branch LIMIT/SKIP, wrap it in a subquery so
            # the limit applies only to that branch, not the whole union.
            if sub_ast.limit is not None or sub_ast.skip is not None:
                sub_sql = exp.select(exp.Star()).from_(
                    exp.alias_(exp.Subquery(this=sub_sql), alias=f"_ub{i}")
                )
            result = exp.Union(this=result, expression=sub_sql, distinct=not is_all)
        return result

    def _apply_order_limit(
        self,
        result: "exp.Select | exp.Union",
        order_exprs: list["exp.Expression"],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "exp.Select | exp.Union":
        """Apply ORDER BY / LIMIT / OFFSET, wrapping in outer SELECT if needed."""
        has_ordering = order_exprs or self._ast.limit is not None or self._ast.skip is not None
        needs_outer = bool(self._ast.union_parts) or bool(self._extra_path_branches)
        if needs_outer and has_ordering:
            outer = exp.select(exp.Star()).from_(
                exp.alias_(exp.Subquery(this=result), alias="_union")
            )
            if order_exprs:
                outer = outer.order_by(*order_exprs)
            if self._ast.limit is not None:
                outer = outer.limit(self._ast.limit)
            if self._ast.skip is not None:
                outer = outer.offset(self._ast.skip)
            return outer
        if order_exprs:
            result = result.order_by(*order_exprs)
        if self._ast.limit is not None:
            result = result.limit(self._ast.limit)
        if self._ast.skip is not None:
            result = result.offset(self._ast.skip)
        return result

    def translate(self) -> tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if self._ast.return_clause is None:
            raise CypherTranslateError(
                "Cannot translate a CALL {}-only query directly. "
                "Use cypher_calls_to_sql_list() instead."
            )

        segments = self._group_pipeline()
        cte_defs: list[tuple[str, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        for n, (match_steps, unwinds, with_clause) in enumerate(segments[:-1]):
            assert with_clause is not None
            cte_name, stage_query = self._build_cte_segment(n, match_steps, unwinds, with_clause)
            cte_defs.append((cte_name, stage_query))
            self._update_var_table_for_with(with_clause.items, cte_name)

        final_match_steps, final_unwinds, _ = segments[-1]
        from_clause, joins, all_matches, stage_where = self._build_final_from(
            final_match_steps, final_unwinds, cte_defs
        )

        lateral_joins = self._translate_correlated_calls(self._ast.call_subqueries)
        joins = list(joins) + lateral_joins

        select_exprs = self._build_select(self._ast.return_clause)
        where_expr, joins = self._apply_where_and_fold(stage_where, all_matches, joins)

        # Short-circuit: if any resolved node's WHERE props are all absent from its schema,
        # the WHERE is always non-true → force FALSE to avoid scanning any tables.
        if self._where_is_impossible_for_resolved_nodes():
            where_expr = exp.false()
        order_exprs = self._build_order_by(self._ast.order_by)

        query = self._build_main_query(from_clause, joins, where_expr, select_exprs)
        result: exp.Select | exp.Union = self._apply_extra_branches(query, select_exprs, where_expr)

        for cte_name, cte_query in cte_defs:
            result = result.with_(cte_name, as_=cte_query)
        for cte_name, cte_expr in self._recursive_ctes:
            result = result.with_(cte_name, as_=cte_expr, recursive=True)

        # If this is the first branch of a UNION ALL and has a per-branch LIMIT/SKIP,
        # apply it to just this branch before folding the union. Without this the limit
        # would be placed on the outer wrapper and constrain the whole union result.
        if self._ast.union_parts and (self._ast.limit is not None or self._ast.skip is not None):
            if self._ast.limit is not None:
                result = result.limit(self._ast.limit)
            if self._ast.skip is not None:
                result = result.offset(self._ast.skip)
            result = exp.select(exp.Star()).from_(
                exp.alias_(exp.Subquery(this=result), alias="_ub_first")
            )
            self._ast.limit = None
            self._ast.skip = None

        result = self._fold_union_parts(result)

        if self._shortestpath_hops_col is not None:
            order_exprs = [self._shortestpath_hops_col] + list(order_exprs)
            if not self._shortestpath_is_all and self._ast.limit is None:
                self._ast.limit = 1

        result = self._apply_order_limit(result, order_exprs)
        return result, self._param_order, self._graph_vars

    def _group_pipeline(
        self,
    ) -> list[tuple[list[MatchStep], list[UnwindClause], WithClause | None]]:
        segments: list = []
        current_matches: list[MatchStep] = []
        current_unwinds: list[UnwindClause] = []
        for item in self._ast.pipeline:
            if isinstance(item, MatchStep):
                current_matches.append(item)
            elif isinstance(item, UnwindClause):
                current_unwinds.append(item)
            elif isinstance(item, WithClause):
                segments.append((list(current_matches), list(current_unwinds), item))
                current_matches = []
                current_unwinds = []
        segments.append((list(current_matches), list(current_unwinds), None))
        return segments

    def _build_unwind_expr(self, expr_text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Parse a Cypher UNWIND source expression into a SQLGlot expression.

        Kept on the text path deliberately: an UNWIND source may be a bare ``$param`` that must survive
        as the executor placeholder ``$N`` in every dialect (the AST parameter node renders ``@N`` in
        some engines). The predicate/projection/order paths use the AST lowering (REQ-913)."""
        text = expr_text.strip()
        # Cypher list literal [...] → ARRAY[...]
        if text.startswith("["):
            text = "ARRAY" + text
        text = self._rewrite_params_in_expr(text)
        text = self._rewrite_cte_vars(text)
        text = self._rewrite_cypher_props(text)
        text = _rewrite_property_access(text)
        try:
            # No dialect: $N executor placeholders must survive as identifiers.
            return sqlglot.parse_one(text)  # pyright: ignore[reportReturnType]
        except Exception as exc:
            # Treating an unparseable UNWIND source as a bare column emits wrong SQL — fail loud.
            raise ValueError(f"Cannot parse UNWIND source expression {text!r}") from exc

    def _build_unwind_joins(
        self,
        unwinds: list[UnwindClause],
        has_from: bool,
    ) -> tuple[exp.Expression | None, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM/CROSS JOIN sources for UNWIND clauses.

        If has_from is False, the first UNWIND becomes the FROM expression.
        Returns (from_expr_or_None, [cross_join_dicts]).
        """
        from_expr: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        cross_joins: list[dict] = []
        for uw in unwinds:
            alias = f"_uw{self._unwind_count}"
            self._unwind_count += 1
            array_expr = self._build_unwind_expr(uw.expression)
            # Detect MAP-element arrays: either literal collect({...}) or a variable
            # assigned from collect({...}) in a preceding WITH clause.
            raw_rewritten = rewrite_bare_map_literals(uw.expression)
            if "MAP(ARRAY[" in raw_rewritten or uw.expression.strip() in self._map_array_vars:
                self._map_unwind_vars.add(uw.variable)
            unnest = exp.Unnest(
                expressions=[array_expr],
                alias=exp.TableAlias(
                    this=exp.Identifier(this=alias),
                    columns=[exp.Identifier(this=uw.variable)],
                ),
            )
            self._var_table[uw.variable] = (alias, None)
            if not has_from and from_expr is None:
                from_expr = unnest
                has_from = True
            else:
                cross_joins.append({"table": unnest, "on": None, "join_type": "CROSS"})
        return from_expr, cross_joins

    def _register_node(self, node: "NodePattern") -> None:
        """Register a single node into var_table and domain_nodes if not already present."""
        if not node.variable or node.variable in self._var_table:
            return
        if node.label_alternation and len(node.labels) > 1:
            ad_hoc = f"__alt_{node.variable}__"
            if ad_hoc not in self._lm.domains:
                from provisa.cypher.label_map import CypherLabelMap

                self._lm = CypherLabelMap(
                    nodes=self._lm.nodes,
                    relationships=self._lm.relationships,
                    domains={**self._lm.domains, ad_hoc: node.labels},
                )
            self._domain_nodes[node.variable] = ad_hoc
            self._var_table[node.variable] = (node.variable, None)
        elif node.labels:
            type_label, domain_label = self._resolve_node_type(node.labels)
            if type_label:
                self._var_table[node.variable] = (node.variable, self._lm.nodes[type_label])
            else:
                self._domain_nodes[node.variable] = domain_label  # type: ignore[assignment]
                self._var_table[node.variable] = (node.variable, None)
        else:
            self._domain_nodes[node.variable] = "__all__"
            self._var_table[node.variable] = (node.variable, None)

    def _build_first_node_from(self, first_node: "NodePattern") -> "exp.Expression | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM expr for the first node; None if lateral-bound or not resolvable."""
        fv = first_node.variable
        if fv and fv in self._lateral_bound:
            return None
        if fv and fv in self._cte_sources:
            sql_alias = self._var_table[fv][0]
            return exp.Table(this=exp.Identifier(this=sql_alias))
        if fv and fv in self._domain_nodes:
            return self._build_domain_union(fv, self._domain_nodes[fv])
        if fv and fv in self._var_table and self._var_table[fv][1]:
            nm = self._var_table[fv][1]
            assert nm is not None
            if getattr(nm, "traversal_only", False):
                raise CypherTranslateError(
                    f"Node '{nm.label}' is in a domain outside your access. "
                    f"Start the pattern from a node in your own domain and traverse to '{nm.label}' via a relationship."
                )
            return _node_table_expr(nm, fv)
        if first_node.labels:
            type_label, _ = self._resolve_node_type(first_node.labels)
            nm = self._lm.nodes.get(type_label) if type_label else None
            if nm:
                if getattr(nm, "traversal_only", False):
                    raise CypherTranslateError(
                        f"Node '{nm.label}' is in a domain outside your access. "
                        f"Start the pattern from a node in your own domain and traverse to '{nm.label}' via a relationship."
                    )
                assert type_label is not None
                alias = fv or nm.table_name
                return _node_table_expr(nm, alias)
        return None

    def _build_standalone_node_join(self, fv: "str | None", clause: MatchClause) -> "dict | None":
        """Build a JOIN dict for a standalone node (no relationships) in a non-first clause."""
        if not fv or fv in self._cte_sources:
            return None
        join_type = "LEFT" if clause.optional else "CROSS"
        on_clause = exp.true() if join_type == "LEFT" else None
        if fv in self._domain_nodes:
            join_table = self._build_domain_union(fv, self._domain_nodes[fv])
            return {"table": join_table, "on": on_clause, "join_type": join_type}
        if fv in self._var_table and self._var_table[fv][1]:
            nm = self._var_table[fv][1]
            assert nm is not None
            join_table = _node_table_expr(nm, fv)
            return {"table": join_table, "on": on_clause, "join_type": join_type}
        return None

    def _rewrite_cypher_props(self, text: str) -> str:
        """Rewrite var.camelProp → var.sql_alias using NodeMapping.properties."""

        def _replace(m: re.Match) -> str:
            var, prop = m.group(1), m.group(2)
            if var in self._all_rels_rel_vars:
                return f"JSON_EXTRACT_SCALAR({var}, '$.{prop}')"
            if var in self._all_rels_node_vars:
                return f"JSON_EXTRACT_SCALAR({var}, '$.properties.{prop}')"
            if var in self._map_unwind_vars:
                return f"JSON_EXTRACT_SCALAR(CAST(element_at({var}, '{prop}') AS JSON), '$')"
            info = self._var_table.get(var)
            if info and info[1]:
                sql_alias = info[1].properties.get(prop)
                if sql_alias:
                    return f"{var}.{sql_alias}"
            return m.group(0)

        return re.sub(r"\b([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)\b", _replace, text)

    def _lower_expr(self, text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Lower a Cypher expression to sqlglot via the AST path (REQ-913): parse to a CypherExpr and
        lower it node-to-node against the translator's traversal state — no text rewriting. Raises on a
        construct the grammar/context cannot handle, so a gap surfaces loudly instead of degrading."""
        from provisa.cypher.expr_context import TranslatorExprContext
        from provisa.cypher.expr_parser import parse_expression
        from provisa.cypher.expr_visitor import ExprLowering

        node = ExprLowering(TranslatorExprContext(self)).lower(parse_expression(text))
        return node.transform(_rewrite_cypher_fn_node)

    def _build_where(self, where: WhereClause | None) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if where is None:
            return None
        return self._lower_expr(where.expression)

    def _parse_expr(self, text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Parse a Cypher expression fragment into a SQLGlot expression via the AST path (REQ-913)."""
        return self._lower_expr(text)

    def _build_order_by(self, order_by: list[OrderItem]) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for item in order_by:
            inner = self._parse_expr(item.expression)
            if item.direction == "DESC":
                exprs.append(exp.Ordered(this=inner, desc=True))
            else:
                exprs.append(exp.Ordered(this=inner, desc=False))
        return exprs

    def _rewrite_cte_vars(self, text: str) -> str:
        for var in self._cte_sources:
            sql_alias = self._var_table.get(var, (var, None))[0]
            if sql_alias != var:
                text = re.sub(rf"\b{re.escape(var)}\.", f"{sql_alias}.", text)
        return text

    def _build_with_select_items(self, items: list[ReturnItem]) -> list[exp.Expr]:
        exprs: list[exp.Expr] = []
        for item in items:
            expr_text = item.expression.strip()
            alias = item.alias
            if _is_bare_variable(expr_text) and expr_text in self._var_table:
                tbl_col = exp.Column(
                    this=exp.Star(),
                    table=exp.Identifier(this=expr_text),
                )
                if alias:
                    exprs.append(exp.alias_(tbl_col, alias))
                else:
                    exprs.append(tbl_col)
            else:
                parsed = self._parse_expr(expr_text)
                if alias:
                    exprs.append(exp.alias_(parsed, alias))
                else:
                    exprs.append(parsed)
        return exprs

    def _update_var_table_for_with(self, items: list[ReturnItem], cte_name: str) -> None:
        new_var_table: dict[str, tuple[str, Any]] = {}
        for item in items:
            expr_text = item.expression.strip()
            alias = item.alias
            key = alias or (
                _safe_alias(expr_text) if not _is_bare_variable(expr_text) else expr_text
            )
            if _is_bare_variable(expr_text) and expr_text in self._var_table:
                original_meta = self._var_table[expr_text][1]
                new_var_table[key] = (cte_name, original_meta)
                if expr_text in self._map_array_vars:
                    self._map_array_vars.add(key)
            else:
                new_var_table[key] = (cte_name, None)
                # collect({...}) produces a MAP array — track the alias for UNWIND resolution
                if re.search(r"\bcollect\s*\(\s*\{", expr_text, re.IGNORECASE) and key:
                    self._map_array_vars.add(key)
        self._var_table = new_var_table
        self._cte_sources = set(new_var_table.keys())

    def _resolve_node_type(self, labels: list[str]) -> tuple[str | None, str | None]:
        """Return (type_name_key, domain_label). Raises on ambiguity.

        Handles all label combinations regardless of order:
          (n:SalesAnalytics:Orders)  — domain + table
          (n:Orders:SalesAnalytics)  — reversed (AND'd, same result)
          (n:Orders)                 — table only (unique or union)
          (n:SalesAnalytics)         — domain only (union over all tables in domain)
          (n:SalesAnalytics_Orders)  — legacy full type_name (backward compat)
        """
        # Normalize labels to canonical case before lookup
        labels = [self._lm.canonical_label(label) for label in labels]
        # Classify each label
        full_type: list[str] = [label for label in labels if label in self._lm.nodes]
        domain_hits: list[str] = [label for label in labels if label in self._lm.domains]
        table_hits: list[str] = [label for label in labels if label in self._lm.nodes_by_table]

        # Legacy: full type_name used directly (e.g. SalesAnalytics_Orders)
        if full_type:
            if len(full_type) > 1:
                raise CypherTranslateError(
                    f"Ambiguous labels — multiple full type labels: {full_type}"
                )
            return full_type[0], domain_hits[0] if domain_hits else None

        # Domain + table (any order): intersect to get unique type_name
        if domain_hits and table_hits:
            domain_set = set(self._lm.domains[domain_hits[0]])
            table_set = set(self._lm.nodes_by_table[table_hits[0]])
            candidates = domain_set & table_set
            if len(candidates) == 1:
                return candidates.pop(), domain_hits[0]
            if len(candidates) > 1:
                raise CypherTranslateError(
                    f"Ambiguous: labels {labels} match multiple types: {sorted(candidates)}"
                )
            raise CypherTranslateError(f"No node type found for labels {labels}")

        # Table only: resolve if unambiguous, otherwise build domain-style union
        if table_hits and not domain_hits:
            candidates = self._lm.nodes_by_table[table_hits[0]]
            if len(candidates) == 1:
                return candidates[0], None
            # Ambiguous table label across domains — treat as ad-hoc domain union
            ad_hoc = f"__tbl_{table_hits[0]}__"
            if ad_hoc not in self._lm.domains:
                from provisa.cypher.label_map import CypherLabelMap

                self._lm = CypherLabelMap(
                    nodes=self._lm.nodes,
                    relationships=self._lm.relationships,
                    domains={**self._lm.domains, ad_hoc: candidates},
                    nodes_by_table=self._lm.nodes_by_table,
                )
            return None, ad_hoc

        # Domain only
        if domain_hits:
            return None, domain_hits[0]

        raise CypherTranslateError(f"Unknown label(s): {labels}")

    def _collect_var_props(self, var: str) -> list[str]:
        """Return ordered list of property names referenced as var.prop in the AST.

        When the var appears bare in RETURN (e.g. ``RETURN n``) with no explicit
        property projections, all properties from the domain's node types are
        included so that the domain union subquery exposes them.
        """
        pattern = re.compile(rf"\b{re.escape(var)}\s*\.\s*([A-Za-z_]\w*)")
        texts: list[str] = []
        for step in self._ast.pipeline:
            if isinstance(step, MatchStep) and step.where:
                texts.append(step.where.expression)
            elif isinstance(step, WithClause):
                texts.extend(i.expression for i in step.items)
                if step.where:
                    texts.append(step.where.expression)
        if self._ast.return_clause:
            texts.extend(i.expression for i in self._ast.return_clause.items)
        seen: set[str] = set()
        props: list[str] = []
        for text in texts:
            for m in pattern.finditer(text):
                p = m.group(1)
                if p not in seen:
                    props.append(p)
                    seen.add(p)

        # If var is returned bare (RETURN n) and is a domain node, include all
        # domain properties so the union subquery exposes them for JSON serialization.
        if not props and var in self._domain_nodes and self._ast.return_clause:
            bare_pattern = re.compile(rf"^\s*{re.escape(var)}\s*$")
            is_bare = any(
                bare_pattern.match(item.expression) for item in self._ast.return_clause.items
            )
            if is_bare:
                domain = self._domain_nodes[var]
                type_labels = (
                    list(self._lm.nodes.keys())
                    if domain == "__all__"
                    else self._lm.domains.get(domain, [])
                )
                _reserved = {"id", "label"}
                all_props: set[str] = set()
                for label in type_labels:
                    nm = self._lm.nodes.get(label)
                    if nm:
                        all_props.update(k for k in nm.properties.keys() if k not in _reserved)
                props = sorted(all_props)

        return props

    def _collect_where_only_props(self, var: str) -> list[str]:
        """Return property names referenced as var.prop in WHERE clauses only (not RETURN/WITH)."""
        pattern = re.compile(rf"\b{re.escape(var)}\s*\.\s*([A-Za-z_]\w*)")
        seen: set[str] = set()
        props: list[str] = []
        for step in self._ast.pipeline:
            if isinstance(step, MatchStep) and step.where:
                for m in pattern.finditer(step.where.expression):
                    p = m.group(1)
                    if p not in seen:
                        props.append(p)
                        seen.add(p)
            elif isinstance(step, WithClause) and step.where:
                for m in pattern.finditer(step.where.expression):
                    p = m.group(1)
                    if p not in seen:
                        props.append(p)
                        seen.add(p)
        return props

    def _where_is_impossible_for_resolved_nodes(self) -> bool:
        """Return True if any resolved node variable's WHERE props are ALL missing from its type.

        When a property referenced in WHERE doesn't exist on the resolved node type, every
        access returns NULL.  If *all* WHERE props for a variable are missing, the WHERE
        condition on that variable is always non-true (NULL comparisons), so the result is
        zero rows regardless of query structure — we can skip the engine round-trip.

        This check is skipped for domain-union / passthrough vars because those are already
        pruned per-branch inside _build_domain_union.
        """
        for var, (_, nm) in self._var_table.items():
            if nm is None:
                continue
            if var in self._domain_nodes or var in self._passthrough_vars:
                continue
            where_props = self._collect_where_only_props(var)
            if where_props and not any(nm.properties.get(p) for p in where_props):
                return True
        return False

    def _rewrite_params_in_expr(self, text: str) -> str:
        """Replace $name with positional $N."""

        def _replace(m: re.Match) -> str:
            name = m.group(1)
            if name not in self._param_seen:
                self._param_order.append(name)
                self._param_seen.add(name)
            idx = self._param_order.index(name) + 1
            return f"${idx}"

        return re.sub(r"\$([A-Za-z_]\w*)", _replace, text)

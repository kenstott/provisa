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

from __future__ import annotations

import re
from enum import Enum
from typing import Any


def _safe_alias(expr: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", expr)

import sqlglot.expressions as exp
import sqlglot

from provisa.cypher.parser import (
    CypherAST,
    CallSubquery,
    MatchClause,
    MatchStep,
    NodePattern,
    PathPattern,
    PathFunction,
    ReturnClause,
    ReturnItem,
    UnwindClause,
    WhereClause,
    WithClause,
    OrderItem,
)
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.comprehension import rewrite_list_comprehensions
from provisa.cypher.path_functions import PathFunctionsMixin
from provisa.cypher.path_comprehension import PathComprehensionMixin
from provisa.cypher.select_builder import SelectBuilderMixin
from provisa.cypher.correlated_call import CorrelatedCallMixin
from provisa.cypher.subquery_exprs import SubqueryExprsMixin
from provisa.cypher.map_projection import MapProjectionMixin
from provisa.cypher.group_by import GroupByMixin, _has_aggregate


class GraphVarKind(str, Enum):
    NODE = "NODE"
    EDGE = "EDGE"
    PATH = "PATH"
    PASSTHROUGH = "PASSTHROUGH"  # pre-built JSON from rel/node union subquery


class CypherTranslateError(Exception):
    pass


class CypherCrossSourceError(CypherTranslateError):
    """Raised when a Cypher query spans multiple incompatible data sources."""
    pass


def cypher_to_sql(
    ast: CypherAST,
    label_map: CypherLabelMap,
    params: dict[str, Any],
) -> tuple[exp.Select | exp.Union, list[str], dict[str, GraphVarKind]]:
    """Translate CypherAST to SQLGlot Select.

    Returns (sql_ast, ordered_param_names, graph_vars).
    """
    translator = _Translator(ast, label_map, params)
    return translator.translate()


def cypher_calls_to_sql_list(
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


class _Translator(PathFunctionsMixin, PathComprehensionMixin, SelectBuilderMixin, CorrelatedCallMixin, SubqueryExprsMixin, MapProjectionMixin, GroupByMixin):
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
        # extra (from, joins) branches from multi-path shortestPath/allShortestPaths
        self._extra_path_branches: list[tuple[exp.Expression, list[dict]]] = []
        # WITH RECURSIVE CTEs for self-referential variable-length paths
        self._recursive_ctes: list[tuple[str, exp.Expression]] = []
        # Set by PathFunctionsMixin when a recursive shortestPath is emitted
        self._shortestpath_hops_col: exp.Expression | None = None
        self._shortestpath_is_all: bool = False
        # Counter for unique UNNEST alias names across the translation
        self._unwind_count: int = 0
        # path_var → (src_var, tgt_var, is_recursive) for RETURN p support
        self._path_vars: dict[str, tuple[str, str, bool]] = {}
        # vars from outer scope bound via CALL { WITH x ... } — skip as FROM source
        self._lateral_bound: set[str] = set()
        # ON conditions from lateral-bound first-node relationships → added as WHERE
        self._lateral_conditions: list[exp.Expression] = []
        # relationship variable → resolved rel_type string (for type(r) resolution)
        self._rel_var_types: dict[str, str] = {}
        # relationship variable → (src_alias, src_nm, tgt_alias, tgt_nm)
        self._rel_var_endpoints: dict[str, tuple[str, "NodeMapping", str, "NodeMapping"]] = {}
        # vars that are pre-built JSON from an all-rels union subquery
        self._passthrough_vars: set[str] = set()
        # alias of the all-rels union subquery (when built)
        self._all_rels_alias: str | None = None

    def translate(self) -> tuple[exp.Select, list[str], dict[str, GraphVarKind]]:
        if self._ast.return_clause is None:
            raise CypherTranslateError(
                "Cannot translate a CALL {}-only query directly. "
                "Use cypher_calls_to_sql_list() instead."
            )

        segments = self._group_pipeline()
        cte_defs: list[tuple[str, exp.Expression]] = []

        # Build CTEs for all segments except the last
        for n, (match_steps, unwinds, with_clause) in enumerate(segments[:-1]):
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
            else:
                raise CypherTranslateError("Pipeline segment has no data source")
            select_exprs = self._build_with_select_items(with_clause.items)
            where_expr = self._build_where(stage_where)

            stage_query = exp.select(*select_exprs).from_(from_clause)
            for join in joins:
                stage_query = stage_query.join(join["table"], on=join["on"], join_type=join["join_type"])
            if where_expr:
                stage_query = stage_query.where(where_expr)
            with_group_exprs = self._build_group_by_for_with(with_clause.items)
            if with_group_exprs:
                stage_query = stage_query.group_by(*with_group_exprs)

            # Apply WITH ... WHERE as outer filter
            if with_clause.where is not None:
                with_where_expr = self._build_where(with_clause.where)
                if with_where_expr:
                    stage_query = exp.select(exp.Star()).from_(
                        exp.alias_(exp.Subquery(this=stage_query), alias="_inner")
                    ).where(with_where_expr)

            cte_name = f"_w{n}"
            cte_defs.append((cte_name, stage_query))
            self._update_var_table_for_with(with_clause.items, cte_name)

        # Build main SELECT from final segment
        final_match_steps, final_unwinds, _ = segments[-1]
        all_matches = [m for step in final_match_steps for m in step.matches]
        stage_where = None
        for step in final_match_steps:
            if step.where is not None:
                stage_where = step.where
                break

        if not all_matches and not final_unwinds and cte_defs:
            # No MATCH, no UNWIND — SELECT directly from last CTE
            last_cte_name = cte_defs[-1][0]
            from_clause: exp.Expression = exp.Table(this=exp.Identifier(this=last_cte_name))
            joins: list[dict] = []
        elif not all_matches and final_unwinds:
            # Pure UNWIND — first may become FROM, rest CROSS JOINs
            if cte_defs:
                last_cte_name = cte_defs[-1][0]
                from_clause = exp.Table(this=exp.Identifier(this=last_cte_name))
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(uw_joins)
            else:
                uw_from, uw_joins = self._build_unwind_joins(final_unwinds, has_from=False)
                from_clause = uw_from
                joins = list(uw_joins)
        elif all_matches:
            from_clause, joins = self._build_from_joins(all_matches)
            if final_unwinds:
                _, uw_joins = self._build_unwind_joins(final_unwinds, has_from=True)
                joins = list(joins) + uw_joins
        else:
            raise CypherTranslateError("Query has no data source")

        # Correlated CALL subqueries → CROSS JOIN LATERAL
        lateral_joins = self._translate_correlated_calls(self._ast.call_subqueries)
        joins = list(joins) + lateral_joins

        select_exprs = self._build_select(self._ast.return_clause)
        where_expr = self._build_where(stage_where)
        order_exprs = self._build_order_by(self._ast.order_by)

        query = exp.select(*select_exprs).from_(from_clause)
        if self._ast.return_clause and self._ast.return_clause.distinct:
            query = query.distinct()
        for join in joins:
            query = query.join(join["table"], on=join["on"], join_type=join["join_type"])
        if where_expr:
            query = query.where(where_expr)
        for lat_cond in self._lateral_conditions:
            query = query.where(lat_cond)
        group_exprs = self._build_group_by(self._ast.return_clause)
        if group_exprs:
            query = query.group_by(*group_exprs)

        # UNION ALL extra branches from multi-path shortestPath/allShortestPaths.
        # Each schema path is its own SQL query; WHERE/SELECT are identical across branches.
        result: exp.Select | exp.Union = query
        for extra_from, extra_joins in self._extra_path_branches:
            branch = exp.select(*select_exprs).from_(extra_from)
            if self._ast.return_clause and self._ast.return_clause.distinct:
                branch = branch.distinct()
            for j in extra_joins:
                branch = branch.join(j["table"], on=j["on"], join_type=j["join_type"])
            if where_expr:
                branch = branch.where(where_expr)
            result = exp.Union(this=result, expression=branch, distinct=False)
        for cte_name, cte_query in cte_defs:
            result = result.with_(cte_name, as_=cte_query)
        for cte_name, cte_expr in self._recursive_ctes:
            result = result.with_(cte_name, as_=cte_expr, recursive=True)

        # Fold UNION / UNION ALL parts (ORDER BY/LIMIT/OFFSET applied after)
        for sub_ast, is_all in self._ast.union_parts:
            sub_sql, sub_params, sub_graph_vars = cypher_to_sql(sub_ast, self._lm, self._params)
            for p in sub_params:
                if p not in self._param_seen:
                    self._param_order.append(p)
                    self._param_seen.add(p)
            self._graph_vars.update(sub_graph_vars)
            result = exp.Union(
                this=result,
                expression=sub_sql,
                distinct=not is_all,
            )

        # For recursive shortestPath: inject ORDER BY _t.hops [LIMIT 1]
        if self._shortestpath_hops_col is not None:
            order_exprs = [self._shortestpath_hops_col] + list(order_exprs)
            if not self._shortestpath_is_all and self._ast.limit is None:
                self._ast.limit = 1

        # Apply ORDER BY / LIMIT / OFFSET.
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
            return outer, self._param_order, self._graph_vars

        if order_exprs:
            result = result.order_by(*order_exprs)
        if self._ast.limit is not None:
            result = result.limit(self._ast.limit)
        if self._ast.skip is not None:
            result = result.offset(self._ast.skip)

        return result, self._param_order, self._graph_vars

    def _group_pipeline(self) -> list[tuple[list[MatchStep], list[UnwindClause], WithClause | None]]:
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

    def _build_unwind_expr(self, expr_text: str) -> exp.Expression:
        """Parse a Cypher UNWIND source expression into a SQLGlot expression."""
        text = expr_text.strip()
        # Cypher list literal [...] → ARRAY[...]
        if text.startswith("["):
            text = "ARRAY" + text
        text = self._rewrite_params_in_expr(text)
        text = self._rewrite_cte_vars(text)
        text = _rewrite_property_access(text)
        try:
            return sqlglot.parse_one(text, dialect="trino")
        except Exception:
            return exp.column(text)

    def _build_unwind_joins(
        self,
        unwinds: list[UnwindClause],
        has_from: bool,
    ) -> tuple[exp.Expression | None, list[dict]]:
        """Build FROM/CROSS JOIN sources for UNWIND clauses.

        If has_from is False, the first UNWIND becomes the FROM expression.
        Returns (from_expr_or_None, [cross_join_dicts]).
        """
        from_expr: exp.Expression | None = None
        cross_joins: list[dict] = []
        for uw in unwinds:
            alias = f"_uw{self._unwind_count}"
            self._unwind_count += 1
            array_expr = self._build_unwind_expr(uw.expression)
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

    def _build_from_joins(self, match_clauses: list[MatchClause]) -> tuple[exp.Expression, list[dict]]:
        """Process MATCH clauses → (from_expr, [join_dict])."""
        from_expr: exp.Expression | None = None
        joins: list[dict] = []

        for clause in match_clauses:
            if isinstance(clause.pattern, PathFunction):
                pf_from, pf_joins = self._translate_path_function(clause)
                if from_expr is None:
                    from_expr = pf_from
                joins.extend(pf_joins)
                continue

            pattern = clause.pattern
            nodes = pattern.nodes
            rels = pattern.rels

            # Register all nodes
            for node in nodes:
                if node.variable and node.variable not in self._var_table:
                    if node.label_alternation and len(node.labels) > 1:
                        # Cypher 5 (n:A|B) — build ad-hoc domain for alternation
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
                            # domain-only: var_table entry has no NodeMapping
                            self._domain_nodes[node.variable] = domain_label  # type: ignore[assignment]
                            self._var_table[node.variable] = (node.variable, None)
                    else:
                        # No labels — UNION ALL of every known type (same mechanism as domain)
                        self._domain_nodes[node.variable] = "__all__"
                        self._var_table[node.variable] = (node.variable, None)

            # First node → FROM (skip if lateral-bound — FROM comes from tgt instead)
            if from_expr is None and nodes:
                first_node = nodes[0]
                fv = first_node.variable
                if fv and fv in self._lateral_bound:
                    pass  # lateral-bound: FROM will be set when processing the first rel's tgt
                elif fv and fv in self._cte_sources:
                    sql_alias = self._var_table[fv][0]
                    from_expr = exp.Table(this=exp.Identifier(this=sql_alias))
                elif fv and fv in self._domain_nodes:
                    from_expr = self._build_domain_union(fv, self._domain_nodes[fv])
                elif fv and fv in self._var_table and self._var_table[fv][1]:
                    nm = self._var_table[fv][1]
                    from_expr = exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                        ),
                        alias=fv,
                    )
                elif first_node.labels:
                    type_label, _ = self._resolve_node_type(first_node.labels)
                    nm = self._lm.nodes.get(type_label) if type_label else None
                    if nm:
                        alias = fv or type_label.lower()
                        from_expr = exp.alias_(
                            exp.Table(
                                this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                                db=exp.Identifier(this=nm.schema_name, quoted=True),
                                catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                            ),
                            alias=alias,
                        )
            elif from_expr is not None and nodes and not rels:
                # Standalone node in a non-first clause (no relationships) → JOIN
                # This handles: OPTIONAL MATCH (x) or MATCH (x) with no connecting pattern
                first_node = nodes[0]
                fv = first_node.variable
                if fv and fv not in self._cte_sources and fv in self._domain_nodes:
                    join_type = "LEFT" if clause.optional else "CROSS"
                    join_table = self._build_domain_union(fv, self._domain_nodes[fv])
                    joins.append({"table": join_table, "on": None, "join_type": join_type})
                elif fv and fv not in self._cte_sources and fv in self._var_table and self._var_table[fv][1]:
                    nm = self._var_table[fv][1]
                    join_type = "LEFT" if clause.optional else "CROSS"
                    join_table = exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                        ),
                        alias=fv,
                    )
                    joins.append({"table": join_table, "on": None, "join_type": join_type})

            # Process relationships → JOINs
            for i, rel in enumerate(rels):
                if i + 1 >= len(nodes):
                    break
                src_node = nodes[i]
                tgt_node = nodes[i + 1]
                src_var = src_node.variable
                tgt_var = tgt_node.variable

                src_nm = self._var_table.get(src_var, (None, None))[1] if src_var else None
                tgt_nm = self._var_table.get(tgt_var, (None, None))[1] if tgt_var else None

                if tgt_nm is None and tgt_node.labels:
                    type_label, _ = self._resolve_node_type(tgt_node.labels)
                    tgt_nm = self._lm.nodes.get(type_label) if type_label else None
                    if tgt_nm and tgt_var:
                        self._var_table[tgt_var] = (tgt_var, tgt_nm)

                # Early rel_mapping for domain-node path and anonymous node inference
                rel_mapping = None
                if rel.types:
                    _rt_early = rel.types[0].upper()
                    _early_matches = self._lm.aliases.get(_rt_early, [])
                    if not _early_matches:
                        _rm_early = self._lm.relationships.get(_rt_early)
                        _early_matches = [_rm_early] if _rm_early else []
                    rel_mapping = _early_matches[0] if _early_matches else None

                # Anonymous nodes with no variable/labels: infer from relationship type
                if (src_nm is None or tgt_nm is None) and rel_mapping:
                    if src_nm is None:
                        src_nm = self._lm.nodes.get(rel_mapping.source_label)
                        if src_nm and src_var:
                            self._var_table[src_var] = (src_var, src_nm)
                            # If this var was registered as a domain/all union, replace
                            # from_expr with the concrete table now that type is known
                            if src_var in self._domain_nodes:
                                self._domain_nodes.pop(src_var)
                                from_expr = exp.alias_(
                                    exp.Table(
                                        this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                                        db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                                        catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                                    ),
                                    alias=src_var,
                                )
                    if tgt_nm is None:
                        tgt_nm = self._lm.nodes.get(rel_mapping.target_label)
                        if tgt_nm and tgt_var:
                            self._var_table[tgt_var] = (tgt_var, tgt_nm)
                            if tgt_var in self._domain_nodes:
                                self._domain_nodes.pop(tgt_var)

                # Set FROM from anonymous src if still unset
                if from_expr is None and src_nm is not None:
                    src_alias = src_var or src_nm.table_name
                    from_expr = exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                        ),
                        alias=src_alias,
                    )

                if src_nm is None or tgt_nm is None:
                    # domain-only JOIN target: use subquery
                    if tgt_var and tgt_var in self._domain_nodes and rel_mapping is not None:
                        join_type = "LEFT" if clause.optional else "INNER"
                        tgt_alias = tgt_var
                        join_table = self._build_domain_union(tgt_var, self._domain_nodes[tgt_var])
                        src_table_ref = self._var_table.get(src_var, (src_var, None))[0] if src_var else src_nm.table_name
                        on_cond = exp.EQ(
                            this=exp.Column(
                                this=exp.Identifier(this=rel_mapping.join_source_column, quoted=True),
                                table=exp.Identifier(this=src_table_ref),
                            ),
                            expression=exp.Column(
                                this=exp.Identifier(this=rel_mapping.join_target_column, quoted=True),
                                table=exp.Identifier(this=tgt_alias),
                            ),
                        )
                        joins.append({"table": join_table, "on": on_cond, "join_type": join_type})
                    elif rel_mapping is None:
                        # Fully unlabeled pattern: UNION ALL over all relationship types
                        from_expr = self._build_all_rels_union(src_var, rel.variable, tgt_var)
                        if src_var:
                            self._domain_nodes.pop(src_var, None)
                            self._var_table[src_var] = (src_var, None)
                            self._passthrough_vars.add(src_var)
                        if rel.variable:
                            self._passthrough_vars.add(rel.variable)
                        if tgt_var:
                            self._domain_nodes.pop(tgt_var, None)
                            self._var_table[tgt_var] = (tgt_var, None)
                            self._passthrough_vars.add(tgt_var)
                    continue

                # Find matching relationship(s)
                # direction=="none" → bidirectional: expand to UNION ALL of all
                # forward and backward directed relationships from the semantic layer.
                bidir = rel.direction == "none"
                backward = rel.direction == "left"

                if rel.types:
                    rel_type = rel.types[0].upper()
                    # Phase 1: exact rel_type match
                    # Phase 2: alias index (supports UNION fan-out for shared aliases)
                    alias_matches = self._lm.aliases.get(rel_type, [])
                    if not alias_matches:
                        rm = self._lm.relationships.get(rel_type)
                        alias_matches = [rm] if rm else []
                    if not alias_matches:
                        raise CypherTranslateError(
                            f"Unknown relationship type or alias: {rel_type!r}"
                        )
                    candidates: list[tuple] = [(m, backward) for m in alias_matches]
                else:
                    if bidir:
                        fwd = self._lm.relationships_for(src_nm.type_name, tgt_nm.type_name)
                        bwd = self._lm.relationships_for(tgt_nm.type_name, src_nm.type_name)
                        candidates = [(m, False) for m in fwd] + [(m, True) for m in bwd]
                    elif backward:
                        fwd_cands = self._lm.relationships_for(tgt_nm.type_name, src_nm.type_name)
                        candidates = [(m, True) for m in fwd_cands]
                    else:
                        fwd_cands = self._lm.relationships_for(src_nm.type_name, tgt_nm.type_name)
                        candidates = [(m, False) for m in fwd_cands]

                if not candidates:
                    continue

                join_type = "LEFT" if clause.optional else "INNER"
                tgt_alias = tgt_var or tgt_nm.table_name
                if src_var and src_var in self._cte_sources:
                    src_table_ref = self._var_table.get(src_var, (src_var, None))[0]
                else:
                    src_table_ref = src_var or src_nm.table_name

                def _make_rel_join(rm, is_bwd: bool) -> dict:
                    jt = exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                        ),
                        alias=tgt_alias,
                    )
                    if is_bwd:
                        cond = exp.EQ(
                            this=exp.Column(
                                this=exp.Identifier(this=rm.join_source_column, quoted=True),
                                table=exp.Identifier(this=tgt_alias),
                            ),
                            expression=exp.Column(
                                this=exp.Identifier(this=rm.join_target_column, quoted=True),
                                table=exp.Identifier(this=src_table_ref),
                            ),
                        )
                    else:
                        cond = exp.EQ(
                            this=exp.Column(
                                this=exp.Identifier(this=rm.join_source_column, quoted=True),
                                table=exp.Identifier(this=src_table_ref),
                            ),
                            expression=exp.Column(
                                this=exp.Identifier(this=rm.join_target_column, quoted=True),
                                table=exp.Identifier(this=tgt_alias),
                            ),
                        )
                    return {"table": jt, "on": cond, "join_type": join_type}

                # Primary candidate → main join; extra candidates → UNION ALL branches
                primary_rm, primary_bwd = candidates[0]
                if rel.variable:
                    self._rel_var_types[rel.variable] = primary_rm.rel_type
                    _src_alias = src_var or (src_nm.table_name if src_nm else None)
                    _tgt_alias = tgt_var or (tgt_nm.table_name if tgt_nm else None)
                    if _src_alias and _tgt_alias and src_nm and tgt_nm:
                        self._rel_var_endpoints[rel.variable] = (_src_alias, src_nm, _tgt_alias, tgt_nm)

                primary_join = _make_rel_join(primary_rm, primary_bwd)

                # Snapshot joins BEFORE adding primary (for extra branch construction)
                joins_before = list(joins)

                # Lateral-bound src: tgt becomes FROM; condition becomes WHERE predicate
                if src_var and src_var in self._lateral_bound and from_expr is None:
                    from_expr = primary_join["table"]
                    self._lateral_conditions.append(primary_join["on"])
                else:
                    joins.append(primary_join)

                # Bidirectional extra candidates → independent UNION ALL branches
                for extra_rm, extra_bwd in candidates[1:]:
                    extra_join = _make_rel_join(extra_rm, extra_bwd)
                    self._extra_path_branches.append(
                        (from_expr, joins_before + [extra_join])
                    )

            # Register path variable (e.g. MATCH p = ()-[r:REL]->())
            if clause.variable and nodes:
                _first = nodes[0]
                _last = nodes[-1]
                if _first.variable:
                    _path_src_alias = _first.variable
                elif rels and rel_mapping is not None:
                    _path_src_alias = rel_mapping.source_label.lower()
                else:
                    _path_src_alias = ""
                if _last.variable:
                    _path_tgt_alias = _last.variable
                elif rels and tgt_nm is not None:
                    _path_tgt_alias = tgt_nm.table_name
                else:
                    _path_tgt_alias = ""
                self._path_vars[clause.variable] = (_path_src_alias, _path_tgt_alias, False)

        if from_expr is None:
            raise CypherTranslateError("No MATCH clause produced a FROM table")

        return from_expr, joins


    def _build_where(self, where: WhereClause | None) -> exp.Expression | None:
        if where is None:
            return None
        expr_text = self._rewrite_params_in_expr(where.expression)
        expr_text = self._rewrite_cte_vars(expr_text)
        expr_text = self._rewrite_map_projections(expr_text)
        expr_text = self._rewrite_graph_fns(expr_text)
        expr_text = self._rewrite_path_comprehensions(expr_text)
        expr_text = rewrite_list_comprehensions(expr_text)
        expr_text = _rewrite_in_list(expr_text)
        expr_text = _rewrite_property_access(expr_text)
        expr_text = _rewrite_string_predicates(expr_text)
        expr_text = _coerce_ts_literals(expr_text)
        expr_text = self._rewrite_subquery_exprs(expr_text)
        try:
            parsed = sqlglot.parse_one(expr_text, dialect="trino")
            return parsed.transform(_rewrite_cypher_fn_node)
        except Exception:
            return exp.condition(expr_text)

    def _build_order_by(self, order_by: list[OrderItem]) -> list[exp.Expression]:
        exprs: list[exp.Expression] = []
        for item in order_by:
            inner = self._parse_expr(item.expression)
            if item.direction == "DESC":
                exprs.append(exp.Ordered(this=inner, desc=True))
            else:
                exprs.append(exp.Ordered(this=inner, desc=False))
        return exprs

    def _rewrite_graph_fns(self, text: str) -> str:
        """Rewrite graph-aware functions using var_table context."""
        def _id_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            info = self._var_table.get(var)
            if info and info[1]:
                return f'{var}."{info[1].id_column}"'
            return m.group(0)

        def _labels_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            info = self._var_table.get(var)
            if info and info[1]:
                return f"ARRAY['{info[1].label}']"
            return m.group(0)

        def _keys_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            info = self._var_table.get(var)
            if info and info[1]:
                keys = ", ".join(f"'{k}'" for k in sorted(info[1].properties.keys()))
                return f"ARRAY[{keys}]"
            return m.group(0)

        def _type_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            rel_type = self._rel_var_types.get(var)
            if rel_type is not None:
                return f"'{rel_type}'"
            return m.group(0)

        # type(r) → 'REL_TYPE' literal (resolved at compile time from semantic layer)
        text = re.sub(r'\btype\s*\(\s*([A-Za-z_]\w*)\s*\)', _type_repl, text, flags=re.IGNORECASE)
        # exists(n.prop) → (n.prop) IS NOT NULL
        text = re.sub(r'\bexists\s*\(([^()]+)\)', r'(\1) IS NOT NULL', text, flags=re.IGNORECASE)
        # id(var) → var."id_col"
        text = re.sub(r'\bid\s*\(\s*([A-Za-z_]\w*)\s*\)', _id_repl, text)
        # labels(var) → ARRAY['Label']
        text = re.sub(r'\blabels\s*\(\s*([A-Za-z_]\w*)\s*\)', _labels_repl, text, flags=re.IGNORECASE)
        # keys(var) → ARRAY['prop1', ...]
        text = re.sub(r'\bkeys\s*\(\s*([A-Za-z_]\w*)\s*\)', _keys_repl, text, flags=re.IGNORECASE)
        # length(p) for recursive CTE paths → _t.hops; for flat paths → 1
        if self._shortestpath_hops_col is not None:
            text = re.sub(r'\blength\s*\(\s*[A-Za-z_]\w*\s*\)', '_t.hops', text, flags=re.IGNORECASE)
        else:
            # flat path: length is always 1 (single hop or variable-length flat join)
            text = re.sub(r'\blength\s*\(\s*[A-Za-z_]\w*\s*\)', '1', text, flags=re.IGNORECASE)
        return text

    def _parse_expr(self, text: str) -> exp.Expression:
        """Parse a Cypher expression fragment into a SQLGlot expression."""
        text = self._rewrite_params_in_expr(text)
        text = self._rewrite_cte_vars(text)
        text = self._rewrite_map_projections(text)
        text = self._rewrite_graph_fns(text)
        text = self._rewrite_path_comprehensions(text)
        text = rewrite_list_comprehensions(text)
        text = _rewrite_in_list(text)
        text = _rewrite_property_access(text)
        text = _rewrite_string_predicates(text)
        text = self._rewrite_subquery_exprs(text)
        try:
            parsed = sqlglot.parse_one(text, dialect="trino")
            return parsed.transform(_rewrite_cypher_fn_node)
        except Exception:
            return exp.column(text)

    def _rewrite_cte_vars(self, text: str) -> str:
        for var in self._cte_sources:
            sql_alias = self._var_table.get(var, (var, None))[0]
            if sql_alias != var:
                text = re.sub(rf'\b{re.escape(var)}\.', f'{sql_alias}.', text)
        return text

    def _build_with_select_items(self, items: list[ReturnItem]) -> list[exp.Expression]:
        exprs: list[exp.Expression] = []
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
            key = alias or (_safe_alias(expr_text) if not _is_bare_variable(expr_text) else expr_text)
            if _is_bare_variable(expr_text) and expr_text in self._var_table:
                original_meta = self._var_table[expr_text][1]
                new_var_table[key] = (cte_name, original_meta)
            else:
                new_var_table[key] = (cte_name, None)
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
        # Classify each label
        full_type: list[str] = [l for l in labels if l in self._lm.nodes]
        domain_hits: list[str] = [l for l in labels if l in self._lm.domains]
        table_hits: list[str] = [l for l in labels if l in self._lm.nodes_by_table]

        # Legacy: full type_name used directly (e.g. SalesAnalytics_Orders)
        if full_type:
            if len(full_type) > 1:
                raise CypherTranslateError(f"Ambiguous labels — multiple full type labels: {full_type}")
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
            raise CypherTranslateError(
                f"No node type found for labels {labels}"
            )

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
        pattern = re.compile(rf'\b{re.escape(var)}\s*\.\s*([A-Za-z_]\w*)')
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
            bare_pattern = re.compile(rf'^\s*{re.escape(var)}\s*$')
            is_bare = any(
                bare_pattern.match(item.expression)
                for item in self._ast.return_clause.items
            )
            if is_bare:
                domain = self._domain_nodes[var]
                type_labels = (
                    list(self._lm.nodes.keys())
                    if domain == "__all__"
                    else self._lm.domains.get(domain, [])
                )
                all_props: set[str] = set()
                for label in type_labels:
                    nm = self._lm.nodes.get(label)
                    if nm:
                        all_props.update(nm.properties.keys())
                props = sorted(all_props)

        return props

    def _build_all_rels_union(
        self,
        src_var: str | None,
        rel_var: str | None,
        tgt_var: str | None,
    ) -> exp.Expression:
        """Build UNION ALL subquery over all relationship types for fully-unlabeled patterns."""
        src_col = src_var or "n"
        rel_col = rel_var or "r"
        tgt_col = tgt_var or "m"
        alias = "_all_rels"
        self._all_rels_alias = alias

        branches: list[exp.Select] = []
        for rm in self._lm.relationships.values():
            src_nm = self._lm.nodes.get(rm.source_label)
            tgt_nm = self._lm.nodes.get(rm.target_label)
            if src_nm is None or tgt_nm is None:
                continue

            sa = f"_s_{rm.rel_type.lower()[:20]}"
            ta = f"_t_{rm.rel_type.lower()[:20]}"

            src_id_col = exp.Column(
                this=exp.Identifier(this=src_nm.id_column, quoted=True),
                table=exp.Identifier(this=sa),
            )
            tgt_id_col = exp.Column(
                this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                table=exp.Identifier(this=ta),
            )
            src_json = exp.JSONObject(expressions=[
                exp.JSONKeyValue(this=exp.Literal.string("id"), expression=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR"))),
                exp.JSONKeyValue(this=exp.Literal.string("label"), expression=exp.Literal.string(src_nm.label)),
            ])
            tgt_json = exp.JSONObject(expressions=[
                exp.JSONKeyValue(this=exp.Literal.string("id"), expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR"))),
                exp.JSONKeyValue(this=exp.Literal.string("label"), expression=exp.Literal.string(tgt_nm.label)),
            ])
            edge_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
                    expression=exp.Literal.string("-"),
                ),
                expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
            )
            edge_json = exp.JSONObject(expressions=[
                exp.JSONKeyValue(this=exp.Literal.string("id"), expression=edge_id),
                exp.JSONKeyValue(this=exp.Literal.string("type"), expression=exp.Literal.string(rm.rel_type)),
                exp.JSONKeyValue(this=exp.Literal.string("startNode"), expression=src_json),
                exp.JSONKeyValue(this=exp.Literal.string("endNode"), expression=tgt_json),
            ])

            branch = exp.select(
                exp.alias_(src_json, src_col),
                exp.alias_(edge_json, rel_col),
                exp.alias_(tgt_json, tgt_col),
            ).from_(
                exp.alias_(
                    exp.Table(
                        this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                    ),
                    alias=sa,
                )
            ).join(
                exp.alias_(
                    exp.Table(
                        this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                    ),
                    alias=ta,
                ),
                on=exp.EQ(
                    this=exp.Column(
                        this=exp.Identifier(this=rm.join_source_column, quoted=True),
                        table=exp.Identifier(this=sa),
                    ),
                    expression=exp.Column(
                        this=exp.Identifier(this=rm.join_target_column, quoted=True),
                        table=exp.Identifier(this=ta),
                    ),
                ),
                join_type="INNER",
            )
            branches.append(branch)

        if not branches:
            raise CypherTranslateError("No relationship types found in schema")

        union: exp.Expression = branches[0]
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=alias)

    def _build_domain_union(self, var: str, domain_name: str) -> exp.Expression:
        """Build UNION ALL subquery over all types in a domain."""
        type_labels = (
            list(self._lm.nodes.keys())
            if domain_name == "__all__"
            else self._lm.domains[domain_name]
        )
        props = self._collect_var_props(var)

        branches: list[exp.Select] = []
        for label in type_labels:
            nm = self._lm.nodes.get(label)
            if nm is None:
                continue
            select_items: list[exp.Expression] = [
                exp.alias_(exp.Literal.string(nm.label), alias="__label"),
                exp.alias_(
                    exp.Cast(
                        this=exp.Column(this=exp.Identifier(this=nm.id_column, quoted=True)),
                        to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                    ),
                    alias="__id",
                ),
            ]
            for prop in props:
                sql_col = nm.properties.get(prop)
                if sql_col:
                    select_items.append(
                        exp.alias_(
                            exp.Column(this=exp.Identifier(this=sql_col, quoted=True)),
                            alias=prop,
                        )
                    )
                else:
                    select_items.append(exp.alias_(exp.null(), alias=prop))
            branch = exp.select(*select_items).from_(
                exp.alias_(
                    exp.Table(
                        this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                    ),
                    alias=f"_{nm.type_name.lower()}",
                )
            )
            branches.append(branch)

        if not branches:
            raise CypherTranslateError(f"Domain {domain_name!r} has no resolvable types")

        union: exp.Expression = branches[0]
        for branch in branches[1:]:
            union = exp.Union(this=union, expression=branch, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=var)

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




def _is_bare_variable(expr: str) -> bool:
    return bool(re.match(r"^[A-Za-z_]\w*$", expr.strip()))


def _rewrite_property_access(expr: str) -> str:
    """Rewrite n.prop → n."prop" for SQL."""
    return re.sub(
        r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b",
        lambda m: f'{m.group(1)}."{m.group(2)}"',
        expr,
    )


# ---------------------------------------------------------------------------
# Cypher → SQL function mapping
# ---------------------------------------------------------------------------

# Simple name renames: Cypher fn (uppercase) → SQL fn name
_CYPHER_FN_RENAMES: dict[str, str] = {
    "TOLOWER": "lower",
    "TOUPPER": "upper",
    "LTRIM": "ltrim",
    "RTRIM": "rtrim",
    "TRIM": "trim",
    "REVERSE": "reverse",
    "REPLACE": "replace",
    "SPLIT": "split",
    "RANGE": "sequence",    # Cypher range(start, end[, step]) → sequence(start, end[, step])
    "LOG": "ln",            # Neo4j log() = natural log = Trino ln()
    "LOG2": "log2",
    "COLLECT": "array_agg",
    "STDEV": "stddev_samp",
    "STDEVP": "stddev_pop",
    "PERCENTILECONT": "approx_percentile",
    "PERCENTILEDISC": "approx_percentile",
}

# Cast functions: Cypher fn (uppercase) → (sql_type, use_try_cast)
_CYPHER_CAST_FNS: dict[str, tuple[str, bool]] = {
    "TOSTRING": ("VARCHAR", False),
    "TOSTRINGORNULL": ("VARCHAR", True),
    "TOINTEGER": ("BIGINT", True),
    "TOINTEGERORNULL": ("BIGINT", True),
    "TOFLOAT": ("DOUBLE", True),
    "TOFLOATORNULL": ("DOUBLE", True),
    "TOBOOLEAN": ("BOOLEAN", True),
    "TOBOOLEANORNULL": ("BOOLEAN", True),
}

# String predicates: (pattern, replacement)
_STRING_PREDICATE_REWRITES: list[tuple[re.Pattern[str], str]] = [
    # n . name STARTS WITH 'x'  →  starts_with(n.name, 'x')
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?)\s+STARTS\s+WITH\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"starts_with({m.group(1).replace(' ', '')}, {m.group(2)})",
    ),
    # n . name ENDS WITH 'x'  →  (n.name LIKE CONCAT('%', 'x'))
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?)\s+ENDS\s+WITH\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"({m.group(1).replace(' ', '')} LIKE CONCAT('%', {m.group(2)}))",
    ),
    # n . name CONTAINS 'x'  →  (strpos(n.name, 'x') > 0)
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?)\s+CONTAINS\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"(strpos({m.group(1).replace(' ', '')}, {m.group(2)}) > 0)",
    ),
    # n.prop =~ 'regex'  →  regexp_like(n.prop, 'regex')
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?)\s*=~\s*('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"regexp_like({m.group(1).replace(' ', '')}, {m.group(2)})",
    ),
]


def _rewrite_string_predicates(text: str) -> str:
    for pattern, repl in _STRING_PREDICATE_REWRITES:
        text = pattern.sub(repl, text)
    return text


_ISO_TS_LITERAL_RE = re.compile(
    r"'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)'"
)


def _coerce_ts_literals(text: str) -> str:
    """Wrap ISO-datetime string literals as TIMESTAMP '...' so Trino doesn't see varchar(N)."""
    return _ISO_TS_LITERAL_RE.sub(lambda m: f"TIMESTAMP {m.group(0)}", text)


_IN_LIST_RE = re.compile(r'\bIN\s*\[([^\[\]]*)\]', re.IGNORECASE)


def _rewrite_in_list(text: str) -> str:
    """Rewrite Cypher IN [...] literal list to SQL IN (...)."""
    return _IN_LIST_RE.sub(r'IN (\1)', text)


def _rewrite_cypher_fn_node(node: exp.Expression) -> exp.Expression:
    """SQLGlot transform: rewrite Cypher function names to SQL equivalents."""
    # Cypher last(list) → element_at(list, -1) — SQLGlot parses last() as exp.Last
    if isinstance(node, exp.Last):
        inner = node.args.get("this")
        if inner is not None:
            return exp.Anonymous(this="element_at", expressions=[inner, exp.Literal.number(-1)])
        return node

    # log(x) in Cypher = natural log → Trino ln(x)
    # exp.Log with one arg (no base) is natural log in Cypher
    if isinstance(node, exp.Log):
        base = node.args.get("this")
        value = node.args.get("expression")
        # sqlglot Log: Log(this=base, expression=value) for log(base, value)
        # single-arg log(x) → Log(this=x, expression=None) or similar
        if value is None:
            # single argument — natural log
            return exp.Anonymous(this="ln", expressions=[base])
        return node

    # exp.Left / exp.Right — SQLGlot parses left()/right() as these; emit as Anonymous
    # so Trino receives LEFT(str, n) rather than a SUBSTRING expansion.
    if isinstance(node, exp.Left):
        return exp.Anonymous(this="left", expressions=[node.this, node.expression])

    if isinstance(node, exp.Right):
        return exp.Anonymous(this="right", expressions=[node.this, node.expression])

    # Handle built-in exp.Substring — adjust 0-indexed Cypher start to 1-indexed SQL
    if isinstance(node, exp.Substring):
        start = node.args.get("start")
        if start is not None:
            return exp.Substring(
                this=node.this,
                start=exp.Add(this=start, expression=exp.Literal.number(1)),
                length=node.args.get("length"),
            )
        return node

    if not isinstance(node, exp.Anonymous):
        return node
    name = node.name.upper()
    args: list[exp.Expression] = node.args.get("expressions") or []

    if name == "HEAD" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(1)])

    if name == "LAST" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(-1)])

    if name == "TAIL" and args:
        return exp.Anonymous(this="slice", expressions=[
            args[0],
            exp.Literal.number(2),
            exp.Anonymous(this="cardinality", expressions=[args[0]]),
        ])

    if name == "ISEMPTY" and args:
        return exp.EQ(
            this=exp.Anonymous(this="cardinality", expressions=args),
            expression=exp.Literal.number(0),
        )

    if name == "SIZE" and args:
        arg = args[0]
        if isinstance(arg, exp.Literal) and arg.is_string:
            return exp.Anonymous(this="char_length", expressions=args)
        return exp.Anonymous(this="cardinality", expressions=args)

    if name in _CYPHER_FN_RENAMES:
        return exp.Anonymous(this=_CYPHER_FN_RENAMES[name], expressions=args)

    if name in _CYPHER_CAST_FNS and args:
        sql_type, use_try = _CYPHER_CAST_FNS[name]
        cls = exp.TryCast if use_try else exp.Cast
        return cls(this=args[0], to=exp.DataType.build(sql_type))

    if name == "SUBSTRING" and len(args) >= 2:
        # Fallback if sqlglot parsed as Anonymous instead of Substring
        start_plus_1 = exp.Add(this=args[1], expression=exp.Literal.number(1))
        return exp.Anonymous(this="substr", expressions=[args[0], start_plus_1, *args[2:]])
    return node

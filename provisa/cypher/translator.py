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
from enum import Enum
from typing import Any, Callable

import sqlglot.expressions as exp
import sqlglot

from provisa.cypher.parser import (
    CypherAST,
    MatchClause,
    MatchStep,
    NodePattern,
    PathPattern,
    PathFunction,
    RelPattern,
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
from provisa.cypher.map_projection import MapProjectionMixin, rewrite_bare_map_literals
from provisa.cypher.group_by import GroupByMixin


def _safe_alias(expr: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", expr)


def _const_literal(v: int | str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v)


def _node_table_expr(nm: "NodeMapping", alias: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build an aliased table expression for a NodeMapping.

    When physical column names differ from SQL aliases (e.g. breedName vs breed_name),
    wraps the physical table in a subquery: SELECT *, "phys" AS "sql_alias" FROM table.
    This keeps physical column names accessible for JOIN conditions while the outer SQL
    can reference SQL aliases throughout — preserving governance on the outer query.
    """
    phys_table = exp.Table(
        this=exp.Identifier(this=nm.sql_table_name, quoted=True),
        db=exp.Identifier(this=nm.schema_name, quoted=True),
        catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
    )
    alias_exprs = [
        exp.alias_(
            exp.Column(this=exp.Identifier(this=phys, quoted=True)),
            sql_al,
        )
        for cql, sql_al in nm.properties.items()
        if (phys := nm.physical_properties.get(cql)) and phys != sql_al
    ]
    if not alias_exprs:
        return exp.alias_(phys_table, alias=alias)  # pyright: ignore[reportReturnType]
    subq = exp.Select(expressions=[exp.Star(), *alias_exprs]).from_(phys_table)
    return exp.alias_(exp.Subquery(this=subq), alias=alias)  # pyright: ignore[reportReturnType]


def _tgt_col_expr_for_rm(rm: "RelationshipMapping", alias: str) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build the target column expression for a RelationshipMapping."""
    if rm.target_expr is not None:
        return exp.maybe_parse(
            rm.target_expr.replace("{alias}", alias),
            dialect="trino",
        )
    return exp.Column(
        this=exp.Identifier(this=rm.join_target_column, quoted=True),
        table=exp.Identifier(this=alias),
    )


def _src_col_expr_for_rm(
    rm: "RelationshipMapping",
    src_table_ref: str,
    src_nm: "NodeMapping | None",
) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build the source column expression for a RelationshipMapping (forward direction)."""
    if rm.source_constant is not None:
        return _const_literal(rm.source_constant)
    if rm.source_expr is not None:
        return exp.maybe_parse(
            rm.source_expr.replace("{alias}", src_table_ref),
            dialect="trino",
        )
    if rm.join_source_column == "_name_" and src_nm is not None:
        from provisa.compiler.naming import domain_to_sql_name as _d2s

        _name_val = f"{_d2s(src_nm.domain_id or src_nm.schema_name or '')}.{src_nm.table_name}"
        return exp.Literal.string(_name_val)
    return exp.Column(
        this=exp.Identifier(this=rm.join_source_column, quoted=True),
        table=exp.Identifier(this=src_table_ref),
    )


def _make_rel_join(
    rm: "RelationshipMapping",
    is_bwd: bool,
    tgt_nm: "NodeMapping",
    tgt_alias: str,
    src_table_ref: str,
    src_nm: "NodeMapping | None",
    join_type: str,
) -> dict:
    """Build a join dict for a single relationship mapping candidate."""
    jt = _node_table_expr(tgt_nm, tgt_alias)
    # The join condition between two fixed tables is identical regardless of which way
    # the pattern is traversed; orientation is determined by which label the source
    # table holds, not the traversal flag. For an undirected pattern the candidate
    # resolver emits a backward variant without swapping src_nm/tgt_nm, so is_bwd alone
    # would place the source column on the target table (REQ-575 regression). Recompute
    # from labels for non-self-referential rels; keep is_bwd for self-refs (same label
    # on both sides, where direction genuinely selects the column pair).
    if rm.source_label != rm.target_label and src_nm is not None:
        is_bwd = src_nm.type_name == rm.target_label
    if is_bwd:
        if rm.source_constant is not None:
            cond = exp.EQ(
                this=_const_literal(rm.source_constant),
                expression=_tgt_col_expr_for_rm(rm, src_table_ref),
            )
        else:
            cond = exp.EQ(
                this=exp.Column(
                    this=exp.Identifier(this=rm.join_source_column, quoted=True),
                    table=exp.Identifier(this=tgt_alias),
                ),
                expression=_tgt_col_expr_for_rm(rm, src_table_ref),
            )
    else:
        src_col = _src_col_expr_for_rm(rm, src_table_ref, src_nm)
        tgt_col = _tgt_col_expr_for_rm(rm, tgt_alias)
        cond = exp.EQ(this=src_col, expression=tgt_col)
    return {"table": jt, "on": cond, "join_type": join_type}


def _is_bwd_for_candidate(
    rm: "RelationshipMapping",
    bidir: bool,
    backward: bool,
    src_nm: "NodeMapping | None",
    tgt_nm: "NodeMapping | None",
    tgt_nm_explicit: bool,
) -> "bool | None":
    """Determine backward-ness for a relationship mapping candidate.

    Returns None if the candidate should be filtered out (direction mismatch).
    """
    if bidir:
        if src_nm is not None:
            canonical_fwd = rm.source_label == src_nm.type_name
            canonical_bwd = rm.target_label == src_nm.type_name
            chains_from_tgt = tgt_nm is not None and rm.source_label == tgt_nm.type_name
            if not canonical_fwd and not canonical_bwd and not chains_from_tgt:
                return None
            return rm.source_label != src_nm.type_name
        return False
    if src_nm is not None:
        canonical_fwd = rm.source_label == src_nm.type_name
        canonical_bwd = rm.target_label == src_nm.type_name
        chains_from_tgt = tgt_nm is not None and rm.source_label == tgt_nm.type_name
        if not canonical_fwd and not canonical_bwd and not chains_from_tgt:
            return None
        if tgt_nm is not None and tgt_nm_explicit:
            # For non-self-referential rels, backward on a fwd-canonical rel is invalid
            # and forward on a bwd-only rel is invalid.  For self-referential rels
            # (canonical_fwd AND canonical_bwd both true), both directions are valid.
            if backward and canonical_fwd and not canonical_bwd:
                return None
            if not backward and not canonical_fwd and canonical_bwd:
                return None
        return not canonical_fwd
    return backward


def _optional_vars(clauses: "list[MatchClause]") -> "set[str]":
    """Return variables first introduced by OPTIONAL MATCH (not already bound by MATCH)."""
    seen: set[str] = set()
    optional_only: set[str] = set()
    for clause in clauses:
        if not isinstance(clause.pattern, PathPattern):
            continue
        new_in_clause = set()
        for node in clause.pattern.nodes:
            if node.variable:
                new_in_clause.add(node.variable)
        for rel in clause.pattern.rels:
            if rel.variable:
                new_in_clause.add(rel.variable)
        if clause.optional:
            optional_only.update(new_in_clause - seen)
        seen.update(new_in_clause)
    return optional_only


def _join_alias(table_expr: "exp.Expression") -> "str | None":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Extract the SQL alias from a join table expression."""
    alias = getattr(table_expr, "alias", None)
    return str(alias) if alias else None


def _split_and(expr: "exp.Expression") -> "list[exp.Expression]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Flatten top-level AND conjuncts into a list."""
    if isinstance(expr, exp.And):
        return _split_and(expr.this) + _split_and(expr.expression)
    return [expr]


def _fold_where_into_optional_joins(
    where_expr: "exp.Expression",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    optional_vars: "set[str]",
    where_text: str,
    joins: "list[dict]",
) -> "tuple[list[dict], exp.Expression | None]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Fold WHERE conditions referencing optional variables into the relevant LEFT JOIN ON clauses.

    Cypher semantics: WHERE after OPTIONAL MATCH constrains the optional pattern.
    In SQL this must be an ON condition, not a global WHERE — a global WHERE turns
    a LEFT JOIN into an implicit INNER JOIN, filtering out rows where the optional
    variable is NULL and eliminating the base MATCH rows.

    Each AND conjunct is assigned only to the LEFT JOIN that introduces its
    last-referenced optional variable, so a condition on variable `c` is never
    placed in an earlier join (e.g. `b`) where `c` is not yet in scope.

    Returns (modified_joins, remaining_where_or_None).
    """
    referenced = {v for v in optional_vars if re.search(rf"\b{re.escape(v)}\b", where_text)}
    if not referenced:
        return joins, where_expr

    # Build position map for LEFT JOIN aliases so we can find the "last" one.
    join_order: dict[str, int] = {}
    for i, join in enumerate(joins):
        alias = _join_alias(join["table"])
        if alias:
            join_order[alias] = i

    # Split into individual AND conjuncts, route each to the appropriate join.
    conjuncts = _split_and(where_expr)
    alias_to_conjuncts: dict[str, list] = {}
    remaining_conjuncts: list = []

    for cond in conjuncts:
        cond_text = cond.sql(dialect="trino")
        # _nf_ conditions must stay in WHERE so nf_extractor can strip them before SQL execution.
        if "_nf_" in cond_text:
            remaining_conjuncts.append(cond)
            continue
        refs = {v for v in optional_vars if re.search(rf"\b{re.escape(v)}\b", cond_text)}
        refs_in_joins = refs & set(join_order.keys())
        if refs_in_joins:
            target = max(refs_in_joins, key=lambda v: join_order[v])
            alias_to_conjuncts.setdefault(target, []).append(cond)
        else:
            remaining_conjuncts.append(cond)

    modified: list[dict] = []
    for join in joins:
        alias = _join_alias(join["table"])
        if alias in alias_to_conjuncts and join["join_type"] == "LEFT":
            existing_on = join["on"]
            new_on = existing_on
            for cond in alias_to_conjuncts[alias]:
                if new_on is None or (
                    hasattr(new_on, "sql") and new_on.sql() in ("TRUE", "true", "1 = 1")
                ):
                    new_on = cond
                else:
                    new_on = exp.And(this=new_on, expression=cond)
            modified.append({**join, "on": new_on})
        else:
            modified.append(join)

    remaining: "exp.Expression | None" = None  # pyright: ignore[reportPrivateImportUsage]
    for cond in remaining_conjuncts:
        remaining = cond if remaining is None else exp.And(this=remaining, expression=cond)

    return modified, remaining


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
    PathComprehensionMixin,
    SelectBuilderMixin,
    CorrelatedCallMixin,
    SubqueryExprsMixin,
    MapProjectionMixin,
    GroupByMixin,
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
        """Parse a Cypher UNWIND source expression into a SQLGlot expression."""
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
            # dialect="postgres" turns $1 into a Parameter node that renders as @1.
            return sqlglot.parse_one(text)  # pyright: ignore[reportReturnType]
        except Exception:
            return exp.column(text)  # pyright: ignore[reportReturnType]

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

    def _resolve_early_rel_mapping(self, rel: "RelPattern") -> "RelationshipMapping | None":
        """Resolve early rel_mapping for domain-node path and anonymous node inference."""
        if not rel.types:
            return None
        _rt_early = rel.types[0].upper()
        _early_matches = self._lm.aliases.get(_rt_early, [])
        if not _early_matches:
            _rm_early = self._lm.relationships.get(_rt_early)
            _early_matches = [_rm_early] if _rm_early else []
        return _early_matches[0] if _early_matches else None

    def _infer_src_from_rel(
        self,
        rel_mapping: "RelationshipMapping",
        src_var: "str | None",
        current_from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "tuple[NodeMapping | None, exp.Expression | None]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Infer src_nm from rel_mapping; update from_expr if src was a domain union."""
        src_nm = self._lm.nodes.get(rel_mapping.source_label)
        if src_nm and src_var:
            self._var_table[src_var] = (src_var, src_nm)
            if src_var in self._domain_nodes:
                self._domain_nodes.pop(src_var)
                current_from_expr = _node_table_expr(src_nm, src_var)
        return src_nm, current_from_expr

    def _infer_tgt_from_rel(
        self,
        rel_mapping: "RelationshipMapping",
        tgt_var: "str | None",
    ) -> "NodeMapping | None":
        """Infer tgt_nm from rel_mapping; update var_table/domain_nodes."""
        tgt_nm = self._lm.nodes.get(rel_mapping.target_label)
        if tgt_nm and tgt_var:
            self._var_table[tgt_var] = (tgt_var, tgt_nm)
            self._domain_nodes.pop(tgt_var, None)
        return tgt_nm

    def _build_domain_target_join(
        self,
        tgt_var: str,
        rel_mapping: "RelationshipMapping",
        src_var: "str | None",
        src_nm: "NodeMapping | None",
        clause: MatchClause,
    ) -> dict:
        """Build a JOIN for a domain-only target node."""
        join_type = "LEFT" if clause.optional else "INNER"
        tgt_alias = tgt_var
        join_table = self._build_domain_union(tgt_var, self._domain_nodes[tgt_var])
        src_table_ref = (
            self._var_table.get(src_var, (src_var, None))[0] if src_var else src_nm.table_name  # type: ignore[union-attr]
        )
        src_col_expr = _src_col_expr_for_rm(rel_mapping, src_table_ref, src_nm)
        tgt_col_expr = _tgt_col_expr_for_rm(rel_mapping, tgt_alias)
        on_cond = exp.EQ(this=src_col_expr, expression=tgt_col_expr)
        return {"table": join_table, "on": on_cond, "join_type": join_type}

    def _handle_unlabeled_rel_pattern(
        self,
        src_var: "str | None",
        rel_var: "str | None",
        tgt_var: "str | None",
    ) -> "exp.Expression":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Handle fully unlabeled rel pattern: UNION ALL over all relationship types."""
        src_domain = self._domain_nodes.get(src_var) if src_var else None
        tgt_domain = self._domain_nodes.get(tgt_var) if tgt_var else None
        from_expr = self._build_all_rels_union(src_var, rel_var, tgt_var, src_domain, tgt_domain)
        if src_var:
            self._domain_nodes.pop(src_var, None)
            self._var_table[src_var] = (src_var, None)
            self._passthrough_vars.add(src_var)
            self._all_rels_node_vars.add(src_var)
        if rel_var:
            self._passthrough_vars.add(rel_var)
            self._all_rels_rel_vars.add(rel_var)
        if tgt_var:
            self._domain_nodes.pop(tgt_var, None)
            self._var_table[tgt_var] = (tgt_var, None)
            self._passthrough_vars.add(tgt_var)
            self._all_rels_node_vars.add(tgt_var)
        return from_expr

    def _resolve_typed_rel_candidates(
        self,
        rel: "RelPattern",
        bidir: bool,
        backward: bool,
        src_nm: "NodeMapping | None",
        tgt_nm: "NodeMapping | None",
        src_nm_explicit: bool,
        tgt_nm_explicit: bool,
    ) -> "list[tuple]":
        """Resolve relationship candidates for a typed relationship."""
        rel_type = rel.types[0].upper()
        alias_matches = self._lm.aliases.get(rel_type, [])
        if not alias_matches:
            rm = self._lm.relationships.get(rel_type)
            alias_matches = [rm] if rm else []
        # Filter to exact src/tgt match when multiple aliases share the same rel type
        if src_nm is not None and src_nm_explicit and len(alias_matches) > 1:
            fwd_exact = [
                m
                for m in alias_matches
                if m.source_label == src_nm.type_name
                and (tgt_nm is None or m.target_label == tgt_nm.type_name)
            ]
            bwd_exact = [
                m
                for m in alias_matches
                if m.target_label == src_nm.type_name
                and (tgt_nm is None or m.source_label == tgt_nm.type_name)
            ]
            if not backward and fwd_exact:
                alias_matches = fwd_exact
            elif bwd_exact:
                alias_matches = bwd_exact
            elif fwd_exact:
                alias_matches = fwd_exact
        if bidir:
            # REQ-575: undirected pattern → UNION ALL of both traversal directions.
            # For each matching mapping, include forward (False) and backward (True) variants
            # so _build_candidate_joins emits both branches.
            candidates: list[tuple] = []
            for m in alias_matches:
                fwd_ok = _is_bwd_for_candidate(m, False, False, src_nm, tgt_nm, tgt_nm_explicit)
                bwd_ok = _is_bwd_for_candidate(m, False, True, src_nm, tgt_nm, tgt_nm_explicit)
                if m.source_label != m.target_label:
                    # Non-self-referential: forward and backward resolve to the same fixed
                    # tables and collapse to an identical join (orientation is derived from
                    # labels in _make_rel_join). Emit a single branch — emitting both would
                    # duplicate every matched path in the UNION ALL.
                    if fwd_ok is not None or bwd_ok is not None:
                        candidates.append((m, False))
                    continue
                if fwd_ok is not None:
                    candidates.append((m, False))
                if bwd_ok is not None:
                    candidates.append((m, True))
            return candidates
        return [
            (m, bwd)
            for m in alias_matches
            if (bwd := _is_bwd_for_candidate(m, bidir, backward, src_nm, tgt_nm, tgt_nm_explicit))
            is not None
        ]

    def _resolve_untyped_rel_candidates(
        self,
        bidir: bool,
        backward: bool,
        src_nm: "NodeMapping",
        tgt_nm: "NodeMapping",
    ) -> "list[tuple]":
        """Resolve relationship candidates for an untyped relationship."""
        if bidir:
            fwd = self._lm.relationships_for(src_nm.type_name, tgt_nm.type_name)
            bwd = self._lm.relationships_for(tgt_nm.type_name, src_nm.type_name)
            return [(m, False) for m in fwd] + [(m, True) for m in bwd]
        if backward:
            fwd_cands = self._lm.relationships_for(tgt_nm.type_name, src_nm.type_name)
            return [(m, True) for m in fwd_cands]
        fwd_cands = self._lm.relationships_for(src_nm.type_name, tgt_nm.type_name)
        return [(m, False) for m in fwd_cands]

    def _resolve_rel_node_types(
        self,
        src_node: "NodePattern",
        tgt_node: "NodePattern",
        rel_mapping: "RelationshipMapping | None",
        from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> "tuple[NodeMapping | None, NodeMapping | None, bool, bool, exp.Expression | None]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Resolve src_nm, tgt_nm, explicitness flags, and updated from_expr.

        Returns (src_nm, tgt_nm, src_nm_explicit, tgt_nm_explicit, from_expr).
        """
        src_var = src_node.variable
        tgt_var = tgt_node.variable
        src_nm = self._var_table.get(src_var, (None, None))[1] if src_var else None
        tgt_nm = self._var_table.get(tgt_var, (None, None))[1] if tgt_var else None
        src_nm_explicit = src_nm is not None
        tgt_nm_explicit = tgt_nm is not None

        if tgt_nm is None and tgt_node.labels:
            type_label, _ = self._resolve_node_type(tgt_node.labels)
            tgt_nm = self._lm.nodes.get(type_label) if type_label else None
            if tgt_nm and tgt_var:
                self._var_table[tgt_var] = (tgt_var, tgt_nm)
            tgt_nm_explicit = tgt_nm is not None

        if (src_nm is None or tgt_nm is None) and rel_mapping:
            if src_nm is None:
                src_nm, from_expr = self._infer_src_from_rel(rel_mapping, src_var, from_expr)
            if tgt_nm is None:
                tgt_nm = self._infer_tgt_from_rel(rel_mapping, tgt_var)

        return src_nm, tgt_nm, src_nm_explicit, tgt_nm_explicit, from_expr

    def _apply_rel_join_candidates(
        self,
        rel: "RelPattern",
        candidates: list,
        src_var: "str | None",
        src_nm: "NodeMapping",
        tgt_nm: "NodeMapping",
        clause: MatchClause,
        from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: "list[dict]",
    ) -> "tuple[exp.Expression | None, list[dict]]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Apply primary and extra JOIN candidates to joins/from_expr."""
        join_type = "LEFT" if clause.optional else "INNER"
        tgt_alias = tgt_nm.table_name
        if src_var and src_var in self._cte_sources:
            src_table_ref = self._var_table.get(src_var, (src_var, None))[0]
        else:
            src_table_ref = src_var or src_nm.table_name

        primary_rm, primary_bwd = candidates[0]
        if rel.variable:
            self._rel_var_types[rel.variable] = primary_rm.rel_type
            _src_alias = src_var or src_nm.table_name
            if _src_alias and src_nm and tgt_nm:
                self._rel_var_endpoints[rel.variable] = (
                    _src_alias,
                    src_nm,
                    tgt_alias,
                    tgt_nm,
                    primary_bwd,
                )

        primary_join = _make_rel_join(
            primary_rm, primary_bwd, tgt_nm, tgt_alias, src_table_ref, src_nm, join_type
        )
        joins_before = list(joins)

        if src_var and src_var in self._lateral_bound and from_expr is None:
            from_expr = primary_join["table"]
            self._lateral_conditions.append(primary_join["on"])
        else:
            joins.append(primary_join)

        for extra_rm, extra_bwd in candidates[1:]:
            extra_join = _make_rel_join(
                extra_rm, extra_bwd, tgt_nm, tgt_alias, src_table_ref, src_nm, join_type
            )
            self._extra_path_branches.append((from_expr, joins_before + [extra_join], {}))

        return from_expr, joins

    def _build_candidate_joins(
        self,
        rel: "RelPattern",
        candidates: list,
        src_var: "str | None",
        src_nm: "NodeMapping",
        tgt_nm: "NodeMapping",
        tgt_var: "str | None",
        clause: MatchClause,
        from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: "list[dict]",
    ) -> "tuple[exp.Expression | None, list[dict]]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build and register joins for a resolved set of rel candidates."""
        join_type = "LEFT" if clause.optional else "INNER"
        tgt_alias = tgt_var or tgt_nm.table_name
        if src_var and src_var in self._cte_sources:
            src_table_ref = self._var_table.get(src_var, (src_var, None))[0]
        else:
            src_table_ref = src_var or src_nm.table_name

        primary_rm, primary_bwd = candidates[0]
        _src_alias = src_var or src_nm.table_name
        _tgt_alias = tgt_var or tgt_nm.table_name
        # _make_rel_join derives orientation from labels for non-self-ref rels; mirror that
        # here so the captured edge's start/end nodes match the emitted join.
        _eff_bwd = (
            (src_nm.type_name == primary_rm.target_label)
            if primary_rm.source_label != primary_rm.target_label
            else primary_bwd
        )
        if _src_alias and _tgt_alias:
            self._rel_step_endpoints[id(rel)] = (
                primary_rm.rel_type,
                _src_alias,
                src_nm,
                _tgt_alias,
                tgt_nm,
                _eff_bwd,
            )
        if rel.variable:
            self._rel_var_types[rel.variable] = primary_rm.rel_type
            if _src_alias and _tgt_alias:
                self._rel_var_endpoints[rel.variable] = (
                    _src_alias,
                    src_nm,
                    _tgt_alias,
                    tgt_nm,
                    primary_bwd,
                )

        primary_join = _make_rel_join(
            primary_rm, primary_bwd, tgt_nm, tgt_alias, src_table_ref, src_nm, join_type
        )
        joins_before = list(joins)

        if src_var and src_var in self._lateral_bound and from_expr is None:
            from_expr = primary_join["table"]
            self._lateral_conditions.append(primary_join["on"])
        else:
            joins.append(primary_join)

        for extra_rm, extra_bwd in candidates[1:]:
            extra_join = _make_rel_join(
                extra_rm, extra_bwd, tgt_nm, tgt_alias, src_table_ref, src_nm, join_type
            )
            self._extra_path_branches.append((from_expr, joins_before + [extra_join], {}))

        return from_expr, joins

    def _process_rel_step(
        self,
        rel: "RelPattern",
        nodes: list,
        i: int,
        clause: MatchClause,
        from_expr: "exp.Expression | None",  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: "list[dict]",
    ) -> "tuple[exp.Expression | None, list[dict], bool]":  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Process a single relationship → update joins/from_expr.

        Returns (from_expr, joins, did_continue) where did_continue=True means
        caller should skip to the next rel iteration.
        """
        src_node = nodes[i]
        tgt_node = nodes[i + 1]
        src_var = src_node.variable
        tgt_var = tgt_node.variable

        rel_mapping = self._resolve_early_rel_mapping(rel)
        src_nm, tgt_nm, src_nm_explicit, tgt_nm_explicit, from_expr = self._resolve_rel_node_types(
            src_node, tgt_node, rel_mapping, from_expr
        )

        if from_expr is None and src_nm is not None:
            src_alias = src_var or src_nm.table_name
            from_expr = _node_table_expr(src_nm, src_alias)

        if src_nm is None or tgt_nm is None:
            if tgt_var and tgt_var in self._domain_nodes and rel_mapping is not None:
                j = self._build_domain_target_join(tgt_var, rel_mapping, src_var, src_nm, clause)
                joins.append(j)
            elif rel_mapping is None:
                from_expr = self._handle_unlabeled_rel_pattern(src_var, rel.variable, tgt_var)
            return from_expr, joins, True

        bidir = rel.direction == "none"
        backward = rel.direction == "left"

        if rel.types:
            candidates = self._resolve_typed_rel_candidates(
                rel, bidir, backward, src_nm, tgt_nm, src_nm_explicit, tgt_nm_explicit
            )
        else:
            candidates = self._resolve_untyped_rel_candidates(bidir, backward, src_nm, tgt_nm)

        if not candidates:
            if tgt_var:
                tgt_alias = tgt_var or tgt_nm.table_name
                jt = _node_table_expr(tgt_nm, tgt_alias)
                no_rel_join_type = "LEFT" if clause.optional else "INNER"
                joins.append({"table": jt, "on": exp.false(), "join_type": no_rel_join_type})
            if rel.variable:
                self._rel_var_types[rel.variable] = ""
            return from_expr, joins, True

        from_expr, joins = self._build_candidate_joins(
            rel, candidates, src_var, src_nm, tgt_nm, tgt_var, clause, from_expr, joins
        )
        return from_expr, joins, False

    def _build_from_joins(
        self, match_clauses: list[MatchClause]
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Process MATCH clauses → (from_expr, [join_dict])."""
        from_expr: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict] = []
        rel_mapping: RelationshipMapping | None = None
        tgt_nm: NodeMapping | None = None

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

            for node in nodes:
                self._register_node(node)

            if from_expr is None and nodes:
                new_from = self._build_first_node_from(nodes[0])
                if new_from is not None:
                    from_expr = new_from
            elif from_expr is not None and nodes and not rels:
                j = self._build_standalone_node_join(nodes[0].variable, clause)
                if j is not None:
                    joins.append(j)

            for i, rel in enumerate(rels):
                if i + 1 >= len(nodes):
                    break
                if rel.variable_length:
                    if len(rels) > 1:
                        raise CypherTranslateError(
                            "Variable-length patterns (e.g. [*..5]) cannot be mixed with other "
                            "relationships in the same MATCH. Use a separate MATCH clause or "
                            "wrap the full pattern: MATCH p = allPaths((a)-[*..5]->(b)) RETURN p"
                        )
                    pf_clause = MatchClause(
                        pattern=PathFunction(
                            func_name="allpaths",
                            pattern=PathPattern(nodes=nodes, rels=rels),
                        ),
                        variable=clause.variable,
                        optional=clause.optional,
                    )
                    if rel.variable and clause.variable:
                        self._varlen_rel_vars[rel.variable] = clause.variable
                    pf_from, pf_joins = self._translate_path_function(pf_clause)
                    if from_expr is None:
                        from_expr = pf_from
                    joins.extend(pf_joins)
                    break
                from_expr, joins, did_continue = self._process_rel_step(
                    rel, nodes, i, clause, from_expr, joins
                )
                if did_continue:
                    continue
                # Capture rel_mapping and tgt_nm for path var registration below
                rel_mapping = self._resolve_early_rel_mapping(rel)
                tgt_var = nodes[i + 1].variable
                tgt_nm = self._var_table.get(tgt_var, (None, None))[1] if tgt_var else None

            if clause.variable and nodes:
                _first = nodes[0]
                _last = nodes[-1]
                if _first.variable:
                    _path_src_alias = _first.variable
                elif rels and rel_mapping is not None:
                    _src_nm_for_path = self._lm.nodes.get(rel_mapping.source_label)
                    _path_src_alias = (
                        _src_nm_for_path.table_name
                        if _src_nm_for_path
                        else rel_mapping.source_label.lower()
                    )
                else:
                    _path_src_alias = ""
                if _last.variable:
                    _path_tgt_alias = _last.variable
                elif rels and tgt_nm is not None:
                    _path_tgt_alias = tgt_nm.table_name
                elif rels and rel_mapping is not None:
                    _tgt_nm_for_path = self._lm.nodes.get(rel_mapping.target_label)
                    _path_tgt_alias = (
                        _tgt_nm_for_path.table_name
                        if _tgt_nm_for_path
                        else rel_mapping.target_label.lower()
                    )
                else:
                    _path_tgt_alias = ""
                self._path_vars[clause.variable] = (_path_src_alias, _path_tgt_alias, False)
                if (
                    rels
                    and clause.variable not in self._path_steps
                    and not any(r.variable_length for r in rels)
                ):
                    _step_nodes: list[tuple[str, NodeMapping]] = []
                    _step_edges: list[tuple] = []
                    _seen_aliases: set[str] = set()

                    def _add_step_node(_alias: str, _nm: "NodeMapping | None") -> None:
                        if _nm and _alias and _alias not in _seen_aliases:
                            _seen_aliases.add(_alias)
                            _step_nodes.append((_alias, _nm))

                    for _node in nodes:
                        if _node.variable:
                            _node_info = self._var_table.get(_node.variable)
                            if _node_info and _node_info[1]:
                                _add_step_node(_node_info[0], _node_info[1])
                    for _rel in rels:
                        # Prefer captured per-step endpoints — covers anonymous nodes and
                        # unnamed relationships that have no variable to look up.
                        _ep = self._rel_step_endpoints.get(id(_rel))
                        if _ep is not None:
                            _rt, _sa, _snm, _ta, _tnm, _rev = _ep
                            _add_step_node(_sa, _snm)
                            _add_step_node(_ta, _tnm)
                            _step_edges.append(_ep)
                        elif _rel.variable and _rel.variable in self._rel_var_endpoints:
                            _vep = self._rel_var_endpoints[_rel.variable]
                            _sa, _snm, _ta, _tnm, _rev = _vep
                            _rt = self._rel_var_types.get(_rel.variable, "")
                            _add_step_node(_sa, _snm)
                            _add_step_node(_ta, _tnm)
                            _step_edges.append((_rt, _sa, _snm, _ta, _tnm, _rev))
                    if _step_nodes or _step_edges:
                        self._path_steps[clause.variable] = (_step_nodes, _step_edges)

        if from_expr is None:
            raise CypherTranslateError("No MATCH clause produced a FROM table")

        # Supplement each extra branch with primary joins it's missing.
        # Extra branches are created with only joins_before+[extra_join], which
        # excludes any joins added AFTER the branch point (e.g. a second OPTIONAL
        # MATCH). Without this, the UNION ALL branch's SELECT can reference aliases
        # that don't exist in that branch.
        if self._extra_path_branches:
            primary_aliases = {_join_alias(j["table"]): j for j in joins}
            patched: list[tuple] = []
            for extra_from, extra_joins, extra_path_steps_map in self._extra_path_branches:
                branch_aliases = {_join_alias(j["table"]) for j in extra_joins}
                supplement = [
                    pj for alias, pj in primary_aliases.items() if alias not in branch_aliases
                ]
                patched.append((extra_from, extra_joins + supplement, extra_path_steps_map))
            self._extra_path_branches = patched

        return from_expr, joins

    def _rewrite_node_var_in_aggs(self, text: str) -> str:
        """Rewrite COUNT(var)/COLLECT(var) where var is a node variable to use id_column.

        Cypher COUNT(n) counts non-null node instances — translates to COUNT(n.id_col) in SQL.
        Without this, the bare var reaches Trino as a table alias which cannot be resolved as a column.
        Cypher COUNT(r) for a relationship variable translates to COUNT(*) — r is not a SQL column.
        """
        _AGG_WITH_NODE_ARG = re.compile(
            r"\b(COUNT|COLLECT|count|collect)\s*\(\s*([A-Za-z_]\w*)\s*\)",
        )

        def _replace(m: re.Match) -> str:
            fn, var = m.group(1), m.group(2)
            if var in self._rel_var_types or var in self._all_rels_rel_vars:
                if fn.upper() == "COUNT":
                    return "COUNT(*)"
                return m.group(0)
            info = self._var_table.get(var)
            if info and info[1]:
                id_col = info[1].id_column
                # COUNT(n) must deduplicate — multiple SQL rows can map to one graph node via JOINs
                if fn.upper() == "COUNT":
                    return f"{fn}(DISTINCT {var}.{id_col})"
                return f"{fn}({var}.{id_col})"
            return m.group(0)

        return _AGG_WITH_NODE_ARG.sub(_replace, text)

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

    def _rewrite_nf_props(self, text: str) -> str:
        """Rewrite var.col or var."col" → var."_nf_col" for native filter columns."""

        def _replace(m: re.Match) -> str:
            var, col = m.group(1), m.group(2)
            info = self._var_table.get(var)
            if info and info[1] and col in info[1].native_filter_columns:
                return f'{var}."_nf_{col}"'
            return m.group(0)

        # Match both quoted (var."col") and unquoted with optional spaces (var . col)
        return re.sub(r'\b([A-Za-z_]\w*)\s*\.\s*"?([A-Za-z_]\w*)"?', _replace, text)

    def _build_where(self, where: WhereClause | None) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if where is None:
            return None
        expr_text = _rewrite_cypher_dquote_strings(self._rewrite_params_in_expr(where.expression))
        expr_text = self._rewrite_cte_vars(expr_text)
        expr_text = self._rewrite_call_bound_vars(expr_text)
        expr_text = self._rewrite_node_var_in_aggs(expr_text)
        expr_text = self._rewrite_map_projections(expr_text)
        expr_text = rewrite_bare_map_literals(expr_text)
        expr_text = self._rewrite_graph_fns(expr_text)
        expr_text = self._rewrite_path_comprehensions(expr_text)
        expr_text = rewrite_list_comprehensions(expr_text)
        expr_text = _rewrite_in_list(expr_text)
        expr_text = self._rewrite_cypher_props(expr_text)
        expr_text = _rewrite_string_predicates(expr_text)
        expr_text = _rewrite_property_access(expr_text)
        expr_text = self._rewrite_nf_props(expr_text)
        expr_text = _coerce_ts_literals(expr_text)
        expr_text = self._rewrite_subquery_exprs(expr_text)
        try:
            parsed = sqlglot.parse_one(expr_text, dialect="postgres")
            return parsed.transform(_rewrite_cypher_fn_node)
        except Exception:
            try:
                from sqlglot.errors import ErrorLevel

                parsed = sqlglot.parse_one(
                    expr_text, dialect="postgres", error_level=ErrorLevel.IGNORE
                )
                if parsed is not None:
                    return parsed.transform(_rewrite_cypher_fn_node)
            except Exception:
                pass
            return exp.true()

    def _build_order_by(self, order_by: list[OrderItem]) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for item in order_by:
            inner = self._parse_expr(item.expression)
            if item.direction == "DESC":
                exprs.append(exp.Ordered(this=inner, desc=True))
            else:
                exprs.append(exp.Ordered(this=inner, desc=False))
        return exprs

    def _rewrite_graph_fns(self, text: str) -> str:
        """Rewrite graph-aware functions using var_table context."""

        def _id_in_list_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            items_text = m.group(2)
            info = self._var_table.get(var)
            if info and info[1]:
                id_ref = f'{var}."{info[1].id_column}"'
                return f"{id_ref} IN ({items_text})"
            elif var in self._domain_nodes:
                # __id is always CAST(id_column AS VARCHAR) — cast integer literals to match
                id_ref = f'{var}."__id"'
                items = [i.strip() for i in items_text.split(",")]
                cast_items = [
                    f"CAST({i} AS VARCHAR)" if i.lstrip("-+").isdigit() else i for i in items
                ]
                return f"{id_ref} IN ({', '.join(cast_items)})"
            else:
                return m.group(0)

        def _id_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            info = self._var_table.get(var)
            if info and info[1]:
                return f'{var}."{info[1].id_column}"'
            if var in self._domain_nodes:
                return f'{var}."__id"'
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
        text = re.sub(r"\btype\s*\(\s*([A-Za-z_]\w*)\s*\)", _type_repl, text, flags=re.IGNORECASE)
        # exists(n.prop) → (n.prop) IS NOT NULL
        text = re.sub(r"\bexists\s*\(([^()]+)\)", r"(\1) IS NOT NULL", text, flags=re.IGNORECASE)
        # id(var) IN [...] — must run before plain id() so domain nodes cast integer literals to VARCHAR
        text = re.sub(
            r"\bid\s*\(\s*([A-Za-z_]\w*)\s*\)\s+IN\s+\[([^\]]*)\]",
            _id_in_list_repl,
            text,
            flags=re.IGNORECASE,
        )
        # id(var) → var."id_col"
        text = re.sub(r"\bid\s*\(\s*([A-Za-z_]\w*)\s*\)", _id_repl, text)
        # labels(var) → ARRAY['Label']
        text = re.sub(
            r"\blabels\s*\(\s*([A-Za-z_]\w*)\s*\)", _labels_repl, text, flags=re.IGNORECASE
        )
        # keys(var) → ARRAY['prop1', ...]
        text = re.sub(r"\bkeys\s*\(\s*([A-Za-z_]\w*)\s*\)", _keys_repl, text, flags=re.IGNORECASE)
        # length(p) for recursive CTE paths → _t.hops; for flat paths → 1
        if self._shortestpath_hops_col is not None:
            text = re.sub(
                r"\blength\s*\(\s*[A-Za-z_]\w*\s*\)", "_t.hops", text, flags=re.IGNORECASE
            )
        else:
            # flat path: length is always 1 (single hop or variable-length flat join)
            text = re.sub(r"\blength\s*\(\s*[A-Za-z_]\w*\s*\)", "1", text, flags=re.IGNORECASE)

        def _relationships_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            path_step_info = getattr(self, "_path_steps", {}).get(var)
            if path_step_info is None:
                _vr = getattr(self, "_varlen_rel_vars", {})
                if var in _vr:
                    path_step_info = getattr(self, "_path_steps", {}).get(_vr[var])
            if path_step_info is not None:
                _, step_edges = path_step_info
                arr = exp.Anonymous(
                    this="JSON_ARRAY",
                    expressions=[
                        self._build_edge_object(rt, sa, snm, ta, tnm, rev)
                        for rt, sa, snm, ta, tnm, rev in step_edges
                    ],
                )
                return arr.sql(dialect="trino")
            return "NULL"

        def _nodes_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            if var not in self._path_vars:
                return m.group(0)
            path_step_info = getattr(self, "_path_steps", {}).get(var)
            if path_step_info is not None:
                step_nodes, _ = path_step_info
                arr = exp.Anonymous(
                    this="JSON_ARRAY",
                    expressions=[
                        self._build_node_object_expr(node_alias, nm)
                        for node_alias, nm in step_nodes
                    ],
                )
                return arr.sql(dialect="trino")
            return "NULL"

        def _startnode_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            endpoints = self._rel_var_endpoints.get(var)
            if endpoints:
                src_alias, src_nm, _, _, _ = endpoints
                return self._build_node_object_expr(src_alias, src_nm).sql(dialect="trino")
            path_info = self._path_vars.get(var)
            if path_info:
                src_alias, _, _ = path_info
                src_info = self._var_table.get(src_alias)
                if src_info and src_info[1]:
                    return self._build_node_object_expr(src_alias, src_info[1]).sql(dialect="trino")
                return f"{src_alias}.*"
            return m.group(0)

        def _endnode_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            endpoints = self._rel_var_endpoints.get(var)
            if endpoints:
                _, _, tgt_alias, tgt_nm, _ = endpoints
                return self._build_node_object_expr(tgt_alias, tgt_nm).sql(dialect="trino")
            path_info = self._path_vars.get(var)
            if path_info:
                _, tgt_alias, _ = path_info
                tgt_info = self._var_table.get(tgt_alias)
                if tgt_info and tgt_info[1]:
                    return self._build_node_object_expr(tgt_alias, tgt_info[1]).sql(dialect="trino")
                return f"{tgt_alias}.*"
            return m.group(0)

        def _properties_repl(m: re.Match) -> str:
            var = m.group(1).strip()
            info = self._var_table.get(var)
            if info and info[1]:
                nm = info[1]
                sql_alias = info[0]
                exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                for prop_name, col_name in nm.properties.items():
                    exprs.append(exp.Literal.string(prop_name))
                    exprs.append(
                        exp.Column(
                            this=exp.Identifier(this=col_name, quoted=True),
                            table=exp.Identifier(this=sql_alias),
                        )
                    )
                return exp.Anonymous(this="JSON_OBJECT", expressions=exprs).sql(dialect="trino")
            if var in self._rel_var_types:
                return "JSON_OBJECT()"
            return m.group(0)

        text = re.sub(
            r"\brelationship(?:s)?\s*\(\s*([A-Za-z_]\w*)\s*\)",
            _relationships_repl,
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\bnodes\s*\(\s*([A-Za-z_]\w*)\s*\)", _nodes_repl, text, flags=re.IGNORECASE)
        text = re.sub(
            r"\bstartNode\s*\(\s*([A-Za-z_]\w*)\s*\)", _startnode_repl, text, flags=re.IGNORECASE
        )
        text = re.sub(
            r"\bendNode\s*\(\s*([A-Za-z_]\w*)\s*\)", _endnode_repl, text, flags=re.IGNORECASE
        )
        text = re.sub(
            r"\bproperties\s*\(\s*([A-Za-z_]\w*)\s*\)", _properties_repl, text, flags=re.IGNORECASE
        )
        return text

    def _parse_expr(self, text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Parse a Cypher expression fragment into a SQLGlot expression."""
        text = self._rewrite_params_in_expr(text)
        text = _rewrite_cypher_dquote_strings(text)
        text = self._rewrite_cte_vars(text)
        text = self._rewrite_call_bound_vars(text)
        text = self._rewrite_node_var_in_aggs(text)
        text = self._rewrite_cypher_props(text)
        text = self._rewrite_map_projections(text)
        text = rewrite_bare_map_literals(text)
        text = self._rewrite_graph_fns(text)
        text = self._rewrite_path_comprehensions(text)
        text = rewrite_list_comprehensions(text)
        text = _rewrite_in_list(text)
        text = _rewrite_list_slices(text)
        text = _rewrite_string_predicates(text)
        text = _rewrite_property_access(text)
        text = self._rewrite_subquery_exprs(text)
        try:
            parsed = sqlglot.parse_one(text, dialect="postgres")
            return parsed.transform(_rewrite_cypher_fn_node)
        except Exception:
            try:
                from sqlglot.errors import ErrorLevel

                parsed = sqlglot.parse_one(text, dialect="postgres", error_level=ErrorLevel.IGNORE)
                if parsed is not None:
                    return parsed.transform(_rewrite_cypher_fn_node)
            except Exception:
                pass
            return exp.column(text)

    def _rewrite_cte_vars(self, text: str) -> str:
        for var in self._cte_sources:
            sql_alias = self._var_table.get(var, (var, None))[0]
            if sql_alias != var:
                text = re.sub(rf"\b{re.escape(var)}\.", f"{sql_alias}.", text)
        return text

    def _rewrite_call_bound_vars(self, text: str) -> str:
        """Qualify CALL-subquery return vars with their CROSS JOIN LATERAL alias.

        e.g. d_list → _call0."d_list" so Trino can resolve the scoped column.
        """
        for var, lateral_alias in self._call_var_to_lateral.items():
            # Match bare var not already table-qualified (not preceded by . or word char)
            text = re.sub(
                rf"(?<![.\w]){re.escape(var)}\b",
                f'{lateral_alias}."{var}"',
                text,
            )
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
        zero rows regardless of query structure — we can skip the Trino round-trip.

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

    def _build_all_rels_union(
        self,
        src_var: str | None,
        rel_var: str | None,
        tgt_var: str | None,
        src_domain: str | None = None,
        tgt_domain: str | None = None,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all relationship types for fully-unlabeled patterns.

        src_domain / tgt_domain constrain the union to only include rels whose
        source / target node belongs to the named domain (PascalCase domain label).
        Pass None or "__all__" to leave that side unconstrained.
        """
        src_col = src_var or "n"
        rel_col = rel_var or "r"
        tgt_col = tgt_var or "m"
        alias = "_all_rels"
        self._all_rels_alias = alias
        self._all_rels_src_col = src_col
        self._all_rels_rel_col = rel_col
        self._all_rels_tgt_col = tgt_col

        src_type_set = (
            set(self._lm.domains.get(src_domain, []))
            if src_domain and src_domain != "__all__"
            else None
        )
        tgt_type_set = (
            set(self._lm.domains.get(tgt_domain, []))
            if tgt_domain and tgt_domain != "__all__"
            else None
        )

        branches: list[exp.Select] = []
        for rm in self._lm.relationships.values():
            src_nm = self._lm.nodes.get(rm.source_label)
            tgt_nm = self._lm.nodes.get(rm.target_label)
            if src_nm is None or tgt_nm is None:
                continue
            if src_type_set is not None and rm.source_label not in src_type_set:
                continue
            if tgt_type_set is not None and rm.target_label not in tgt_type_set:
                continue
            # Skip synthetic constant-join rels (e.g. HAS_TABLE to meta tables).
            # These are not real FK traversals and would pull in unrelated domain nodes.
            if rm.source_constant is not None:
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
            src_compound_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Literal.string(src_nm.label),
                    expression=exp.Literal.string("|"),
                ),
                expression=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
            )
            src_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for prop_name, col_name in src_nm.properties.items():
                src_props_exprs.extend(
                    [
                        exp.Literal.string(prop_name),
                        exp.Column(
                            this=exp.Identifier(this=col_name, quoted=True),
                            table=exp.Identifier(this=sa),
                        ),
                    ]
                )
            src_json = exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    src_compound_id,
                    exp.Literal.string("label"),
                    exp.Literal.string(src_nm.label),
                    exp.Literal.string("tableLabel"),
                    exp.Literal.string(src_nm.table_label),
                    exp.Literal.string("properties"),
                    exp.Anonymous(this="JSON_OBJECT", expressions=src_props_exprs),
                ],
            )
            tgt_compound_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Literal.string(tgt_nm.label),
                    expression=exp.Literal.string("|"),
                ),
                expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
            )
            tgt_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
            for prop_name, col_name in tgt_nm.properties.items():
                tgt_props_exprs.extend(
                    [
                        exp.Literal.string(prop_name),
                        exp.Column(
                            this=exp.Identifier(this=col_name, quoted=True),
                            table=exp.Identifier(this=ta),
                        ),
                    ]
                )
            tgt_json = exp.Anonymous(
                this="JSON_OBJECT",
                expressions=[
                    exp.Literal.string("id"),
                    tgt_compound_id,
                    exp.Literal.string("label"),
                    exp.Literal.string(tgt_nm.label),
                    exp.Literal.string("tableLabel"),
                    exp.Literal.string(tgt_nm.table_label),
                    exp.Literal.string("properties"),
                    exp.Anonymous(this="JSON_OBJECT", expressions=tgt_props_exprs),
                ],
            )
            edge_id = exp.DPipe(
                this=exp.DPipe(
                    this=exp.Cast(this=src_id_col, to=exp.DataType.build("VARCHAR")),
                    expression=exp.Literal.string("-"),
                ),
                expression=exp.Cast(this=tgt_id_col, to=exp.DataType.build("VARCHAR")),
            )
            edge_json = exp.JSONObject(
                expressions=[
                    exp.JSONKeyValue(this=exp.Literal.string("id"), expression=edge_id),
                    exp.JSONKeyValue(
                        this=exp.Literal.string("type"), expression=exp.Literal.string(rm.rel_type)
                    ),
                    exp.JSONKeyValue(this=exp.Literal.string("startNode"), expression=src_json),
                    exp.JSONKeyValue(this=exp.Literal.string("endNode"), expression=tgt_json),
                ]
            )

            branch = (
                exp.select(
                    exp.alias_(src_json, src_col),
                    exp.alias_(edge_json, rel_col),
                    exp.alias_(tgt_json, tgt_col),
                )
                .from_(
                    exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                        ),
                        alias=sa,
                    )
                )
                .join(
                    exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                        ),
                        alias=ta,
                    ),
                    on=exp.EQ(
                        this=(
                            _const_literal(rm.source_constant)
                            if rm.source_constant is not None
                            else (
                                exp.maybe_parse(
                                    rm.source_expr.replace("{alias}", sa),
                                    dialect="trino",
                                )
                                if rm.source_expr is not None
                                else exp.Column(
                                    this=exp.Identifier(this=rm.join_source_column, quoted=True),
                                    table=exp.Identifier(this=sa),
                                )
                            )
                        ),
                        expression=(
                            exp.maybe_parse(
                                rm.target_expr.replace("{alias}", ta),
                                dialect="trino",
                            )
                            if rm.target_expr is not None
                            else exp.Column(
                                this=exp.Identifier(this=rm.join_target_column, quoted=True),
                                table=exp.Identifier(this=ta),
                            )
                        ),
                    ),
                    join_type="INNER",
                )
            )
            branches.append(branch)

        if not branches:
            raise CypherTranslateError("No relationship types found in schema")

        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=alias)  # pyright: ignore[reportReturnType]

    def _build_domain_union(self, var: str, domain_name: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all types in a domain."""
        type_labels = (
            list(self._lm.nodes.keys())
            if domain_name == "__all__"
            else self._lm.domains[domain_name]
        )
        props = self._collect_var_props(var)

        branches: list[exp.Select] = []
        had_resolvable = False
        for label in type_labels:
            nm = self._lm.nodes.get(label)
            if nm is None:
                continue
            if nm.native_filter_columns:
                continue
            had_resolvable = True
            # Metadata-based prune: skip branches where no required property exists.
            # Those branches can only contribute NULL rows, which a WHERE filter would discard.
            # This prevents full-table scans across all entity types on Trino.
            if props and not any(nm.properties.get(p) for p in props):
                continue
            select_items: list[exp.Expr] = [
                exp.alias_(exp.Literal.string(nm.label), alias="__label"),
                exp.alias_(
                    exp.DPipe(
                        this=exp.DPipe(
                            this=exp.Literal.string(nm.label),
                            expression=exp.Literal.string("|"),
                        ),
                        expression=exp.Cast(
                            this=exp.Column(this=exp.Identifier(this=nm.id_column, quoted=True)),
                            to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                        ),
                    ),
                    alias="__id",
                ),
            ]
            for prop in props:
                phys_col = nm.physical_properties.get(prop)
                if phys_col:
                    select_items.append(
                        exp.alias_(
                            exp.Cast(
                                this=exp.Column(this=exp.Identifier(this=phys_col, quoted=True)),
                                to=exp.DataType(this=exp.DataType.Type.VARCHAR),
                            ),
                            alias=exp.Identifier(this=prop, quoted=True),
                        )
                    )
                else:
                    select_items.append(
                        exp.alias_(exp.null(), alias=exp.Identifier(this=prop, quoted=True))
                    )
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
            if not had_resolvable:
                raise CypherTranslateError(f"Domain {domain_name!r} has no resolvable types")
            # All types exist but none have the required properties — return zero rows without
            # scanning any tables (metadata resolved this at translation time).
            zero_items: list[exp.Expr] = [
                exp.alias_(exp.null(), alias="__label"),
                exp.alias_(exp.null(), alias="__id"),
            ] + [exp.alias_(exp.null(), alias=exp.Identifier(this=p, quoted=True)) for p in props]
            zero_row = exp.select(*zero_items).where(exp.false())
            return exp.alias_(exp.Subquery(this=zero_row), alias=var)  # pyright: ignore[reportReturnType]

        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for branch in branches[1:]:
            union = exp.Union(this=union, expression=branch, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=var)  # pyright: ignore[reportReturnType]

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


_CYPHER_DQUOTE_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _rewrite_cypher_dquote_strings(expr: str) -> str:
    """Convert Cypher double-quoted string literals to SQL single-quoted literals.

    Only converts strings not preceded by `.` (those are quoted identifiers, not literals).
    Runs before _rewrite_property_access so no quoted identifiers exist yet.
    """
    result = []
    pos = 0
    for m in _CYPHER_DQUOTE_RE.finditer(expr):
        start = m.start()
        result.append(expr[pos:start])
        # If preceded by `.`, it's a property name — leave as-is
        if start > 0 and expr[start - 1] == ".":
            result.append(m.group(0))
        else:
            inner = m.group(1).replace("'", "\\'")
            result.append(f"'{inner}'")
        pos = m.end()
    result.append(expr[pos:])
    return "".join(result)


def _rewrite_property_access(expr: str) -> str:
    """Rewrite n.prop → n."prop" for SQL."""
    return re.sub(
        r"\b([A-Za-z_]\w*)\s*\.\s*([A-Za-z_]\w*)\b",
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
    "RANGE": "sequence",  # Cypher range(start, end[, step]) → sequence(start, end[, step])
    "LOG": "ln",  # Neo4j log() = natural log = Trino ln()
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
_STRING_PREDICATE_REWRITES: list[tuple[re.Pattern[str], str | Callable[[re.Match[str]], str]]] = [
    # n . name STARTS WITH 'x'  →  starts_with(n.name, 'x')
    # Also handles function calls on the left: toLower(n.name) STARTS WITH 'x'
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?|\w+\s*\([^)]*\))\s+STARTS\s+WITH\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"starts_with({m.group(1).replace(' ', '')}, {m.group(2)})",
    ),
    # n . name ENDS WITH 'x'  →  (n.name LIKE CONCAT('%', 'x'))
    # Also handles function calls on the left: toLower(n.name) ENDS WITH 'x'
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?|\w+\s*\([^)]*\))\s+ENDS\s+WITH\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
            re.IGNORECASE,
        ),
        lambda m: f"({m.group(1).replace(' ', '')} LIKE CONCAT('%', {m.group(2)}))",
    ),
    # n . name CONTAINS 'x'  →  (strpos(n.name, 'x') > 0)
    # Also handles function calls on the left: toLower(n.name) CONTAINS 'x'
    (
        re.compile(
            r"([\w]+(?:\s*\.\s*[\w]+)?|\w+\s*\([^)]*\))\s+CONTAINS\s+('(?:[^'\\]|\\.)*'|[\w.$]+)",
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


_ISO_TS_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)'")


def _coerce_ts_literals(text: str) -> str:
    """Wrap ISO-datetime string literals as TIMESTAMP '...' so Trino doesn't see varchar(N)."""
    return _ISO_TS_LITERAL_RE.sub(lambda m: f"TIMESTAMP {m.group(0)}", text)


_IN_LIST_RE = re.compile(r"\bIN\s*\[([^\[\]]*)\]", re.IGNORECASE)


def _rewrite_in_list(text: str) -> str:
    """Rewrite Cypher IN [...] literal list to SQL IN (...)."""
    return _IN_LIST_RE.sub(r"IN (\1)", text)


_LIST_SLICE_RE = re.compile(r"(\w+\s*\(\s*[^)]*\s*\)|[A-Za-z_]\w*)\s*\[\s*\.\.\s*(\d+)\s*\]")


def _rewrite_list_slices(text: str) -> str:
    """Rewrite Cypher list-slice expr[..n] → slice(expr, 1, n) for Trino.

    Cypher's [..n] returns the first n elements (0-indexed, exclusive end).
    Trino's slice(arr, start, length) is 1-indexed with a length argument.
    """
    return _LIST_SLICE_RE.sub(r"slice(\1, 1, \2)", text)


def _rewrite_cypher_fn_node(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
    args: list[exp.Expression] = node.args.get("expressions") or []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    if name == "HEAD" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(1)])

    if name == "LAST" and args:
        return exp.Anonymous(this="element_at", expressions=[args[0], exp.Literal.number(-1)])

    if name == "TAIL" and args:
        return exp.Anonymous(
            this="slice",
            expressions=[
                args[0],
                exp.Literal.number(2),
                exp.Anonymous(this="cardinality", expressions=[args[0]]),
            ],
        )

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

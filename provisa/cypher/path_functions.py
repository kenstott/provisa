# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3a9c1f-4b2d-4e8a-9f5c-1d6b8e2a4f7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PathFunctionsMixin — shortestPath / allShortestPaths translation.

Separated from translator.py to keep that file under 1000 lines.
Mixed into _Translator; relies on _lm, _var_table, _extra_path_branches,
_recursive_ctes, _shortestpath_hops_col, _shortestpath_is_all.
"""

from __future__ import annotations

from typing import Any, cast

import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import MatchClause, NodePattern, PathFunction, RelPattern


def _parse_sql_expr(sql: str) -> Any:
    return sqlglot.parse_one(sql, dialect="trino")


# ---------------------------------------------------------------------------
# Module-level pure helpers (no mixin state)
# ---------------------------------------------------------------------------


def _extract_path_nodes(
    pf: PathFunction,
    CypherTranslateError: type,
) -> tuple[NodePattern, NodePattern, RelPattern | None]:
    """Validate and extract src/tgt nodes and optional rel from a PathFunction."""
    pattern = pf.pattern
    if len(pattern.nodes) < 2:
        raise CypherTranslateError("Path function requires at least two nodes")
    src_node = pattern.nodes[0]
    tgt_node = pattern.nodes[-1]
    if not src_node.labels or not tgt_node.labels:
        raise CypherTranslateError(
            "shortestPath/allShortestPaths require labeled source and target nodes"
        )
    rel: RelPattern | None = pattern.rels[0] if pattern.rels else None
    return src_node, tgt_node, rel


def _register_path_vars(
    var_table: dict,
    src_var: str | None,
    tgt_var: str | None,
    src_nm: NodeMapping,
    tgt_nm: NodeMapping,
) -> None:
    """Register src/tgt node variables in the var_table if not already present."""
    if src_var and src_var not in var_table:
        var_table[src_var] = (src_var, src_nm)
    if tgt_var and tgt_var not in var_table:
        var_table[tgt_var] = (tgt_var, tgt_nm)


def _extract_rel_attrs(
    rel: RelPattern | None,
) -> tuple[list[str] | None, bool, bool, int]:
    """Extract rel_types, variable_length, is_undirected, max_hops from an optional RelPattern."""
    rel_types = [rt.upper() for rt in rel.types] if rel and rel.types else None
    variable_length = bool(rel and rel.variable_length)
    is_undirected = bool(rel and getattr(rel, "direction", "right") == "none")
    if variable_length:
        max_hops = rel.max_hops if rel is not None and rel.max_hops is not None else 10
    else:
        max_hops = 1
    return rel_types, variable_length, is_undirected, max_hops


def _filter_allowed_rels(
    lm: CypherLabelMap,
    rel_types: list[str] | None,
) -> list[RelationshipMapping]:
    """Return schema rels matching the given rel_types filter (None = all).

    Relationships with source_constant are synthetic meta-navigation joins
    (e.g. HAS_TABLE).  They anchor every data row to a single registered_tables
    entry via a constant, not a real FK column, so they cannot be traversed as
    intermediate hops in a variable-length path without producing impossible
    contradictory join conditions.  Exclude them unless the caller explicitly
    names them.
    """
    return [
        r
        for r in lm.relationships.values()
        if (rel_types is None or r.rel_type in rel_types)
        and (rel_types is not None or r.source_constant is None)
    ]


def _needs_recursive_cte(
    variable_length: bool,
    is_undirected: bool,
    src_type: str,
    tgt_type: str,
    allowed_rels: list[RelationshipMapping],
) -> bool:
    """Return True when a recursive CTE is required instead of flat JOINs.

    Recursive is needed when src==tgt label (same-type start/end) OR any
    allowed rel loops back to the same node type (e.g. KNOWS: Person→Person).
    """
    return (
        variable_length
        and not is_undirected
        and (src_type == tgt_type or any(r.source_label == r.target_label for r in allowed_rels))
    )


def _select_candidate_paths(
    all_paths: list[list[RelationshipMapping]],
    is_all_paths: bool,
    is_undirected: bool,
) -> list[list[RelationshipMapping]]:
    """Choose which schema paths to emit as UNION branches."""
    if is_all_paths and not is_undirected:
        return all_paths
    min_hops = min(len(p) for p in all_paths)
    return [p for p in all_paths if len(p) == min_hops]


class PathFunctionsMixin:
    """Mixin for _Translator: translates shortestPath and allShortestPaths."""

    # Injected by _Translator.__init__
    _lm: CypherLabelMap
    _var_table: dict
    _extra_path_branches: list
    _recursive_ctes: list  # list[(cte_name, exp.Expression)]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _shortestpath_hops_col: exp.Expression | None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _shortestpath_is_all: bool
    _path_vars: dict  # path_var → (src_var, tgt_var, is_recursive)

    def _translate_path_function(self, clause: MatchClause) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Entry point: route to flat-JOIN or recursive-CTE path."""
        from provisa.cypher.translator import CypherTranslateError  # avoid circular at module level

        pf: PathFunction = clause.pattern  # type: ignore[assignment]
        src_node, tgt_node, rel = _extract_path_nodes(pf, CypherTranslateError)
        src_type, tgt_type, src_nm, tgt_nm = self._resolve_path_node_types(
            src_node, tgt_node, CypherTranslateError
        )
        src_var = src_node.variable
        tgt_var = tgt_node.variable
        _register_path_vars(self._var_table, src_var, tgt_var, src_nm, tgt_nm)

        rel_types, variable_length, is_undirected, max_hops = _extract_rel_attrs(rel)
        allowed_rels = _filter_allowed_rels(self._lm, rel_types)
        needs_recursive = _needs_recursive_cte(
            variable_length, is_undirected, src_type, tgt_type, allowed_rels
        )

        if clause.variable and src_var and tgt_var:
            self._path_vars[clause.variable] = (src_var, tgt_var, needs_recursive)

        is_all_paths = pf.func_name.lower() == "allpaths"

        if needs_recursive:
            return self._translate_path_recursive_branch(
                clause,
                pf,
                src_var,
                tgt_var,
                src_nm,
                tgt_nm,
                src_type,
                tgt_type,
                allowed_rels,
                max_hops,
                is_all_paths,
                CypherTranslateError,
            )

        return self._translate_path_flat_branch(
            clause,
            src_var,
            tgt_var,
            src_nm,
            tgt_nm,
            src_type,
            tgt_type,
            rel_types,
            max_hops,
            is_undirected,
            is_all_paths,
            CypherTranslateError,
        )

    def _translate_path_recursive_branch(
        self,
        clause: MatchClause,
        pf: PathFunction,
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
        src_type: str,
        tgt_type: str,
        allowed_rels: list[RelationshipMapping],
        max_hops: int,
        is_all_paths: bool,
        CypherTranslateError: type,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Dispatch to recursive CTE translation after validating base rels exist."""
        base_rels = [r for r in allowed_rels if r.source_label == src_type]
        if not base_rels:
            raise CypherTranslateError(
                f"No schema path found from {src_type!r} to {tgt_type!r} within {max_hops} hops"
            )
        return self._translate_path_function_recursive(
            clause,
            src_var,
            tgt_var,
            src_nm,
            tgt_nm,
            src_type,
            tgt_type,
            allowed_rels,
            max_hops,
            is_all=pf.func_name.lower() in ("allshortestpaths", "allpaths"),
            suppress_hops_order=is_all_paths,
        )

    def _translate_path_flat_branch(
        self,
        clause: MatchClause,
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
        src_type: str,
        tgt_type: str,
        rel_types: list[str] | None,
        max_hops: int,
        is_undirected: bool,
        is_all_paths: bool,
        CypherTranslateError: type,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build flat JOIN path for non-self-referential variable-length or fixed-length."""
        all_paths = self._lm.find_paths(
            src_type, tgt_type, rel_types, max_hops, bidirectional=is_undirected
        )
        if not all_paths:
            direction_hint = " (undirected — both directions searched)" if is_undirected else ""
            raise CypherTranslateError(
                f"No schema path found from {src_type!r} to {tgt_type!r} "
                f"within {max_hops} hops{direction_hint}"
            )

        candidate_paths = _select_candidate_paths(all_paths, is_all_paths, is_undirected)
        return self._build_flat_join_result(
            clause, candidate_paths, src_var, tgt_var, src_nm, tgt_nm
        )

    def _build_flat_join_result(
        self,
        clause: MatchClause,
        candidate_paths: list[list[RelationshipMapping]],
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build primary join chain and register extra path branches."""
        primary_from, primary_joins, step_nodes, step_edges = self._build_path_join_chain(
            candidate_paths[0], src_var, tgt_var, src_nm, tgt_nm, clause.optional
        )
        if clause.variable is not None:
            self._path_steps[clause.variable] = (step_nodes, step_edges)  # type: ignore[attr-defined]
        for extra_path in candidate_paths[1:]:
            self._register_extra_path_branch(clause, extra_path, src_var, tgt_var, src_nm, tgt_nm)
        return primary_from, primary_joins

    def _register_extra_path_branch(
        self,
        clause: MatchClause,
        extra_path: list[RelationshipMapping],
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
    ) -> None:
        """Build and append one extra UNION branch for an alternate schema path."""
        extra_from, extra_joins, extra_step_nodes, extra_step_edges = self._build_path_join_chain(
            extra_path, src_var, tgt_var, src_nm, tgt_nm, clause.optional
        )
        extra_path_steps_map: dict[str, tuple[list, list]] = {}
        if clause.variable is not None:
            extra_path_steps_map[clause.variable] = (extra_step_nodes, extra_step_edges)
        self._extra_path_branches.append((extra_from, extra_joins, extra_path_steps_map))

    def _resolve_path_node_types(
        self,
        src_node: NodePattern,
        tgt_node: NodePattern,
        CypherTranslateError: type,
    ) -> tuple[str, str, NodeMapping, NodeMapping]:
        """Resolve src/tgt labels to type strings and NodeMappings."""
        src_type, _ = self._resolve_node_type(src_node.labels)  # type: ignore[attr-defined]
        tgt_type, _ = self._resolve_node_type(tgt_node.labels)  # type: ignore[attr-defined]
        if src_type is None or tgt_type is None:
            raise CypherTranslateError(
                "shortestPath/allShortestPaths require type-labeled (not domain-only) nodes"
            )
        src_nm = self._lm.nodes[src_type]
        tgt_nm = self._lm.nodes[tgt_type]
        return src_type, tgt_type, src_nm, tgt_nm

    # ------------------------------------------------------------------
    # Flat JOIN chain (non-self-referential)
    # ------------------------------------------------------------------

    def _build_path_join_chain(
        self,
        path: list[RelationshipMapping],
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
        optional: bool,
    ) -> tuple[
        exp.Expression,
        list[dict],
        list[tuple[str, NodeMapping]],
        list[tuple[str, str, NodeMapping, str, NodeMapping, bool]],
    ]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build FROM + JOIN list for a single flat schema path."""
        src_alias = src_var or src_nm.table_name
        from_expr = exp.alias_(
            exp.Table(
                this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
            ),
            alias=src_alias,
        )
        joins: list[dict] = []
        join_type = "LEFT" if optional else "INNER"
        prev_alias = src_alias
        prev_nm = src_nm
        step_nodes: list[tuple[str, NodeMapping]] = [(src_alias, src_nm)]
        step_edges: list[tuple[str, str, NodeMapping, str, NodeMapping, bool]] = []

        for i, rel_mapping in enumerate(path):
            nxt_nm = self._lm.nodes[rel_mapping.target_label]
            is_last = i == len(path) - 1
            nxt_alias = (tgt_var or nxt_nm.table_name) if is_last else f"_hop{i + 1}"
            on_cond = exp.EQ(
                this=(
                    exp.Literal.string(str(rel_mapping.source_constant))
                    if rel_mapping.source_constant is not None
                    else (
                        _parse_sql_expr(
                            rel_mapping.source_expr.replace("{alias}", f'"{prev_alias}"')
                        )
                        if rel_mapping.source_expr is not None
                        else exp.Column(
                            this=exp.Identifier(this=rel_mapping.join_source_column, quoted=True),
                            table=exp.Identifier(this=prev_alias),
                        )
                    )
                ),
                expression=(
                    _parse_sql_expr(rel_mapping.target_expr.replace("{alias}", f'"{nxt_alias}"'))
                    if rel_mapping.target_expr is not None
                    else exp.Column(
                        this=exp.Identifier(this=rel_mapping.join_target_column, quoted=True),
                        table=exp.Identifier(this=nxt_alias),
                    )
                ),
            )
            join_table = exp.alias_(
                exp.Table(
                    this=exp.Identifier(this=nxt_nm.sql_table_name, quoted=True),
                    db=exp.Identifier(this=nxt_nm.schema_name, quoted=True),
                    catalog=exp.Identifier(this=nxt_nm.catalog_name, quoted=True),
                ),
                alias=nxt_alias,
            )
            joins.append({"table": join_table, "on": on_cond, "join_type": join_type})
            step_nodes.append((nxt_alias, nxt_nm))
            # Detect if this edge is traversed in reverse of its canonical direction.
            # Canonical source_label is stored on the original RelationshipMapping in self._lm.
            canonical_rel = self._lm.relationships.get(rel_mapping.rel_type)
            is_reversed = (
                canonical_rel is not None and canonical_rel.source_label != rel_mapping.source_label
            )
            step_edges.append(
                (rel_mapping.rel_type, prev_alias, prev_nm, nxt_alias, nxt_nm, is_reversed)
            )
            prev_alias = nxt_alias
            prev_nm = nxt_nm

        return cast(exp.Expression, from_expr), joins, step_nodes, step_edges  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # Recursive CTE (self-referential variable-length paths)
    # ------------------------------------------------------------------

    def _translate_path_function_recursive(
        self,
        clause: MatchClause,
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
        src_type: str,
        tgt_type: str,
        allowed_rels: list[RelationshipMapping],
        max_hops: int,
        is_all: bool,
        suppress_hops_order: bool = False,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Emit WITH RECURSIVE CTE for paths that may repeat edge traversals in data."""
        cte_name = f"_traverse_{src_var or src_type.lower()}"
        cte_expr = self._build_recursive_cte(cte_name, src_nm, allowed_rels, max_hops)
        self._recursive_ctes.append((cte_name, cte_expr))

        src_alias = src_var or src_nm.table_name
        tgt_alias = tgt_var or tgt_nm.table_name

        from_expr = exp.alias_(
            exp.Table(
                this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
            ),
            alias=src_alias,
        )
        join_type = "LEFT" if clause.optional else "INNER"

        # JOIN _traverse_X AS _t ON _t.src_id = src.id AND _t.cur_type = tgt_label
        traverse_join_on = exp.And(
            this=exp.EQ(
                this=exp.Column(
                    this=exp.Identifier(this="src_id"),
                    table=exp.Identifier(this="_t"),
                ),
                expression=exp.Column(
                    this=exp.Identifier(this=src_nm.id_column, quoted=True),
                    table=exp.Identifier(this=src_alias),
                ),
            ),
            expression=exp.EQ(
                this=exp.Column(
                    this=exp.Identifier(this="cur_type"),
                    table=exp.Identifier(this="_t"),
                ),
                expression=exp.Literal.string(tgt_type),
            ),
        )

        # JOIN tgt_table AS tgt_alias ON tgt_alias.id = _t.cur_id
        tgt_join_on = exp.EQ(
            this=exp.Column(
                this=exp.Identifier(this=tgt_nm.id_column, quoted=True),
                table=exp.Identifier(this=tgt_alias),
            ),
            expression=exp.Column(
                this=exp.Identifier(this="cur_id"),
                table=exp.Identifier(this="_t"),
            ),
        )

        joins = [
            {
                "table": exp.alias_(
                    exp.Table(this=exp.Identifier(this=cte_name)),
                    alias="_t",
                ),
                "on": traverse_join_on,
                "join_type": join_type,
            },
            {
                "table": exp.alias_(
                    exp.Table(
                        this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                    ),
                    alias=tgt_alias,
                ),
                "on": tgt_join_on,
                "join_type": join_type,
            },
        ]

        # Signal translate() to add ORDER BY hops [LIMIT 1] — suppressed for allPaths()
        if not suppress_hops_order:
            self._shortestpath_hops_col = exp.Column(
                this=exp.Identifier(this="hops"),
                table=exp.Identifier(this="_t"),
            )
        self._shortestpath_is_all = is_all
        return cast(exp.Expression, from_expr), joins  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _build_recursive_cte(
        self,
        cte_name: str,
        src_nm: NodeMapping,
        allowed_rels: list[RelationshipMapping],
        max_hops: int,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build the UNION ALL body of the recursive CTE.

        Base case: one branch per allowed rel whose source_label == src_nm.label.
        Recursive step: one branch per allowed rel expanding from any current node type.

        CTE schema: (src_id, cur_type, cur_id, hops)
          src_id  — source row id (anchored to the start of the path)
          cur_type — label of the current frontier node
          cur_id   — id of the current frontier node
          hops     — number of edges traversed so far (>= 1)
        """

        def _tbl(nm: NodeMapping, alias: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            return cast(
                exp.Expression,
                exp.alias_(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                    exp.Table(
                        this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                        db=exp.Identifier(this=nm.schema_name, quoted=True),
                        catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
                    ),
                    alias=alias,
                ),
            )

        # ------------------------------------------------------------------
        # Base case: seed with 1-hop expansions from the source type
        # ------------------------------------------------------------------
        base_branches: list[exp.Select] = []
        for rel in allowed_rels:
            if rel.source_label != src_nm.type_name:
                continue
            src_node_m = self._lm.nodes.get(rel.source_label)
            tgt_node_m = self._lm.nodes.get(rel.target_label)
            if src_node_m is None or tgt_node_m is None:
                continue

            branch = (
                exp.select(
                    exp.alias_(
                        exp.Column(
                            this=exp.Identifier(this=src_node_m.id_column, quoted=True),
                            table=exp.Identifier(this="_seed"),
                        ),
                        alias="src_id",
                    ),
                    exp.alias_(exp.Literal.string(tgt_node_m.type_name), alias="cur_type"),
                    exp.alias_(
                        exp.Column(
                            this=exp.Identifier(this=tgt_node_m.id_column, quoted=True),
                            table=exp.Identifier(this="_nxt"),
                        ),
                        alias="cur_id",
                    ),
                    exp.alias_(exp.Literal.number(1), alias="hops"),
                )
                .from_(_tbl(src_node_m, "_seed"))
                .join(
                    _tbl(tgt_node_m, "_nxt"),
                    on=exp.EQ(
                        this=(
                            exp.Literal.string(str(rel.source_constant))
                            if rel.source_constant is not None
                            else (
                                _parse_sql_expr(rel.source_expr.replace("{alias}", '"_seed"'))
                                if rel.source_expr is not None
                                else exp.Column(
                                    this=exp.Identifier(this=rel.join_source_column, quoted=True),
                                    table=exp.Identifier(this="_seed"),
                                )
                            )
                        ),
                        expression=(
                            _parse_sql_expr(rel.target_expr.replace("{alias}", '"_nxt"'))
                            if rel.target_expr is not None
                            else exp.Column(
                                this=exp.Identifier(this=rel.join_target_column, quoted=True),
                                table=exp.Identifier(this="_nxt"),
                            )
                        ),
                    ),
                    join_type="INNER",
                )
            )
            base_branches.append(branch)

        # ------------------------------------------------------------------
        # Recursive step: extend frontier by one hop for each allowed rel
        # ------------------------------------------------------------------
        rec_branches: list[exp.Select] = []
        for rel in allowed_rels:
            src_node_m = self._lm.nodes.get(rel.source_label)
            tgt_node_m = self._lm.nodes.get(rel.target_label)
            if src_node_m is None or tgt_node_m is None:
                continue

            branch = (
                exp.select(
                    exp.Column(
                        this=exp.Identifier(this="src_id"),
                        table=exp.Identifier(this="t"),
                    ),
                    exp.alias_(exp.Literal.string(tgt_node_m.type_name), alias="cur_type"),
                    exp.alias_(
                        exp.Column(
                            this=exp.Identifier(this=tgt_node_m.id_column, quoted=True),
                            table=exp.Identifier(this="_nxt"),
                        ),
                        alias="cur_id",
                    ),
                    exp.alias_(
                        exp.Add(
                            this=exp.Column(
                                this=exp.Identifier(this="hops"),
                                table=exp.Identifier(this="t"),
                            ),
                            expression=exp.Literal.number(1),
                        ),
                        alias="hops",
                    ),
                )
                .from_(
                    exp.alias_(
                        exp.Table(this=exp.Identifier(this=cte_name)),
                        alias="t",
                    )
                )
                # JOIN current-node table on cur_id + cur_type guard
                .join(
                    _tbl(src_node_m, "_cur"),
                    on=exp.And(
                        this=exp.EQ(
                            this=exp.Column(
                                this=exp.Identifier(this=src_node_m.id_column, quoted=True),
                                table=exp.Identifier(this="_cur"),
                            ),
                            expression=exp.Column(
                                this=exp.Identifier(this="cur_id"),
                                table=exp.Identifier(this="t"),
                            ),
                        ),
                        expression=exp.EQ(
                            this=exp.Column(
                                this=exp.Identifier(this="cur_type"),
                                table=exp.Identifier(this="t"),
                            ),
                            expression=exp.Literal.string(src_node_m.type_name),
                        ),
                    ),
                    join_type="INNER",
                )
                # JOIN next-node table
                .join(
                    _tbl(tgt_node_m, "_nxt"),
                    on=exp.EQ(
                        this=(
                            exp.Literal.string(str(rel.source_constant))
                            if rel.source_constant is not None
                            else (
                                _parse_sql_expr(rel.source_expr.replace("{alias}", '"_cur"'))
                                if rel.source_expr is not None
                                else exp.Column(
                                    this=exp.Identifier(this=rel.join_source_column, quoted=True),
                                    table=exp.Identifier(this="_cur"),
                                )
                            )
                        ),
                        expression=(
                            _parse_sql_expr(rel.target_expr.replace("{alias}", '"_nxt"'))
                            if rel.target_expr is not None
                            else exp.Column(
                                this=exp.Identifier(this=rel.join_target_column, quoted=True),
                                table=exp.Identifier(this="_nxt"),
                            )
                        ),
                    ),
                    join_type="INNER",
                )
                .where(
                    exp.LT(
                        this=exp.Column(
                            this=exp.Identifier(this="hops"),
                            table=exp.Identifier(this="t"),
                        ),
                        expression=exp.Literal.number(max_hops),
                    )
                )
            )
            rec_branches.append(branch)

        all_branches = base_branches + rec_branches
        if not all_branches:
            raise ValueError(f"No traversal branches for recursive CTE {cte_name!r}")

        result: exp.Expression = all_branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for branch in all_branches[1:]:
            result = exp.Union(this=result, expression=branch, distinct=False)
        return result

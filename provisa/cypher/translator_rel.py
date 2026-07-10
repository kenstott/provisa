# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship-resolution and JOIN-building mixin for the Cypher→SQL translator.

Methods operate on _Translator state via self; mixed into _Translator, never
instantiated alone.
"""

from __future__ import annotations


import sqlglot.expressions as exp

from provisa.cypher.parser import (
    MatchClause,
    NodePattern,
    PathPattern,
    PathFunction,
    RelPattern,
)
from provisa.cypher.label_map import NodeMapping, RelationshipMapping
from provisa.cypher.translator_helpers import (
    _is_bwd_for_candidate,
    _join_alias,
    _make_rel_join,
    _node_table_expr,
    _src_col_expr_for_rm,
    _tgt_col_expr_for_rm,
)

from provisa.cypher.translator_types import CypherTranslateError


class _RelJoinMixin:  # mixin for _Translator
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

    # complexity-gate: allow-cc=54 reason="relocated verbatim from translator.py; CC is the per-MATCH-clause FROM/JOIN assembly dispatch (first-node vs rel-step vs standalone vs domain-union across optional/varlen/lateral cases); decomposition is separately-tracked debt"
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

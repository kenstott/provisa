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

import dataclasses
from typing import Any, cast

import sqlglot
import sqlglot.expressions as exp

from provisa.core import settings_registry
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
)
from provisa.gql.ir import MatchClause, NodePattern, PathFunction, RelPattern
from provisa.gql.path_walk import UNLABELLED_ENDS
from provisa.gql.translator_helpers import _make_junction_joins  # REQ-1586

# Requirements: REQ-345, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351


def _parse_sql_expr(sql: str) -> Any:
    return sqlglot.parse_one(sql, dialect="postgres")


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
    rel: RelPattern | None = pattern.rels[0] if pattern.rels else None
    src_node = pattern.nodes[0]
    tgt_node = pattern.nodes[-1]
    if rel is not None and rel.direction == "left":
        # (a)<-[...]-(b) is (b)-[...]->(a): the path leaves the node the arrow leaves (#151).
        src_node, tgt_node = tgt_node, src_node
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
) -> tuple[list[str] | None, bool, bool, int, int, bool]:
    """Extract rel_types, variable_length, is_undirected, max_hops, min_hops and by_limit from an
    optional RelPattern. A variable-length pattern with no lower bound starts at one hop, as
    Cypher's does; one with no upper bound walks to the operator's depth limit (REQ-2135), and
    ``by_limit`` says so, for the result to say when the walk was cut there."""
    rel_types = list(rel.types) if rel and rel.types else None
    variable_length = bool(rel and rel.variable_length)
    is_undirected = bool(rel and getattr(rel, "direction", "right") == "none")
    by_limit = variable_length and rel is not None and rel.max_hops is None
    max_hops: int = 1
    if by_limit:
        max_hops = settings_registry.value("graph.depth_limit")
    elif variable_length and rel is not None and rel.max_hops is not None:
        max_hops = rel.max_hops
    min_hops = (
        rel.min_hops if variable_length and rel is not None and rel.min_hops is not None else 1
    )
    return rel_types, variable_length, is_undirected, max_hops, min_hops, by_limit


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
        and (rel_types is not None or not r.constant_source)
    ]


def _rels_between(
    rels: list[RelationshipMapping], src_type: str, tgt_type: str
) -> list[RelationshipMapping]:
    """The relationships that lie on some directed path from ``src_type`` to ``tgt_type`` (#157):
    one whose source the path can reach and whose target the path can go on from. A relationship
    elsewhere in the model is no part of the pattern, whatever its shape."""

    def reach(start: str, step_from: str, step_to: str) -> set[str]:
        seen, frontier = {start}, [start]
        while frontier:
            at = frontier.pop()
            for r in rels:
                if getattr(r, step_from) == at and getattr(r, step_to) not in seen:
                    seen.add(getattr(r, step_to))
                    frontier.append(getattr(r, step_to))
        return seen

    from_src = reach(src_type, "source_label", "target_label")
    to_tgt = reach(tgt_type, "target_label", "source_label")
    return [r for r in rels if r.source_label in from_src and r.target_label in to_tgt]


@dataclasses.dataclass(frozen=True)
class _FreeEnd:
    """The end of a path search that no label places and no earlier clause binds to a table
    (REQ-348, ONE END BOUND OR BINDABLE): whatever node the walk reaches. ``var`` names it (None:
    anonymous); ``domain`` is the ad-hoc domain of the labels the walk can reach; ``types`` pairs
    each one's traversal type with its label; ``bound``: an earlier clause holds it, as a union of
    labels, so the walk is tied to that row rather than reading the union again."""

    var: str | None
    domain: str
    types: tuple[tuple[str, str], ...]
    bound: bool


def _free_id(var: str) -> exp.Column:
    """The identity column of a free end's union (``label|key``)."""
    return exp.Column(this=exp.Identifier(this="__id"), table=exp.Identifier(this=var))


def _within_reach(
    rels: list[RelationshipMapping], src_type: str, depth: int
) -> list[RelationshipMapping]:
    """The relationships a walk from ``src_type`` can take within ``depth`` hops: one whose source
    the walk reaches in fewer. A relationship of the model's other labels -- another domain's --
    is no step of the walk, and a label no relationship touches takes none."""
    seen, frontier = {src_type}, [src_type]
    for _ in range(depth - 1):
        frontier = [
            r.target_label
            for r in rels
            if r.source_label in frontier and r.target_label not in seen
        ]
        seen.update(frontier)
    return [r for r in rels if r.source_label in seen]


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
    if is_undirected:
        # #310: a walk from a type back to the same type, either way, recurses; one between two
        # types follows the schema's paths between them, flat.
        return variable_length and src_type == tgt_type
    return variable_length and (
        src_type == tgt_type or any(r.source_label == r.target_label for r in allowed_rels)
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


def _hop_on(rel: RelationshipMapping, src_ref: str, tgt_alias: str) -> Any:
    """The ON condition of a single FK/PK hop from ``src_ref`` to ``tgt_alias``."""
    return exp.EQ(
        this=(
            exp.Literal.string(str(rel.source_constant))
            if rel.source_constant is not None
            else (
                _parse_sql_expr(rel.source_expr.replace("{alias}", f'"{src_ref}"'))
                if rel.source_expr is not None
                else exp.Column(
                    this=exp.Identifier(this=rel.join_source_column, quoted=True),
                    table=exp.Identifier(this=src_ref),
                )
            )
        ),
        expression=(
            _parse_sql_expr(rel.target_expr.replace("{alias}", f'"{tgt_alias}"'))
            if rel.target_expr is not None
            else exp.Column(
                this=exp.Identifier(this=rel.join_target_column, quoted=True),
                table=exp.Identifier(this=tgt_alias),
            )
        ),
    )


def _reversed(rel: RelationshipMapping) -> RelationshipMapping:
    """``rel`` walked from its target to its source: an undirected pattern's step the other way
    (#310). Its key columns, join expressions and junction foreign keys trade places."""
    via = rel.via
    return dataclasses.replace(
        rel,
        source_label=rel.target_label,
        target_label=rel.source_label,
        join_source_column=rel.join_target_column,
        join_target_column=rel.join_source_column,
        source_expr=rel.target_expr,
        target_expr=rel.source_expr,
        via=None
        if via is None
        else dataclasses.replace(
            via, source_columns=via.target_columns, target_columns=via.source_columns
        ),
    )


def _text(expr: Any) -> Any:
    return exp.cast(expr, "VARCHAR")


def _edge_key(rel: RelationshipMapping, from_key: Any, to_key: Any, backward: bool) -> Any:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The edge a hop walks, named the same whichever way it is walked: its type and its two ends
    in the relationship's own direction. A hop walked backward (``backward``) names its ends the
    other way round, so walking an edge back is walking the same edge."""
    first, last = (to_key, from_key) if backward else (from_key, to_key)
    return exp.DPipe(
        this=exp.DPipe(
            this=exp.DPipe(this=exp.Literal.string(f"{rel.rel_type}:"), expression=_text(first)),
            expression=exp.Literal.string(">"),
        ),
        expression=_text(last),
    )


def _bar(expr: Any) -> Any:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``|expr|``: one edge of a trail, delimited so one edge's name never matches inside another."""
    return exp.DPipe(
        this=exp.DPipe(this=exp.Literal.string("|"), expression=expr),
        expression=exp.Literal.string("|"),
    )


def _apply_hop(
    select: Any,
    rel: RelationshipMapping,
    src_ref: str,
    tgt_nm: NodeMapping,
    tgt_alias: str,
    tgt_table: Any,
) -> Any:
    """Join one relationship hop onto a recursive-CTE branch.

    REQ-1586: a junction-backed edge joins the junction table first and the target second, with the
    discriminator on the junction hop, so a variable-length pattern counts one hop per edge rather
    than one per half-edge. Both CTE branches walk source to target, so the hop is never backwards.
    """
    if rel.via is not None:
        for d in _make_junction_joins(
            rel, False, tgt_nm, tgt_alias, src_ref, "INNER", f"{tgt_alias}__via"
        ):
            select = select.join(d["table"], on=d["on"], join_type=d["join_type"])
        return select
    return select.join(tgt_table, on=_hop_on(rel, src_ref, tgt_alias), join_type="INNER")


class PathFunctionsMixin:  # REQ-345, REQ-348, REQ-349, REQ-350, REQ-351
    """Mixin for _Translator: translates shortestPath and allShortestPaths."""

    # Injected by _Translator.__init__
    _lm: CypherLabelMap
    _var_table: dict
    _extra_path_branches: list
    _recursive_ctes: list  # list[(cte_name, exp.Expression)]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _shortestpath_hops_col: exp.Expression | None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _shortestpath_is_all: bool
    _path_vars: dict  # path_var → (src_var, tgt_var, is_recursive)
    _cut_checks: list  # list[exp.Expression]: true when a walk was cut at the depth limit

    def _translate_path_function(
        self, clause: MatchClause, joined: set[str] | None = None
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Entry point: route to flat-JOIN or recursive-CTE path. ``joined``: the variables the
        statement holds before this pattern (those the var table holds now, when None)."""
        from provisa.gql.translator_types import (
            CypherTranslateError,
        )  # avoid circular at module level

        pf: PathFunction = clause.pattern  # type: ignore[assignment]
        src_node, tgt_node, rel = _extract_path_nodes(pf, CypherTranslateError)
        held = set(self._var_table) if joined is None else joined
        if not src_node.labels or not tgt_node.labels:
            walk = self._lm.session_walk
            # REQ-347, REQ-348: a variable an earlier clause bound to a table keeps that table's
            # label, written bare or not.
            src_bound, tgt_bound = self._as_bound(src_node, held), self._as_bound(tgt_node, held)
            on_model = any(self._on_model(n) for n in (src_bound, tgt_bound))
            if walk is not None and not (on_model and self._walks_model_hops(walk, rel)):  # type: ignore[attr-defined]
                # A graph session walks its one relationships relation from any end (path_walk).
                return self._translate_session_walk(  # type: ignore[attr-defined]
                    clause, pf, src_node, tgt_node, rel, walk, held, CypherTranslateError
                )
            # A walk from an end the model places takes the model's relationships (REQ-348).
            src_node, tgt_node = src_bound, tgt_bound
        if not src_node.labels and not tgt_node.labels:
            # REQ-348: a path search starts from a bound or labelled end; both free is refused.
            raise CypherTranslateError(UNLABELLED_ENDS)
        # The walk starts from the end the statement holds, or else from a labelled one: an end
        # held already is joined where it is read, and the other is whatever the walk reaches.
        src_held = bool(src_node.variable and src_node.variable in held)
        tgt_held = bool(tgt_node.variable and tgt_node.variable in held)
        from_tgt = not src_node.labels or (tgt_held and not src_held)
        if from_tgt or not tgt_node.labels:
            return self._translate_free_end_walk(
                clause, pf, src_node, tgt_node, rel, held, from_tgt=from_tgt
            )
        src_type, tgt_type, src_nm, tgt_nm = self._resolve_path_node_types(
            src_node, tgt_node, CypherTranslateError
        )
        src_var = src_node.variable
        tgt_var = tgt_node.variable
        _register_path_vars(self._var_table, src_var, tgt_var, src_nm, tgt_nm)

        rel_types, variable_length, is_undirected, max_hops, min_hops, by_limit = (
            _extract_rel_attrs(rel)
        )
        allowed_rels = _filter_allowed_rels(self._lm, rel_types)
        if not is_undirected:
            allowed_rels = _rels_between(allowed_rels, src_type, tgt_type)
        needs_recursive = _needs_recursive_cte(
            variable_length, is_undirected, src_type, tgt_type, allowed_rels
        )

        if clause.variable and src_var and tgt_var:
            self._path_vars[clause.variable] = (src_var, tgt_var, needs_recursive)

        # #310: an undirected walk takes each relationship either way. A link from a constant
        # (HAS_TABLE's anchor) has no row to walk back from.
        backward: list[RelationshipMapping] = []
        if needs_recursive and is_undirected:
            backward = [_reversed(r) for r in allowed_rels if not r.constant_source]
            allowed_rels = _within_reach(
                [*allowed_rels, *backward], src_type, max_hops + 1 if by_limit else max_hops
            )

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
                min_hops,
                by_limit,
                is_all_paths,
                CypherTranslateError,
                backward=frozenset(id(r) for r in backward),
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
            min_hops,
            by_limit,
            is_undirected,
            is_all_paths,
            CypherTranslateError,
        )

    def _on_model(self, node: NodePattern) -> bool:
        """Whether ``node``'s labels place it on the model graph: a table's, or a domain's."""
        known = (*self._lm.nodes, *self._lm.domains)
        return any(self._lm.canonical_label(label) in known for label in node.labels)

    def _as_bound(self, node: NodePattern, held: set[str]) -> NodePattern:
        """``node`` labelled with the table an earlier clause bound its variable to (REQ-347: a
        bound variable keeps its labels); ``node`` itself when it is labelled or not so bound."""
        if node.labels or not node.variable or node.variable not in held:
            return node
        nm = (self._var_table.get(node.variable) or (None, None))[1]
        key = next((k for k, m in self._lm.nodes.items() if m is nm), None)
        return node if key is None else dataclasses.replace(node, labels=[key])

    def _translate_free_end_walk(
        self,
        clause: MatchClause,
        pf: PathFunction,
        src_node: NodePattern,
        tgt_node: NodePattern,
        rel: RelPattern | None,
        held: set[str],
        *,
        from_tgt: bool,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """REQ-348 (ONE END BOUND OR BINDABLE): a path search from one placed end (labelled, or
        bound to a table) to an end that is free, or that only the statement or its labels place.
        The walk starts from the placed end and takes, hop by hop, only the relationships leaving
        the tables it has reached; the far end is whichever node it reaches within the bound, of
        any of those tables its labels admit. ``from_tgt``: the walk starts from the pattern's
        target, each relationship taken backward."""
        from provisa.gql.label_map import CypherLabelMap
        from provisa.gql.translator_types import CypherTranslateError
        from provisa.gql.translator_union import union_labels

        rel_types, _, is_undirected, max_hops, min_hops, by_limit = _extract_rel_attrs(rel)
        reverse = from_tgt
        anchor, far = (tgt_node, src_node) if reverse else (src_node, tgt_node)
        anchor_type, _ = self._resolve_node_type(anchor.labels)  # type: ignore[attr-defined]
        if anchor_type is None:
            raise CypherTranslateError(
                "shortestPath/allShortestPaths require type-labeled (not domain-only) nodes"
            )
        anchor_nm = self._lm.nodes[anchor_type]
        _register_path_vars(self._var_table, anchor.variable, None, anchor_nm, anchor_nm)

        rels = _filter_allowed_rels(self._lm, rel_types)
        backward: list[RelationshipMapping] = []
        if is_undirected:
            backward = [_reversed(r) for r in rels if not r.constant_source]
            rels = [*rels, *backward]
        elif reverse:
            rels = [_reversed(r) for r in rels if not r.constant_source]
        rels = _within_reach(rels, anchor_type, max_hops + 1 if by_limit else max_hops)

        reached = [r.target_label for r in rels] + ([anchor_type] if min_hops == 0 else [])
        labels = [k for k in dict.fromkeys(reached) if k in self._lm.nodes]
        if far.labels:
            far_type, far_domain = self._resolve_node_type(far.labels)  # type: ignore[attr-defined]
            admitted = {far_type} if far_type else set(union_labels(self._lm, far_domain or ""))
            labels = [k for k in labels if k in admitted]
        # An anonymous end its labels place is read all the same, to keep only those nodes.
        far_var = far.variable or ("_path_far" if far.labels else None)
        domain = f"__reach_{far_var or 'anon'}__"
        self._lm = CypherLabelMap(
            nodes=self._lm.nodes,
            relationships=self._lm.relationships,
            domains={**self._lm.domains, domain: labels},
        )
        bound = bool(far_var and far_var in held)
        if far_var and not bound:
            self._domain_nodes[far_var] = domain  # type: ignore[attr-defined]
            self._var_table[far_var] = (far_var, None)
        free_end = _FreeEnd(
            var=far_var,
            domain=domain,
            types=tuple((self._lm.nodes[k].type_name, self._lm.nodes[k].label) for k in labels),
            bound=bound,
        )
        if clause.variable:
            self._path_vars[clause.variable] = (src_node.variable, tgt_node.variable, True)
        is_all_paths = pf.func_name.lower() == "allpaths"
        result = self._translate_path_function_recursive(
            clause,
            anchor.variable,
            None,
            anchor_nm,
            anchor_nm,
            anchor_type,
            "",
            rels,
            max_hops,
            min_hops,
            by_limit,
            is_all=pf.func_name.lower() in ("allshortestpaths", "allpaths"),
            suppress_hops_order=is_all_paths,
            backward=frozenset(id(r) for r in backward),
            free_end=free_end,
        )
        if clause.variable and far_var:
            from provisa.gql.select_builder import node_identity

            anchor_id = node_identity(anchor.variable or anchor_nm.table_name, anchor_nm)
            far_id = exp.Column(
                this=exp.Identifier(this="__id"), table=exp.Identifier(this=far_var)
            )
            ends = (far_id, anchor_id) if reverse else (anchor_id, far_id)
            self._walk_paths[clause.variable] = ends  # type: ignore[attr-defined]
        return result

    @staticmethod
    def _reached_identity(free_end: _FreeEnd) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The identity (``label|key``) of the node the walk's row reached: its traversal type
        read as its label, then its key text."""
        label = exp.Case(
            this=exp.Column(this=exp.Identifier(this="cur_type"), table=exp.Identifier(this="_t")),
            ifs=[
                exp.If(this=exp.Literal.string(t), true=exp.Literal.string(label))
                for t, label in free_end.types
            ],
        )
        return exp.DPipe(
            this=exp.DPipe(this=label, expression=exp.Literal.string("|")),
            expression=exp.Column(
                this=exp.Identifier(this="cur_id"), table=exp.Identifier(this="_t")
            ),
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
        min_hops: int,
        by_limit: bool,
        is_all_paths: bool,
        CypherTranslateError: type,
        *,
        backward: frozenset[int] = frozenset(),
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Dispatch to recursive CTE translation after validating base rels exist. ``backward``:
        the ids of the relationships an undirected walk takes from target to source (#310). A
        walk no relationship leaves the start of takes no hop: it reads no rows (REQ-347)."""
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
            min_hops,
            by_limit,
            is_all=pf.func_name.lower() in ("allshortestpaths", "allpaths"),
            suppress_hops_order=is_all_paths,
            backward=backward,
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
        min_hops: int,
        by_limit: bool,
        is_undirected: bool,
        is_all_paths: bool,
        CypherTranslateError: type,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build flat JOIN path for non-self-referential variable-length or fixed-length.

        Each schema path has a fixed length, so the lower bound keeps the paths at least that long
        (#283). A zero-length path pairs a node with itself, which only a same-typed pattern can do.
        A path bounded by the depth limit is cut there when the schema has a longer path between
        the two types (REQ-2135).
        """
        if min_hops == 0 and src_type == tgt_type:
            raise CypherTranslateError(
                "A zero-length undirected path (e.g. [*0..3]) between nodes of one type is not "
                "supported; give the path a direction or a lower bound of 1"
            )
        all_paths = [
            p
            for p in self._lm.find_paths(
                src_type, tgt_type, rel_types, max_hops, bidirectional=is_undirected
            )
            if len(p) >= min_hops
        ]
        if by_limit and any(
            len(p) > max_hops
            for p in self._lm.find_paths(
                src_type, tgt_type, rel_types, max_hops + 1, bidirectional=is_undirected
            )
        ):
            self._cut_checks.append(exp.true())
        if not all_paths:
            # REQ-347: a pattern no relationship can match answers no rows (null for an OPTIONAL
            # MATCH), never an error.
            return self._no_path(clause, src_var, tgt_var, src_nm, tgt_nm)

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

    def _no_path(
        self,
        clause: MatchClause,
        src_var: str | None,
        tgt_var: str | None,
        src_nm: NodeMapping,
        tgt_nm: NodeMapping,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """A path the schema has no relationships for: the start, joined to the end on nothing."""
        src_alias = src_var or src_nm.table_name
        tgt_alias = tgt_var or tgt_nm.table_name
        if clause.variable is not None:
            nodes = [(src_alias, src_nm), (tgt_alias, tgt_nm)]
            self._path_steps[clause.variable] = (nodes, [])  # type: ignore[attr-defined]
        tgt_join = {
            "table": exp.alias_(
                exp.Table(
                    this=exp.Identifier(this=tgt_nm.sql_table_name, quoted=True),
                    db=exp.Identifier(this=tgt_nm.schema_name, quoted=True),
                    catalog=exp.Identifier(this=tgt_nm.catalog_name, quoted=True),
                ),
                alias=tgt_alias,
            ),
            "on": exp.false(),
            "join_type": "LEFT" if clause.optional else "INNER",
        }
        src_from = exp.alias_(
            exp.Table(
                this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
            ),
            alias=src_alias,
        )
        return cast(exp.Expression, src_from), [tgt_join]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

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
        self._extra_path_branches.append((extra_from, extra_joins, extra_path_steps_map, {}))

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
        _tgt_nm: NodeMapping,
        optional: bool,
    ) -> tuple[
        exp.Expression,  # pyright: ignore[reportPrivateImportUsage]
        list[dict],
        list[tuple[str, NodeMapping]],
        # REQ-1586 added the trailing (junction alias, JunctionMapping) element; the annotation
        # still described the six-element step edge that preceded it.
        list[
            tuple[str, str, NodeMapping, str, NodeMapping, bool, tuple[str, JunctionMapping] | None]
        ],
    ]:
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
        # REQ-1586: the last element is the (junction alias, JunctionMapping) of a
        # junction-backed hop, so the step edge can carry the junction's columns as properties.
        step_edges: list[
            tuple[str, str, NodeMapping, str, NodeMapping, bool, tuple[str, JunctionMapping] | None]
        ] = []

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
            if rel_mapping.via is not None:
                # REQ-1586: a junction hop is two joins; the path is already oriented source →
                # target, so the hop is never backwards here.
                joins.extend(
                    _make_junction_joins(
                        rel_mapping,
                        False,
                        nxt_nm,
                        nxt_alias,
                        prev_alias,
                        join_type,
                        f"{nxt_alias}__via",
                    )
                )
            else:
                joins.append({"table": join_table, "on": on_cond, "join_type": join_type})
            step_nodes.append((nxt_alias, nxt_nm))
            # Detect if this edge is traversed in reverse of its canonical direction.
            # Canonical source_label is stored on the original RelationshipMapping in self._lm.
            canonical_rel = self._lm.relationships.get(rel_mapping.rel_type)
            is_reversed = (
                canonical_rel is not None and canonical_rel.source_label != rel_mapping.source_label
            )
            step_edges.append(
                (
                    rel_mapping.rel_type,
                    prev_alias,
                    prev_nm,
                    nxt_alias,
                    nxt_nm,
                    is_reversed,
                    None if rel_mapping.via is None else (f"{nxt_alias}__via", rel_mapping.via),
                )
            )
            prev_alias = nxt_alias
            prev_nm = nxt_nm

        return cast(exp.Expression, from_expr), joins, step_nodes, step_edges  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # Recursive CTE (self-referential variable-length paths)
    # ------------------------------------------------------------------

    def _textual_keys(
        self, src_nm: NodeMapping, tgt_nm: NodeMapping, allowed_rels: list[RelationshipMapping]
    ) -> bool:
        """Whether a traversal carries each node's key as text: where a node it can reach has a
        compound key, a node is told apart by every key column (node_key_text), and the one
        frontier column holds keys of every type the walk meets."""
        reached = [src_nm, tgt_nm]
        for rel in allowed_rels:
            reached += [self._lm.nodes.get(rel.source_label), self._lm.nodes.get(rel.target_label)]
        return any(nm is not None and len(nm.key_columns) > 1 for nm in reached)

    @staticmethod
    def _path_key(nm: NodeMapping, alias: str, textual: bool) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The key a traversal tells ``alias``'s node by: its key column, or its key text."""
        if textual:
            from provisa.gql.select_builder import node_key_text

            return node_key_text(alias, nm)
        return exp.Column(
            this=exp.Identifier(this=nm.id_column, quoted=True), table=exp.Identifier(this=alias)
        )

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
        min_hops: int,
        by_limit: bool,
        is_all: bool,
        suppress_hops_order: bool = False,
        backward: frozenset[int] = frozenset(),
        free_end: _FreeEnd | None = None,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Emit WITH RECURSIVE CTE for paths that may repeat edge traversals in data.

        ``free_end``: the target is whatever node the walk reaches (REQ-348), read from the union
        of the labels it can reach by its identity, so the walk carries every key as text.

        The CTE walks from one hop up to the bound; a lower bound above one keeps only the rows
        that far out (#283), and a lower bound of zero adds each start node as its own target.
        A walk bounded by the depth limit goes one hop further than the limit and keeps only the
        rows within it: a row past it means the walk was cut there (REQ-2135).
        """
        cte_name = f"_traverse_{src_var or src_type.lower()}"
        textual = free_end is not None or self._textual_keys(src_nm, tgt_nm, allowed_rels)
        cte_expr = self._build_recursive_cte(
            cte_name,
            src_nm,
            allowed_rels,
            max_hops + 1 if by_limit else max_hops,
            zero_length=min_hops == 0,
            textual=textual,
            backward=backward,
        )
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
        traverse_join_on: exp.Expression = exp.EQ(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            this=exp.Column(
                this=exp.Identifier(this="src_id"),
                table=exp.Identifier(this="_t"),
            ),
            expression=self._path_key(src_nm, src_alias, textual),
        )
        if free_end is None:
            traverse_join_on = exp.And(
                this=traverse_join_on,
                expression=exp.EQ(
                    this=exp.Column(
                        this=exp.Identifier(this="cur_type"),
                        table=exp.Identifier(this="_t"),
                    ),
                    expression=exp.Literal.string(tgt_type),
                ),
            )
        if by_limit:
            traverse_join_on = exp.And(
                this=traverse_join_on,
                expression=exp.LTE(
                    this=exp.Column(
                        this=exp.Identifier(this="hops"), table=exp.Identifier(this="_t")
                    ),
                    expression=exp.Literal.number(max_hops),
                ),
            )
            self._cut_checks.append(
                exp.Exists(
                    this=exp.select(exp.Literal.number(1))
                    .from_(exp.Table(this=exp.Identifier(this=cte_name)))
                    .where(
                        exp.GT(
                            this=exp.Column(this=exp.Identifier(this="hops")),
                            expression=exp.Literal.number(max_hops),
                        )
                    )
                )
            )
        if min_hops > 1:
            traverse_join_on = exp.And(
                this=traverse_join_on,
                expression=exp.GTE(
                    this=exp.Column(
                        this=exp.Identifier(this="hops"), table=exp.Identifier(this="_t")
                    ),
                    expression=exp.Literal.number(min_hops),
                ),
            )

        # JOIN tgt_table AS tgt_alias ON tgt_alias.id = _t.cur_id
        tgt_join_on = exp.EQ(
            this=self._path_key(tgt_nm, tgt_alias, textual),
            expression=exp.Column(
                this=exp.Identifier(this="cur_id"),
                table=exp.Identifier(this="_t"),
            ),
        )

        tgt_join: dict | None = {
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
        }
        if free_end is not None:
            tgt_join = None
            reached = self._reached_identity(free_end)
            if free_end.var is not None and free_end.bound:
                traverse_join_on = exp.And(
                    this=traverse_join_on,
                    expression=exp.EQ(this=_free_id(free_end.var), expression=reached),
                )
            elif free_end.var is not None:
                tgt_join = {
                    "table": self._build_domain_union(free_end.var, free_end.domain),  # type: ignore[attr-defined]
                    "on": exp.EQ(this=_free_id(free_end.var), expression=reached),
                    "join_type": join_type,
                }
        joins = [
            {
                "table": exp.alias_(
                    exp.Table(this=exp.Identifier(this=cte_name)),
                    alias="_t",
                ),
                "on": traverse_join_on,
                "join_type": join_type,
            },
            *([tgt_join] if tgt_join is not None else []),
        ]

        # Signal translate() to add ORDER BY hops [LIMIT 1] — suppressed for allPaths()
        if not suppress_hops_order:
            self._shortestpath_hops_col = exp.Column(
                this=exp.Identifier(this="hops"),
                table=exp.Identifier(this="_t"),
            )
        self._shortestpath_is_all = is_all
        return cast(exp.Expression, from_expr), joins  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _one_recursive_term(
        self,
        cte_name: str,
        allowed_rels: list[RelationshipMapping],
        max_hops: int,
        _tbl: Any,
        textual: bool,
        backward: frozenset[int] = frozenset(),
    ) -> exp.Select:
        """The recursive step over several relationships as one term: the traversal joined to
        the union of every candidate hop, each a (from_type, from_id, cur_type, cur_id) row."""

        def col(name: str, table: str, quoted: bool = False) -> exp.Column:
            return exp.Column(
                this=exp.Identifier(this=name, quoted=quoted), table=exp.Identifier(this=table)
            )

        hops: exp.Expression | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for rel in allowed_rels:
            src_node_m = self._lm.nodes.get(rel.source_label)
            tgt_node_m = self._lm.nodes.get(rel.target_label)
            if src_node_m is None or tgt_node_m is None:
                continue
            hop = exp.select(
                exp.alias_(exp.Literal.string(src_node_m.type_name), alias="from_type"),
                exp.alias_(self._path_key(src_node_m, "_cur", textual), alias="from_id"),
                exp.alias_(exp.Literal.string(tgt_node_m.type_name), alias="cur_type"),
                exp.alias_(self._path_key(tgt_node_m, "_nxt", textual), alias="cur_id"),
            ).from_(_tbl(src_node_m, "_cur"))
            if backward:
                hop = hop.select(
                    exp.alias_(
                        _edge_key(
                            rel,
                            self._path_key(src_node_m, "_cur", textual),
                            self._path_key(tgt_node_m, "_nxt", textual),
                            id(rel) in backward,
                        ),
                        alias="edge",
                    )
                )
            hop = _apply_hop(hop, rel, "_cur", tgt_node_m, "_nxt", _tbl(tgt_node_m, "_nxt"))
            hops = hop if hops is None else exp.Union(this=hops, expression=hop, distinct=False)
        assert hops is not None  # called with more than one branch built from these rels
        trail = (
            [
                exp.alias_(
                    exp.DPipe(
                        this=exp.DPipe(this=col("trail", "t"), expression=col("edge", "_hop")),
                        expression=exp.Literal.string("|"),
                    ),
                    alias="trail",
                )
            ]
            if backward
            else []
        )
        step = (
            exp.select(
                col("src_id", "t"),
                col("cur_type", "_hop"),
                col("cur_id", "_hop"),
                exp.alias_(
                    exp.Add(this=col("hops", "t"), expression=exp.Literal.number(1)), alias="hops"
                ),
                *trail,
            )
            .from_(exp.alias_(exp.Table(this=exp.Identifier(this=cte_name)), alias="t"))
            .join(
                exp.alias_(exp.Subquery(this=hops), alias="_hop"),
                on=exp.And(
                    this=exp.EQ(this=col("from_type", "_hop"), expression=col("cur_type", "t")),
                    expression=exp.EQ(this=col("from_id", "_hop"), expression=col("cur_id", "t")),
                ),
                join_type="INNER",
            )
            .where(exp.LT(this=col("hops", "t"), expression=exp.Literal.number(max_hops)))
        )
        if backward:
            # A walk never takes an edge it has already taken (openCypher: a path repeats no
            # relationship), so an undirected walk does not go back over the edge it came by.
            step = step.where(
                exp.EQ(
                    this=exp.StrPosition(this=col("trail", "t"), substr=_bar(col("edge", "_hop"))),
                    expression=exp.Literal.number(0),
                )
            )
        return step

    def _build_recursive_cte(
        self,
        cte_name: str,
        src_nm: NodeMapping,
        allowed_rels: list[RelationshipMapping],
        max_hops: int,
        zero_length: bool = False,
        textual: bool = False,
        backward: frozenset[int] = frozenset(),
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build the UNION ALL body of the recursive CTE.

        Base case: one branch per allowed rel whose source_label == src_nm.label.
        Recursive step: one branch per allowed rel expanding from any current node type.
        A zero-length path seeds with each source row as its own frontier at hops 0 instead, and
        the recursive step walks every hop from there (#283).

        CTE schema: (src_id, cur_type, cur_id, hops)
          src_id  — source row id (anchored to the start of the path)
          cur_type — label of the current frontier node
          cur_id   — id of the current frontier node
          hops     — number of edges traversed so far (>= 1)
          trail    — an undirected walk only (``backward`` names the relationships it takes
                     from target to source, #310): the edges it has taken, ``|edge|edge|``, so it
                     takes none twice
        """

        def _tbl(nm: NodeMapping, alias: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            return cast(
                exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
        if zero_length:
            seed_id = self._path_key(src_nm, "_seed", textual)
            base_branches.append(
                exp.select(
                    exp.alias_(seed_id, alias="src_id"),
                    exp.alias_(exp.Literal.string(src_nm.type_name), alias="cur_type"),
                    exp.alias_(seed_id.copy(), alias="cur_id"),
                    exp.alias_(exp.Literal.number(0), alias="hops"),
                    *([exp.alias_(exp.Literal.string("|"), alias="trail")] if backward else []),
                ).from_(_tbl(src_nm, "_seed"))
            )
        for rel in allowed_rels:
            if zero_length:
                break
            if rel.source_label != src_nm.type_name:
                continue
            src_node_m = self._lm.nodes.get(rel.source_label)
            tgt_node_m = self._lm.nodes.get(rel.target_label)
            if src_node_m is None or tgt_node_m is None:
                continue

            branch = exp.select(
                exp.alias_(self._path_key(src_node_m, "_seed", textual), alias="src_id"),
                exp.alias_(exp.Literal.string(tgt_node_m.type_name), alias="cur_type"),
                exp.alias_(self._path_key(tgt_node_m, "_nxt", textual), alias="cur_id"),
                exp.alias_(exp.Literal.number(1), alias="hops"),
            ).from_(_tbl(src_node_m, "_seed"))
            if backward:
                edge = _edge_key(
                    rel,
                    self._path_key(src_node_m, "_seed", textual),
                    self._path_key(tgt_node_m, "_nxt", textual),
                    id(rel) in backward,
                )
                branch = branch.select(exp.alias_(_bar(edge), alias="trail"))
            branch = _apply_hop(branch, rel, "_seed", tgt_node_m, "_nxt", _tbl(tgt_node_m, "_nxt"))
            base_branches.append(branch)

        if not base_branches:
            # No relationship leaves the start: the walk takes no first hop and reads no rows. The
            # traversal keeps its columns, typed as the start's key, for the joins that read it.
            seed_id = self._path_key(src_nm, "_seed", textual)
            return (
                exp.select(
                    exp.alias_(seed_id, alias="src_id"),
                    exp.alias_(exp.Literal.string(src_nm.type_name), alias="cur_type"),
                    exp.alias_(seed_id.copy(), alias="cur_id"),
                    exp.alias_(exp.Literal.number(0), alias="hops"),
                    *([exp.alias_(exp.Literal.string("|"), alias="trail")] if backward else []),
                )
                .from_(_tbl(src_nm, "_seed"))
                .where(exp.false())
            )

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
                    exp.alias_(self._path_key(tgt_node_m, "_nxt", textual), alias="cur_id"),
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
                            this=self._path_key(src_node_m, "_cur", textual),
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
            )
            # JOIN next-node table
            branch = _apply_hop(
                branch, rel, "_cur", tgt_node_m, "_nxt", _tbl(tgt_node_m, "_nxt")
            ).where(
                exp.LT(
                    this=exp.Column(
                        this=exp.Identifier(this="hops"),
                        table=exp.Identifier(this="t"),
                    ),
                    expression=exp.Literal.number(max_hops),
                )
            )
            rec_branches.append(branch)

        if len(rec_branches) > 1 or backward:
            # One recursive term, reading the traversal once (#158): engines admit a single
            # recursive term, and one that names the traversal a single time. An undirected walk
            # always has two (each relationship either way), and keeps its trail there.
            rec_branches = [
                self._one_recursive_term(
                    cte_name, allowed_rels, max_hops, _tbl, textual, backward=backward
                )
            ]

        all_branches = base_branches + rec_branches

        result: exp.Expression = all_branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for branch in all_branches[1:]:
            result = exp.Union(this=result, expression=branch, distinct=False)
        return result

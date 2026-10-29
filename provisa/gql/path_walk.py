# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8d2e71-0c4a-4f93-a6e1-9d27c3f0b548
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SessionWalkMixin — a path function with an unlabelled end, in a graph session (REQ-348).

On the model graph a path function's ends are labelled: a label places an end in its table and
each hop in its relationship tables. In a graph session every relationship is a row of the
session's one relationships relation (``label_map.SessionWalk``), so a walk from an end with no
label reads that relation, as a walk between labelled ends does. The walk is one recursive CTE
of ``(src_id, cur_id, hops, trail)`` over the relation's ``(rel_id, type, from_id, to_id)``, its
ends joined by their graph identity; an end with no label is drawn from the union of every
label's nodes, as a match of any node is. A relationship of the model's would be a hop the
relation does not hold: a walk the model's relationships can take is refused as on the model
graph.

Mixed into _Translator beside PathFunctionsMixin, which routes a path function with an
unlabelled end here.
"""

# Requirements: REQ-348, REQ-751, REQ-753, REQ-767

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.gql.ir import MatchClause, NodePattern, PathFunction, RelPattern
from provisa.gql.label_map import GRAPH_ID, CypherLabelMap, NodeMapping, SessionWalk

UNLABELLED_ENDS = "shortestPath/allShortestPaths require labeled source and target nodes"


def graph_identity(walk: SessionWalk, nm: NodeMapping, alias: str | None) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The graph identity of ``alias``'s node, which the session's relationships name their ends
    by: a session relation's own column, else the model node's ``Label|key``."""
    from provisa.gql.select_builder import node_identity

    if nm.catalog_name == walk.catalog_name:
        return _col(GRAPH_ID, alias)
    return node_identity(alias, nm)


def _col(name: str, table: str | None = None) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=name, quoted=True),
        table=exp.Identifier(this=table) if table else None,
    )


def _bar(expr: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``|expr|``: one relationship of a trail, delimited so one id never matches inside another."""
    return exp.DPipe(
        this=exp.DPipe(this=exp.Literal.string("|"), expression=expr),
        expression=exp.Literal.string("|"),
    )


def _hops(walk: SessionWalk, types: list[str] | None, undirected: bool) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Each step a walk may take, ``(rel_id, a, b)`` from ``a`` to ``b``: a relationship of one
    of ``types`` (any, for None) its own way, and the other way too when ``undirected`` (a
    relationship to its own start once)."""
    relation = exp.Table(
        this=exp.Identifier(this=walk.table_name, quoted=True),
        db=exp.Identifier(this=walk.schema_name, quoted=True),
        catalog=exp.Identifier(this=walk.catalog_name, quoted=True),
    )

    def way(start: str, end: str) -> exp.Select:
        step = exp.select(
            _col("rel_id", "_r"),
            exp.alias_(_col(start, "_r"), "a", quoted=True),
            exp.alias_(_col(end, "_r"), "b", quoted=True),
        ).from_(exp.alias_(relation.copy(), "_r"))
        if types is not None:
            step = step.where(
                exp.In(this=_col("type", "_r"), expressions=[exp.Literal.string(t) for t in types])
            )
        return step

    forward = way("from_id", "to_id")
    if not undirected:
        return forward
    back = way("to_id", "from_id").where(
        exp.NEQ(this=_col("from_id", "_r"), expression=_col("to_id", "_r"))
    )
    return exp.Union(this=forward, expression=back, distinct=False)


class SessionWalkMixin:  # REQ-348
    """Mixin for _Translator: a path function with an unlabelled end, walked over the session's
    relationships."""

    _lm: CypherLabelMap
    _var_table: dict
    _domain_nodes: dict
    _recursive_ctes: list
    _shortestpath_hops_col: exp.Expression | None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    _shortestpath_is_all: bool
    _path_vars: dict
    _cut_checks: list
    _varlen_rel_vars: dict
    _graph_id_unions: set
    # path variable -> (its start's identity, its end's identity) as RETURN p names them
    _walk_paths: dict

    def _walk_end(
        self, node: NodePattern, alias: str, walk: SessionWalk, CypherTranslateError: type
    ) -> tuple[exp.Expression, exp.Expression, exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``node`` as a walk's end read as ``alias``: (its relation, its graph identity, its
        identity in an answer). A node with no label is any node: the union of every label's."""
        from provisa.gql.select_builder import node_identity
        from provisa.gql.translator_helpers import _node_table_expr

        var = node.variable
        if var:
            self._register_node(node)  # type: ignore[attr-defined]
            nm = self._var_table[var][1]
            domain = self._domain_nodes.get(var)
        elif node.labels:
            type_label, domain = self._resolve_node_type(node.labels)  # type: ignore[attr-defined]
            nm = self._lm.nodes[type_label] if type_label else None
        else:
            nm, domain = None, "__all__"
        if nm is not None:
            return (
                _node_table_expr(nm, alias),
                graph_identity(walk, nm, alias),
                node_identity(alias, nm),
            )
        if domain is None:
            raise CypherTranslateError(
                "shortestPath/allShortestPaths require type-labeled (not domain-only) nodes"
            )
        relation = self._build_domain_union(alias, domain)  # type: ignore[attr-defined]
        graph_id = GRAPH_ID if alias in self._graph_id_unions else "__id"
        return relation, _col(graph_id, alias), _col("__id", alias)

    def _walks_model_hops(self, walk: SessionWalk, rel: RelPattern | None) -> bool:
        """Whether the model's relationships could take a hop of the walk: on the model graph each
        is its own table, which a walk from no placed end would read as a union of them all."""
        wanted = {t.upper() for t in rel.types} if rel and rel.types else None
        for r in self._lm.relationships.values():
            if r.via is not None and r.via.catalog_name == walk.catalog_name:
                continue
            if r.constant_source and wanted is None:
                continue
            if wanted is None or r.rel_type.upper() in wanted:
                return True
        return False

    def _refuse_model_hops(self, walk: SessionWalk, rel: RelPattern | None, error: type) -> None:
        """Refuse a walk the model's relationships could take a hop of when neither end places
        it (REQ-348, ONE END BOUND OR BINDABLE): a walk from a placed end is the model graph's."""
        if self._walks_model_hops(walk, rel):
            raise error(f"{UNLABELLED_ENDS} where the model's relationships are walked")

    def _translate_session_walk(  # complexity-gate: allow-cc=16 reason="one walk's ends, bounds and path function, each a short branch of the one CTE"
        self,
        clause: MatchClause,
        pf: PathFunction,
        src_node: NodePattern,
        tgt_node: NodePattern,
        rel: RelPattern | None,
        walk: SessionWalk,
        joined: set[str],
        CypherTranslateError: type,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The path function ``pf`` walked over the session's relationships from ``src_node`` to
        ``tgt_node``. shortestPath reads the walk's rows by hops, the first one (REQ-767);
        allShortestPaths the fewest hops between each pair; a variable-length pattern every
        walk. ``joined``: the variables the statement holds already, an end among them joined where
        it is read. The walk takes no relationship twice (openCypher's paths repeat none) and goes no
        further than the pattern's upper bound, or the depth limit one past it (REQ-2135)."""
        from provisa.gql.path_functions import _extract_rel_attrs

        self._refuse_model_hops(walk, rel, CypherTranslateError)
        _, _, undirected, max_hops, min_hops, by_limit = _extract_rel_attrs(rel)
        types = list(rel.types) if rel and rel.types else None
        func = pf.func_name.lower()

        src_alias = src_node.variable or "_walk_src"
        tgt_alias = tgt_node.variable or "_walk_tgt"
        tgt_bound = tgt_node.variable is not None and tgt_node.variable in joined
        src_from, src_id, src_answer = self._walk_end(
            src_node, src_alias, walk, CypherTranslateError
        )
        same = tgt_node.variable is not None and tgt_node.variable == src_node.variable
        tgt_from, tgt_id, tgt_answer = (
            (None, src_id, src_answer)
            if same
            else self._walk_end(tgt_node, tgt_alias, walk, CypherTranslateError)
        )

        cte_name = f"_walk_{src_alias.lower()}"
        bound = max_hops + 1 if by_limit else max_hops
        hops = _hops(walk, types, undirected)
        t, h = "t", "_h"
        if min_hops == 0:
            seed_from, seed_id, _ = self._walk_end(src_node, "_seed", walk, CypherTranslateError)
            seed = exp.select(
                exp.alias_(seed_id, "src_id"),
                exp.alias_(seed_id.copy(), "cur_id"),
                exp.alias_(exp.Literal.number(0), "hops"),
                exp.alias_(exp.Literal.string("|"), "trail"),
            ).from_(seed_from)
        else:
            seed = exp.select(
                exp.alias_(_col("a", h), "src_id"),
                exp.alias_(_col("b", h), "cur_id"),
                exp.alias_(exp.Literal.number(1), "hops"),
                exp.alias_(_bar(_col("rel_id", h)), "trail"),
            ).from_(exp.alias_(exp.Subquery(this=hops.copy()), h))
        step = (
            exp.select(
                _col("src_id", t),
                exp.alias_(_col("b", h), "cur_id"),
                exp.alias_(exp.Add(this=_col("hops", t), expression=exp.Literal.number(1)), "hops"),
                exp.alias_(
                    exp.DPipe(
                        this=exp.DPipe(this=_col("trail", t), expression=_col("rel_id", h)),
                        expression=exp.Literal.string("|"),
                    ),
                    "trail",
                ),
            )
            .from_(exp.alias_(exp.Table(this=exp.Identifier(this=cte_name)), t))
            .join(
                exp.alias_(exp.Subquery(this=hops), h),
                on=exp.EQ(this=_col("a", h), expression=_col("cur_id", t)),
                join_type="INNER",
            )
            .where(exp.LT(this=_col("hops", t), expression=exp.Literal.number(bound)))
            .where(
                exp.EQ(
                    this=exp.StrPosition(this=_col("trail", t), substr=_bar(_col("rel_id", h))),
                    expression=exp.Literal.number(0),
                )
            )
        )
        body: exp.Expression = exp.Union(this=seed, expression=step, distinct=False)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        self._recursive_ctes.append((cte_name, body))

        walked: exp.Expression = exp.Table(this=exp.Identifier(this=cte_name))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if func == "allshortestpaths":
            # The fewest hops between each pair of ends: every walk of that many.
            fewest = exp.Window(
                this=exp.Min(this=_col("hops")),
                partition_by=[_col("src_id"), _col("cur_id")],
            )
            ranked = exp.select(exp.Star(), exp.alias_(fewest, "__fewest", quoted=True)).from_(
                walked
            )
            walked = exp.Subquery(
                this=exp.select(exp.Star())
                .from_(exp.alias_(exp.Subquery(this=ranked), "_w"))
                .where(exp.EQ(this=_col("hops"), expression=_col("__fewest")))
            )

        on: exp.Condition = exp.EQ(this=_col("src_id", "_t"), expression=src_id.copy())
        if same or tgt_bound:
            on = exp.and_(on, exp.EQ(this=_col("cur_id", "_t"), expression=tgt_id.copy()))
        if by_limit:
            on = exp.and_(
                on, exp.LTE(this=_col("hops", "_t"), expression=exp.Literal.number(max_hops))
            )
            self._cut_checks.append(
                exp.Exists(
                    this=exp.select(exp.Literal.number(1))
                    .from_(exp.Table(this=exp.Identifier(this=cte_name)))
                    .where(exp.GT(this=_col("hops"), expression=exp.Literal.number(max_hops)))
                )
            )
        if min_hops > 1:
            on = exp.and_(
                on, exp.GTE(this=_col("hops", "_t"), expression=exp.Literal.number(min_hops))
            )
        join_type = "LEFT" if clause.optional else "INNER"
        joins: list[dict[str, Any]] = [
            {"table": exp.alias_(walked, "_t"), "on": on, "join_type": join_type}
        ]
        if tgt_from is not None and not tgt_bound:
            joins.append(
                {
                    "table": tgt_from,
                    "on": exp.EQ(this=tgt_id.copy(), expression=_col("cur_id", "_t")),
                    "join_type": join_type,
                }
            )

        if rel is not None and rel.variable:
            # The walk's relationships, as a recursive path's are: not projected (REQ-751).
            self._varlen_rel_vars.setdefault(rel.variable, clause.variable or "")
        if clause.variable:
            self._path_vars[clause.variable] = (src_node.variable, tgt_node.variable, True)
            self._walk_paths[clause.variable] = (src_answer, tgt_answer)
        if func != "allpaths":
            self._shortestpath_hops_col = _col("hops", "_t")
        self._shortestpath_is_all = func != "shortestpath"
        return src_from, joins

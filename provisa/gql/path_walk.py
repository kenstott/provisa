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


def _hops(
    walk: SessionWalk,
    types: list[str] | None,
    undirected: bool,
    objects: tuple[exp.Expression, exp.Expression] | None = None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Each step a walk may take, ``(rel_id, a, b)`` from ``a`` to ``b``: a relationship of one
    of ``types`` (any, for None) its own way, and the other way too when ``undirected`` (a
    relationship to its own start once). With ``objects`` (the relations of the session's node
    objects and of its relationships' properties, _node_objects and _rel_properties), each step
    also carries what the walk's trail is made of: the relationship as an answer gives it
    (``edge``) and the nodes it leaves (``a_node``) and reaches (``b_node``)."""
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
        if objects is not None:
            step = _with_objects(step, objects, forward=start == "from_id")
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


#: The columns of the session's node objects (_node_objects): a node's graph identity, its
#: identity in an answer and the node as an answer gives it.
_NODE_OBJECT = "__node"
_PROPS = "__props"


def _with_objects(
    step: exp.Select,
    objects: tuple[exp.Expression, exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    *,
    forward: bool,
) -> exp.Select:
    """``step`` (a walk's step over the relationship ``_r``) with the relationship as an answer
    gives it, and its two ends' node objects, the one it leaves and the one it reaches."""
    nodes, props = objects
    edge = exp.Anonymous(
        this="JSON_OBJECT",
        expressions=[
            exp.Literal.string("identity"),
            _col("rel_id", "_r"),
            exp.Literal.string("start"),
            _col("__id", "_nf"),
            exp.Literal.string("end"),
            _col("__id", "_nt"),
            exp.Literal.string("type"),
            _col("type", "_r"),
            exp.Literal.string("properties"),
            _col(_PROPS, "_p"),
            exp.Literal.string("startNode"),
            _col(_NODE_OBJECT, "_nf"),
            exp.Literal.string("endNode"),
            _col(_NODE_OBJECT, "_nt"),
        ],
    )
    left, right = ("_nf", "_nt") if forward else ("_nt", "_nf")
    return (
        step.select(
            exp.alias_(edge, "edge", quoted=True),
            exp.alias_(_col(_NODE_OBJECT, left), "a_node", quoted=True),
            exp.alias_(_col(_NODE_OBJECT, right), "b_node", quoted=True),
        )
        .join(
            exp.alias_(props.copy(), "_p"),
            on=exp.EQ(this=_col("rel_id", "_p"), expression=_col("rel_id", "_r")),
            join_type="INNER",
        )
        .join(
            exp.alias_(nodes.copy(), "_nf"),
            on=exp.EQ(this=_col(GRAPH_ID, "_nf"), expression=_col("from_id", "_r")),
            join_type="INNER",
        )
        .join(
            exp.alias_(nodes.copy(), "_nt"),
            on=exp.EQ(this=_col(GRAPH_ID, "_nt"), expression=_col("to_id", "_r")),
            join_type="INNER",
        )
    )


def _node_object(nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A row of ``nm``'s relation as an answer gives the node: its identity, label and
    properties, each of its own type."""
    from provisa.gql.select_builder import node_identity

    props: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for prop in sorted(nm.properties):
        phys = nm.physical_properties.get(prop) or nm.properties[prop]
        props += [exp.Literal.string(prop), _col(phys)]
    return exp.Anonymous(
        this="JSON_OBJECT",
        expressions=[
            exp.Literal.string("id"),
            node_identity(None, nm),
            exp.Literal.string("label"),
            exp.Literal.string(nm.label),
            exp.Literal.string("tableLabel"),
            exp.Literal.string(nm.table_label),
            exp.Literal.string("properties"),
            exp.Anonymous(this="JSON_OBJECT", expressions=props),
        ],
    )


def _json_list(items: list[exp.Expression]) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A list of JSON values, typed even when it is empty (a recursive CTE's seed)."""
    return exp.Cast(
        this=exp.Array(expressions=items), to=exp.DataType.build("JSON[]", dialect="postgres")
    )


def _appended(trail: exp.Expression, item: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return exp.ArrayConcat(this=trail, expressions=[exp.Array(expressions=[item])])


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
    # path or relationship variable -> its walk's (nodes, relationships) lists, as RETURN names them
    _walk_trails: dict

    def _walk_end(
        self, node: NodePattern, alias: str, walk: SessionWalk, CypherTranslateError: type
    ) -> tuple[exp.Expression, exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``node`` as a walk's end read as ``alias``: (its relation, its graph identity). A node
        with no label is any node: the union of every label's."""
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
            return _node_table_expr(nm, alias), graph_identity(walk, nm, alias)
        if domain is None:
            raise CypherTranslateError(
                "shortestPath/allShortestPaths require type-labeled (not domain-only) nodes"
            )
        relation = self._build_domain_union(alias, domain)  # type: ignore[attr-defined]
        graph_id = GRAPH_ID if alias in self._graph_id_unions else "__id"
        return relation, _col(graph_id, alias)

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

    def _node_objects(self, walk: SessionWalk, labels: set[str] | None) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The nodes a walk's trail may pass, each once with its graph identity, its identity in an
        answer and its node object: every session label's nodes, and a model label's where a
        session relationship ends at one, or where it is in ``labels`` (every model label, for
        None) -- the start of a walk of no hops. Read once per node (ephemeral.read)."""
        from provisa.gql.select_builder import node_identity
        from provisa.gql.translator_union import union_labels

        ends = {
            label
            for r in self._lm.relationships.values()
            if r.via is not None and r.via.catalog_name == walk.catalog_name
            for label in (r.source_label, r.target_label)
        }
        arms: list[exp.Select] = []
        for label in union_labels(self._lm, "__all__"):
            nm = self._lm.nodes.get(label)
            if nm is None or nm.native_filter_columns:
                continue
            own = nm.catalog_name == walk.catalog_name
            if not own and label not in ends and labels is not None and nm.type_name not in labels:
                continue
            table = exp.Table(
                this=exp.Identifier(this=nm.sql_table_name, quoted=True),
                db=exp.Identifier(this=nm.schema_name, quoted=True),
                catalog=exp.Identifier(this=nm.catalog_name, quoted=True),
            )
            arms.append(
                exp.select(
                    exp.alias_(exp.Literal.string(nm.label), "__label"),
                    exp.alias_(node_identity(None, nm), "__id"),
                    exp.alias_(graph_identity(walk, nm, None), GRAPH_ID),
                    exp.alias_(_node_object(nm), _NODE_OBJECT),
                ).from_(exp.alias_(table, f"_{nm.type_name.lower()}"))
            )
        if not arms:
            text = exp.DataType.build("VARCHAR")
            arms.append(
                exp.select(
                    *(
                        exp.alias_(exp.Cast(this=exp.null(), to=text), name)
                        for name in ("__label", "__id", GRAPH_ID)
                    ),
                    exp.alias_(
                        exp.Cast(this=exp.null(), to=exp.DataType.build("JSON")), _NODE_OBJECT
                    ),
                ).where(exp.false())
            )
        union: exp.Expression = arms[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for arm in arms[1:]:
            union = exp.Union(this=union, expression=arm, distinct=False)
        return exp.Subquery(this=union)

    def _rel_properties(self, walk: SessionWalk, types: list[str] | None) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Each session relationship of one of ``types`` (any, for None) and its properties, as
        one JSON value: read from its type's relation, as a relationship matched alone is."""
        wanted = {t.upper() for t in types} if types is not None else None
        held: dict[str, exp.Select] = {}
        for r in self._lm.relationships.values():
            via = r.via
            if via is None or via.catalog_name != walk.catalog_name or via.table_name in held:
                continue
            if wanted is not None and r.rel_type.upper() not in wanted:
                continue
            # The overlay names every session relationship by its id (ephemeral.read.overlay).
            assert via.identity_column is not None, via.table_name
            props: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            for name, column in sorted(via.attributes.items()):
                props += [exp.Literal.string(name), _col(column, "_v")]
            relation = exp.Table(
                this=exp.Identifier(this=via.table_name, quoted=True),
                db=exp.Identifier(this=via.schema_name, quoted=True),
                catalog=exp.Identifier(this=via.catalog_name, quoted=True),
            )
            held[via.table_name] = exp.select(
                exp.alias_(_col(via.identity_column, "_v"), "rel_id", quoted=True),
                exp.alias_(exp.Anonymous(this="JSON_OBJECT", expressions=props), _PROPS),
            ).from_(exp.alias_(relation, "_v"))
        arms = list(held.values())
        if not arms:
            arms.append(
                exp.select(
                    exp.alias_(
                        exp.Cast(this=exp.null(), to=exp.DataType.build("VARCHAR")),
                        "rel_id",
                        quoted=True,
                    ),
                    exp.alias_(exp.Cast(this=exp.null(), to=exp.DataType.build("JSON")), _PROPS),
                ).where(exp.false())
            )
        union: exp.Expression = arms[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for arm in arms[1:]:
            union = exp.Union(this=union, expression=arm, distinct=False)
        return exp.Subquery(this=union)

    def _start_labels(self, node: NodePattern) -> set[str] | None:
        """The labels a walk's start may be read from: its own label's, or every label (None)
        for a node with none."""
        var = node.variable
        if var and var in self._var_table:
            nm = self._var_table[var][1]
            return {nm.type_name} if nm is not None else None
        if node.labels:
            type_label, _ = self._resolve_node_type(node.labels)  # type: ignore[attr-defined]
            return {self._lm.nodes[type_label].type_name} if type_label else None
        return None

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
        src_from, src_id = self._walk_end(src_node, src_alias, walk, CypherTranslateError)
        same = tgt_node.variable is not None and tgt_node.variable == src_node.variable
        tgt_from, tgt_id = (
            (None, src_id)
            if same
            else self._walk_end(tgt_node, tgt_alias, walk, CypherTranslateError)
        )

        cte_name = f"_walk_{src_alias.lower()}"
        bound = max_hops + 1 if by_limit else max_hops
        # REQ-751: a walk whose relationships or path the statement names carries them, in the
        # order it took them, with every node it passed.
        named = bool(clause.variable or (rel is not None and rel.variable))
        nodes = self._node_objects(walk, self._start_labels(src_node)) if named else None
        objects = (nodes, self._rel_properties(walk, types)) if nodes is not None else None
        hops = _hops(walk, types, undirected, objects)
        t, h = "t", "_h"
        if min_hops == 0:
            seed_from, seed_id = self._walk_end(src_node, "_seed", walk, CypherTranslateError)
            seed = exp.select(
                exp.alias_(seed_id, "src_id"),
                exp.alias_(seed_id.copy(), "cur_id"),
                exp.alias_(exp.Literal.number(0), "hops"),
                exp.alias_(exp.Literal.string("|"), "trail"),
            ).from_(seed_from)
            if nodes is not None:
                seed = seed.select(
                    exp.alias_(_json_list([_col(_NODE_OBJECT, "_sn")]), "nodes", quoted=True),
                    exp.alias_(_json_list([]), "rels", quoted=True),
                ).join(
                    exp.alias_(nodes.copy(), "_sn"),
                    on=exp.EQ(this=_col(GRAPH_ID, "_sn"), expression=seed_id.copy()),
                    join_type="INNER",
                )
        else:
            seed = exp.select(
                exp.alias_(_col("a", h), "src_id"),
                exp.alias_(_col("b", h), "cur_id"),
                exp.alias_(exp.Literal.number(1), "hops"),
                exp.alias_(_bar(_col("rel_id", h)), "trail"),
            ).from_(exp.alias_(exp.Subquery(this=hops.copy()), h))
            if nodes is not None:
                seed = seed.select(
                    exp.alias_(
                        _json_list([_col("a_node", h), _col("b_node", h)]), "nodes", quoted=True
                    ),
                    exp.alias_(_json_list([_col("edge", h)]), "rels", quoted=True),
                )
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
        if nodes is not None:
            step = step.select(
                exp.alias_(_appended(_col("nodes", t), _col("b_node", h)), "nodes", quoted=True),
                exp.alias_(_appended(_col("rels", t), _col("edge", h)), "rels", quoted=True),
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
            # The walk's relationships, in the order it took them (REQ-751).
            self._varlen_rel_vars.setdefault(rel.variable, clause.variable or "")
            self._walk_trails[rel.variable] = (_col("nodes", "_t"), _col("rels", "_t"))
        if clause.variable:
            self._path_vars[clause.variable] = (src_node.variable, tgt_node.variable, True)
            # The path: every node and relationship of the walk's trail.
            self._walk_trails[clause.variable] = (_col("nodes", "_t"), _col("rels", "_t"))
        if func != "allpaths":
            self._shortestpath_hops_col = _col("hops", "_t")
        self._shortestpath_is_all = func != "shortestpath"
        return src_from, joins

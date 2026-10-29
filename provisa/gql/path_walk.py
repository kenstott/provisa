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
graph. A walk whose path or relationship variable the statement names also carries its trail
(REQ-751): the nodes it passed and the relationships it took, in order, each as an answer gives
it, which RETURN p, RETURN r, relationships(p) and nodes(p) read.

Mixed into _Translator beside PathFunctionsMixin, which routes a path function with an
unlabelled end here.
"""

# Requirements: REQ-348, REQ-751, REQ-753, REQ-767

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import Any

import sqlglot.expressions as exp

from provisa.gql.expr_ast import CypherExpr, FunctionCall, Variable, walk
from provisa.gql.ir import MatchClause, NodePattern, PathFunction, PathPattern, RelPattern
from provisa.gql.label_map import (
    GRAPH_ID,
    LABELS_COLUMN,
    CypherLabelMap,
    NodeMapping,
    SessionWalk,
)

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
    *,
    backward: bool = False,
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Each step a walk may take, ``(rel_id, a, b)`` from ``a`` to ``b``: a relationship of one
    of ``types`` (any, for None) its own way (from its end to its start, ``backward``), and the
    other way too when ``undirected`` (a
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
        return way("to_id", "from_id") if backward else forward
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


def _node_object(walk: SessionWalk, nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A row of ``nm``'s relation as an answer gives the node: its identity, label and
    properties, each of its own type."""
    from provisa.gql.select_builder import node_identity

    props: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for prop in sorted(nm.properties):
        # As a node union's arm reads it (translator_union): its physical column, null with none.
        phys = nm.physical_properties.get(prop)
        props += [exp.Literal.string(prop), _col(phys) if phys else exp.null()]
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
            # Its every label, which a label test or labels() of a hop's node held as this value
            # reads in the statement (expr_context._session_labels); the answer's own come from
            # ephemeral.seam.answered.
            exp.Literal.string("labels"),
            _labels_of(nm),
            # Its graph identity, which a walk from it reads (_walk_end).
            exp.Literal.string("graphId"),
            graph_identity(walk, nm, None),
        ],
    )


_TEXT_LIST = exp.DataType.build("VARCHAR[]", dialect="postgres")


def _labels_of(nm: NodeMapping) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The labels of a row of ``nm``'s relation, every one: a graph session's node's own column,
    a model node's one label."""
    from provisa.gql.select_builder import node_labels

    held = node_labels(None, nm)
    if held is not None:
        return held
    return exp.Cast(
        this=exp.Array(expressions=[exp.Literal.string(nm.label)]), to=_TEXT_LIST.copy()
    )


def _json_list(items: list[exp.Expression]) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A list of JSON values, typed even when it is empty (a recursive CTE's seed)."""
    return exp.Cast(
        this=exp.Array(expressions=items), to=exp.DataType.build("JSON[]", dialect="postgres")
    )


def _appended(trail: exp.Expression, item: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return exp.ArrayConcat(this=trail, expressions=[exp.Array(expressions=[item])])


def _expressions(value: object) -> Iterator[CypherExpr]:
    """Every expression a statement's IR holds, at any depth."""
    if isinstance(value, CypherExpr):
        yield from walk(value)
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        for f in dataclasses.fields(value):
            yield from _expressions(getattr(value, f.name))
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _expressions(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _expressions(item)


def reads_trail(statement: object, var: str) -> bool:
    """Whether ``statement`` reads more of the walk ``var`` names than its length: its nodes or
    relationships, which only then does the walk carry (a walk's cost is its relationships
    table's, REQ-348)."""
    refs = lengths = 0
    for e in _expressions(statement):
        if isinstance(e, Variable) and e.name == var:
            refs += 1
        elif (
            isinstance(e, FunctionCall)
            and e.name.lower() == "length"
            and len(e.args) == 1
            and isinstance(e.args[0], Variable)
            and e.args[0].name == var
        ):
            lengths += 1
    return refs > lengths


def _distinct(
    mine: tuple[str, exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    other: tuple[str, exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """That two hops of a pattern take no relationship in common (openCypher's relationship
    uniqueness): each an edge's id or a walk's trail of ``|id|`` (_translate_session_chain)."""
    (kind, value), (other_kind, other_value) = mine, other
    if kind == "edge" and other_kind == "edge":
        return exp.NEQ(this=value.copy(), expression=other_value.copy())
    if kind == "edge" or other_kind == "edge":
        edge, trail = (value, other_value) if kind == "edge" else (other_value, value)
        return exp.EQ(
            this=exp.StrPosition(this=trail.copy(), substr=_bar(edge.copy())),
            expression=exp.Literal.number(0),
        )
    # Two trails: no id of one is in the other.
    item = exp.to_identifier("_id")
    shared = exp.and_(
        exp.NEQ(this=exp.column(item.copy()), expression=exp.Literal.string("")),
        exp.GT(
            this=exp.StrPosition(this=other_value.copy(), substr=_bar(exp.column(item.copy()))),
            expression=exp.Literal.number(0),
        ),
    )
    found = exp.Anonymous(
        this="list_filter",
        expressions=[
            exp.Anonymous(this="string_split", expressions=[value.copy(), exp.Literal.string("|")]),
            exp.Lambda(this=shared, expressions=[item]),
        ],
    )
    return exp.EQ(
        this=exp.Anonymous(this="len", expressions=[found]), expression=exp.Literal.number(0)
    )


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
    _ast: Any  # the statement translated (gql.ir.CypherAST)
    # path or relationship variable -> its walk's (nodes, relationships) lists, as RETURN names them
    _walk_trails: dict
    _all_rels_ends: dict
    _all_rels_mappings: list
    _all_rels_node_vars: set
    _all_rels_var_alias: dict

    def _is_held_value(self, node: NodePattern) -> bool:
        """Whether ``node`` is one a hop of the pattern holds as its value, of no one table."""
        var = node.variable
        return bool(
            var
            and var in self._all_rels_node_vars
            and self._var_table.get(var, (None, None))[1] is None
        )

    def _walk_end(
        self, node: NodePattern, alias: str, walk: SessionWalk, CypherTranslateError: type
    ) -> tuple[exp.Expression, exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """``node`` as a walk's end read as ``alias``: (its relation, its graph identity). A node
        with no label is any node: the union of every label's."""
        from provisa.gql.translator_helpers import _node_table_expr

        var = node.variable
        if self._is_held_value(node):
            # A node a hop of the pattern holds as its value (_build_session_hop): its graph
            # identity, which the node carries.
            assert var is not None
            held = exp.column(var, table=self._all_rels_var_alias.get(var))
            graph_id = exp.Anonymous(
                this="JSON_EXTRACT_SCALAR", expressions=[held, exp.Literal.string("$.graphId")]
            )
            return exp.Subquery(this=exp.select(exp.Literal.number(1))), graph_id
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
                    exp.alias_(_node_object(walk, nm), _NODE_OBJECT),
                    exp.alias_(_labels_of(nm), LABELS_COLUMN),
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
                    exp.alias_(exp.Cast(this=exp.null(), to=_TEXT_LIST.copy()), LABELS_COLUMN),
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

    def _session_hop_ends(self, type_name: str | None, domain: str | None) -> list[str]:
        """The labels a session hop's end may be read by: its type's, its domain's, or any."""
        from provisa.gql.translator_union import union_labels

        if type_name is not None:
            return [type_name]
        return union_labels(self._lm, domain if domain else "__all__")

    def _session_end_filter(
        self, alias: str, type_name: str | None, domain: str | None
    ) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The condition a session hop's end ``alias`` (a row of _node_objects) meets: every
        label of its type (a label set's every one, label_map.node_of_all), or one of its
        domain's types' labels; None for any node."""
        if type_name is not None:
            labels = self._lm.label_sets.get(type_name) or (self._lm.nodes[type_name].label,)
            return exp.ArrayContainsAll(
                this=_col(LABELS_COLUMN, alias),
                expression=exp.Array(expressions=[exp.Literal.string(x) for x in labels]),
            )
        if domain:
            named = sorted({self._lm.nodes[t].label for t in self._session_hop_ends(None, domain)})
            return exp.In(
                this=_col("__label", alias), expressions=[exp.Literal.string(x) for x in named]
            )
        return None

    def _build_session_hop(  # complexity-gate: allow-cc=14 reason="one hop's ends, direction and columns, each a short branch of its one select"
        self,
        src_col: str,
        rel_col: str,
        tgt_col: str,
        *,
        types: list[str] | None,
        src_type: str | None,
        tgt_type: str | None,
        src_domain: str | None,
        tgt_domain: str | None,
        alias: str,
        undirected: bool,
        loop: bool,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """One hop of a graph session's pattern read from its one relationships relation (REQ-348,
        REQ-2176): every relationship of one of ``types`` (any, for None), its own way and, where
        ``undirected``, the other way too, each end a node of the session or of the model it
        ends at, carrying all of its labels and properties. The columns are those of a read of
        every relationship (translator_union._build_all_rels_union): each end's node, the edge,
        and the edge's own properties, under the hop's ``alias``. ``loop``: a relationship from
        a node to itself."""
        from provisa.gql.translator_helpers import edge_properties_column

        walk = self._lm.session_walk
        assert walk is not None
        relation = exp.Table(
            this=exp.Identifier(this=walk.table_name, quoted=True),
            db=exp.Identifier(this=walk.schema_name, quoted=True),
            catalog=exp.Identifier(this=walk.catalog_name, quoted=True),
        )

        def way(near: str, far: str, forward: bool) -> exp.Select:
            step = exp.select(
                _col("rel_id", "_r"),
                _col("type", "_r"),
                exp.alias_(_col(near, "_r"), "__near", quoted=True),
                exp.alias_(_col(far, "_r"), "__far", quoted=True),
                exp.alias_(exp.true() if forward else exp.false(), "__fwd", quoted=True),
            ).from_(exp.alias_(relation.copy(), "_r"))
            if types is not None:
                wanted = sorted({t.upper() for t in types})
                step = step.where(
                    exp.In(
                        this=exp.Upper(this=_col("type", "_r")),
                        expressions=[exp.Literal.string(t) for t in wanted],
                    )
                )
            return step

        steps: exp.Expression = way("from_id", "to_id", True)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if undirected and not loop:
            # The other way too, a relationship to its own start once.
            back = way("to_id", "from_id", False).where(
                exp.NEQ(this=_col("from_id", "_r"), expression=_col("to_id", "_r"))
            )
            steps = exp.Union(this=steps, expression=back, distinct=False)
        h, ns, nt, p = "_h", "_hs", "_ht", "_hp"
        src_json, tgt_json = _col(_NODE_OBJECT, ns), _col(_NODE_OBJECT, nt)
        # The edge's own start and end, whichever way the hop reads it.
        start = exp.Case(
            ifs=[exp.If(this=_col("__fwd", h), true=src_json.copy())], default=tgt_json.copy()
        )
        end = exp.Case(
            ifs=[exp.If(this=_col("__fwd", h), true=tgt_json.copy())], default=src_json.copy()
        )
        # And its ends' graph identities, as an edge read by its type names them (identity,
        # start, end: select_builder._build_edge_object), which a DELETE of it reads.
        start_id = exp.Case(
            ifs=[exp.If(this=_col("__fwd", h), true=_col(GRAPH_ID, ns))], default=_col(GRAPH_ID, nt)
        )
        end_id = exp.Case(
            ifs=[exp.If(this=_col("__fwd", h), true=_col(GRAPH_ID, nt))], default=_col(GRAPH_ID, ns)
        )
        edge = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=[
                exp.Literal.string("id"),
                _col("rel_id", h),
                exp.Literal.string("identity"),
                _col("rel_id", h),
                exp.Literal.string("start"),
                start_id,
                exp.Literal.string("end"),
                end_id,
                exp.Literal.string("type"),
                _col("type", h),
                exp.Literal.string("startNode"),
                start,
                exp.Literal.string("endNode"),
                end,
                exp.Literal.string("properties"),
                _col(_PROPS, p),
            ],
        )
        select = (
            exp.select(
                exp.alias_(src_json, src_col),
                exp.alias_(edge, rel_col),
                exp.alias_(tgt_json.copy(), tgt_col),
                exp.alias_(_col(_PROPS, p), edge_properties_column(rel_col)),
            )
            .from_(exp.alias_(exp.Subquery(this=steps), h))
            .join(
                exp.alias_(self._node_objects(walk, None), ns),
                on=exp.EQ(this=_col(GRAPH_ID, ns), expression=_col("__near", h)),
                join_type="INNER",
            )
            .join(
                exp.alias_(self._node_objects(walk, None), nt),
                on=exp.EQ(this=_col(GRAPH_ID, nt), expression=_col("__far", h)),
                join_type="INNER",
            )
            .join(
                exp.alias_(self._rel_properties(walk, types), p),
                on=exp.EQ(this=_col("rel_id", p), expression=_col("rel_id", h)),
                join_type="INNER",
            )
        )
        conditions = [
            c
            for c in (
                self._session_end_filter(ns, src_type, src_domain),
                self._session_end_filter(nt, tgt_type, tgt_domain),
                exp.EQ(this=_col("__near", h), expression=_col("__far", h)) if loop else None,
            )
            if c is not None
        ]
        if conditions:
            select = select.where(exp.and_(*conditions))
        # What a property read of each end or of the edge takes its one type from.
        self._all_rels_ends.setdefault(src_col, []).extend(
            self._session_hop_ends(src_type, src_domain)
        )
        self._all_rels_ends.setdefault(tgt_col, []).extend(
            self._session_hop_ends(tgt_type, tgt_domain)
        )
        wanted = {t.upper() for t in types} if types is not None else None
        self._all_rels_mappings.extend(
            r
            for r in self._lm.relationships.values()
            if r.via is not None
            and r.via.catalog_name == walk.catalog_name
            and (wanted is None or r.rel_type.upper() in wanted)
        )
        return exp.Subquery(this=select, alias=exp.TableAlias(this=exp.to_identifier(alias)))

    def _session_chain(self, rels: list[RelPattern]) -> bool:
        """Whether a pattern of several relationships, a variable-length one among them, is read
        hop by hop over a graph session's relationships (_translate_session_chain): in a session,
        where no relationship of the model could take any of its hops."""
        walk = self._lm.session_walk
        return (
            walk is not None
            and len(rels) > 1
            and any(r.variable_length for r in rels)
            and all(r.path is None and not self._walks_model_hops(walk, r) for r in rels)
        )

    def _translate_session_chain(  # complexity-gate: allow-cc=16 reason="each hop a walk or one relationship, each a short branch joined in turn"
        self,
        clause: MatchClause,
        nodes: list[NodePattern],
        rels: list[RelPattern],
        joined: set[str],
        from_expr: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        own_first: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        joins: list[dict],
        clause_index: int,
        CypherTranslateError: type,
    ) -> exp.Expression | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """A graph session's pattern of several relationships, variable-length ones among them
        (REQ-348, REQ-2176), read hop by hop from its first node: a variable-length hop a walk
        from the node the pattern has reached (_translate_session_walk), any other one hop read
        from the relationships relation (_build_session_hop). No relationship is matched twice in
        the pattern. Its path is every node and relationship of its hops, in order. Returns the
        statement's FROM."""
        walk = self._lm.session_walk
        assert walk is not None
        self._chain_count = getattr(self, "_chain_count", 0) + 1
        n = self._chain_count
        # Every node of the pattern named, so a later hop goes on from the one an earlier reached.
        nodes = [
            node if node.variable else dataclasses.replace(node, variable=f"__c{n}_{i}")
            for i, node in enumerate(nodes)
        ]
        held = set(joined)
        # Each hop's (nodes, relationships) lists, in pattern order, and what its relationships
        # are told apart by: an edge's id, or a walk's trail.
        segments: list[tuple[exp.Expression, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        taken: list[tuple[str, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for i, rel in enumerate(rels):
            src, tgt = nodes[i], nodes[i + 1]
            name = f"__c{n}_p{i}"
            replaced = i == 0 and (from_expr is None or from_expr is own_first)
            if rel.variable_length:
                hop = MatchClause(
                    pattern=PathFunction(
                        func_name="allpaths", pattern=PathPattern(nodes=[src, tgt], rels=[rel])
                    ),
                    variable=name,
                    optional=clause.optional,
                )
                assert isinstance(hop.pattern, PathFunction)
                alias = f"_cw{n}_{i}"
                src_from, walked = self._translate_session_walk(
                    hop,
                    hop.pattern,
                    src,
                    tgt,
                    rel,
                    walk,
                    held,
                    CypherTranslateError,
                    alias=alias,
                    backward=rel.direction == "left",
                    trail=True,
                )
                self._path_vars.pop(name, None)
                trail = self._walk_trails.pop(name)
                if replaced:
                    from_expr = src_from
                elif src.variable not in held:
                    joins.append({"table": src_from, "on": None, "join_type": "CROSS"})
                mine = ("trail", _col("trail", alias))
                distinct = [_distinct(mine, other) for other in taken]
                if distinct:
                    walked[0]["on"] = exp.and_(walked[0]["on"], *distinct)
                joins.extend(walked)
                segments.append(trail)
            else:
                part = dataclasses.replace(clause, variable=name)
                count = len(joins)
                start = self._every_rel_hop(  # type: ignore[attr-defined]
                    rel,
                    src,
                    tgt,
                    part,
                    clause_index,
                    from_expr,
                    joins,
                    held_source=src.variable in held,
                    held_target=tgt.variable in held,
                    replaceable=replaced,
                    src_nm=self._named_type(src),  # type: ignore[attr-defined]
                    tgt_nm=self._named_type(tgt),  # type: ignore[attr-defined]
                )
                ((first, edge, last),) = self._every_rel_paths.pop(name)  # type: ignore[attr-defined]
                table = self._all_rels_var_alias[edge]
                values = [exp.column(c, table=table) for c in (first, edge, last)]
                mine = (
                    "edge",
                    exp.Anonymous(
                        this="JSON_EXTRACT_SCALAR",
                        expressions=[values[1], exp.Literal.string("$.id")],
                    ),
                )
                distinct = [_distinct(mine, other) for other in taken if other[0] == "trail"]
                if distinct:
                    if replaced:
                        self._held_end_conditions.extend(distinct)  # type: ignore[attr-defined]
                    else:
                        assert len(joins) == count + 1, "one hop is one join"
                        joins[-1]["on"] = exp.and_(joins[-1]["on"] or exp.true(), *distinct)
                from_expr = start
                segments.append(
                    (_json_list([values[0], values[2]]), _json_list([values[1].copy()]))
                )
            taken.append(mine)
            held.update(v for v in (src.variable, tgt.variable) if v)
        if clause.variable:
            # The path: each hop's nodes after the first hop's without the one it starts at, the
            # node the hop before it reached; and every hop's relationships.
            path_nodes, path_rels = segments[0][0].copy(), segments[0][1].copy()
            for hop_nodes, hop_rels in segments[1:]:
                rest = exp.Anonymous(
                    this="slice",
                    expressions=[
                        hop_nodes.copy(),
                        exp.Literal.number(2),
                        exp.Anonymous(this="cardinality", expressions=[hop_nodes.copy()]),
                    ],
                )
                path_nodes = exp.ArrayConcat(this=path_nodes, expressions=[rest])
                path_rels = exp.ArrayConcat(this=path_rels, expressions=[hop_rels.copy()])
            self._walk_trails[clause.variable] = (path_nodes, path_rels)
            self._path_vars[clause.variable] = (
                nodes[0].variable or "",
                nodes[-1].variable or "",
                True,
            )
        return from_expr

    def _session_hop(self, rel: RelPattern) -> bool:
        """Whether ``rel``, one hop of a pattern, is read from a graph session's relationships
        (_build_session_hop): in a session, where no relationship of the model could take it."""
        walk = self._lm.session_walk
        return (
            walk is not None and not rel.variable_length and not self._walks_model_hops(walk, rel)
        )

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
        *,
        alias: str = "_t",
        backward: bool = False,
        trail: bool = False,
    ) -> tuple[exp.Expression, list[dict]]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """The path function ``pf`` walked over the session's relationships from ``src_node`` to
        ``tgt_node``. ``alias`` names the walk's rows in the statement (one per walk of a
        pattern of several hops, _translate_session_chain); ``backward``: each relationship
        followed from its end to its start (a pattern written leftward from its held end);
        ``trail``: the walk carries its nodes and relationships whatever the statement reads. shortestPath reads the walk's rows by hops, the first one (REQ-767);
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

        cte_name = f"_walk_{src_alias.lower()}" if alias == "_t" else f"_walk{alias}"
        bound = max_hops + 1 if by_limit else max_hops
        # REQ-751: a walk whose relationships or path the statement reads carries them, in the
        # order it took them, with every node it passed.
        named = trail or any(
            v is not None and reads_trail(self._ast, v)
            for v in (clause.variable, rel.variable if rel is not None else None)
        )
        nodes = self._node_objects(walk, self._start_labels(src_node)) if named else None
        objects = (nodes, self._rel_properties(walk, types)) if nodes is not None else None
        hops = _hops(walk, types, undirected, objects, backward=backward)
        t, h = "t", "_h"
        if min_hops == 0:
            if self._is_held_value(src_node):
                # A start a hop of the pattern holds as its value: the walk seeds from every
                # node, its join to the start (src_id) picking that one.
                seed_from = exp.alias_(self._node_objects(walk, None), "_seed")
                seed_id: exp.Expression = _col(GRAPH_ID, "_seed")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            else:
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

        on: exp.Condition = exp.EQ(this=_col("src_id", alias), expression=src_id.copy())
        if same or tgt_bound:
            on = exp.and_(on, exp.EQ(this=_col("cur_id", alias), expression=tgt_id.copy()))
        if by_limit:
            on = exp.and_(
                on, exp.LTE(this=_col("hops", alias), expression=exp.Literal.number(max_hops))
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
                on, exp.GTE(this=_col("hops", alias), expression=exp.Literal.number(min_hops))
            )
        join_type = "LEFT" if clause.optional else "INNER"
        joins: list[dict[str, Any]] = [
            {"table": exp.alias_(walked, alias), "on": on, "join_type": join_type}
        ]
        if tgt_from is not None and not tgt_bound:
            joins.append(
                {
                    "table": tgt_from,
                    "on": exp.EQ(this=tgt_id.copy(), expression=_col("cur_id", alias)),
                    "join_type": join_type,
                }
            )

        trail_cols = (_col("nodes", alias), _col("rels", alias))
        if rel is not None and rel.variable:
            # The walk's relationships, in the order it took them (REQ-751).
            self._varlen_rel_vars.setdefault(rel.variable, clause.variable or "")
            if nodes is not None:
                self._walk_trails[rel.variable] = trail_cols
        if clause.variable:
            self._path_vars[clause.variable] = (src_node.variable, tgt_node.variable, True)
            if nodes is not None:
                # The path: every node and relationship of the walk's trail.
                self._walk_trails[clause.variable] = trail_cols
        if func != "allpaths":
            self._shortestpath_hops_col = _col("hops", alias)
        self._shortestpath_is_all = func != "shortestpath"
        return src_from, joins

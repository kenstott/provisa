# Copyright (c) 2026 Kenneth Stott
# Canary: 0c9f9931-0236-48bd-9413-69ffe826535d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""All-relationships and domain UNION-building mixin for the Cypher→SQL translator.

Methods operate on _Translator state via self; mixed into _Translator, never
instantiated alone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from provisa.compiler.sql_types import key_list
from provisa.gql.label_map import GRAPH_ID
from provisa.gql.path_walk import graph_identity
from provisa.gql.select_builder import node_identity, node_key_text
from provisa.gql.translator_helpers import (
    _const_literal,
    _make_junction_joins,
    edge_properties_column,
    via_alias_for,
    via_column,
)

from provisa.gql.translator_types import NODE_COLUMN, CypherTranslateError

if TYPE_CHECKING:
    from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping


_INTEGERS = frozenset({"smallint", "integer", "bigint"})
_NUMBERS = _INTEGERS | {"float", "double", "numeric"}


def union_labels(lm: "CypherLabelMap", domain_name: str) -> list[str]:
    """The node types a node matched by a domain alone (or by no label, ``__all__``) is drawn
    from."""
    return list(lm.nodes.keys()) if domain_name == "__all__" else lm.domains[domain_name]


def attribute_type(prop: str, rels: "list[RelationshipMapping]") -> exp.DataType | None:
    """The one type a relationship property read across several relationships has (as
    union_type for nodes): the type every junction holding it registers it as, integers of
    several widths a bigint, integers and floats a double; None when they disagree or one does
    not say."""
    from provisa.core.ir_types import to_ir

    registered = [
        dict(rm.via.attribute_types).get(prop)
        for rm in rels
        if rm.via is not None and prop in rm.via.attributes
    ]
    if not registered or None in registered:
        return None
    if len(set(registered)) == 1:
        return exp.DataType.build(_spelled(str(registered[0])), dialect="postgres")
    kinds = {to_ir(str(t)) for t in registered}
    if kinds <= _INTEGERS:
        return exp.DataType.build("BIGINT")
    if kinds <= _NUMBERS:
        return exp.DataType.build("DOUBLE")
    return None


def _spelled(kind: str) -> str:
    """A registered type as SQL spells it: a session's ``list<elem>`` an array of ``elem``."""
    return f"{_spelled(kind[5:-1])}[]" if kind.startswith("list<") else kind


def union_type(prop: str, labels: "list[str]", lm: "CypherLabelMap") -> exp.DataType | None:
    """The one type a property of a node drawn from several tables has: the type every table
    holding it registers it as; integers of several widths a bigint, integers and floats a
    double. None when the tables disagree or one does not say: the UNION then reads it as text."""
    from provisa.core.ir_types import to_ir

    registered = []
    for label in labels:
        nm = lm.nodes.get(label)
        if nm is None or not nm.physical_properties.get(prop):
            continue
        registered.append(dict(nm.property_types).get(prop))
    if not registered or None in registered:
        return None
    if len(set(registered)) == 1:
        return exp.DataType.build(_spelled(str(registered[0])), dialect="postgres")
    kinds = {to_ir(str(t)) for t in registered}
    if kinds <= _INTEGERS:
        return exp.DataType.build("BIGINT")
    if kinds <= _NUMBERS:
        return exp.DataType.build("DOUBLE")
    return None


def key_names(nm: "NodeMapping") -> list[str]:
    """The columns of ``nm``'s key, in key order (select_builder.node_key_text)."""
    return list(nm.key_columns) if len(nm.key_columns) > 1 else [nm.id_column]


def _key_column(alias: str, column: str) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=column, quoted=True), table=exp.Identifier(this=alias)
    )


def _one_row_edge(
    rm: "RelationshipMapping", sa: str, ta: str, nm: "NodeMapping", via_alias: str | None
) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether an edge of ``rm``, a relationship from ``nm``'s table to itself read from ``sa`` to
    ``ta``, relates a row to itself: read from one row's own columns where the relationship joins
    on the table's key -- the junction's two keys, or the row's reference and its own key -- so
    no second read of the table is compared with the first (REQ-603); else the two reads' keys."""
    keys = tuple(key_names(nm))
    src_cols, tgt_cols = key_list(rm.join_source_column), key_list(rm.join_target_column)
    computed = rm.source_expr is not None or rm.target_expr is not None
    pairs: list[tuple[exp.Expression, exp.Expression]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if rm.via is not None and via_alias is not None and src_cols == tgt_cols == keys:
        pairs = [
            (via_column(via_alias, s), via_column(via_alias, t))
            for s, t in zip(rm.via.source_columns, rm.via.target_columns, strict=True)
        ]
    elif rm.via is None and not computed and rm.source_constant is None and tgt_cols == keys:
        pairs = [(_key_column(sa, s), _key_column(sa, t)) for s, t in zip(src_cols, tgt_cols)]
    elif rm.via is None and not computed and rm.source_constant is None and src_cols == keys:
        pairs = [(_key_column(ta, t), _key_column(ta, s)) for s, t in zip(src_cols, tgt_cols)]
    if not pairs or len(pairs) != len(keys):
        return _same_row(sa, ta, nm)
    same = exp.EQ(this=pairs[0][0], expression=pairs[0][1])
    for left, right in pairs[1:]:
        same = exp.And(this=same, expression=exp.EQ(this=left, expression=right))
    return same


def _same_row(left: str, right: str, nm: "NodeMapping") -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Whether ``left`` and ``right``, two reads of ``nm``'s table, are one row: its whole key."""
    keys = [
        exp.EQ(this=_key_column(left, k), expression=_key_column(right, k)) for k in key_names(nm)
    ]
    same = keys[0]
    for key in keys[1:]:
        same = exp.And(this=same, expression=key)
    return same


def _node_value(nm: "NodeMapping") -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``nm``'s row as the node JSON value a domain union's arm carries (NODE_COLUMN)."""
    kv: list[exp.Expression] = [  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        exp.JSONKeyValue(
            this=exp.Literal.string("id"),
            expression=node_identity(None, nm),
        ),
        exp.JSONKeyValue(this=exp.Literal.string("label"), expression=exp.Literal.string(nm.label)),
    ]
    # In name order, as the union's property columns were; every column under its own name, ``id``
    # and ``label`` included, held apart from the node's own keys.
    props: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for prop in sorted(nm.properties):
        # As a property column of the arm is: its physical column as text, null with none.
        phys_col = nm.physical_properties.get(prop)
        value = (
            exp.Cast(
                this=exp.Column(this=exp.Identifier(this=phys_col, quoted=True)),
                to=exp.DataType(this=exp.DataType.Type.VARCHAR),
            )
            if phys_col
            else exp.null()
        )
        props.append(exp.JSONKeyValue(this=exp.Literal.string(prop), expression=value))
    kv.append(
        exp.JSONKeyValue(
            this=exp.Literal.string("properties"), expression=exp.JSONObject(expressions=props)
        )
    )
    return exp.JSONObject(expressions=kv)


class _UnionMixin:  # mixin for _Translator
    if TYPE_CHECKING:
        # State and collaborating methods this mixin reads off ``self``; they belong to
        # ``_Translator``, the only class it is mixed into. Guarded, because an unconditional stub
        # would sit ahead of the real definition in the MRO and shadow it at runtime.
        from provisa.gql.label_map import CypherLabelMap

        _lm: CypherLabelMap
        _all_rels_ends: dict[str, list[str]]
        _all_rels_mappings: list[RelationshipMapping]
        _union_columns: dict[str, list[str]]
        _graph_id_unions: set[str]

        def _collect_var_props(self, var: str) -> list[str]: ...

        def _node_as_one_value(self, var: str) -> bool: ...

        def _referenced_var_props(self, var: str) -> list[str]: ...

    def _build_all_rels_union(
        self,
        src_var: str | None,
        rel_var: str | None,
        tgt_var: str | None,
        src_domain: str | None = None,
        tgt_domain: str | None = None,
        *,
        src_type: str | None = None,
        tgt_type: str | None = None,
        rel_type: str | None = None,
        alias: str = "_all_rels",
        undirected: bool = False,
        src_props_of: str | None = None,
        tgt_props_of: str | None = None,
        loop: bool = False,
        src_keys: bool = False,
        tgt_keys: bool = False,
    ) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all relationship types for fully-unlabeled patterns
        (only ``rel_type``'s when it names one).

        src_domain / tgt_domain constrain the union to only include rels whose
        source / target node belongs to the named domain (PascalCase domain label).
        Pass None or "__all__" to leave that side unconstrained. src_type / tgt_type constrain it
        to the rels whose source / target is that node type (an endpoint labeled with a table).

        ``alias`` names the subquery: a statement reading several such hops reads each under its
        own (translator_every_rel). ``undirected``: each relationship read from either end, a
        self-loop once. ``src_props_of`` / ``tgt_props_of``: the variable whose referenced
        properties an end carries, where its column is not named by it (an end the statement
        already holds). ``loop``: only each relationship from a row to itself (a self-loop),
        that row's key on both ends. ``src_keys`` / ``tgt_keys``: each key column of an end's row
        beside it, ``<column>__key<i>``, for a node the statement already holds to be joined on.
        """
        # An anonymous end's column is named as no variable is, so an outer statement's variable
        # an inner statement reads (its ``n``) is never this union's column.
        src_col = src_var or "__src"
        rel_col = rel_var or "__rel"
        tgt_col = tgt_var or "__tgt"
        if alias == "_all_rels":
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

        # Scope startNode/endNode properties to what the query actually references (mirrors
        # _build_domain_union's use of _collect_var_props). An unreferenced n/m — e.g.
        # `MATCH (n)-[r]->(m) RETURN r` — pulls in zero property columns, so masked columns
        # on node types the query never asked for don't trip V003 column-visibility checks.
        src_props_wanted = set(self._collect_var_props(src_props_of or src_col))
        tgt_props_wanted = set(self._collect_var_props(tgt_props_of or tgt_col))

        branches: list[exp.Select] = []
        # The node types each end may be, and the relationships the edge may be, which a property
        # read of one of them takes its one type from (expr_context.resolve_property).
        # Held across the hops of a statement: each hop's columns are its own.
        self._all_rels_ends.setdefault(src_col, [])
        self._all_rels_ends.setdefault(tgt_col, [])
        for rm in self._lm.relationships.values():
            src_nm = self._lm.nodes.get(rm.source_label)
            tgt_nm = self._lm.nodes.get(rm.target_label)
            if src_nm is None or tgt_nm is None:
                continue
            # REQ-1132: mirror _build_domain_union — an unlabeled relationship match roots
            # only on directly-queryable nodes, so an endpoint reachable only by traversal
            # (e.g. the meta/catalog domain for a role without a meta grant) is excluded here
            # too, or this union would emit a direct FROM the role is V001-blocked on.
            if src_nm.traversal_only or tgt_nm.traversal_only:
                continue
            if loop and rm.source_label != rm.target_label:
                continue  # a relationship between two tables relates no row to itself
            # A self-loop read from either end is one match: read forward.
            for reverse in (False, True) if undirected and not loop else (False,):
                # The end the pattern reads first, and the one it reads last: read backward, the
                # relationship's target comes first.
                near, far = (
                    (rm.target_label, rm.source_label)
                    if reverse
                    else (rm.source_label, rm.target_label)
                )
                if src_type_set is not None and near not in src_type_set:
                    continue
                if tgt_type_set is not None and far not in tgt_type_set:
                    continue
                if src_type is not None and near != src_type:
                    continue
                if tgt_type is not None and far != tgt_type:
                    continue
                if rel_type is not None and rm.rel_type.upper() != rel_type:
                    continue
                # Skip synthetic constant-join rels (e.g. HAS_TABLE to meta tables).
                # These are not real FK traversals and would pull in unrelated domain nodes.
                if rm.constant_source:
                    continue

                sa = f"_s_{rm.rel_type.lower()[:20]}"
                ta = f"_t_{rm.rel_type.lower()[:20]}"

                # The properties each end carries are those of the column it lands in.
                s_wanted, t_wanted = (
                    (tgt_props_wanted, src_props_wanted)
                    if reverse
                    else (src_props_wanted, tgt_props_wanted)
                )
                # Each end's identity and key text hold every column of a compound key.
                src_compound_id, tgt_compound_id = (
                    node_identity(sa, src_nm),
                    node_identity(ta, tgt_nm),
                )
                src_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
                for prop_name, col_name in src_nm.properties.items():
                    if prop_name not in s_wanted:
                        continue
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
                tgt_props_exprs: list[exp.Expression] = []  # pyright: ignore[reportPrivateImportUsage]
                for prop_name, col_name in tgt_nm.properties.items():
                    if prop_name not in t_wanted:
                        continue
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
                edge_id: exp.Expression = exp.DPipe(
                    this=exp.DPipe(
                        this=node_key_text(sa, src_nm), expression=exp.Literal.string("-")
                    ),
                    expression=node_key_text(ta, tgt_nm),
                )
                # REQ-1586: the junction row's own columns are this edge's properties, so an
                # all-relationships result carries them exactly as a named pattern does.
                via_alias = via_alias_for(rm, rel_var or ta) if rm.via is not None else None
                if rm.via is not None and rm.via.identity_column is not None:
                    assert via_alias is not None
                    edge_id = via_column(via_alias, rm.via.identity_column)  # REQ-2176
                edge_props: list[exp.Expression] = []
                if rm.via is not None:
                    assert via_alias is not None
                    for prop_name, col in rm.via.attributes.items():
                        edge_props.append(exp.Literal.string(prop_name))
                        edge_props.append(via_column(via_alias, col))
                edge_json = exp.JSONObject(
                    expressions=[
                        exp.JSONKeyValue(this=exp.Literal.string("id"), expression=edge_id),
                        exp.JSONKeyValue(
                            this=exp.Literal.string("type"),
                            expression=exp.Literal.string(rm.rel_type),
                        ),
                        exp.JSONKeyValue(this=exp.Literal.string("startNode"), expression=src_json),
                        exp.JSONKeyValue(this=exp.Literal.string("endNode"), expression=tgt_json),
                        exp.JSONKeyValue(
                            this=exp.Literal.string("properties"),
                            expression=exp.Anonymous(this="JSON_OBJECT", expressions=edge_props),
                        ),
                        # REQ-1586: a junction-backed edge names the table its attributes came from,
                        # beside properties rather than inside them so it cannot collide with a column.
                        *(
                            []
                            if rm.via is None
                            else [
                                exp.JSONKeyValue(
                                    this=exp.Literal.string("junctionTable"),
                                    expression=exp.Literal.string(rm.via.table_name),
                                )
                            ]
                        ),
                    ]
                )

                branch = exp.select(
                    exp.alias_(tgt_json if reverse else src_json, src_col),
                    exp.alias_(edge_json, rel_col),
                    exp.alias_(src_json if reverse else tgt_json, tgt_col),
                    # The edge's own properties alone, which r.<prop> reads: the edge's JSON carries
                    # an id built from both endpoints' keys, and a condition on a value of it would
                    # match the two tables by their keys as well as by the relationship (REQ-603).
                    exp.alias_(
                        exp.Anonymous(this="JSON_OBJECT", expressions=list(edge_props)),
                        edge_properties_column(rel_col),
                    ),
                ).from_(
                    exp.alias_(
                        exp.Table(
                            this=exp.Identifier(this=src_nm.sql_table_name, quoted=True),
                            db=exp.Identifier(this=src_nm.schema_name, quoted=True),
                            catalog=exp.Identifier(this=src_nm.catalog_name, quoted=True),
                        ),
                        alias=sa,
                    )
                )
                if rm.via is not None:
                    # REQ-1586: a junction-backed edge in the union reaches its target through the
                    # junction table, so the branch carries the same two joins a named pattern emits.
                    assert via_alias is not None
                    for d in _make_junction_joins(rm, False, tgt_nm, ta, sa, "INNER", via_alias):
                        branch = branch.join(d["table"], on=d["on"], join_type=d["join_type"])
                else:
                    branch = branch.join(
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
                                        dialect="postgres",
                                    )
                                    if rm.source_expr is not None
                                    else exp.Column(
                                        this=exp.Identifier(
                                            this=rm.join_source_column, quoted=True
                                        ),
                                        table=exp.Identifier(this=sa),
                                    )
                                )
                            ),
                            expression=(
                                exp.maybe_parse(
                                    rm.target_expr.replace("{alias}", ta),
                                    dialect="postgres",
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
                if loop:
                    # The relationship's two ends are one row.
                    branch = branch.where(_one_row_edge(rm, sa, ta, src_nm, via_alias))
                elif reverse and rm.source_label == rm.target_label:
                    # A self-loop read from either end is one match, read forward.
                    one_row = _one_row_edge(rm, sa, ta, src_nm, via_alias)
                    branch = branch.where(exp.Not(this=exp.paren(one_row)))
                near_alias, near_nm, far_alias, far_nm = (
                    (ta, tgt_nm, sa, src_nm) if reverse else (sa, src_nm, ta, tgt_nm)
                )
                for wanted, col, a, nm in (
                    (src_keys, src_col, near_alias, near_nm),
                    (tgt_keys, tgt_col, far_alias, far_nm),
                ):
                    if wanted:
                        branch = branch.select(
                            *(
                                exp.alias_(_key_column(a, k), f"{col}__key{i}")
                                for i, k in enumerate(key_names(nm))
                            ),
                            append=True,
                        )
                branches.append(branch)
                self._all_rels_ends[src_col].append(near)
                self._all_rels_ends[tgt_col].append(far)
                self._all_rels_mappings.append(rm)

        if not branches:
            raise CypherTranslateError("No relationship types found in schema")

        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for b in branches[1:]:
            union = exp.Union(this=union, expression=b, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=alias)  # pyright: ignore[reportReturnType]

    def _build_domain_union(self, var: str, domain_name: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Build UNION ALL subquery over all types in a domain."""
        type_labels = union_labels(self._lm, domain_name)
        # A node returned bare is one JSON value per arm, of that type's own properties (NODE_COLUMN),
        # beside the properties the statement reads of it (a WHERE, a WITH, an ORDER BY).
        as_one_value = self._node_as_one_value(var)
        props = self._referenced_var_props(var) if as_one_value else self._collect_var_props(var)

        held_as = {
            p: union_type(p, type_labels, self._lm) or exp.DataType.build("VARCHAR") for p in props
        }
        branches: list[exp.Select] = []
        had_resolvable = False
        walk = self._lm.session_walk
        arms: list[NodeMapping] = []
        for label in type_labels:
            nm = self._lm.nodes.get(label)
            if nm is None:
                continue
            if nm.native_filter_columns:
                continue
            # REQ-1132: a bare MATCH (n) roots only on directly-queryable nodes. Traversal-only nodes
            # (e.g. the meta/catalog domain for a role without a meta grant) are reachable only by
            # traversing from an owned node, never as a bare-match root — skip them from the __all__
            # union so this never emits a direct FROM the role would be V001-blocked on.
            if nm.traversal_only:
                continue
            had_resolvable = True
            # Metadata-based prune: skip branches where no required property exists.
            # Those branches can only contribute NULL rows, which a WHERE filter would discard.
            # This prevents full-table scans across all entity types on the engine.
            # A node returned bare is every node's, whichever properties it has.
            if props and not as_one_value and not any(nm.properties.get(p) for p in props):
                continue
            select_items: list[exp.Expr] = [
                exp.alias_(exp.Literal.string(nm.label), alias="__label"),
                exp.alias_(node_identity(None, nm), alias="__id"),
            ]
            if as_one_value:
                select_items.append(exp.alias_(_node_value(nm), alias=NODE_COLUMN))
            for prop in props:
                phys_col = nm.physical_properties.get(prop)
                value = (
                    exp.Column(this=exp.Identifier(this=phys_col, quoted=True))
                    if phys_col
                    else exp.null()
                )
                # Each arm's value as the one type the property has in every table holding it.
                select_items.append(
                    exp.alias_(
                        exp.Cast(this=value, to=held_as[prop]),
                        alias=exp.Identifier(this=prop, quoted=True),
                    )
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
            arms.append(nm)

        if walk is not None and any(nm.catalog_name == walk.catalog_name for nm in arms):
            # REQ-348: a union holding a graph session's nodes names each one's graph identity,
            # which the session's relationships, and a path function's walk, join it by. A union
            # of the model's nodes alone has it already: __id.
            for branch, nm in zip(branches, arms):
                branch.select(
                    exp.alias_(graph_identity(walk, nm, None), alias=GRAPH_ID), copy=False
                )
            self._graph_id_unions.add(var)

        if not branches:
            if not had_resolvable:
                # No type to draw from (a role served no tables, a session holding no nodes): the
                # match is over nothing, so it reads no rows, each column typed as an arm types it.
                text = exp.DataType.build("VARCHAR")
                typed: list[exp.Expr] = [
                    exp.alias_(exp.Cast(this=exp.null(), to=text), alias="__label"),
                    exp.alias_(exp.Cast(this=exp.null(), to=text), alias="__id"),
                    *(
                        [
                            exp.alias_(
                                exp.Cast(this=exp.null(), to=exp.DataType.build("JSON")),
                                alias=NODE_COLUMN,
                            )
                        ]
                        if as_one_value
                        else []
                    ),
                ] + [
                    exp.alias_(
                        exp.Cast(this=exp.null(), to=held_as[p]),
                        alias=exp.Identifier(this=p, quoted=True),
                    )
                    for p in props
                ]
                empty = exp.select(*typed).where(exp.false())
                self._union_columns[var] = empty.named_selects
                return exp.alias_(exp.Subquery(this=empty), alias=var)  # pyright: ignore[reportReturnType]
            # All types exist but none have the required properties — return zero rows without
            # scanning any tables (metadata resolved this at translation time).
            zero_items: list[exp.Expr] = [
                exp.alias_(exp.null(), alias="__label"),
                exp.alias_(exp.null(), alias="__id"),
                *([exp.alias_(exp.null(), alias=NODE_COLUMN)] if as_one_value else []),
            ] + [exp.alias_(exp.null(), alias=exp.Identifier(this=p, quoted=True)) for p in props]
            zero_row = exp.select(*zero_items).where(exp.false())
            self._union_columns[var] = zero_row.named_selects
            return exp.alias_(exp.Subquery(this=zero_row), alias=var)  # pyright: ignore[reportReturnType]

        self._union_columns[var] = branches[0].named_selects
        union: exp.Expression = branches[0]  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for branch in branches[1:]:
            union = exp.Union(this=union, expression=branch, distinct=False)
        return exp.alias_(exp.Subquery(this=union), alias=var)  # pyright: ignore[reportReturnType]

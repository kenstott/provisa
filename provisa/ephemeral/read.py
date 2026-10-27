# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The read overlay (REQ-2176): a label with session matches, and a relationship type with
session relationships, read the session's part beside the governed compiler's own answer.

Two steps around the governed compiler, which is not changed:

1. :func:`overlay` -- before it compiles: each such label maps to a session relation named
   ``"__session__"."s"."node__<key>"`` (each such type to ``"rel__<type>"``); every other label
   keeps its model mapping, the very object it had.
2. :func:`expand` -- after it compiles, before the governed pipeline: each session relation is
   replaced by its rows. A label's are its model table's rows -- the registered table itself,
   so the pipeline governs them as any read -- beside the session's: ephemeral nodes carrying
   the label (their anchor tables, by node_labels) and model nodes the session tags with it
   (their model table joined to node_labels, governed likewise). Columns are unioned by name: a
   column a part lacks reads NULL, and two parts' types widen to one.

A node of such a relation has ``__node_id`` (a model node's key, so its identity in an answer is
what it is without the session; an ephemeral node's id) and ``__graph_id`` (``Label|key`` or the
ephemeral id), which the session's relationships join on.
"""

# Requirements: REQ-2176

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

import sqlglot.expressions as exp

from provisa.compiler.temp_tables import TempSession
from provisa.ephemeral import store

SESSION_CATALOG = "__session__"
SESSION_SCHEMA = "s"
NODE_ID = "__node_id"
GRAPH_ID = "__graph_id"


def _node_relation(key: str) -> str:
    return f"node__{key}"


def _rel_relation(rel_type: str) -> str:
    return f"rel__{rel_type}"


def involved(session: TempSession | None) -> bool:
    """Whether the session holds ephemeral objects: only then is a statement read through the
    overlay."""
    return session is not None and not store.graph(session).empty


def _view_keys(graph: store.Graph) -> set[str]:
    """The labels (as the session holds them) with session matches: each label an ephemeral
    node carries or a tag gives, and each label a session relationship's end is read by."""
    keys = set(graph.labels)
    for _, source, target in graph.rel_pairs:
        keys.update((source, target))
    keys.discard("")
    return keys


# --------------------------------------------------------------------------- columns


def _widened(a: str, b: str) -> str:
    from provisa.ephemeral import types

    if a == b:
        return a
    if {a, b} <= {"bigint", "integer", "smallint", "double"}:
        return "double" if "double" in {a, b} else "bigint"
    return types.widened(a, b) or a


def _model_columns(nm: Any) -> list[tuple[str, str, str]]:
    """A model node's (property, the column its relation names it by, type)."""
    types = dict(nm.property_types)
    return [(prop, alias, str(types.get(prop, "text"))) for prop, alias in nm.properties.items()]


def _session_columns(session: TempSession, anchors: set[str]) -> list[tuple[str, str]]:
    """The property columns the anchor tables hold, each name once, its types widened."""
    held: dict[str, str] = {}
    for anchor in sorted(anchors):
        for name, kind in session.tables[anchor].columns:
            if name == store.NODE_ID:
                continue
            held[name] = _widened(held[name], kind) if name in held else kind
    return list(held.items())


# --------------------------------------------------------------------------- 1. the overlay


def overlay(model: Any, session: TempSession) -> Any:
    """``model`` (the role's label map) with each label of session matches, and each type of
    session relationships, mapped to its session relation. ``model`` itself when the session
    holds nothing."""
    from provisa.gql.label_map import JunctionMapping, NodeMapping, RelationshipMapping

    graph = store.graph(session)
    if graph.empty:
        return model
    nodes = dict(model.nodes)
    nodes_by_table = {k: list(v) for k, v in model.nodes_by_table.items()}
    for key in sorted(_view_keys(graph)):
        held = model.nodes.get(key)
        anchors = graph.label_anchors.get(key, set())
        properties = {prop: alias for prop, alias, _ in _model_columns(held)} if held else {}
        types = {alias: kind for _, alias, kind in _model_columns(held)} if held else {}
        for name, kind in _session_columns(session, anchors):
            properties.setdefault(name, name)
            types[name] = _widened(types[name], kind) if name in types else kind
        nodes[key] = NodeMapping(
            label=held.label if held else key,
            type_name=key,
            domain_label=held.domain_label if held else None,
            domain_id=held.domain_id if held else None,
            table_label=held.table_label if held else key,
            table_id=held.table_id if held else -1,
            source_id=held.source_id if held else "",
            id_column=NODE_ID,
            pk_columns=[NODE_ID],
            catalog_name=SESSION_CATALOG,
            schema_name=SESSION_SCHEMA,
            table_name=_node_relation(key),
            properties=properties,
            physical_properties={prop: alias for prop, alias in properties.items()},
            property_types=tuple((prop, types[alias]) for prop, alias in properties.items()),
        )
        if held is None:
            nodes_by_table.setdefault(key, [key])
    relationships = dict(model.relationships)
    aliases = {k: list(v) for k, v in model.aliases.items()}
    for rel_type, source, target in sorted(graph.rel_pairs):
        if source not in nodes or target not in nodes:
            continue  # an end with no label: read by a match of any node (not yet overlaid)
        columns = session.tables[store.rel_table(rel_type)].columns
        mapping = RelationshipMapping(
            rel_type=rel_type,
            source_label=source,
            target_label=target,
            join_source_column=GRAPH_ID,
            join_target_column=GRAPH_ID,
            field_name=f"{SESSION_CATALOG}:{rel_type}:{source}:{target}",
            via=JunctionMapping(
                catalog_name=SESSION_CATALOG,
                schema_name=SESSION_SCHEMA,
                table_name=_rel_relation(rel_type),
                type_name=_rel_relation(rel_type),
                source_columns=("from_id",),
                target_columns=("to_id",),
                attributes={name: name for name, _ in columns if name != store.REL_ID},
            ),
        )
        relationships[mapping.field_name] = mapping
        aliases.setdefault(rel_type.upper(), []).append(mapping)
    # A copy of the model's map with its node, table and relationship maps replaced; everything
    # else it holds is the model's own.
    out = copy.copy(model)
    out.nodes = nodes
    out.nodes_by_table = nodes_by_table
    out.relationships = relationships
    out.aliases = aliases
    out._nodes_ci = {k.lower(): k for k in nodes}
    out._nodes_by_table_ci = {k.lower(): k for k in nodes_by_table}
    return out


# --------------------------------------------------------------------------- 2. the rows


def _col(name: str, table: str | None = None) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=name, quoted=True),
        table=exp.Identifier(this=table, quoted=True) if table else None,
    )


def _typed(value: exp.Expression, kind: str) -> exp.Expression:
    """``value`` as a column of ``kind`` (a registered type, or a session type of types.py)."""
    spelled = {"zoned_time": "TIMETZ", "json": "JSON"}.get(kind, kind)
    if spelled.startswith("list<"):
        spelled = f"{spelled[5:-1]}[]"
    return exp.Cast(this=value, to=exp.DataType.build(spelled, dialect="postgres"))


def _node_rows(key: str, nm: Any, model: Any, session: TempSession) -> exp.Expression:
    """The rows of the label ``key``'s session relation (``nm``, its overlay mapping)."""
    from provisa.gql.select_builder import node_identity, node_key_text

    graph = store.graph(session)
    held = model.nodes.get(key)
    columns = [(alias, dict(nm.property_types)[prop]) for prop, alias in nm.properties.items()]
    parts: list[exp.Select] = []

    def select(
        values: Mapping[str, exp.Expression], node_id: exp.Expression, graph_id: exp.Expression
    ) -> exp.Select:
        return exp.select(
            exp.alias_(node_id, NODE_ID, quoted=True),
            exp.alias_(graph_id, GRAPH_ID, quoted=True),
            *(
                exp.alias_(_typed(values.get(alias, exp.null()), kind), alias, quoted=True)
                for alias, kind in columns
            ),
        )

    def model_rows(model_nm: Any, alias: str) -> exp.Select:
        # The node's relation as the compiler reads it, each column by its published name.
        from provisa.gql.translator_helpers import _node_table_expr

        values = {a: _col(a, alias) for _, a, _ in _model_columns(model_nm)}
        return select(values, node_key_text(alias, model_nm), node_identity(alias, model_nm)).from_(
            _node_table_expr(model_nm, alias)
        )

    def tagged(rows: exp.Select, graph_id: exp.Expression) -> exp.Select:
        return rows.join(
            exp.alias_(exp.to_table(store.NODE_LABELS, quoted=True), "_l"),
            on=exp.and_(
                exp.EQ(this=_col(store.NODE_ID, "_l"), expression=graph_id),
                exp.EQ(this=_col("label", "_l"), expression=exp.Literal.string(key)),
            ),
            join_type="inner",
        )

    if held is not None:
        parts.append(model_rows(held, "_m"))
    for type_name in sorted(graph.tag_types.get(key, set())):
        if type_name == key:
            continue  # a model label's own rows are all read already
        tagged_nm = model.nodes[type_name]
        parts.append(tagged(model_rows(tagged_nm, "_t"), node_identity("_t", tagged_nm)))
    for anchor in sorted(graph.label_anchors.get(key, set())):
        anchor_columns = {name for name, _ in session.tables[anchor].columns}
        values = {a: _col(a, "_a") for a, _ in columns if a in anchor_columns}
        rows = select(values, _col(store.NODE_ID, "_a"), _col(store.NODE_ID, "_a")).from_(
            exp.alias_(exp.to_table(anchor, quoted=True), "_a")
        )
        parts.append(tagged(rows, _col(store.NODE_ID, "_a")))
    union: exp.Expression = parts[0]
    for part in parts[1:]:
        union = exp.Union(this=union, expression=part, distinct=False)
    return union


def _rel_rows(rel_type: str, session: TempSession) -> exp.Expression:
    """The rows of a relationship type's session relation: each relationship's ends and its
    properties."""
    props = [n for n, _ in session.tables[store.rel_table(rel_type)].columns if n != store.REL_ID]
    return (
        exp.select(
            _col("from_id", "_r"),
            _col("to_id", "_r"),
            *(_col(p, "_p") for p in props),
        )
        .from_(exp.alias_(exp.to_table(store.RELS, quoted=True), "_r"))
        .join(
            exp.alias_(exp.to_table(store.rel_table(rel_type), quoted=True), "_p"),
            on=exp.EQ(this=_col(store.REL_ID, "_p"), expression=_col(store.REL_ID, "_r")),
            join_type="inner",
        )
        .where(exp.EQ(this=_col("type", "_r"), expression=exp.Literal.string(rel_type)))
    )


def expand(sql: exp.Expression, overlaid: Any, model: Any, session: TempSession) -> exp.Expression:
    """``sql`` (compiled over ``overlaid``) with each session relation replaced by its rows."""
    graph = store.graph(session)
    by_name = {_node_relation(key): key for key in _view_keys(graph) if key in overlaid.nodes}
    types = {_rel_relation(t): t for t in graph.types}
    for table in list(sql.find_all(exp.Table)):
        if table.catalog != SESSION_CATALOG:
            continue
        alias = table.alias or table.name
        if table.name in by_name:
            key = by_name[table.name]
            rows = _node_rows(key, overlaid.nodes[key], model, session)
        else:
            rows = _rel_rows(types[table.name], session)
        table.replace(exp.alias_(exp.Subquery(this=rows), alias))
    return sql

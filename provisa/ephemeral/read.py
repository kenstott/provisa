# Copyright (c) 2026 Kenneth Stott
# Canary: 1b3a6e53-5c8c-4299-b83f-107a6fe06f19
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

A node matched by no label, or by a domain, is drawn from a union of every type's relation (the
compiler's own); :func:`expand` reads such a union once per node, by ``__graph_id``, the first
arm to hold a node giving its row -- a model type's arms come before the session's labels'. The
session's unlabeled nodes are the relation of the label ``""``, which no statement can write, so
only a match of any node reads it.

A node of such a relation has ``__node_id`` (a model node's key under its own label, so its
identity in an answer is what it is without the session; its graph identity under a tag; an
ephemeral node's id) and ``__graph_id`` (``Label|key`` or the
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
from provisa.gql.label_map import (
    GRAPH_ID,
    IDENTITY_COLUMN,
    LABELS_COLUMN,
    SESSION_CATALOG,
)

SESSION_SCHEMA = "s"
NODE_ID = "__node_id"
#: The session relation of every session relationship, which a path function walks (REQ-348).
WALK_RELATION = "rels"


def _node_relation(key: str) -> str:
    return f"node__{key}"


def _rel_relation(rel_type: str) -> str:
    return f"rel__{rel_type}"


def involved(session: TempSession | None) -> bool:
    """Whether the session holds ephemeral objects: only then is a statement read through the
    overlay."""
    return session is not None and not store.graph(session).empty


def _model_columns(nm: Any) -> list[tuple[str, str, str]]:
    """A model node's (property, the column its relation names it by, type)."""
    types = dict(nm.property_types)
    return [(prop, alias, str(types.get(prop, "text"))) for prop, alias in nm.properties.items()]


def walkable(model: Any) -> Any:
    """``model`` in a graph session holding no ephemeral object: its own maps, and the session's
    relationships for a path function with an unlabelled end to walk (REQ-348), none yet. A read
    that walks none compiles to the very SQL it compiles over ``model``."""
    from provisa.gql.label_map import SessionWalk

    out = copy.copy(model)
    out.session_walk = SessionWalk(SESSION_CATALOG, SESSION_SCHEMA, WALK_RELATION)
    return out


def overlay(model: Any, session: TempSession) -> Any:
    """``model`` (the role's label map) with each label of session matches, and each type of
    session relationships, mapped to its session relation. ``model`` itself when the session
    holds nothing."""
    from provisa.gql.label_map import (
        JunctionMapping,
        NodeMapping,
        RelationshipMapping,
        SessionWalk,
    )

    graph = store.graph(session)
    if graph.empty:
        return model
    nodes = dict(model.nodes)
    nodes_by_table = {k: list(v) for k, v in model.nodes_by_table.items()}
    for key, resolved in sorted(graph.resolved.items()):
        held = model.nodes.get(key)
        properties = {prop: alias for prop, alias, _ in _model_columns(held)} if held else {}
        types = {alias: kind for _, alias, kind in _model_columns(held)} if held else {}
        # A tag's label reads its model nodes' columns beside its ephemeral nodes'.
        tagged = [
            (alias, kind)
            for type_name in resolved.tagged
            if type_name != key
            for _, alias, kind in _model_columns(model.nodes[type_name])
        ]
        for name, kind in [*tagged, *resolved.columns]:
            properties.setdefault(name, name)
            types[name] = store.widened_kind(types[name], kind) if name in types else kind
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
    # While a read after a write runs (then.py), a relationship's id is read as a property too,
    # which the read finds the write's relationships again by.
    from provisa.ephemeral import then

    rel_id = {then.REL_ID: store.REL_ID} if then.rel_ids_read(session) else {}
    for rel_type, source, target in sorted(graph.rel_pairs):
        # Each end is a label the overlay maps: an ephemeral end's (its unlabeled nodes' "" among
        # them, store.view_keys), a model end's type name.
        assert source in nodes and target in nodes, (rel_type, source, target)
        held_rel = session.tables.get(store.rel_table(rel_type))
        columns = held_rel.columns if held_rel else []
        mapping = RelationshipMapping(
            rel_type=rel_type,
            source_label=source,
            target_label=target,
            join_source_column=GRAPH_ID,
            join_target_column=GRAPH_ID,
            field_name=f"{SESSION_CATALOG}:{rel_type}:{source}:{target}",
            directed=True,
            via=JunctionMapping(
                catalog_name=SESSION_CATALOG,
                schema_name=SESSION_SCHEMA,
                table_name=_rel_relation(rel_type),
                type_name=_rel_relation(rel_type),
                source_columns=("from_id",),
                target_columns=("to_id",),
                identity_column=store.REL_ID,
                attributes={
                    **{name: name for name, _ in columns if name != store.REL_ID},
                    **rel_id,
                },
                attribute_types=tuple(
                    (name, _spelled(kind)) for name, kind in columns if name != store.REL_ID
                )
                + tuple((name, "text") for name in rel_id),
            ),
        )
        relationships[mapping.field_name] = mapping
        aliases.setdefault(rel_type, []).append(mapping)
    # A copy of the model's map with its node, table and relationship maps replaced; everything
    # else it holds is the model's own.
    if then.rel_ids_read(session) and "" in nodes:
        # While a read after a write runs, the session's nodes with no label are read by a name
        # of their own (then.UNLABELED), which no model label ever unions with.
        nodes[then.UNLABELED] = nodes[""]
    out = copy.copy(model)
    out.nodes = nodes
    out.nodes_by_table = nodes_by_table
    out.relationships = relationships
    out.aliases = aliases
    # REQ-348: every session relationship in one relation, which a path function with an
    # unlabelled end walks.
    out.session_walk = SessionWalk(SESSION_CATALOG, SESSION_SCHEMA, WALK_RELATION)
    return out


# --------------------------------------------------------------------------- 2. the rows


def _col(name: str, table: str | None = None) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=name, quoted=True),
        table=exp.Identifier(this=table, quoted=True) if table else None,
    )


def _spelled(kind: str) -> str:
    """A session type of types.py (or a registered type) as SQL spells it."""
    spelled = {"zoned_time": "TIMETZ", "json": "JSON"}.get(kind, kind)
    return f"{_spelled(spelled[5:-1])}[]" if spelled.startswith("list<") else spelled


def _typed(value: exp.Expression, kind: str) -> exp.Expression:
    """``value`` as a column of ``kind`` (a registered type, or a session type of types.py)."""
    return exp.Cast(this=value, to=exp.DataType.build(_spelled(kind), dialect="postgres"))


def _session_table(
    name: str, alias: str, session: TempSession, fixed: list[tuple[str, str]]
) -> exp.Expr:
    """The session table ``name`` as ``alias``. A table not created yet is empty by definition
    (REQ-2176: tables are created as statements write): it reads as no rows of its known columns,
    ``fixed`` (name, session type), not as an engine error."""
    if name in session.tables:
        return exp.alias_(exp.to_table(name, quoted=True), alias)
    empty = exp.select(
        *(exp.alias_(_typed(exp.null(), kind), column, quoted=True) for column, kind in fixed)
    ).where(exp.false())
    return exp.alias_(exp.Subquery(this=empty), alias)


def _labels_of(graph_id: exp.Expression, session: TempSession) -> exp.Expression:
    """Every label the session holds for the node ``graph_id`` names (node_labels), as a list:
    empty for none."""
    held = (
        exp.select(exp.ArrayAgg(this=_col("label", "_ll")))
        .from_(
            _session_table(
                store.NODE_LABELS, "_ll", session, store.fixed_columns(store.NODE_LABELS)
            )
        )
        .where(exp.EQ(this=_col(store.NODE_ID, "_ll"), expression=graph_id))
    )
    empty = exp.Cast(this=exp.Array(expressions=[]), to=exp.DataType.build("TEXT[]"))
    return exp.Coalesce(this=exp.Subquery(this=held), expressions=[empty])


def _node_rows(
    key: str, nm: Any, model: Any, session: TempSession, labels: bool = False
) -> exp.Expression:
    """The rows of the label ``key``'s session relation (``nm``, its overlay mapping); with
    ``labels``, each node's every label too (LABELS_COLUMN), which labels() reads."""
    from provisa.gql.select_builder import node_identity, node_key_text

    graph = store.graph(session)
    held = model.nodes.get(key)
    columns = [(alias, dict(nm.property_types)[prop]) for prop, alias in nm.properties.items()]
    parts: list[exp.Select] = []

    def created_identity(node_id: exp.Expression) -> exp.Expression:
        # A node the session created: the first of its labels in name order, and its id.
        first = (
            exp.select(exp.Min(this=_col("label", "_lf")))
            .from_(
                _session_table(
                    store.NODE_LABELS, "_lf", session, store.fixed_columns(store.NODE_LABELS)
                )
            )
            .where(exp.EQ(this=_col(store.NODE_ID, "_lf"), expression=node_id.copy()))
        )
        named = exp.Coalesce(this=exp.Subquery(this=first), expressions=[exp.Literal.string("")])
        return exp.DPipe(
            this=exp.DPipe(this=named, expression=exp.Literal.string("|")),
            expression=node_id.copy(),
        )

    def select(
        values: Mapping[str, exp.Expression],
        node_id: exp.Expression,
        graph_id: exp.Expression,
        identity: exp.Expression,
        node_labels: exp.Expression | None = None,
    ) -> exp.Select:
        # ``identity``: the node's identity in an answer, the same under whichever label reads
        # it (IDENTITY_COLUMN): a model node's graph identity, a created node's created_identity.
        listed = (
            [
                exp.alias_(
                    node_labels if node_labels is not None else _labels_of(graph_id, session),
                    LABELS_COLUMN,
                    quoted=True,
                )
            ]
            if labels
            else []
        )
        return exp.select(
            exp.alias_(node_id, NODE_ID, quoted=True),
            exp.alias_(graph_id, GRAPH_ID, quoted=True),
            exp.alias_(identity, IDENTITY_COLUMN, quoted=True),
            *(
                exp.alias_(_typed(values.get(alias, exp.null()), kind), alias, quoted=True)
                for alias, kind in columns
            ),
            *listed,
        )

    def model_rows(model_nm: Any, alias: str, own: bool) -> exp.Select:
        # The node's relation as the compiler reads it, each column by its published name. Read
        # under its own label a model node is its key; under another (a tag) its graph identity,
        # which names its type and keeps it apart from that label's own rows.
        from provisa.gql.translator_helpers import _node_table_expr

        values = {a: _col(a, alias) for _, a, _ in _model_columns(model_nm)}
        identity = node_identity(alias, model_nm)
        node_id = node_key_text(alias, model_nm) if own else identity
        # A model node's labels are its own and the tags the session gives it (node_labels, by
        # its graph identity), which a label set's every label is matched against too.
        own_labels = exp.ArrayConcat(
            this=exp.Array(expressions=[exp.Literal.string(model_nm.label)]),
            expressions=[_labels_of(identity.copy(), session)],
        )
        return select(values, node_id, identity, identity.copy(), own_labels).from_(
            _node_table_expr(model_nm, alias)
        )

    def tagged(rows: exp.Select, graph_id: exp.Expression) -> exp.Select:
        return rows.join(
            _session_table(
                store.NODE_LABELS, "_l", session, store.fixed_columns(store.NODE_LABELS)
            ),
            on=exp.and_(
                exp.EQ(this=_col(store.NODE_ID, "_l"), expression=graph_id),
                exp.EQ(this=_col("label", "_l"), expression=exp.Literal.string(key)),
            ),
            join_type="inner",
        )

    if held is not None:
        parts.append(model_rows(held, "_m", True))
    resolved = graph.resolved[key]
    for type_name in resolved.tagged:
        if type_name == key:
            continue  # a model label's own rows are all read already
        tagged_nm = model.nodes[type_name]
        parts.append(tagged(model_rows(tagged_nm, "_t", False), node_identity("_t", tagged_nm)))

    def unlabeled(rows: exp.Select, node_id: exp.Expression) -> exp.Select:
        held_labels = (
            exp.select(_col(store.NODE_ID, "_l"))
            .from_(
                _session_table(
                    store.NODE_LABELS, "_l", session, store.fixed_columns(store.NODE_LABELS)
                )
            )
            .where(exp.EQ(this=_col(store.NODE_ID, "_l"), expression=node_id))
        )
        return rows.where(exp.Not(this=exp.Exists(this=held_labels)))

    for anchor in resolved.anchors:
        held_columns = session.tables[anchor].columns if anchor in session.tables else []
        anchor_columns = {name for name, _ in held_columns}
        values = {a: _col(a, "_a") for a, _ in columns if a in anchor_columns}
        # An anchor not created yet holds no node: the columns the statement reads, all empty.
        source = _session_table(anchor, "_a", session, [(store.NODE_ID, "text"), *columns])
        rows = select(
            values,
            _col(store.NODE_ID, "_a"),
            _col(store.NODE_ID, "_a"),
            created_identity(_col(store.NODE_ID, "_a")),
        ).from_(source)
        node_id = _col(store.NODE_ID, "_a")
        parts.append(unlabeled(rows, node_id) if key == "" else tagged(rows, node_id))
    if not parts:
        # A label no node holds now (its nodes all deleted): no rows of the columns it reads.
        none = exp.Cast(this=exp.null(), to=exp.DataType.build("TEXT[]"))
        parts.append(
            select({}, exp.null(), exp.null(), _typed(exp.null(), "text"), none).where(exp.false())
        )
    union: exp.Expression = parts[0]
    for part in parts[1:]:
        union = exp.Union(this=union, expression=part, distinct=False)
    return union


def _walk_rows(session: TempSession) -> exp.Expression:
    """The rows of the walk's relation (REQ-348): every session relationship, its type and its
    ends, read from the relationships table alone, indexed on both ends; none before the session
    holds a relationship (REQ-2176)."""
    return exp.select(
        _col(store.REL_ID, "_r"), _col("type", "_r"), _col("from_id", "_r"), _col("to_id", "_r")
    ).from_(_session_table(store.RELS, "_r", session, store.fixed_columns(store.RELS)))


def _rel_rows(rel_type: str, session: TempSession) -> exp.Expression:
    """The rows of a relationship type's session relation: each relationship's ends and its
    properties."""
    held = session.tables.get(store.rel_table(rel_type))
    props = [n for n, _ in (held.columns if held else []) if n != store.REL_ID]
    return (
        exp.select(
            _col(store.REL_ID, "_r"),
            _col("from_id", "_r"),
            _col("to_id", "_r"),
            *(_col(p, "_p") for p in props),
        )
        .from_(exp.alias_(exp.to_table(store.RELS, quoted=True), "_r"))
        .join(
            _session_table(store.rel_table(rel_type), "_p", session, [(store.REL_ID, "text")]),
            on=exp.EQ(this=_col(store.REL_ID, "_p"), expression=_col(store.REL_ID, "_r")),
            join_type="inner",
        )
        .where(exp.EQ(this=_col("type", "_r"), expression=exp.Literal.string(rel_type)))
    )


_ARM = "__arm"
_RANK = "__rank"


def _union_arms(body: exp.Expression) -> list[exp.Select] | None:
    """The arms of a node union (the compiler's union of every type's relation a node matched
    by no label or a domain is drawn from): each a select of ``__label``, ``__id``, ... from one
    relation. None for anything else."""
    arms: list[exp.Expression] = []
    while isinstance(body, exp.Union) and not body.args.get("distinct"):
        arms.insert(0, body.expression)
        body = body.this
    arms.insert(0, body)
    for arm in arms:
        if not isinstance(arm, exp.Select) or arm.named_selects[:2] != ["__label", "__id"]:
            return None
        source = arm.args.get("from_")
        if arm.args.get("joins") or source is None or not isinstance(source.this, exp.Table):
            return None
    return arms  # pyright: ignore[reportReturnType]


def _arm_table(arm: exp.Select) -> exp.Table:
    """The one relation a node union's arm (:func:`_union_arms`) reads."""
    source = arm.args["from_"]
    assert isinstance(source.this, exp.Table)
    return source.this


def _session_arm(arm: exp.Select) -> bool:
    return _arm_table(arm).catalog == SESSION_CATALOG


def _once_per_node(sub: exp.Subquery, arms: list[exp.Select]) -> None:
    """``sub``'s node union read once per node: by ``__graph_id`` (a model arm's ``__id``, its
    graph identity), the first arm holding it giving the row."""
    names = arms[0].named_selects
    for i, arm in enumerate(arms):
        table = _arm_table(arm)
        graph_id = (
            _col(GRAPH_ID, table.alias or table.name)
            if _session_arm(arm)
            else arm.selects[1].this.copy()
        )
        # A union the compiler already gave its graph identity (a walk's end, REQ-348) keeps it.
        held = GRAPH_ID in arm.named_selects
        arm.select(
            *([] if held else [exp.alias_(graph_id, GRAPH_ID, quoted=True)]),
            exp.alias_(exp.Literal.number(i), _ARM, quoted=True),
            copy=False,
        )
    rank = exp.Window(
        this=exp.RowNumber(),
        partition_by=[_col(GRAPH_ID)],
        order=exp.Order(expressions=[exp.Ordered(this=_col(_ARM))]),
    )
    ranked = exp.select(*(_col(n) for n in names), exp.alias_(rank, _RANK, quoted=True)).from_(
        exp.alias_(exp.Subquery(this=sub.this.copy()), "_u")
    )
    once = (
        exp.select(*(_col(n) for n in names))
        .from_(exp.alias_(exp.Subquery(this=ranked), "_d"))
        .where(exp.EQ(this=_col(_RANK), expression=exp.Literal.number(1)))
    )
    sub.set("this", once)


_CTE_ROW = "_s"


def _conjuncts(where: exp.Expression | None) -> list[exp.Expression]:
    """The conditions a WHERE (or a condition) is the AND of."""
    if where is None:
        return []
    if isinstance(where, exp.Where):
        return _conjuncts(where.this)
    if isinstance(where, exp.And):
        return _conjuncts(where.this) + _conjuncts(where.expression)
    if isinstance(where, exp.Paren):
        return _conjuncts(where.this) if isinstance(where.this, exp.And) else [where]
    return [where]


def _only_of(condition: exp.Expression, alias: str) -> bool:
    """Whether ``condition`` reads nothing but ``alias``'s own columns (no subquery, no
    parameter-free constant alone, no other row)."""
    columns = list(condition.find_all(exp.Column))
    if not columns or condition.find(exp.Subquery, exp.Select) is not None:
        return False
    return all(c.table == alias for c in columns)


def _pushable(table: exp.Table) -> tuple[exp.Select, list[exp.Expression]] | None:
    """The select ``table`` is read in, and the top-level WHERE conditions on its alias alone,
    where they filter its rows before anything is joined to them: ``table`` in the FROM or an
    inner join of that select. None where they do not (an outer join: its rows are kept)."""
    alias = table.alias
    select = table.parent
    if isinstance(select, exp.From):
        select = select.parent
    elif isinstance(select, exp.Join):
        if (select.args.get("side") or "").upper() in ("LEFT", "RIGHT", "FULL") or select.args.get(
            "kind"
        ) not in (None, "INNER", "CROSS"):
            return None
        select = select.parent
    else:
        return None
    if not isinstance(select, exp.Select) or not alias:
        return None
    mine = [c for c in _conjuncts(select.args.get("where")) if _only_of(c, alias)]
    return select, mine


def expand(sql: exp.Expression, overlaid: Any, model: Any, session: TempSession) -> exp.Expression:
    """``sql`` (compiled over ``overlaid``) with each session relation read from a CTE of its
    rows, and each node union reading one read once per node.

    Each node pattern instance reads a CTE of its label's union carrying the top-level WHERE
    conditions on its variable alone (moved there, so they filter before anything is joined);
    instances whose CTEs come out the same share one. A relationship type is one CTE. Each is
    defined once, ahead of the statement's own CTEs (a variable-length path's recursive one
    reads it too)."""
    for sub in list(sql.find_all(exp.Subquery)):
        arms = _union_arms(sub.this)
        if arms is not None and any(_session_arm(arm) for arm in arms):
            _once_per_node(sub, arms)
    graph = store.graph(session)
    # Each node's every label only where a statement reads them (labels()).
    labels_read = any(c.name == LABELS_COLUMN for c in sql.find_all(exp.Column))
    by_name = {_node_relation(key): key for key in graph.resolved if key in overlaid.nodes}
    types = {_rel_relation(t): t for t in graph.types}
    defined: dict[str, str] = {}  # a CTE's definition (its SQL) -> its name
    ctes: list[exp.CTE] = []

    def cte(stem: str, body: exp.Expression) -> str:
        text = body.sql(dialect="postgres")
        if text not in defined:
            # Engines name CTEs regardless of case: types T2 and t2 are two names, two CTEs.
            taken = {c.alias.lower() for c in ctes}
            name = stem if stem.lower() not in taken else f"{stem}__{len(ctes)}"
            defined[text] = name
            ctes.append(
                exp.CTE(this=body, alias=exp.TableAlias(this=exp.to_identifier(name, quoted=True)))
            )
        return defined[text]

    for table in list(sql.find_all(exp.Table)):
        if table.catalog != SESSION_CATALOG:
            continue
        alias = table.alias or table.name
        if table.name in by_name or table.name in overlaid.label_sets:
            if table.name in by_name:
                key = by_name[table.name]
                body: exp.Expression = _node_rows(
                    key, overlaid.nodes[key], model, session, labels_read
                )
            else:
                # The nodes having every label of a set (label_map.node_of_all): its first
                # label's, those having the rest too, read by their every label (LABELS_COLUMN).
                held = overlaid.label_sets[table.name]
                key = "&".join(held)
                every = exp.Array(expressions=[exp.Literal.string(label) for label in held])
                body = (
                    exp.select("*")
                    .from_(
                        exp.alias_(
                            exp.Subquery(
                                this=_node_rows(held[0], overlaid.nodes[key], model, session, True)
                            ),
                            "_all",
                            quoted=True,
                        )
                    )
                    .where(exp.ArrayContainsAll(this=_col(LABELS_COLUMN), expression=every))
                )
            pushed = _pushable(table)
            if pushed is not None and pushed[1]:
                select, mine = pushed
                conditions = [c.copy() for c in mine]
                for c in conditions:
                    for column in c.find_all(exp.Column):
                        column.set("table", exp.to_identifier(_CTE_ROW, quoted=True))
                body = (
                    exp.select("*")
                    .from_(exp.alias_(exp.Subquery(this=body), _CTE_ROW, quoted=True))
                    .where(exp.and_(*conditions))
                )
                _drop_conditions(select, mine)
            name = cte(f"__session_node__{key}", body)
        elif table.name == WALK_RELATION:
            name = cte("__session_rels", _walk_rows(session))
        else:
            rel_type = types[table.name]
            name = cte(f"__session_rel__{rel_type}", _rel_rows(rel_type, session))
        table.replace(
            exp.alias_(exp.Table(this=exp.to_identifier(name, quoted=True)), alias, quoted=True)
        )
    if ctes:
        held = sql.args.get("with_")
        if held is not None:
            held.set("expressions", ctes + list(held.expressions))
        else:
            sql.set("with_", exp.With(expressions=ctes))
    return sql


def _drop_conditions(select: exp.Select, gone: list[exp.Expression]) -> None:
    """``select``'s WHERE without the conditions ``gone`` (each one of its conjuncts)."""
    kept = [c for c in _conjuncts(select.args.get("where")) if not any(c is g for g in gone)]
    select.set("where", exp.Where(this=exp.and_(*kept)) if kept else None)

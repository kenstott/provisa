# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MERGE (REQ-2176): MATCH, then CREATE under the same rules.

``[MATCH ... | UNWIND ... | WITH ...] MERGE <pattern> [ON CREATE SET ...] [ON MATCH SET ...]``
reads its leading clauses and the pattern as an OPTIONAL MATCH, its inline properties the match's
conditions, through the surface's own governed read over the session's overlay, so a model row, a
tagged model row and a session node all match. A row the pattern matches takes ON MATCH SET; a row
it does not makes the pattern (classify: a node naming one model table is a governed row, any
other a session node) and takes ON CREATE SET. A later row with the same values sees what an
earlier one made, and matches it.

It is MATCH, then insert -- not an upsert. Transactions are a no-op by design (REQ-883), so two
MERGEs at once may both find nothing and both insert. A MERGE of a model relationship is the
governed path's (it refuses a MERGE until the governed writer takes one).
"""

# Requirements: REQ-2176, REQ-883

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import pattern, store
from provisa.ephemeral.classify import classify_create, resolve
from provisa.ephemeral.labels import add_labels
from provisa.ephemeral.pattern import CreatePattern
from provisa.ephemeral.write import Created, Read, RowIds, Write, write_rows

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _name(text: str) -> str:
    return text if _NAME.match(text) else "`" + text.replace("`", "``") + "`"


@dataclass(frozen=True)
class MergeOf:
    """A statement's MERGE, read from the graph IR: its pattern (each anonymous element given a
    name), the variables its leading clauses bind, its read, its ON CREATE and ON MATCH label
    items, and the clause following it, if any."""

    pattern: CreatePattern
    bound: set[str]
    read: str
    #: The read when the pattern names a label or type nothing carries yet: the leading clauses
    #: alone, the pattern's own variables null, since nothing can match it.
    read_unmatched: str
    own: tuple[str, ...]  # the variables the pattern itself introduces
    on_create: tuple[tuple[str, tuple[str, ...]], ...] = field(default=())
    on_match: tuple[tuple[str, tuple[str, ...]], ...] = field(default=())
    trailing: str | None = None


def _labels_only(items: list[Any], when: str) -> tuple[tuple[str, tuple[str, ...]], ...]:
    for item in items:
        if not item.labels:
            raise CypherParseError(
                f"ON {when} SET of a property in a MERGE is not written yet: set labels only"
            )
    return tuple((item.variable, tuple(item.labels)) for item in items)


def merge_of(statement: str) -> MergeOf | None:
    """The statement's MERGE, or None when it merges nothing."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.gql.ir import MergeClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    merges = [i for i, step in enumerate(ast.pipeline) if isinstance(step, MergeClause)]
    if not merges:
        return None
    if len(merges) > 1 or len(ast.writes) > 1 and ast.writes[0] is not ast.pipeline[merges[0]]:
        raise CypherParseError("A statement merging ephemeral objects has one MERGE clause")
    at = merges[0]
    merge = ast.pipeline[at]
    assert isinstance(merge, MergeClause)
    after = ast.pipeline[at + 1 :]
    trailing = (
        pattern.TRAILING[type(after[0]).__name__]
        if after
        else "RETURN"
        if ast.return_clause is not None
        else None
    )
    p = pattern.paths_pattern([merge.pattern])
    bound = pattern.bound_by(ast.pipeline[:at])
    for i, node in enumerate(p.nodes):
        if node.variable is None:
            node.variable = f"_m{i}"
    for j, rel in enumerate(p.rels):
        if rel.variable is None:
            rel.variable = f"_mr{j}"
    own = tuple(
        dict.fromkeys(
            [n.variable for n in p.nodes if n.variable not in bound] + [r.variable for r in p.rels]
        )
    )
    prefix = statement[: merge.start].strip()
    return MergeOf(
        p,
        bound,
        _read(prefix, p, bound, own),
        _read(prefix, p, bound, own, unmatched=True),
        tuple(v for v in own if v is not None),
        _labels_only(merge.on_create, "CREATE"),
        _labels_only(merge.on_match, "MATCH"),
        trailing,
    )


def _read(
    prefix: str,
    p: CreatePattern,
    bound: set[str],
    own: tuple[str | None, ...],
    *,
    unmatched: bool = False,
) -> str:
    """The leading clauses, the pattern as an OPTIONAL MATCH, and every value the pattern writes
    (named as write._items names them) and node it reaches back to."""

    def node(i: int) -> str:
        n = p.nodes[i]
        assert n.variable is not None
        if n.variable in bound or any(o.variable == n.variable for o in p.nodes[:i]):
            return f"({n.variable})"
        return f"({n.variable}{''.join(':' + _name(label) for label in n.labels)})"

    text = node(0)
    for j, rel in enumerate(p.rels):
        assert rel.variable is not None
        left = rel.target == j  # its target is the node before it: written right to left
        body = f"[{rel.variable}:{_name(rel.rel_type)}]"
        text += (f"<-{body}-" if left else f"-{body}->") + node(j + 1)
    conditions = [
        f"{n.variable}.{_name(k)} = {v}"
        for i, n in enumerate(p.nodes)
        if n.variable not in bound
        for k, v in n.properties
    ] + [f"{r.variable}.{_name(k)} = {v}" for r in p.rels for k, v in r.properties]
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    reached = sorted({n.variable for n in p.nodes if n.variable in bound})
    created_nodes = [
        i
        for i, n in enumerate(p.nodes)
        if n.variable not in bound and n.variable not in {m.variable for m in p.nodes[:i]}
    ]
    items = [f"{'NULL' if unmatched else v} AS _v_{v}" for v in own]
    items += [f"{v} AS _v_{v}" for v in reached]
    items += [f"{v} AS _n{i}_{k}" for i in created_nodes for k, v in p.nodes[i].properties]
    items += [f"{v} AS _r{j}_{k}" for j, r in enumerate(p.rels) for k, v in r.properties]
    if unmatched:
        return f"{prefix} RETURN {', '.join(items)}".strip()
    return f"{prefix} OPTIONAL MATCH {text}{where} RETURN {', '.join(items)}".strip()


async def merge(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
    governed_write: Write,
) -> Created | None:
    """Run ``statement`` when it is a MERGE the ephemeral layer takes; None when it merges
    nothing or merges a model relationship (the governed path's)."""
    from provisa.ephemeral.write import _identity

    m = merge_of(statement)
    if m is None:
        return None
    graph = store.graph(session)
    kind = classify_create(m.pattern, m.bound, label_map, graph)
    if any(kind.rels_governed):
        return None
    if m.trailing is not None:
        raise CypherParseError(
            f"A MERGE of ephemeral objects ends its statement; it is followed by {m.trailing}"
        )
    rows = await read(m.read if _matchable(m, kind, label_map, graph) else m.read_unmatched, params)
    index = {n.variable: i for i, n in reversed(list(enumerate(m.pattern.nodes)))}
    matched: list[tuple[str, str, str | None]] = []  # (node id, label, model type) to give
    to_make: list[dict[str, Any]] = []
    made_for: dict[tuple, int] = {}  # a row's values -> the row of to_make making them
    again: list[int] = []  # rows matching what an earlier row of the statement makes
    for row in rows:
        if all(row[f"_v_{v}"] is not None for v in m.own):
            for var, labels in m.on_match:
                node_id, read_by = _identity(row[f"_v_{var}"], label_map, graph)
                model = None if node_id in graph.node_keys else read_by[0]
                matched.extend((node_id, k, model) for k in resolve(list(labels), label_map).keys)
            continue
        key = tuple(
            repr(v) for k, v in sorted(row.items()) if not k.startswith("_v_") or k[3:] in m.bound
        )
        if key in made_for:
            again.append(made_for[key])
            continue
        made_for[key] = len(to_make)
        to_make.append(row)
    named = {var for var, _ in m.on_create + m.on_match}
    created, ids = await write_rows(
        m.pattern,
        kind,
        to_make,
        label_map,
        session,
        state,
        governed_write,
        identify=frozenset(index[v] for v in named if v in index),
    )
    on_create = _resolved(m.on_create, label_map)
    on_match = _resolved(m.on_match, label_map)
    given = list(matched)
    given += _give(ids, on_create, index, graph)
    given += _give([ids[i] for i in again], on_match, index, graph)
    await add_labels(state, session, given)
    return created


def _matchable(m: MergeOf, kind: Any, label_map: Any, graph: store.Graph) -> bool:
    """Whether anything can match the pattern: every label it names is the model's or carried in
    the session, and every relationship type the model's or written in the session."""
    from provisa.ephemeral.classify import _model_type

    for c in kind.created:
        if any(label not in graph.labels for label in c.labels.free):
            return False
    return all(
        _model_type(label_map, rel.rel_type) or rel.rel_type in graph.types
        for rel in m.pattern.rels
    )


def _resolved(
    items: tuple[tuple[str, tuple[str, ...]], ...], label_map: Any
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Each item's labels as the session holds them (classify.resolve)."""
    return tuple((var, resolve(list(labels), label_map).keys) for var, labels in items)


def _give(
    ids: list[RowIds],
    items: tuple[tuple[str, tuple[str, ...]], ...],
    index: dict[str | None, int],
    graph: store.Graph,
) -> list[tuple[str, str, str | None]]:
    """The labels ``items`` give the nodes rows made (``ids``)."""
    out: list[tuple[str, str, str | None]] = []
    for row_ids in ids:
        for var, labels in items:
            node_id, read_by = row_ids[index[var]]
            model = None if node_id in graph.node_keys else read_by[0]
            out.extend((node_id, label, model) for label in labels)
    return out

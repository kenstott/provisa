# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3e6b1a-4f27-4c85-b0e9-7a2c5d8f1e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governed MERGE of model nodes and registered junction relationships (REQ-2176, amended
2026-10-28).

A MERGE matches on the properties it states: a node on its label and property map, a relationship
on its two ends and its type (and any property map it states). Each MERGE clause, per row of what
came before it, is a read of its pattern through the surface's governed read; a row it matches
takes ON MATCH SET, as an UPDATE of the part named, and when nothing matches, each node the
pattern introduces is an INSERT returning its key, then each relationship an INSERT of its
junction row between the two keys, with ON CREATE SET's properties written into the part they
name. Every INSERT and UPDATE is admitted as any write (write grants, row rules).

Every relationship's junction is resolved, and every property checked against its node or
junction, before anything is read or written: a relationship a foreign key derives is refused by
name. Transactions are a no-op by design (REQ-883), so a MERGE is these ordered statements, not
an upsert: two MERGEs at once may both find nothing and both insert.
"""

# Requirements: REQ-2176, REQ-665, REQ-883

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.compiler.sql_types import key_list
from provisa.cypher.counters import set_values, written_rows
from provisa.cypher.write_translator import (
    _KEY_PARAM,
    CypherParam,
    CypherWriteParseError,
    MergePart,
    WriteAST,
    WriteTranslator,
    _junction_table,
    _q,
    _read_column,
    _write_value,
    _written_column,
    bind_write_params,
    created_link,
    created_node,
    properties_set,
)
from provisa.gql.label_map import NodeMapping, RelationshipMapping
from provisa.gql.select_builder import NODE_KEY_PROPERTY, node_key


@dataclass(frozen=True)
class _Bound:
    """A node a MERGE has matched or made: its mapping, and the properties it is read with --
    its ``_id``, its key and each column a junction row takes from it."""

    mapping: NodeMapping
    props: dict[str, Any]


def _value(value: Any, provided: dict[str, Any]) -> Any:
    """A stated value: a parameter's the request supplies, refused by name when it does not."""
    from provisa.cypher.params import CypherParamError

    if isinstance(value, CypherParam):
        if value.name not in provided:
            raise CypherParamError(
                f"Unbound Cypher parameters: [{value.name!r}]. Provide values in the request's "
                "parameters."
            )
        return provided[value.name]
    return value


class _Merge:
    """One ``merge`` write's plan: each variable's mapping, each relationship's junction, and the
    properties each node is read with, all settled before anything runs."""

    def __init__(self, ast: WriteAST, translator: WriteTranslator) -> None:
        self.ast = ast
        self.translator = translator
        labels = dict(ast.matched_labels)
        for part in ast.merges:
            for node in part.nodes:
                if node.label:
                    labels[node.variable] = node.label
                elif node.variable not in labels:
                    raise CypherWriteParseError(
                        f"a MERGE reaches back to {node.variable}, which is matched with no "
                        "label: match it with its model label"
                    )
        self.labels = labels
        self.mappings = {v: translator._resolve_mapping(label) for v, label in labels.items()}
        self.junctions: list[list[RelationshipMapping]] = []
        #: Each node's columns it is read with, each and its property.
        needed: dict[str, dict[str, str]] = {}

        def need(var: str, columns: Any) -> None:
            nm = self.mappings[var]
            for column in key_list(columns):
                needed.setdefault(var, {})[column] = nm.property_of(column)

        for part in ast.merges:
            on_set = part.on_create + part.on_match
            rels = []
            for link in part.links:
                start, end = part.nodes[link.start].variable, part.nodes[link.end].variable
                rel = translator._junction(link.rel_type, labels[start], labels[end])[0]
                via = rel.via
                assert via is not None  # _junction refuses a relationship no junction backs
                written = [*link.props, *(p for v, p, _ in on_set if v == link.variable)]
                for prop in written:
                    if prop not in via.attributes:
                        raise CypherWriteParseError(f"{link.rel_type} has no property {prop}")
                need(start, rel.join_source_column)
                need(end, rel.join_target_column)
                rels.append(rel)
            self.junctions.append(rels)
            for node in part.nodes:
                nm = self.mappings[node.variable]
                for prop in [*node.props, *(p for v, p, _ in on_set if v == node.variable)]:
                    _written_column(nm, prop)
                need(node.variable, nm.key_columns or nm.id_column)
        self.needed = needed

    def items(self, variables: list[str]) -> list[tuple[str, str]]:
        """The (variable, property) a read returns for ``variables``: each one's ``_id``, then
        the properties it is read with."""
        return [
            (v, p) for v in variables for p in [NODE_KEY_PROPERTY, *self.needed.get(v, {}).values()]
        ]

    def bound(self, items: list[tuple[str, str]], row: list[Any]) -> dict[str, _Bound]:
        out: dict[str, dict[str, Any]] = {}
        for (var, prop), value in zip(items, row, strict=True):
            if value is None:
                raise CypherWriteParseError(
                    f"a MERGE reaches back to {var}, which its MATCH did not match"
                )
            out.setdefault(var, {})[prop] = value
        return {var: _Bound(self.mappings[var], props) for var, props in out.items()}


def _returning(nm: NodeMapping, columns: dict[str, str]) -> str:
    """A made row's ``_id`` and each column it is read with (``columns``, column -> property)."""
    key = node_key(None, nm).sql(dialect="postgres")
    return ", ".join(
        [f"{key} AS {_q(NODE_KEY_PROPERTY)}"] + [f"{_q(c)} AS {_q(p)}" for c, p in columns.items()]
    )


def _keys(bound: _Bound, columns: Any) -> list[tuple[str, str]]:
    """Each of ``columns`` of the row ``bound`` names, and its value as a literal of its type."""
    nm = bound.mapping
    types = dict(nm.property_types)
    out = []
    for column in key_list(columns):
        prop = nm.property_of(column)
        out.append((column, _write_value(bound.props[prop], types.get(prop))))
    return out


def _node_update(translator: WriteTranslator, bound: _Bound, sets: list[tuple[str, Any]]) -> str:
    nm = bound.mapping
    assignments = ", ".join(f"{_written_column(nm, p)} = {_write_value(v)}" for p, v in sets)
    where = " AND ".join(
        f"{_read_column(nm, nm.property_of(c))} = {literal}"
        for c, literal in _keys(bound, nm.key_columns or nm.id_column)
    )
    return f"UPDATE {translator._qualified_table(nm)} SET {assignments} WHERE {where}"


def _junction_update(
    rel: RelationshipMapping, start: _Bound, end: _Bound, sets: list[tuple[str, Any]]
) -> str:
    via = rel.via
    assert via is not None  # a merge's relationships are junctions (_Merge)
    assignments = ", ".join(f"{_q(via.attributes[p])} = {_write_value(v)}" for p, v in sets)
    conditions = [
        f"{_q(jc)} = {literal}"
        for junction_columns, held in (
            (via.source_columns, _keys(start, rel.join_source_column)),
            (via.target_columns, _keys(end, rel.join_target_column)),
        )
        for jc, (_, literal) in zip(junction_columns, held, strict=True)
    ]
    if via.type_column:
        typed = via.type_value if via.type_value is not None else rel.rel_type
        conditions.append(f"{_q(via.type_column)} = {_write_value(typed)}")
    return f"UPDATE {_junction_table(via)} SET {assignments} WHERE {' AND '.join(conditions)}"


async def _set(
    plan: _Merge,
    part: MergePart,
    rels: list[RelationshipMapping],
    binding: dict[str, _Bound],
    items: list[tuple[str, str, Any]],
    skip: set[str],
    provided: dict[str, Any],
    write: Any,
) -> None:
    """The UPDATEs ``items`` make, one per part they name (none of ``skip``)."""
    by_var: dict[str, list[tuple[str, Any]]] = {}
    for var, prop, value in items:
        if var not in skip:
            by_var.setdefault(var, []).append((prop, value))
    links = {link.variable: (link, rel) for link, rel in zip(part.links, rels, strict=True)}
    for var, sets in by_var.items():
        if var in links:
            link, rel = links[var]
            start = binding[part.nodes[link.start].variable]
            end = binding[part.nodes[link.end].variable]
            sql = _junction_update(rel, start, end, sets)
        else:
            sql = _node_update(plan.translator, binding[var], sets)
        await written_rows(
            write,
            *bind_write_params(sql, provided),
            **properties_set([v for _, v in sets], provided),
        )


def _made(
    per_row: dict[str, int],
    stated: dict[str, Any],
    part: MergePart,
    variable: str,
    provided: dict[str, Any],
) -> dict[str, int]:
    """What the row a MERGE makes counts, its pattern's properties and its ON CREATE SET items
    each set in turn, as Neo4j sets them: ``per_row`` counts the row as written, one value per
    property. An ON CREATE SET of a property the pattern also states is a second set; one to null
    sets it only when the pattern gave it a value."""
    held = {k for k, v in zip(stated, set_values(list(stated.values()), provided)) if v is not None}
    items = [(p, v) for var, p, v in part.on_create if var == variable]
    resolved = set_values([v for _, v in items], provided)
    sets = sum(v is not None for v in set_values(list(stated.values()), provided))
    for (prop, _), value in zip(items, resolved, strict=True):
        if value is not None:
            sets += 1
            held.add(prop)
        elif prop in held:
            sets += 1
            held.discard(prop)
    return {**per_row, "properties_set": sets}


async def _merge_part(
    plan: _Merge,
    part: MergePart,
    rels: list[RelationshipMapping],
    binding: dict[str, _Bound],
    provided: dict[str, Any],
    read: Any,
    write: Any,
) -> list[dict[str, _Bound]]:
    """One MERGE clause for one row of what came before it: the bindings it gives on."""
    params: dict[str, Any] = {}

    def param(value: Any) -> str:
        if isinstance(value, CypherParam):
            _value(value, provided)
            return f"${value.name}"
        name = f"{_KEY_PARAM}p{len(params)}"
        params[name] = value
        return f"${name}"

    shown: list[str] = []

    def node(i: int) -> str:
        n = part.nodes[i]
        if n.variable in shown:
            return f"({n.variable})"
        shown.append(n.variable)
        label = plan.labels[n.variable]
        if n.variable in binding:
            props = {NODE_KEY_PROPERTY: binding[n.variable].props[NODE_KEY_PROPERTY]}
        else:
            props = n.props
        held = ", ".join(f"{k}: {param(v)}" for k, v in props.items())
        return f"({n.variable}:{label}{' {' + held + '}' if held else ''})"

    text = node(0)
    for j, link in enumerate(part.links):
        held = ", ".join(f"{k}: {param(v)}" for k, v in link.props.items())
        body = f"[{link.variable}:{link.rel_type}{' {' + held + '}' if held else ''}]"
        text += (f"-{body}->" if link.start == j else f"<-{body}-") + node(j + 1)
    items = plan.items(shown)
    returned = ", ".join(f"{v}.{p} AS {_KEY_PARAM}c{i}" for i, (v, p) in enumerate(items))
    _, rows = await read(f"MATCH {text} RETURN {returned}", {**provided, **params})
    if rows:
        out = []
        for row in rows:
            matched = {**binding, **plan.bound(items, row)}
            await _set(plan, part, rels, matched, part.on_match, set(), provided, write)
            out.append(matched)
        return out
    made = dict(binding)
    created: set[str] = set()
    for n in part.nodes:
        if n.variable in made:
            continue
        nm = plan.mappings[n.variable]
        props = {**n.props, **{p: v for var, p, v in part.on_create if var == n.variable}}
        if not props:
            raise CypherWriteParseError(
                f"the node a MERGE makes, ({n.variable}:{n.label}), has no property"
            )
        create = WriteAST(kind="create", label=n.label, variable=n.variable, props=props)
        insert = plan.translator._translate_create(create)
        needed = plan.needed.get(n.variable, {})
        sql = f"{insert} RETURNING {_returning(nm, needed)}"
        columns, answer = await written_rows(
            write,
            *bind_write_params(sql, provided),
            **_made(
                created_node(plan.translator, create, provided), n.props, part, n.variable, provided
            ),
        )
        if len(answer) != 1:
            raise CypherWriteParseError(f"a MERGE wrote {len(answer)} rows, where it writes one")
        made[n.variable] = _Bound(nm, dict(zip(columns, answer[0], strict=True)))
        created.add(n.variable)
    for link, rel in zip(part.links, rels, strict=True):
        start = made[part.nodes[link.start].variable]
        end = made[part.nodes[link.end].variable]
        keys = [start.props[start.mapping.property_of(c)] for c in key_list(rel.join_source_column)]
        keys += [end.props[end.mapping.property_of(c)] for c in key_list(rel.join_target_column)]
        props = {**link.props, **{p: v for var, p, v in part.on_create if var == link.variable}}
        sql = plan.translator._junction_insert(rel, link.rel_type, props, [keys])
        await written_rows(
            write,
            *bind_write_params(sql, provided),
            **_made(created_link(props, provided), link.props, part, link.variable, provided),
        )
        created.add(link.variable)
    # ON CREATE SET of a node the pattern reaches back to: an UPDATE of its row.
    await _set(plan, part, rels, made, part.on_create, created, provided, write)
    return [made]


async def written_merge(
    ast: WriteAST, translator: WriteTranslator, provided: dict[str, Any], read: Any, write: Any
) -> None:
    """Run a ``merge`` write: its plan settled, the leading MATCHes read, then each MERGE clause
    in turn per row of what came before it. Patterns of the MATCHes unrelated to one another are
    each read apart, and their rows combined here (``ast.apart``)."""
    plan = _Merge(ast, translator)
    bindings: list[dict[str, _Bound]] = [{}]
    for reads, variables in ast.apart:
        reached = [v for v in ast.matched_labels if v in plan.needed and v in variables]
        items = plan.items(reached)
        returned = ", ".join(f"{v}.{p} AS {_KEY_PARAM}c{i}" for i, (v, p) in enumerate(items))
        _, rows = await read(f"{reads} RETURN {returned or f'1 AS {_KEY_PARAM}row'}", provided)
        found = [plan.bound(items, row[: len(items)]) for row in rows]
        bindings = [{**binding, **more} for binding in bindings for more in found]
    for part, rels in zip(ast.merges, plan.junctions, strict=True):
        following: list[dict[str, _Bound]] = []
        for binding in bindings:
            following += await _merge_part(plan, part, rels, binding, provided, read, write)
        bindings = following

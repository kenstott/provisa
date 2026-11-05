# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8d1f64-9c27-4e05-a6b3-5d0e7c2a9f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The APOC introspection procedures, answered from the acting role's label map (REQ-2168).

Cypher tools and agent frameworks (LangChain's Neo4j integration, neo4j-graphrag) learn a graph's
schema with ``apoc.meta.data`` before they write Cypher. Each answer here is read off the graph the
role is served -- the Bolt database ``provisa_<role>`` -- so a caller sees only the labels,
properties and relationships the role reads. Counts are not taken: APOC samples them, and a
schema read must not scan the data, so a count is null.
"""

from __future__ import annotations

from typing import Any

# Each IR type (core/ir_types) as APOC names a property's type.
_APOC_TYPES = {
    "boolean": "BOOLEAN",
    "smallint": "INTEGER",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "float": "FLOAT",
    "double": "FLOAT",
    "numeric": "FLOAT",
    "date": "DATE",
    "timestamp": "LOCAL_DATE_TIME",
    "timestamptz": "DATE_TIME",
    "time": "TIME",
    "json": "MAP",
}


def _property_types(nm: Any) -> dict[str, str]:
    """Each property of a node as APOC types it: a declared object is a MAP, any other its
    registered type's (a text, uuid or binary column is a STRING)."""
    from provisa.core.ir_types import to_ir

    registered = dict(nm.property_types)
    out: dict[str, str] = {}
    for prop in nm.properties:
        if prop in nm.map_properties:
            out[prop] = "MAP"
            continue
        out[prop] = _APOC_TYPES.get(to_ir(registered[prop]), "STRING")
    return out


def _graph(state: Any, role_id: str) -> Any:
    """The role's label map, as its Bolt database ``provisa_<role>`` serves it."""
    from provisa.bolt.session import _bolt_label_map

    ctx = state.contexts.get(role_id)
    if ctx is None:
        raise PermissionError("Schema not loaded")
    return _bolt_label_map(ctx, role_id, False, state)


def _row(label: str, prop: str, kind: str, element: str, other: list[str]) -> dict:
    return {
        "label": label,
        "property": prop,
        "count": None,
        "unique": False,
        "index": False,
        "existence": False,
        "type": kind,
        "array": False,
        "sample": None,
        "left": None,
        "right": None,
        "other": other,
        "otherLabels": [],
        "elementType": element,
    }


def _node_label(graph: Any, label: str) -> str:
    return graph.display_label(graph.nodes[label])


def _outgoing(graph: Any) -> dict[tuple[str, str], list[str]]:
    """(source label, relationship type) -> the labels it reaches, in order."""
    out: dict[tuple[str, str], list[str]] = {}
    for rels in graph.aliases.values():
        for rm in rels:
            if rm.source_label not in graph.nodes or rm.target_label not in graph.nodes:
                continue
            targets = out.setdefault((_node_label(graph, rm.source_label), rm.rel_type), [])
            target = _node_label(graph, rm.target_label)
            if target not in targets:
                targets.append(target)
    return out


def _relationship_properties(graph: Any) -> dict[str, dict[str, str]]:
    props: dict[str, dict[str, str]] = {}
    for rels in graph.aliases.values():
        for rm in rels:
            props.setdefault(rm.rel_type, {}).update(rm.properties)
    return props


async def meta_data(args: dict, state: Any, role_id: str) -> list[dict]:
    """``apoc.meta.data(config)``: one row per node property, per relationship a node starts
    (type RELATIONSHIP, ``other`` the labels it reaches) and per relationship property."""
    graph = _graph(state, role_id)
    rows: list[dict] = []
    for name in sorted(graph.nodes):
        nm = graph.nodes[name]
        label = graph.display_label(nm)
        for prop, kind in _property_types(nm).items():
            rows.append(_row(label, prop, kind, "node", []))
    for (label, rel_type), targets in sorted(_outgoing(graph).items()):
        rows.append(_row(label, rel_type, "RELATIONSHIP", "node", targets))
    for rel_type, props in sorted(_relationship_properties(graph).items()):
        for prop in props:
            rows.append(_row(rel_type, prop, "STRING", "relationship", []))
    return rows


async def meta_schema(args: dict, state: Any, role_id: str) -> list[dict]:
    """``apoc.meta.schema(config)``: one row, ``value`` a map of each label and relationship type
    to its properties (and, for a label, the relationships it starts)."""
    graph = _graph(state, role_id)
    value: dict[str, dict] = {}

    def _props(types: dict[str, str]) -> dict[str, dict]:
        return {
            p: {"type": kind, "indexed": False, "unique": False, "existence": False, "array": False}
            for p, kind in types.items()
        }

    for name in sorted(graph.nodes):
        nm = graph.nodes[name]
        value[graph.display_label(nm)] = {
            "type": "node",
            "count": None,
            "labels": [],
            "properties": _props(_property_types(nm)),
            "relationships": {},
        }
    for (label, rel_type), targets in sorted(_outgoing(graph).items()):
        value[label]["relationships"][rel_type] = {
            "direction": "out",
            "labels": targets,
            "count": None,
            "properties": {},
        }
    for rel_type, props in sorted(_relationship_properties(graph).items()):
        value.setdefault(rel_type, {"type": "relationship", "count": None, "properties": {}})
        value[rel_type]["properties"] = _props({p: "STRING" for p in props})
    return [{"value": value}]


async def schema_nodes(args: dict, state: Any, role_id: str) -> list[dict]:
    """``apoc.schema.nodes()``: the graph's indexes and constraints. A Provisa graph declares
    none (its tables' indexes are its sources'), so there are no rows."""
    _graph(state, role_id)
    return []

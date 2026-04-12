# Copyright (c) 2026 Kenneth Stott
# Canary: 8c5e1a3f-7b2d-4f6a-9c8e-2d4b6f8a1c3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Post-execution response assembler for Cypher query results.

Deserializes JSON columns from raw rows into typed Node, Edge, and Path objects.
Groups path rows sharing the same _path_id into a single Path per group.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from provisa.cypher.translator import GraphVarKind


class CypherAssemblyError(Exception):
    """Raised when a graph column cannot be deserialized."""


@dataclass
class Node:
    id: str
    label: str
    properties: dict[str, Any]


@dataclass
class Edge:
    id: str
    type: str
    start_node: Node
    end_node: Node
    properties: dict[str, Any]


@dataclass
class Path:
    nodes: list[Node]
    edges: list[Edge]


def assemble_rows(
    raw_rows: list[dict],
    graph_vars: dict[str, GraphVarKind],
) -> list[dict]:
    """Deserialize graph columns in raw_rows and collapse path groups.

    For NODE columns: JSON → Node dataclass.
    For EDGE columns: JSON → Edge dataclass (with embedded start/end Node).
    For PATH columns: rows sharing _path_id are grouped → Path dataclass.
    Scalar columns are passed through unchanged.
    """
    if not raw_rows:
        return []

    path_cols = {k for k, v in graph_vars.items() if v == GraphVarKind.PATH}

    if path_cols:
        return _assemble_with_paths(raw_rows, graph_vars, path_cols)

    return [_assemble_row(row, graph_vars) for row in raw_rows]


def _assemble_row(row: dict, graph_vars: dict[str, GraphVarKind]) -> dict:
    result: dict = {}
    for key, value in row.items():
        if key in graph_vars:
            kind = graph_vars[key]
            result[key] = _deserialize_graph_value(key, value, kind)
        else:
            result[key] = value
    return result


def _deserialize_graph_value(col: str, value: Any, kind: GraphVarKind) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            data = json.loads(value)
        elif isinstance(value, dict):
            data = value
        else:
            raise CypherAssemblyError(f"Unexpected graph column value type for {col!r}: {type(value)}")
    except (json.JSONDecodeError, TypeError) as exc:
        raise CypherAssemblyError(f"Malformed JSON in graph column {col!r}: {exc}") from exc

    if kind == GraphVarKind.NODE:
        return _parse_node(data)
    if kind == GraphVarKind.EDGE:
        return _parse_edge(data)
    if kind == GraphVarKind.PASSTHROUGH:
        # Auto-detect: edge JSON has 'type' + ('identity' or 'startNode'); node JSON has 'label'
        if "type" in data and ("identity" in data or "startNode" in data or "start_node" in data):
            return _parse_edge(data)
        return _parse_node(data)
    raise CypherAssemblyError(f"Unknown GraphVarKind {kind!r} for column {col!r}")


def _parse_node(data: dict) -> Node:
    return Node(
        id=str(data.get("id", "")),
        label=str(data.get("label", "")),
        properties=data["properties"] if "properties" in data and isinstance(data["properties"], dict)
        else {k: v for k, v in data.items() if k not in ("id", "label", "properties")},
    )


def _parse_edge(data: dict) -> Edge:
    # Support both Neo4j format (identity/start/end) and legacy format (id/startNode/endNode).
    has_neo4j = "identity" in data
    if has_neo4j:
        edge_id = str(data.get("identity", ""))
        start_raw = data.get("startNode") or data.get("start_node") or {}
        end_raw = data.get("endNode") or data.get("end_node") or {}
        if not isinstance(start_raw, dict):
            start_raw = {"id": str(data.get("start", "")), "label": "", "properties": {}}
        if not isinstance(end_raw, dict):
            end_raw = {"id": str(data.get("end", "")), "label": "", "properties": {}}
    else:
        edge_id = str(data.get("id", ""))
        start_raw = data.get("startNode") or data.get("start_node") or {}
        end_raw = data.get("endNode") or data.get("end_node") or {}
        if not isinstance(start_raw, dict):
            start_raw = {"id": str(start_raw), "label": "", "properties": {}}
        if not isinstance(end_raw, dict):
            end_raw = {"id": str(end_raw), "label": "", "properties": {}}
    return Edge(
        id=edge_id,
        type=str(data.get("type", "")),
        start_node=_parse_node(start_raw),
        end_node=_parse_node(end_raw),
        properties=data["properties"] if "properties" in data and isinstance(data["properties"], dict)
        else {k: v for k, v in data.items() if k not in ("id", "identity", "type", "start", "end", "startNode", "endNode", "start_node", "end_node", "properties")},
    )


def _assemble_with_paths(
    raw_rows: list[dict],
    graph_vars: dict[str, GraphVarKind],
    path_cols: set[str],
) -> list[dict]:
    """Group rows by _path_id and collapse into Path objects."""
    # Partition rows that have _path_id from those that don't
    has_path_id = "_path_id" in (raw_rows[0].keys() if raw_rows else set())

    if not has_path_id:
        # No _path_id column — treat each row individually
        return [_assemble_row(row, graph_vars) for row in raw_rows]

    # Group by _path_id
    path_groups: dict[str, list[dict]] = {}
    non_path_rows: list[dict] = []

    for row in raw_rows:
        path_id = row.get("_path_id")
        if path_id is not None:
            path_groups.setdefault(str(path_id), []).append(row)
        else:
            non_path_rows.append(_assemble_row(row, graph_vars))

    result: list[dict] = []

    for path_id, group in path_groups.items():
        # Sort by _depth
        group.sort(key=lambda r: r.get("_depth", 0))
        path_result: dict = {}

        for col in path_cols:
            nodes: list[Node] = []
            edges: list[Edge] = []
            for row in group:
                val = row.get(col)
                if val is not None:
                    if isinstance(val, str):
                        try:
                            data = json.loads(val)
                        except json.JSONDecodeError as exc:
                            raise CypherAssemblyError(f"Malformed JSON in path column {col!r}: {exc}") from exc
                    elif isinstance(val, dict):
                        data = val
                    else:
                        continue

                    # Extract nodes and edges arrays from path data
                    if "nodes" in data and "edges" in data:
                        nodes.extend(_parse_node(n) for n in data["nodes"])
                        edges.extend(_parse_edge(e) for e in data["edges"])
                    elif "id" in data and "label" in data:
                        nodes.append(_parse_node(data))
                    elif "id" in data and "type" in data:
                        edges.append(_parse_edge(data))

            path_result[col] = Path(nodes=nodes, edges=edges)

        # Include scalar columns from first row
        first_row = group[0]
        for key, value in first_row.items():
            if key.startswith("_path_id") or key.startswith("_depth") or key.startswith("_direction"):
                continue
            if key not in path_cols and key not in path_result:
                path_result[key] = value

        result.append(path_result)

    result.extend(non_path_rows)
    return result


def to_serializable(obj: Any) -> Any:
    """Recursively convert Node/Edge/Path to JSON-serializable dicts."""
    if isinstance(obj, Node):
        return {"id": obj.id, "label": obj.label, "properties": obj.properties}
    if isinstance(obj, Edge):
        return {
            "identity": obj.id,
            "start": obj.start_node.id,
            "end": obj.end_node.id,
            "type": obj.type,
            "properties": obj.properties,
            "startNode": to_serializable(obj.start_node),
            "endNode": to_serializable(obj.end_node),
        }
    if isinstance(obj, Path):
        return {
            "nodes": [to_serializable(n) for n in obj.nodes],
            "edges": [to_serializable(e) for e in obj.edges],
        }
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_serializable(i) for i in obj]
    return obj

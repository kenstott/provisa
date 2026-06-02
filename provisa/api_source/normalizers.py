# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Response normalizers for query-API sources (Phase AO).

Normalizers transform raw API responses into a list of flat row dicts
before the standard root-path navigation and column extraction in flattener.py.

Neo4j formats supported
-----------------------
neo4j_tabular       Transaction API row format  (/db/{db}/tx/commit, resultDataContents=["row"])
neo4j_legacy_cypher Legacy cypher endpoint      (/db/data/cypher)
neo4j_graph_nodes   Transaction API graph format (resultDataContents=["graph"]) — nodes only
neo4j_graph_rels    Transaction API graph format — relationships only
neo4j_query_v2      Query API v2               (/db/{db}/query/v2, Neo4j 5.5+)
"""

from __future__ import annotations

from collections.abc import Callable


def _as_list(value: object) -> list[object]:
    """Return value as a list, or an empty list if it is not a list."""
    return value if isinstance(value, list) else []


def _as_dict(value: object) -> dict[str, object]:
    """Return value as a dict, or an empty dict if it is not a dict."""
    return value if isinstance(value, dict) else {}


# ── Neo4j: Transaction API row format ─────────────────────────────────────────


def neo4j_tabular(response: object) -> list[dict]:
    """Normalize a Neo4j HTTP legacy transaction API response to flat row dicts.

    Endpoint: /db/{db}/tx/commit  (resultDataContents defaults to ["row"])

    Response shape:
      {"results": [{"columns": ["col1", "col2"],
                    "data": [{"row": [v1, v2], "meta": [...]}]}],
       "errors": []}
    """
    response_dict = _as_dict(response)
    rows: list[dict] = []
    for result in _as_list(response_dict.get("results")):
        result_dict = _as_dict(result)
        columns = _as_list(result_dict.get("columns"))
        for entry in _as_list(result_dict.get("data")):
            row_values = _as_list(_as_dict(entry).get("row"))
            rows.append(dict(zip(columns, row_values)))
    return rows


# ── Neo4j: Legacy cypher endpoint ─────────────────────────────────────────────


def neo4j_legacy_cypher(response: object) -> list[dict]:
    """Normalize the deprecated /db/data/cypher endpoint response.

    Response shape:
      {"columns": ["col1", "col2"], "data": [[v1, v2], [v3, v4]]}

    Each element of data is a plain list (no row/meta wrapper).
    """
    response_dict = _as_dict(response)
    columns = _as_list(response_dict.get("columns"))
    rows: list[dict] = []
    for row_values in _as_list(response_dict.get("data")):
        if isinstance(row_values, list):
            rows.append(dict(zip(columns, row_values)))
    return rows


# ── Neo4j: Transaction API graph format — nodes ───────────────────────────────


def neo4j_graph_nodes(response: object) -> list[dict]:
    """Normalize Neo4j graph-format response, emitting one row per node.

    Endpoint: /db/{db}/tx/commit  with resultDataContents=["graph"]

    Response shape:
      {"results": [{"columns": [...],
                    "data": [{"graph": {
                      "nodes": [{"id": "1", "elementId": "...", "labels": [...],
                                 "properties": {...}}],
                      "relationships": [...]
                    }}]}],
       "errors": []}

    Each node becomes a row: {"_id": "1", "_labels": ["Person"], **properties}.
    Deduplicates by node id across result sets.
    """
    response_dict = _as_dict(response)
    seen: set[object] = set()
    rows: list[dict] = []
    for result in _as_list(response_dict.get("results")):
        for entry in _as_list(_as_dict(result).get("data")):
            graph = _as_dict(_as_dict(entry).get("graph"))
            for node in _as_list(graph.get("nodes")):
                node_dict = _as_dict(node)
                node_id = node_dict.get("id") or node_dict.get("elementId", "")
                if node_id in seen:
                    continue
                seen.add(node_id)
                row: dict = {
                    "_id": node_id,
                    "_labels": node_dict.get("labels", []),
                }
                row.update(_as_dict(node_dict.get("properties")))
                rows.append(row)
    return rows


# ── Neo4j: Transaction API graph format — relationships ───────────────────────


def neo4j_graph_rels(response: object) -> list[dict]:
    """Normalize Neo4j graph-format response, emitting one row per relationship.

    Each relationship becomes a row:
      {"_id": "9", "_type": "KNOWS", "_start": "1", "_end": "2", **properties}.
    Deduplicates by relationship id across result sets.
    """
    response_dict = _as_dict(response)
    seen: set[object] = set()
    rows: list[dict] = []
    for result in _as_list(response_dict.get("results")):
        for entry in _as_list(_as_dict(result).get("data")):
            graph = _as_dict(_as_dict(entry).get("graph"))
            for rel in _as_list(graph.get("relationships")):
                rel_dict = _as_dict(rel)
                rel_id = rel_dict.get("id") or rel_dict.get("elementId", "")
                if rel_id in seen:
                    continue
                seen.add(rel_id)
                row: dict = {
                    "_id": rel_id,
                    "_type": rel_dict.get("type", ""),
                    "_start": rel_dict.get("startNode", ""),
                    "_end": rel_dict.get("endNode", ""),
                }
                row.update(_as_dict(rel_dict.get("properties")))
                rows.append(row)
    return rows


# ── Neo4j: Query API v2 (Neo4j 5.5+) ─────────────────────────────────────────


def neo4j_query_v2(response: object) -> list[dict]:
    """Normalize a Neo4j Query API v2 response to flat row dicts.

    Endpoint: /db/{db}/query/v2  (Neo4j 5.5+)

    Response shape:
      {"data": {"fields": ["col1", "col2"],
                "values": [[v1, v2], [v3, v4]]},
       "bookmarks": [...]}

    Values may be primitives or node/relationship objects.  Node objects
    (dicts with "labels" and "properties") are flattened to their properties.
    All other dicts are left as-is.
    """
    data = _as_dict(_as_dict(response).get("data"))
    fields = _as_list(data.get("fields"))
    rows: list[dict] = []
    for row_values in _as_list(data.get("values")):
        row: dict = {}
        for col, val in zip(fields, _as_list(row_values)):
            if isinstance(val, dict) and "properties" in val:
                # Node or relationship object — flatten properties under the column name
                row[col] = val["properties"]
            else:
                row[col] = val
        rows.append(row)
    return rows


# ── Registry ──────────────────────────────────────────────────────────────────


def sparql_bindings(response: object) -> list[dict]:
    """Normalize a SPARQL 1.1 SELECT response to flat row dicts.

    SPARQL JSON format:
      {"results": {"bindings": [{"var": {"type": "literal", "value": "v"}, ...}]}}

    Each binding dict maps variable names to their scalar values.
    Non-literal types (uri, bnode) are also converted to their string value.
    """
    bindings = _as_list(_as_dict(_as_dict(response).get("results")).get("bindings"))
    rows: list[dict] = []
    for binding in bindings:
        row: dict = {}
        for var_name, term in _as_dict(binding).items():
            row[var_name] = term.get("value") if isinstance(term, dict) else term
        rows.append(row)
    return rows


# ── Registry ──────────────────────────────────────────────────────────────────

NORMALIZERS: dict[str, Callable[[object], list[dict]]] = {
    "neo4j_tabular": neo4j_tabular,
    "neo4j_legacy_cypher": neo4j_legacy_cypher,
    "neo4j_graph_nodes": neo4j_graph_nodes,
    "neo4j_graph_rels": neo4j_graph_rels,
    "neo4j_query_v2": neo4j_query_v2,
    "sparql_bindings": sparql_bindings,
}


def get_normalizer(name: str) -> Callable[[object], list[dict]]:
    """Return a normalizer by name, raising ValueError for unknown names."""
    if name not in NORMALIZERS:
        raise ValueError(f"Unknown response_normalizer {name!r}. Available: {sorted(NORMALIZERS)}")
    return NORMALIZERS[name]

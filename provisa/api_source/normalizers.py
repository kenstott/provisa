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
from typing import Any


# ── Neo4j: Transaction API row format ─────────────────────────────────────────

def neo4j_tabular(response: Any) -> list[dict]:
    """Normalize a Neo4j HTTP legacy transaction API response to flat row dicts.

    Endpoint: /db/{db}/tx/commit  (resultDataContents defaults to ["row"])

    Response shape:
      {"results": [{"columns": ["col1", "col2"],
                    "data": [{"row": [v1, v2], "meta": [...]}]}],
       "errors": []}
    """
    results: list = response.get("results", [])
    rows: list[dict] = []
    for result in results:
        columns: list[str] = result.get("columns", [])
        for entry in result.get("data", []):
            row_values = entry.get("row", [])
            rows.append(dict(zip(columns, row_values)))
    return rows


# ── Neo4j: Legacy cypher endpoint ─────────────────────────────────────────────

def neo4j_legacy_cypher(response: Any) -> list[dict]:
    """Normalize the deprecated /db/data/cypher endpoint response.

    Response shape:
      {"columns": ["col1", "col2"], "data": [[v1, v2], [v3, v4]]}

    Each element of data is a plain list (no row/meta wrapper).
    """
    columns: list[str] = response.get("columns", [])
    rows: list[dict] = []
    for row_values in response.get("data", []):
        if isinstance(row_values, list):
            rows.append(dict(zip(columns, row_values)))
    return rows


# ── Neo4j: Transaction API graph format — nodes ───────────────────────────────

def neo4j_graph_nodes(response: Any) -> list[dict]:
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
    seen: set[str] = set()
    rows: list[dict] = []
    for result in response.get("results", []):
        for entry in result.get("data", []):
            for node in entry.get("graph", {}).get("nodes", []):
                node_id = node.get("id") or node.get("elementId", "")
                if node_id in seen:
                    continue
                seen.add(node_id)
                row: dict = {
                    "_id": node_id,
                    "_labels": node.get("labels", []),
                }
                row.update(node.get("properties", {}))
                rows.append(row)
    return rows


# ── Neo4j: Transaction API graph format — relationships ───────────────────────

def neo4j_graph_rels(response: Any) -> list[dict]:
    """Normalize Neo4j graph-format response, emitting one row per relationship.

    Each relationship becomes a row:
      {"_id": "9", "_type": "KNOWS", "_start": "1", "_end": "2", **properties}.
    Deduplicates by relationship id across result sets.
    """
    seen: set[str] = set()
    rows: list[dict] = []
    for result in response.get("results", []):
        for entry in result.get("data", []):
            for rel in entry.get("graph", {}).get("relationships", []):
                rel_id = rel.get("id") or rel.get("elementId", "")
                if rel_id in seen:
                    continue
                seen.add(rel_id)
                row: dict = {
                    "_id": rel_id,
                    "_type": rel.get("type", ""),
                    "_start": rel.get("startNode", ""),
                    "_end": rel.get("endNode", ""),
                }
                row.update(rel.get("properties", {}))
                rows.append(row)
    return rows


# ── Neo4j: Query API v2 (Neo4j 5.5+) ─────────────────────────────────────────

def neo4j_query_v2(response: Any) -> list[dict]:
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
    data = response.get("data", {})
    fields: list[str] = data.get("fields", [])
    rows: list[dict] = []
    for row_values in data.get("values", []):
        row: dict = {}
        for col, val in zip(fields, row_values):
            if isinstance(val, dict) and "properties" in val:
                # Node or relationship object — flatten properties under the column name
                row[col] = val["properties"]
            else:
                row[col] = val
        rows.append(row)
    return rows


# ── Registry ──────────────────────────────────────────────────────────────────

def sparql_bindings(response: Any) -> list[dict]:
    """Normalize a SPARQL 1.1 SELECT response to flat row dicts.

    SPARQL JSON format:
      {"results": {"bindings": [{"var": {"type": "literal", "value": "v"}, ...}]}}

    Each binding dict maps variable names to their scalar values.
    Non-literal types (uri, bnode) are also converted to their string value.
    """
    bindings: list[dict] = response.get("results", {}).get("bindings", [])
    rows: list[dict] = []
    for binding in bindings:
        row: dict = {}
        for var_name, term in binding.items():
            row[var_name] = term.get("value") if isinstance(term, dict) else term
        rows.append(row)
    return rows


# ── Registry ──────────────────────────────────────────────────────────────────

NORMALIZERS: dict[str, Callable[[Any], list[dict]]] = {
    "neo4j_tabular": neo4j_tabular,
    "neo4j_legacy_cypher": neo4j_legacy_cypher,
    "neo4j_graph_nodes": neo4j_graph_nodes,
    "neo4j_graph_rels": neo4j_graph_rels,
    "neo4j_query_v2": neo4j_query_v2,
    "sparql_bindings": sparql_bindings,
}


def get_normalizer(name: str) -> Callable[[Any], list[dict]]:
    """Return a normalizer by name, raising ValueError for unknown names."""
    if name not in NORMALIZERS:
        raise ValueError(
            f"Unknown response_normalizer {name!r}. "
            f"Available: {sorted(NORMALIZERS)}"
        )
    return NORMALIZERS[name]

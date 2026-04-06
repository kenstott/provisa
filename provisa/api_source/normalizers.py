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
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def neo4j_tabular(response: Any) -> list[dict]:
    """Normalize a Neo4j HTTP API v2 query response to flat row dicts.

    Neo4j returns:
      {"data": {"fields": ["col1", "col2"], "values": [[v1, v2], [v3, v4]]}}

    Each entry in values[] is zipped with fields[] to produce one row dict.
    """
    data_block = response.get("data", {})
    fields: list[str] = data_block.get("fields", [])
    values: list[list] = data_block.get("values", [])
    rows: list[dict] = []
    for entry in values:
        if not isinstance(entry, list):
            continue
        rows.append(dict(zip(fields, entry)))
    return rows


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


NORMALIZERS: dict[str, Callable[[Any], list[dict]]] = {
    "neo4j_tabular": neo4j_tabular,
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

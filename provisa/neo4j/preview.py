# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Neo4j query preview and shape validation (Phase AO).

Before registering a Neo4j table, stewards preview the Cypher query to
confirm it returns flat scalar projections. Node objects are rejected.
"""

from __future__ import annotations

import re

import httpx

from provisa.api_source.normalizers import neo4j_tabular


class Neo4jNodeObjectError(ValueError):
    """Raised when a Cypher RETURN clause yields non-scalar (node/map) values."""


def _ensure_limit(cypher: str, limit: int = 5) -> str:
    """Append LIMIT N to a Cypher query if not already present.

    If LIMIT already appears (case-insensitive), leaves the query unchanged.
    """
    if re.search(r"\bLIMIT\b", cypher, re.IGNORECASE):
        return cypher
    return cypher.rstrip().rstrip(";") + f" LIMIT {limit}"


async def preview_query(
    base_url: str,
    database: str,
    cypher: str,
    auth: tuple[str, str] | None = None,
    timeout: float = 10.0,
) -> list[dict]:
    """Execute a Cypher preview (LIMIT 5) and return flat row dicts.

    Args:
        base_url: e.g. "http://neo4j-host:7474"
        database: target database name
        cypher: steward-supplied Cypher SELECT query
        auth: optional (username, password) tuple for HTTP Basic auth
        timeout: request timeout in seconds

    Returns:
        List of row dicts produced by neo4j_tabular normalizer.

    Raises:
        httpx.HTTPError: on network or HTTP errors
    """
    preview_cypher = _ensure_limit(cypher, limit=5)
    url = f"{base_url}/db/{database}/query/v2"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth_arg = httpx.BasicAuth(*auth) if auth else None

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            json={"statement": preview_cypher},
            headers=headers,
            auth=auth_arg,
            timeout=timeout,
        )
        resp.raise_for_status()

    return neo4j_tabular(resp.json())


def validate_shape(rows: list[dict], columns: list[str] | None = None) -> None:
    """Validate that all values in the preview rows are scalars.

    Raises Neo4jNodeObjectError if any value is a dict or list, indicating
    a Cypher RETURN clause that returns node objects rather than scalar
    projections.

    Args:
        rows: preview rows from preview_query()
        columns: optional list of expected column names (ignored if None)

    Raises:
        Neo4jNodeObjectError: if any value is a dict or list
    """
    for row in rows:
        for col_name, value in row.items():
            if isinstance(value, (dict, list)):
                raise Neo4jNodeObjectError(
                    f"Column {col_name!r} returned a node object or list. "
                    "Cypher queries must project scalar values only "
                    "(e.g. RETURN n.name AS name, n.age AS age). "
                    "Use property accessors (n.prop) instead of returning the node itself."
                )

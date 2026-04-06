# Copyright (c) 2026 Kenneth Stott
# Canary: f6a7b8c9-d0e1-2345-f012-456789012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL source builder (Phase AO).

Translates a SPARQL endpoint config into an ApiSource + ApiEndpoint
that the existing API source pipeline can execute.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import httpx

from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
)
from provisa.api_source.normalizers import sparql_bindings


@dataclass
class SparqlSourceConfig:
    """User-supplied SPARQL endpoint configuration."""

    source_id: str
    endpoint_url: str  # Full SPARQL endpoint URL, e.g. http://fuseki:3030/ds/sparql
    default_graph_uri: str | None = None
    auth: object | None = None  # ApiAuth instance
    extra_params: dict[str, str] = field(default_factory=dict)


def build_api_source(cfg: SparqlSourceConfig) -> ApiSource:
    """Build an ApiSource record for a SPARQL endpoint."""
    # Strip the path so base_url is just scheme://host:port
    from urllib.parse import urlparse
    parsed = urlparse(cfg.endpoint_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    return ApiSource(
        id=cfg.source_id,
        type=ApiSourceType.openapi,  # treated as a generic POST API
        base_url=base_url,
        auth=cfg.auth,
    )


def build_endpoint(
    cfg: SparqlSourceConfig,
    table_name: str,
    sparql_query: str,
    columns: list[ApiColumn],
    ttl: int = 300,
) -> ApiEndpoint:
    """Build an ApiEndpoint for a single SPARQL table.

    The endpoint POSTs the SPARQL query as form-encoded data following
    the SPARQL 1.1 Protocol and uses the sparql_bindings normalizer.
    """
    from urllib.parse import urlparse
    parsed = urlparse(cfg.endpoint_url)
    path = parsed.path or "/"

    return ApiEndpoint(
        source_id=cfg.source_id,
        path=path,
        method="POST",
        table_name=table_name,
        columns=columns,
        ttl=ttl,
        response_root=None,
        body_encoding="form",
        query_template=sparql_query,
        response_normalizer="sparql_bindings",
    )


_VAR_RE = re.compile(r"\?\s*(\w+)")


def extract_variables(sparql_query: str) -> list[str]:
    """Extract SELECT variable names from a SPARQL SELECT query.

    Parses the SELECT clause for ?var patterns. Handles SELECT * by
    returning an empty list (caller must infer from probe results).
    """
    # Find the SELECT ... WHERE section
    select_match = re.search(
        r"\bSELECT\b\s*(DISTINCT\s+|REDUCED\s+)?(.+?)\s*\bWHERE\b",
        sparql_query,
        re.IGNORECASE | re.DOTALL,
    )
    if not select_match:
        return []
    select_clause = select_match.group(2).strip()
    if select_clause == "*":
        return []
    return _VAR_RE.findall(select_clause)


def _probe_limit(sparql_query: str, limit: int = 5) -> str:
    """Append LIMIT to a SPARQL query for the registration probe."""
    if re.search(r"\bLIMIT\b", sparql_query, re.IGNORECASE):
        return sparql_query
    return sparql_query.rstrip().rstrip(";") + f"\nLIMIT {limit}"


async def probe_endpoint(
    cfg: SparqlSourceConfig,
    sparql_query: str,
    timeout: float = 10.0,
) -> list[dict]:
    """Execute a SPARQL probe (LIMIT 5) to validate the endpoint and infer columns.

    Returns flat row dicts via sparql_bindings normalizer.

    Raises:
        httpx.HTTPError: on network or HTTP errors
    """
    probe = _probe_limit(sparql_query, limit=5)
    data: dict[str, str] = {"query": probe}
    if cfg.default_graph_uri:
        data["default-graph-uri"] = cfg.default_graph_uri
    data.update(cfg.extra_params)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/sparql-results+json",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            cfg.endpoint_url,
            data=data,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()

    return sparql_bindings(resp.json())


def infer_columns(rows: list[dict]) -> list[ApiColumn]:
    """Infer column definitions from SPARQL probe results."""
    if not rows:
        return []
    return [ApiColumn(name=var, type=ApiColumnType.string) for var in rows[0].keys()]

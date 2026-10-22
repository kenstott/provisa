# Copyright (c) 2026 Kenneth Stott
# Canary: 6c726d35-c1ce-4ce4-9c33-ebc5debffd1a
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

# Requirements: REQ-297, REQ-298, REQ-299

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
from provisa.core.auth_models import ApiAuth
from provisa.core.secrets import resolve_secrets


_REFERENCE = "${"  # a credential reference (provisa.core.secrets), resolved where it is used


def sparql_headers() -> dict[str, str]:  # REQ-2138
    """The headers every call to a SPARQL endpoint carries. A public endpoint asks its clients to
    say who they are (Wikidata's User-Agent policy refuses a library's default): Provisa, its
    version and where to reach it."""
    from provisa.core.demo_bundles import running_provisa

    return {"User-Agent": f"Provisa/{running_provisa()} (https://provisa.dev)"}


@dataclass
class SparqlSourceConfig:  # REQ-297
    """User-supplied SPARQL endpoint configuration."""

    source_id: str
    endpoint_url: str  # Full SPARQL endpoint URL, e.g. http://fuseki:3030/ds/sparql
    default_graph_uri: str | None = None
    auth: ApiAuth | None = None
    extra_params: dict[str, str] = field(default_factory=dict)


def build_api_source(cfg: SparqlSourceConfig) -> ApiSource:  # REQ-297
    """Build an ApiSource record for a SPARQL endpoint."""
    # Strip the path so base_url is just scheme://host:port
    from urllib.parse import urlparse

    if _REFERENCE in cfg.endpoint_url:
        # The URL carries a credential reference and is stored as written, whole (its parts
        # cannot be told apart until it is resolved): the endpoint's own path is then empty,
        # and the caller resolves the address at each call.
        base_url = cfg.endpoint_url
    else:
        parsed = urlparse(cfg.endpoint_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
    return ApiSource(
        id=cfg.source_id,
        type=ApiSourceType.sparql,
        base_url=base_url,
        auth=cfg.auth,
        headers=sparql_headers(),
    )


def build_endpoint(
    cfg: SparqlSourceConfig,
    table_name: str,
    sparql_query: str,
    columns: list[ApiColumn],
    ttl: int = 300,
    reasoner: tuple[str, str] | None = None,
) -> ApiEndpoint:  # REQ-297, REQ-298, REQ-299, REQ-2138
    """Build an ApiEndpoint for a single SPARQL table.

    The endpoint POSTs the SPARQL query as form-encoded data following
    the SPARQL 1.1 Protocol and uses the sparql_bindings normalizer.

    A table whose query is one IRI is an inferred class or property of the source's
    ``reasoner`` (its name and axioms, provisa/sparql/reasoning): its rows are the reasoner's,
    and the reasoner and axioms travel in its default parameters.
    """
    from urllib.parse import urlparse

    from provisa.sparql import reasoning

    path = "" if _REFERENCE in cfg.endpoint_url else (urlparse(cfg.endpoint_url).path or "/")

    if reasoning.inferred_iri(sparql_query) is not None:
        if reasoner is None:
            raise ValueError(
                f"table {table_name!r}: a query that names one IRI reads an inferred class or "
                f"property, and source {cfg.source_id!r} names no reasoner"
            )
        return ApiEndpoint(
            source_id=cfg.source_id,
            path=path,
            method="POST",
            table_name=table_name,
            columns=columns,
            ttl=ttl,
            body_encoding="form",
            query_template=sparql_query,
            response_normalizer=reasoning.NORMALIZER,
            default_params={
                reasoning.REASONER_PARAM: reasoner[0],
                reasoning.AXIOMS_PARAM: reasoner[1],
            },
        )

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
) -> dict:  # REQ-297, REQ-298
    """Execute a SPARQL probe (LIMIT 5) to validate the endpoint and infer columns.

    Returns the SPARQL 1.1 JSON results document as answered: its terms keep their datatypes,
    which type the columns (``infer_columns``).

    Raises:
        httpx.HTTPError: on network or HTTP errors
    """
    probe = _probe_limit(sparql_query, limit=5)
    data: dict[str, str] = {"query": probe}
    if cfg.default_graph_uri:
        data["default-graph-uri"] = cfg.default_graph_uri
    data.update(cfg.extra_params)

    headers = {
        **sparql_headers(),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/sparql-results+json",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            resolve_secrets(cfg.endpoint_url),  # stored as written; resolved at the call
            data=data,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()

    return resp.json()


_XSD = "http://www.w3.org/2001/XMLSchema#"

# A literal's XSD datatype → the column type its values land as. Any other term (an IRI, a blank
# node, a plain or language-tagged string, a datatype not listed) is text.
_XSD_TYPES: dict[str, ApiColumnType] = {
    **{
        _XSD + name: ApiColumnType.integer
        for name in (
            "integer", "int", "long", "short", "byte", "nonNegativeInteger", "positiveInteger",
            "nonPositiveInteger", "negativeInteger", "unsignedLong", "unsignedInt",
            "unsignedShort", "unsignedByte",
        )
    },
    _XSD + "decimal": ApiColumnType.number,
    _XSD + "double": ApiColumnType.number,
    _XSD + "float": ApiColumnType.number,
    _XSD + "boolean": ApiColumnType.boolean,
    _XSD + "date": ApiColumnType.date,
    _XSD + "dateTime": ApiColumnType.timestamp,
    _XSD + "dateTimeStamp": ApiColumnType.timestamp,
}  # fmt: skip


def term_type(term: dict) -> ApiColumnType:
    """The column type one bound term of a SPARQL JSON result lands as."""
    if term.get("type") in ("literal", "typed-literal"):
        return _XSD_TYPES.get(term.get("datatype", ""), ApiColumnType.string)
    return ApiColumnType.string


def infer_columns(results: dict) -> list[ApiColumn]:  # REQ-297
    """The columns of a SPARQL 1.1 JSON results document: one per variable its head names, typed
    by the datatype its bound terms share; a variable bound to terms of different types, or never
    bound, is text."""
    variables = results.get("head", {}).get("vars", [])
    bindings = results.get("results", {}).get("bindings", [])
    columns = []
    for var in variables:
        kinds = {term_type(b[var]) for b in bindings if var in b}
        kind = kinds.pop() if len(kinds) == 1 else ApiColumnType.string
        columns.append(ApiColumn(name=var, type=kind))
    return columns

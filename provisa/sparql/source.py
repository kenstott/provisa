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
    version and where to reach it. And the results format its tables are read in, JSON: an
    endpoint's default is its own (Wikidata answers XML). A CONSTRUCT asks for its own format
    (reasoning.fetch_slice)."""
    from provisa.core.demo_bundles import running_provisa

    return {
        "User-Agent": f"Provisa/{running_provisa()} (https://provisa.dev)",
        "Accept": "application/sparql-results+json",
    }


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


# REQ-1923: a query is read as text, its parts told apart by masking what may hold any
# character -- comments, IRIs and string literals -- with spaces of the same length, so the
# offsets found in the mask are the query's own.
_MASKED = re.compile(
    r'#[^\n]*|<[^<>"{}|^`\\\s]*>|"""(?:.|\n)*?"""|\'\'\'(?:.|\n)*?\'\'\'|"(?:[^"\\\n]|\\.)*"'
    r"|'(?:[^'\\\n]|\\.)*'"
)
_SELECT = re.compile(r"\bSELECT\b\s*(?:DISTINCT\b|REDUCED\b)?", re.IGNORECASE)
_PROJECTED_AS = re.compile(r"\bAS\s*[?$](\w+)\s*$", re.IGNORECASE)
_AGGREGATE = re.compile(
    r"\bGROUP\s+BY\b|\b(?:COUNT|SUM|MIN|MAX|AVG|SAMPLE|GROUP_CONCAT)\s*\(", re.IGNORECASE
)
_OUTER_LIMIT = re.compile(r"\bLIMIT\s+(\d+)\s*(?:OFFSET\s+\d+\s*)?;?\s*$", re.IGNORECASE)
_LIMIT = re.compile(r"\bLIMIT\b", re.IGNORECASE)


def _mask(query: str) -> str:
    return _MASKED.sub(lambda m: re.sub(r"\S", " ", m.group(0)), query)


def _closing_brace(masked: str, opening: int) -> int:
    """The offset of the brace that closes the one at ``opening``."""
    depth = 0
    for at in range(opening, len(masked)):
        if masked[at] == "{":
            depth += 1
        elif masked[at] == "}":
            depth -= 1
            if depth == 0:
                return at
    raise ValueError("the query's braces do not balance")


def extract_variables(sparql_query: str) -> list[str]:  # REQ-297, REQ-1923
    """The names of the columns a SELECT query answers, read from its projection: each plain
    variable, and the variable each ``(expression AS ?name)`` binds -- never a variable an
    expression only reads. Empty for ``SELECT *`` (its columns are known only from an answer)
    and for a query that is not a SELECT."""
    masked = _mask(sparql_query)
    found = _SELECT.search(masked)
    if found is None:
        return []
    names: list[str] = []
    at, depth, expression_from = found.end(), 0, 0
    while at < len(masked):
        char = masked[at]
        if char == "(":
            if depth == 0:
                expression_from = at + 1
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                bound = _PROJECTED_AS.search(masked[expression_from:at])
                if bound is not None:
                    names.append(bound.group(1))
        elif depth == 0:
            if char == "{" or re.match(r"WHERE\b", masked[at:], re.IGNORECASE):
                break
            if char == "*":
                return []
            if char in "?$":
                name = re.match(r"\w+", masked[at + 1 :])
                if name is not None:
                    names.append(name.group(0))
                    at += len(name.group(0))
        at += 1
    return names


def _bounded_rows_pattern(query: str, masked: str, limit: int) -> str:
    """``query`` with its rows bounded where they are produced: in a query that aggregates, the
    first sub-select that neither aggregates, nests another, nor limits itself -- the pattern
    the aggregate's rows come from -- is limited, since a limit after the aggregate bounds only
    what is answered, not what is read. Unchanged when the query does not aggregate or has no
    such sub-select (its outer limit is then the one bound)."""
    if _AGGREGATE.search(masked) is None:
        return query
    for opening in (m.start() for m in re.finditer(r"\{\s*SELECT\b", masked, re.IGNORECASE)):
        closing = _closing_brace(masked, opening)
        body = masked[opening + 1 : closing]
        first = _SELECT.search(body)
        nested = first is not None and _SELECT.search(body, first.end()) is not None
        if nested or _AGGREGATE.search(body) or _LIMIT.search(body):
            continue
        return f"{query[:closing].rstrip()} LIMIT {limit} {query[closing:]}"
    return query


def _probe_limit(sparql_query: str, limit: int = 5) -> str:  # REQ-1923
    """The query a preview runs: at most ``limit`` rows, with the limit placed where it bounds
    the work (``_bounded_rows_pattern``) and on the answer itself -- an outer LIMIT of the
    query's own is lowered to ``limit``, never raised."""
    query = sparql_query.strip().rstrip(";").rstrip()
    query = _bounded_rows_pattern(query, _mask(query), limit)
    masked = _mask(query)
    outer = _OUTER_LIMIT.search(masked)
    if outer is not None and _closing_brace_count(masked[: outer.start()]) == 0:
        if int(outer.group(1)) <= limit:
            return query
        return f"{query[: outer.start(1)]}{limit}{query[outer.end(1) :]}"
    return f"{query}\nLIMIT {limit}"


def _closing_brace_count(masked: str) -> int:
    """How many braces are still open at the end of ``masked``: 0 at the top level."""
    return masked.count("{") - masked.count("}")


def query_timeout() -> float:  # REQ-1923
    """How long one SPARQL query may take before it is given up: the operator's setting."""
    from provisa.core import settings_registry

    return float(settings_registry.value("sparql.query_timeout"))


async def probe_endpoint(
    cfg: SparqlSourceConfig,
    sparql_query: str,
    timeout: float | None = None,
) -> dict:  # REQ-297, REQ-298, REQ-1923
    """Execute a SPARQL probe (LIMIT 5, placed where it bounds the work: ``_probe_limit``) to
    validate the endpoint and infer columns, under ``timeout`` seconds -- the operator's SPARQL
    query timeout when none is given.

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

    headers = {**sparql_headers(), "Content-Type": "application/x-www-form-urlencoded"}

    from provisa.security import sandbox_egress

    # REQ-2006: a held org's endpoint is checked at each connection, each request.
    async with httpx.AsyncClient(event_hooks=sandbox_egress.http_event_hooks()) as client:
        resp = await client.post(
            resolve_secrets(cfg.endpoint_url),  # stored as written; resolved at the call
            data=data,
            headers=headers,
            timeout=query_timeout() if timeout is None else timeout,
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

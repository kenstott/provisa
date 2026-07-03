# Copyright (c) 2026 Kenneth Stott
# Canary: 9834c4ac-2679-4438-b6d5-1ddfb7866940
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Route queries to Trino or direct RDBMS based on source analysis (REQ-027, REQ-028, REQ-030).

Single RDBMS source with direct driver → direct (sub-100ms target).
Single NoSQL source → always Trino (no SQL support).
Multi-source → Trino (REQ-028, 300-500ms target).
Steward override hint respected (REQ-030).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from provisa.executor.drivers.registry import has_driver

# Requirements: REQ-027, REQ-028, REQ-030, REQ-031, REQ-066, REQ-067, REQ-151, REQ-152


class Route(str, Enum):
    CACHE = "cached"  # REQ-865
    DIRECT = "direct"
    TRINO = "virtual"
    API = "api"


# API sources — route through API caller pipeline
API_SOURCES: set[str] = {"openapi", "graphql_api", "grpc_api", "grpc_remote"}

# Virtual sources — always route through Trino (no direct SQL driver)
VIRTUAL_SOURCES: set[str] = {
    # NoSQL
    "mongodb",
    "cassandra",
    "redis",
    "kudu",
    "accumulo",
    # Data Lake
    "delta_lake",
    "iceberg",
    "hive",
    "hive_s3",
    # Specialized
    "google_sheets",
    "prometheus",
    # Streaming
    "kafka",
}


@dataclass(frozen=True)
class RouteDecision:
    route: Route
    source_id: str | None  # set when route=DIRECT
    dialect: str | None  # SQLGlot target dialect for DIRECT route
    reason: str


def decide_route(  # REQ-027, REQ-028, REQ-030, REQ-031, REQ-066, REQ-067, REQ-151, REQ-152
    sources: set[str],
    source_types: dict[str, str],
    source_dialects: dict[str, str],
    steward_hint: str | None = None,
    *,
    has_json_extract: bool = False,
    is_mutation: bool = False,
    source_dsns: dict[str, str] | None = None,
    cache_hit: bool = False,
    no_cache: bool = False,
) -> RouteDecision:
    """Decide whether to route a query cached, direct, or through Trino.

    Args:
        sources: Set of source_ids involved in the query.
        source_types: {source_id: source_type} e.g. {"sales-pg": "postgresql"}.
        source_dialects: {source_id: sqlglot_dialect} e.g. {"sales-pg": "postgres"}.
        steward_hint: Optional "direct" or "trino" override from steward.
        has_json_extract: Query uses json_extract_scalar (path columns).
        is_mutation: True for mutations — always route direct (never Trino).
        cache_hit: True when the result cache (keyed per REQ-864/REQ-544 on the
            governance-normalized IR) holds an entry for this query.
        no_cache: True when the @noCache/no_cache bypass (REQ-544) removes CACHED
            from the candidate set for this query.

    Returns:
        RouteDecision with route, target source (if direct), and reason.
    """
    # Result cache is the first candidate route (REQ-865). A hit serves the
    # stored result with no direct or federated execution. The cache key is
    # derived from the persona-resolved governed IR, so a serve is inherently
    # isolated (REQ-866). Mutations never serve from cache; the no-cache bypass
    # removes CACHED from the candidate set (REQ-544).
    if cache_hit and not is_mutation and not no_cache:
        return RouteDecision(
            route=Route.CACHE,
            source_id=None,
            dialect=None,
            reason="result cache hit",
        )

    # Mutations always route direct — Trino doesn't support writes
    if is_mutation and len(sources) >= 1:  # REQ-031
        sid = next(iter(sources))
        return RouteDecision(
            route=Route.DIRECT,
            source_id=sid,
            dialect=source_dialects.get(sid),
            reason="mutation (always direct)",
        )

    # Steward override
    if steward_hint in ("trino", "federated"):  # REQ-030
        return RouteDecision(
            route=Route.TRINO,
            source_id=None,
            dialect=None,
            reason="steward override: federated",
        )
    if steward_hint == "direct" and len(sources) == 1:  # REQ-030
        sid = next(iter(sources))
        stype = source_types.get(sid, "")
        if has_driver(stype):
            return RouteDecision(
                route=Route.DIRECT,
                source_id=sid,
                dialect=source_dialects.get(sid),
                reason="steward override: direct",
            )

    # Colocated sources: same physical DB → treat as single source, route DIRECT
    # Any source missing a DSN (e.g. iceberg/Trino-only) means the query cannot be direct.
    if len(sources) > 1 and source_dsns:
        unique_dsns = {source_dsns.get(s) for s in sources}
        if None not in unique_dsns and len(unique_dsns) == 1:
            primary = next((s for s in sources if s != "provisa-admin"), next(iter(sources)))
            stype = source_types.get(primary, "")
            if has_driver(stype):
                return RouteDecision(
                    route=Route.DIRECT,
                    source_id=primary,
                    dialect=source_dialects.get(primary),
                    reason="colocated sources on same physical DB (direct)",
                )

    # Multi-source → Trino
    if len(sources) > 1:  # REQ-028
        return RouteDecision(
            route=Route.TRINO,
            source_id=None,
            dialect=None,
            reason="multi-source query",
        )

    sid = next(iter(sources))
    stype = source_types.get(sid, "")

    # API sources — route through API caller, not Trino
    if stype in API_SOURCES:
        return RouteDecision(
            route=Route.API,
            source_id=sid,
            dialect=None,
            reason=f"api source ({stype})",
        )

    # Trino-only sources (NoSQL, data lake, non-SQL)
    if stype in VIRTUAL_SOURCES:
        return RouteDecision(
            route=Route.TRINO,
            source_id=None,
            dialect=None,
            reason=f"virtual source ({stype})",
        )

    # JSON path extraction — PG supports ->> natively; other dialects may not
    if has_json_extract:  # REQ-151, REQ-152
        _JSON_SAFE_DIALECTS = {"postgres", "postgresql"}
        dialect = source_dialects.get(sid, "")
        if dialect not in _JSON_SAFE_DIALECTS:
            return RouteDecision(
                route=Route.TRINO,
                source_id=None,
                dialect=None,
                reason=f"query uses JSON path extraction, unsupported for direct {stype}",
            )

    # RDBMS with direct driver → direct
    if has_driver(stype):  # REQ-027, REQ-066, REQ-067
        return RouteDecision(
            route=Route.DIRECT,
            source_id=sid,
            dialect=source_dialects.get(sid),
            reason=f"single rdbms source with direct driver ({stype})",
        )

    # RDBMS without direct driver → Trino
    return RouteDecision(
        route=Route.TRINO,
        source_id=None,
        dialect=None,
        reason=f"no direct driver for source type ({stype})",
    )

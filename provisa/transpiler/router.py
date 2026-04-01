# Copyright (c) 2025 Kenneth Stott
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


class Route(str, Enum):
    DIRECT = "direct"
    TRINO = "virtual"


# Virtual sources — always route through Trino (no direct SQL driver)
VIRTUAL_SOURCES: set[str] = {
    # NoSQL
    "mongodb", "cassandra", "redis", "kudu", "accumulo",
    # Data Lake
    "delta_lake", "iceberg", "hive",
    # Specialized
    "google_sheets", "prometheus",
    # API / Streaming
    "openapi", "graphql_api", "grpc_api", "kafka",
}


@dataclass(frozen=True)
class RouteDecision:
    route: Route
    source_id: str | None  # set when route=DIRECT
    dialect: str | None  # SQLGlot target dialect for DIRECT route
    reason: str


def decide_route(
    sources: set[str],
    source_types: dict[str, str],
    source_dialects: dict[str, str],
    steward_hint: str | None = None,
    *,
    has_json_extract: bool = False,
) -> RouteDecision:
    """Decide whether to route a query direct or through Trino.

    Args:
        sources: Set of source_ids involved in the query.
        source_types: {source_id: source_type} e.g. {"sales-pg": "postgresql"}.
        source_dialects: {source_id: sqlglot_dialect} e.g. {"sales-pg": "postgres"}.
        steward_hint: Optional "direct" or "trino" override from steward.
        has_json_extract: Query uses json_extract_scalar (path columns).

    Returns:
        RouteDecision with route, target source (if direct), and reason.
    """
    # Steward override
    if steward_hint == "trino":
        return RouteDecision(
            route=Route.TRINO, source_id=None, dialect=None,
            reason="steward override: trino",
        )
    if steward_hint == "direct" and len(sources) == 1:
        sid = next(iter(sources))
        stype = source_types.get(sid, "")
        if has_driver(stype):
            return RouteDecision(
                route=Route.DIRECT, source_id=sid,
                dialect=source_dialects.get(sid),
                reason="steward override: direct",
            )

    # Multi-source → Trino
    if len(sources) > 1:
        return RouteDecision(
            route=Route.TRINO, source_id=None, dialect=None,
            reason="multi-source query",
        )

    sid = next(iter(sources))
    stype = source_types.get(sid, "")

    # Trino-only sources (NoSQL, data lake, non-SQL)
    if stype in VIRTUAL_SOURCES:
        return RouteDecision(
            route=Route.TRINO, source_id=None, dialect=None,
            reason=f"virtual source ({stype})",
        )

    # JSON path extraction — PG supports ->> natively; other dialects may not
    if has_json_extract:
        _JSON_SAFE_DIALECTS = {"postgres", "postgresql"}
        dialect = source_dialects.get(sid, "")
        if dialect not in _JSON_SAFE_DIALECTS:
            return RouteDecision(
                route=Route.TRINO, source_id=None, dialect=None,
                reason=f"query uses JSON path extraction, unsupported for direct {stype}",
            )

    # RDBMS with direct driver → direct
    if has_driver(stype):
        return RouteDecision(
            route=Route.DIRECT, source_id=sid,
            dialect=source_dialects.get(sid),
            reason=f"single rdbms source with direct driver ({stype})",
        )

    # RDBMS without direct driver → Trino
    return RouteDecision(
        route=Route.TRINO, source_id=None, dialect=None,
        reason=f"no direct driver for source type ({stype})",
    )

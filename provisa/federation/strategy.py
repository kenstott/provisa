# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9d4b71-6a08-4f53-9e12-3c7a0d4f8b70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Federation strategy resolution — federate(datasource, table) (REQ-826).

The binding between a datasource table and the engine is a FEDERATION STRATEGY, the
method by which that source joins the engine's unified surface. ``federate`` resolves a
source to one of three strategies (and returns which was chosen, because freshness differs
per strategy):

- VIRTUAL      — the engine reaches the source live via an ATTACH connector (the engine
                 connector, or DuckDB ATTACH to postgres/mysql/sqlite). No copy, always
                 fresh; cache_ttl is irrelevant.
- SCAN         — the source is a file/object the engine reads in place, exposed as a view
                 with no data moved (read_csv/read_parquet, Iceberg/Delta). Freshness
                 follows the underlying file; cache_ttl is irrelevant.
- MATERIALIZED — no virtual or scan representation (live APIs, NoSQL, or an RDBMS
                 deliberately cached for latency); data is loaded into the engine's
                 reachable store. This is the ONLY strategy where cache_ttl is a reload
                 interval and residency/reload scheduling runs.

The chosen strategy depends on BOTH datasource capability (the source type) and engine
capability (whether a connector exists and its mechanism), so the same source may federate
by different strategies on different engines.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from provisa.federation.connector import Mechanism
from provisa.federation.engine import UnreachableSource

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.federation.cardinality import Estimate
    from provisa.federation.engine import FederationEngine
    from provisa.federation.promote import PushdownDemand


class Strategy(str, Enum):  # REQ-826
    VIRTUAL = "virtual"
    SCAN = "scan"
    MATERIALIZED = "materialized"


# File/object source types the engine reads in place (a SCAN view, no data moved).
_SCANNABLE = frozenset(
    {"csv", "parquet", "files", "iceberg", "delta_lake", "hive", "hive_s3", "google_sheets"}
)

# Sources with no live/scan representation — APIs, NoSQL, and streaming feeds. They
# federate only by being loaded into the tenant materialization store (MATERIALIZED).
_MATERIALIZE_ONLY = frozenset(
    {
        "openapi",
        "graphql_remote",
        "grpc_remote",
        "mongodb",
        "cassandra",
        "redis",
        "elasticsearch",
        "kafka",
        "websocket",
        "rss",
        "prometheus",
        "sparql",
        "neo4j",
        "splunk",
        "sharepoint",
        "govdata",
        "ingest",
    }
)


def federate(
    source: Source,
    engine: FederationEngine,
    *,
    prefer_materialized: bool = False,
    demand: PushdownDemand | None = None,
    estimate: Estimate | None = None,
) -> Strategy:
    """Resolve a source's federation strategy on the given engine (REQ-826).

    ``prefer_materialized`` forces MATERIALIZED for a source that could federate live but is
    deliberately cached for latency. A source the engine can neither attach/scan nor
    materialize is rejected as unreachable (REQ-841).

    ``demand`` + ``estimate`` enable COST-BASED promotion: a VIRTUAL/SCAN source whose
    connector cannot push down a reducing operator this query needs, and whose scan is
    known-large, is promoted to MATERIALIZED (see promote.should_promote). Both must be
    supplied to arm promotion; absent them, resolution is capability-only as before.
    """
    source_type = source.type.value
    connector = engine.connectors.get(source_type)

    if connector is not None and not prefer_materialized:
        modes = connector.reach_modes
        # A source the engine can read LIVE in place (ATTACH_*): a file/object type is a SCAN view,
        # a live source is VIRTUAL.
        if Mechanism.ATTACH_RW in modes or Mechanism.ATTACH_R in modes:
            strategy = Strategy.SCAN if source_type in _SCANNABLE else Strategy.VIRTUAL
            if demand is not None and estimate is not None:
                from provisa.federation.promote import should_promote

                if should_promote(connector.capability(), demand, estimate):
                    return Strategy.MATERIALIZED  # reachable but weak pushdown on a large scan
            return strategy
        # Only DIRECT/FETCH — the engine cannot read the source live, so it is ALWAYS materialized
        # (landed + refreshed) so the engine can see it via the replica (REQ-951).
        return Strategy.MATERIALIZED

    # No connector (or forced): only materializable sources federate via the store.
    if prefer_materialized or source_type in _MATERIALIZE_ONLY:
        return Strategy.MATERIALIZED

    raise UnreachableSource(engine.name, source_type)


def requires_residency(strategy: Strategy) -> bool:  # REQ-825 stage-4b prep phase
    """Whether the PLAN stage must emit a residency prep step (load/refresh) before execute.

    Only MATERIALIZED owns residency; VIRTUAL and SCAN are effectively free (no prep phase).
    """
    return strategy is Strategy.MATERIALIZED

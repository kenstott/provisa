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
from typing import TYPE_CHECKING, Any

from provisa.federation.connector import Mechanism
from provisa.federation.engine import UnreachableSource

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.federation.cardinality import Estimate
    from provisa.federation.engine import FederationEngine
    from provisa.federation.promote import PushdownDemand


def engine_attaches(engine: Any, source_type: str) -> bool:
    """True iff ``engine`` reaches ``source_type`` LIVE in place (an ATTACH_* connector). Such a
    source MUST NOT be materialized/landed — the engine reads the file/db directly (DuckDB ATTACHes
    sqlite). Only FETCH/DIRECT source types are landed. ``engine`` may be the EngineRuntime wrapper
    or the bare FederationEngine; None (unknown) → False (fall back to landing)."""
    if engine is None:
        return False

    fed = (
        engine
        if getattr(engine, "connectors", None) is not None
        else getattr(engine, "engine", engine)
    )
    connector = getattr(fed, "connectors", {}).get(source_type)
    if connector is None:
        return False
    return bool(
        connector.reads_in_place
    )  # ATTACH_* or SCAN — read live in place, never landed (REQ-951)


class Strategy(str, Enum):  # REQ-826
    VIRTUAL = "virtual"
    SCAN = "scan"
    MATERIALIZED = "materialized"


# Types Provisa reaches only through their connector's Calcite pgwire server (REQ-947): Provisa
# starts that server from the source's backing-store config, connects as generic PostgreSQL, and
# lands a replica. Reachable as REPLICA on ANY fed engine (the pgwire bridge is the reader, not the
# engine's own connectors). ``files`` also SCANs live where an engine has a file connector (Trino);
# off such engines it federates by this replica path. An engine whose own postgres reach can speak
# to that server ATTACHes it live instead (DuckDB, REQ-1690) — such an engine has a connector for
# the type, so it never reaches this gate.
_CONNECTOR_PGWIRE_REPLICA = frozenset({"files", "sharepoint", "splunk"})

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
        # REQ-1443: a data-quality checker produces rows by running a contract; there is no remote
        # table for the engine to federate a query against, so scan results are landed and read from
        # the materialization store like any other produced dataset.
        "soda",
        "great_expectations",
        # REQ-1730: sqlite/firebird/airport are DuckDB-ATTACH-only (no Trino/pg connector exists for
        # any of them) — before this, an engine with no connector raised UnreachableSource outright
        # rather than falling back to landing, the ONLY of Provisa's DIRECT/FETCH-shaped source
        # types this was true for. Each now has a working row-fetch wired in
        # events/source_loader.py's build_adapter_loaders (sqlite: stdlib sqlite3; firebird/airport:
        # a scratch DuckDB connection ATTACHed via their own community extension).
        "sqlite",
        "firebird",
        "airport",
        # REQ-1730: pinot/druid/hive_s3 have real Trino ATTACH connectors
        # (TrinoPinotConnector/TrinoDruidConnector/TrinoHiveS3Connector) but no DuckDB driver at
        # all — same "no connector means UnreachableSource" gap sqlite/firebird/airport closed
        # above, just for a type Trino DOES attach live (so `engine_attaches` correctly stays
        # False only for the engines that actually lack one). Each has a working row-fetch wired
        # in events/source_loader.py's build_adapter_loaders (pinot/druid: the broker's own
        # SQL-over-HTTP query API; hive_s3: a direct S3 Parquet read by Hive's own conventional
        # table-directory layout — see provisa.hive.fetch's own module doc for why this is a
        # documented narrowing, not a full Hive Metastore reader).
        #
        # Plain `hive` (Hadoop-local storage, not S3) is DELIBERATELY NOT here: its warehouse
        # lives in a Docker named volume shared only between the metastore and Trino containers
        # (docker-compose.core.yml's `hive_warehouse`), with no host-reachable path for a native
        # DuckDB-side process to read it directly, and no Hive Metastore Thrift client exists
        # anywhere in this codebase's dependencies to resolve a real table location generically
        # either way. Genuinely un-reachable from DuckDB today, not a missed wiring step — adding
        # it here without a working loader would trade a clean UnreachableSource for a confusing
        # "catalog not found" once query execution actually tried to read it.
        "pinot",
        "druid",
        "hive_s3",
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
        # A source the engine reads LIVE in place: a SCAN reach (file/object read as a view, no copy)
        # is Strategy.SCAN; an ATTACH reach (live DB) is Strategy.VIRTUAL. The connector's declared
        # reach mode decides — no source-type name heuristic (REQ-951).
        if connector.reads_in_place:
            strategy = Strategy.SCAN if Mechanism.SCAN in modes else Strategy.VIRTUAL
            if demand is not None and estimate is not None:
                from provisa.federation.promote import should_promote

                if should_promote(connector.capability(), demand, estimate):
                    return Strategy.MATERIALIZED  # reachable but weak pushdown on a large scan
            return strategy
        # Only DIRECT/FETCH — the engine cannot read the source live, so it is ALWAYS materialized
        # (landed + refreshed) so the engine can see it via the replica (REQ-951).
        return Strategy.MATERIALIZED

    # No connector (or forced): only materializable sources federate via the store — API/NoSQL/stream
    # feeds and the connector-pgwire-replica types (files/sharepoint/splunk read via their Calcite
    # pgwire server as generic postgres, then landed).
    if (
        prefer_materialized
        or source_type in _MATERIALIZE_ONLY
        or source_type in _CONNECTOR_PGWIRE_REPLICA
    ):
        return Strategy.MATERIALIZED

    raise UnreachableSource(engine.name, source_type)


def requires_residency(strategy: Strategy) -> bool:  # REQ-825 stage-4b prep phase
    """Whether the PLAN stage must emit a residency prep step (load/refresh) before execute.

    Only MATERIALIZED owns residency; VIRTUAL and SCAN are effectively free (no prep phase).
    """
    return strategy is Strategy.MATERIALIZED

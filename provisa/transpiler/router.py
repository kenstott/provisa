# Copyright (c) 2026 Kenneth Stott
# Canary: 9834c4ac-2679-4438-b6d5-1ddfb7866940
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Route queries to the federation engine or a direct RDBMS based on source analysis (REQ-027, REQ-028, REQ-030).

Single RDBMS source with direct driver → direct (sub-100ms target).
Single NoSQL source → always the engine (no direct SQL driver).
Multi-source → the engine (REQ-028, 300-500ms target).
Steward override hint respected (REQ-030).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from provisa.core.operator_floor import OperatorFloorError
from provisa.executor.drivers.registry import has_driver
from provisa.otel_compat import get_tracer, timed_stage

_tracer = get_tracer(__name__)

if TYPE_CHECKING:
    from provisa.federation.engine import FederationEngine

# Requirements: REQ-027, REQ-028, REQ-030, REQ-031, REQ-066, REQ-067, REQ-151, REQ-152


class Route(str, Enum):
    CACHE = "cached"  # REQ-865
    DIRECT = "direct"
    ENGINE = "virtual"  # the federation ENGINE terminal (was Route.ENGINE)
    API = "api"


# API sources — route through API caller pipeline
API_SOURCES: set[str] = {"openapi", "graphql_api", "grpc_api", "grpc_remote"}

# REQ-2100: code sources, whose function is their access path; their rows are landed (REQ-863).
CODE_SOURCES: frozenset[str] = frozenset({"node", "python"})

# Virtual sources — always route through the engine (no direct SQL driver)
VIRTUAL_SOURCES: set[str] = {
    # File-based: exposed only via the engine's ATTACH (DuckDBSqliteConnector). There is no
    # network direct pool for a file source, and only the engine attach maps its physical
    # layout (sqlite exposes tables under ``main``) onto the registered catalog/schema.
    "sqlite",
    # NoSQL
    "mongodb",
    "cosmosdb",  # REQ-1982: read by its SDK and landed; no driver
    "node",  # REQ-1984: read in its worker and landed; no driver
    "python",  # REQ-1993: read in its worker and landed; no driver
    "cassandra",
    "redis",
    # Query-API graph sources: a Neo4jDriver factory is registered (has_driver("neo4j") is True,
    # see executor/drivers/registry.py) but is not routed to -- direct Cypher execution would need
    # a general method to resolve arbitrary SQL join patterns back into correct Cypher, which the
    # reverse compiler (provisa.nl.runner.best_effort_cypher_for_sql) does not have (REQ-1864,
    # reverted 2026-09-26: it mis-translated a reshaped junction table). Kept in VIRTUAL_SOURCES so
    # decide_route never treats a neo4j source as an ordinary RDBMS and hands it un-translated SQL
    # text (GitHub issue #119); row-level materialization (REQ-1865) is the sanctioned mechanism
    # for neo4j read performance instead.
    "neo4j",
    # Data Lake
    "delta_lake",
    "iceberg",
    "hive",
    "hive_s3",  # REQ-229: Hive-on-S3 lake — always routes to Trino, like hive/iceberg/delta_lake
    # Specialized
    "google_sheets",
    "prometheus",
    # Streaming
    "kafka",
}


class OperatorFloorViolation(OperatorFloorError):
    """A request hint would read below the operator's floor (REQ-030, amended 2026-09-30).

    The operator's settings are the FLOOR of every read: a request may trade recency for speed
    above it, never go beneath it. A PermissionError so every transport's existing mapping carries
    it to the caller (HTTP 403, SQLSTATE 42501, Flight/gRPC PERMISSION_DENIED)."""

    def __init__(self, source_id: str, setting: str, hint: str) -> None:
        self.source_id = source_id
        self.setting = setting
        super().__init__(
            f"route={hint} would read source {source_id!r} live, below the operator's floor: the "
            f"operator set {setting} on it, so its reads are served from the platform's landed "
            "copy. Remove the route hint."
        )


@dataclass(frozen=True)
class RouteDecision:
    route: Route
    source_id: str | None  # set when route=DIRECT
    dialect: str | None  # SQLGlot target dialect for DIRECT route
    reason: str


@dataclass(frozen=True)
class RouteInputs:
    """What the gates read of one statement, after optimization (REQ-863)."""

    sources: frozenset[str]
    source_types: Mapping[str, str]
    source_dialects: Mapping[str, str]
    statement: str  # the governed statement, PostgreSQL text: what its source would run
    json_path: bool
    source_dsns: Mapping[str, str] | None
    operator_floor: Mapping[str, str]
    reads_fakes: bool
    # Why the engine must run it, named by the caller: a re-injected view, landed rows, a
    # live-only part, no connection on this node, a region's own ops record, every source inlined.
    forced: tuple[str, ...]
    # Whether the source's proxy planned the read as its complete dataset (federation.
    # source_proxy), and if not why. Not planned is not complete: the read lands.
    complete_dataset: bool
    incomplete: str | None
    engine: FederationEngine | None


@dataclass(frozen=True)
class Gate:
    """One condition a statement passes to be read on its source directly, not landed: the reason
    it fails (None when it passes)."""

    name: str
    check: Callable[[RouteInputs], str | None]


def _database(dsn: str) -> str:
    # A colocation key is host:port/database|user|digest (federation.colocation).
    return dsn.split("|", 1)[0]


def _one_source(i: RouteInputs) -> str | None:
    if len(i.sources) == 1:
        return None
    if i.source_dsns:
        keys = [i.source_dsns.get(s) for s in i.sources]
        if None not in keys and len({_database(k) for k in keys if k is not None}) == 1:
            return None
    return f"reads {len(i.sources)} sources"


def _primary(i: RouteInputs) -> str:
    return next((s for s in sorted(i.sources) if s != "provisa-admin"), sorted(i.sources)[0])


def _dialect_supports(i: RouteInputs) -> str | None:
    from provisa.transpiler.dialect_capabilities import unsupported

    sid = _primary(i)
    if i.source_types.get(sid, "") in API_SOURCES | CODE_SOURCES:
        return None  # no SQL is sent to it: its access path is the API caller, the code function
    construct = unsupported(i.statement, i.source_dialects.get(sid) or "", json_path=i.json_path)
    return None if construct is None else f"source {sid!r} cannot run {construct}"


def _access_path(i: RouteInputs) -> str | None:
    stype = i.source_types.get(_primary(i), "")
    if stype in API_SOURCES or stype in CODE_SOURCES:
        return None  # the API caller, the code function
    if stype in VIRTUAL_SOURCES:
        # The engine reads it: in place on a file_native engine, else landed (REQ-897).
        return _engine_source_reason(i.engine, stype)
    if has_driver(stype):
        return None
    return f"no direct driver for source type ({stype})"


def _colocated_login(i: RouteInputs) -> str | None:
    if len(i.sources) == 1:
        return None
    assert i.source_dsns is not None  # one database (one_source) is judged by their keys
    if len({i.source_dsns[s] for s in i.sources}) == 1:
        return None
    return "its sources share a database under different logins"


def _not_forced(i: RouteInputs) -> str | None:
    floored = sorted((s, i.operator_floor[s]) for s in i.sources if s in i.operator_floor)
    if floored:
        sid, setting = floored[0]
        return f"operator floor: {setting} on {sid} (read from the replica)"
    if i.reads_fakes:
        return "reads faked columns (computed by the engine)"
    if i.forced:
        return i.forced[0]
    return None


def _trusted_to_answer(i: RouteInputs) -> str | None:
    # A source with a proxy answers only a complete dataset (federation.source_proxy); a SQL
    # database's driver runs the statement itself (dialect_supports judged its constructs).
    from provisa.federation.connector_contract import contract_for

    sid = _primary(i)
    if contract_for(i.source_types.get(sid, "")) is None or i.complete_dataset:
        return None
    return f"incomplete dataset from {sid}" + (f": {i.incomplete}" if i.incomplete else "")


#: The gates a statement passes, in order, to be read on its source directly; the first it fails
#: lands it and names the reason (REQ-863).
GATES: tuple[Gate, ...] = (
    Gate("one_source", _one_source),
    Gate("dialect_supports", _dialect_supports),
    Gate("access_path", _access_path),
    Gate("colocated_login", _colocated_login),
    Gate("not_forced", _not_forced),
    Gate("trusted_to_answer", _trusted_to_answer),
)


def _failed_gate(i: RouteInputs) -> str | None:
    for gate in GATES:
        why = gate.check(i)
        if why is not None:
            return f"landed: {gate.name}: {why}"
    return None


@timed_stage(_tracer, "router.decide_route", name="route")  # REQ-1910
def decide_route(  # REQ-027, REQ-028, REQ-030, REQ-031, REQ-066, REQ-067, REQ-151, REQ-152, REQ-863
    sources: set[str],
    source_types: dict[str, str],
    source_dialects: dict[str, str],
    steward_hint: str | None = None,
    *,
    statement: str,
    has_json_extract: bool = False,
    is_mutation: bool = False,
    source_dsns: dict[str, str] | None = None,
    cache_hit: bool = False,
    cache_opt_in: bool = False,
    engine: FederationEngine | None = None,
    operator_floor: Mapping[str, str],
    # REQ-1494: whether the statement reads faked columns. False is safe, not lax: a faked read sent
    # to a source fails there, the source holding no fake functions -- it never reads unfaked.
    reads_fakes: bool = False,
    forced: tuple[str, ...] = (),
    complete_dataset: bool = False,
    incomplete: str | None = None,
) -> RouteDecision:
    """Decide whether a statement is read on its source directly or landed and run by the engine
    (REQ-863). Landing is the default; a statement is read directly only when it passes every
    gate (``GATES``), in order, and the first it fails names why it is landed.

    ``steward_hint`` (REQ-030): ``engine``/``federated`` lands it without the gates; ``direct``
    tries them, and is ignored -- noted in the reason -- when one fails. ``statement`` is the
    governed statement after optimization, PostgreSQL text; ``forced`` names each condition the
    caller found that makes it the engine's (a re-injected view, landed rows, a live-only part, no
    connection on this node, a region's own ops record, every source inlined).

    A response-cache hit is answered before any route (REQ-865), and a write goes to its source's
    own driver where it has one, else through the engine's connector (REQ-031, REQ-910).
    """
    if cache_hit and cache_opt_in and not is_mutation:
        return RouteDecision(
            route=Route.CACHE,
            source_id=None,
            dialect=None,
            reason="result cache hit",
        )

    if is_mutation and len(sources) >= 1:  # REQ-031, REQ-910
        from provisa.executor.writable import WritePath, resolve_write_path

        sid = next(iter(sources))
        if resolve_write_path(source_types.get(sid, ""), engine) is WritePath.ENGINE:
            return RouteDecision(
                route=Route.ENGINE,
                source_id=None,
                dialect=None,
                reason="mutation through the engine's connector (no direct driver)",
            )
        return RouteDecision(
            route=Route.DIRECT,
            source_id=sid,
            dialect=source_dialects.get(sid),
            reason="mutation (direct driver)",
        )

    if steward_hint in ("engine", "federated"):  # REQ-030
        return RouteDecision(
            route=Route.ENGINE,
            source_id=None,
            dialect=None,
            reason="steward override: federated",
        )

    inputs = RouteInputs(
        sources=frozenset(sources),
        source_types=source_types,
        source_dialects=source_dialects,
        statement=statement,
        json_path=has_json_extract,
        source_dsns=source_dsns,
        operator_floor=operator_floor,
        reads_fakes=reads_fakes,
        forced=forced,
        complete_dataset=complete_dataset,
        incomplete=incomplete,
        engine=engine,
    )
    failed = _failed_gate(inputs)
    if failed is not None:
        # REQ-030 (amended 2026-10-23): a direct hint that cannot apply is ignored, and said.
        reason = f"route hint ignored: {failed}" if steward_hint == "direct" else failed
        return RouteDecision(route=Route.ENGINE, source_id=None, dialect=None, reason=reason)
    sid = _primary(inputs)
    if steward_hint == "direct":
        reason = "steward override: direct"
    elif len(sources) > 1:
        reason = "colocated sources on same physical DB, one login (direct)"
    else:
        reason = f"single source with a direct access path ({source_types.get(sid, '')})"
    return RouteDecision(
        route=Route.DIRECT,
        source_id=sid,
        dialect=source_dialects.get(sid),
        reason=reason,
    )


# File/object source types the engine reads IN PLACE only when it declares the file_native trait
# (REQ-897) — otherwise the source is LANDED before the engine can read it.
_FILE_SOURCES: frozenset[str] = frozenset({"delta_lake", "iceberg", "hive", "google_sheets"})


def _engine_source_reason(engine: FederationEngine | None, stype: str) -> str:
    """Reason for an ENGINE-routed connector source, refined by the engine's DECLARED file_native
    trait (REQ-897). For a file/object source the trait is the planner INPUT that decides read-in-
    place (SCAN) vs land: a file_native engine scans it live; a non-file_native engine must land it.
    Fails loud (UndeclaredTrait) if the trait is unset — never a guessed default."""
    if engine is None or stype not in _FILE_SOURCES:
        return f"virtual source ({stype})"
    if engine.file_native:
        return f"file source scanned in place on file_native engine {engine.name!r} ({stype})"
    return f"file source landed — engine {engine.name!r} is not file_native ({stype})"

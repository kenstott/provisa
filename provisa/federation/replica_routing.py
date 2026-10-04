# Copyright (c) 2026 Kenneth Stott
# Canary: 8048626b-2984-4e6a-8cf4-c82b2de1ada4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which registered tables are served from a replica on an engine (REQ-826, REQ-1912).

One decision, asked by everything that has to agree about a table: the reconcile that creates its
replica, the copy that fills it, the engine attach that must not expose it live, and the read that
is addressed to it. A table is served from its replica when the engine cannot read its source in
place, or when the operator's setting says its reads come from the replica.

``replica_routes`` turns that decision into the published map a lowered statement is rewritten
with (``replica_address.address_replicas``).
"""

# Requirements: REQ-826, REQ-1912, REQ-1141, REQ-030, REQ-238

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from provisa.mv.models import TableIdentity

from provisa.federation.replica_address import (
    ReplicaRoute,
    ReplicaRoutes,
    TableKey,
    engine_table_keys,
)

_log = logging.getLogger(__name__)

#: Source types that federate MATERIALIZED on every engine (``strategy._MATERIALIZE_ONLY``) and
#: yet own no replica, because their reads never go through one.
#:
#: ``ingest`` (REQ-1730): its rows are written straight into its own table by
#: ``provisa/ingest/router.py``, whose shape always carries an ``id SERIAL PRIMARY KEY`` and
#: ``_received_at`` / ``_updated_at`` the registered columns never list. Reconciling it as a
#: replica compared that shape to the registered columns, found a permanent mismatch and dropped
#: and recreated the table on every pass, discarding every row ingest had written.
#:
#: ``govdata`` (REQ-1730): its reads are dispatched to the askamerica JDBC connection live on
#: every request (``pgwire/_pipeline`` → ``_execute_govdata``) and never reach the engine. A
#: replica of it would be one no query path reads, built by scanning a relation no engine has.
_NO_REPLICA_TYPES = frozenset({"ingest", "govdata"})


def _source_type(source: Any) -> str:
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


def table_floor(source: Any, table: Any, *, promoted: bool) -> str | None:
    """The operator setting that puts ``table``'s reads on its replica (REQ-030, REQ-826), or
    None when they may be live: the floor of its source (``core.operator_floor.floor_setting`` —
    a floored source has no live attach, so every table of it is served from its replica), else
    the table's own resolved ``load_protected`` / ``replicate`` (``core.replicate.floor_of``).
    ``promoted``: the table passed its threshold AND its replica exists in this engine's store
    (``replica_state.promotion``'s serving set) — until then it is read live."""
    from provisa.core.operator_floor import floor_setting
    from provisa.core.replicate import floor_of, resolved_load_protected, resolved_replicate

    of_source = floor_setting(source)
    if of_source is not None:
        return of_source
    return floor_of(
        resolved_replicate(source, table),
        resolved_load_protected(source, table),
        promoted=promoted,
    )


def reads_replica(source: Any, table: Any, engine: Any, *, promoted: bool) -> bool:
    """Whether reads of ``table`` on ``engine`` go to its replica.

    True when the engine cannot read the source in place — the replica is then the only way to
    reach it, whatever the table's setting, Never (-1) included — or when the operator's setting
    puts the table on its replica (``table_floor``). False for a table the engine reads live, for
    a source the engine cannot reach at all, and for a source type that owns no replica.
    ``engine`` is the ``FederationEngine``."""
    from provisa.federation.engine import UnreachableSource
    from provisa.federation.strategy import Strategy, federate

    if _source_type(source) in _NO_REPLICA_TYPES:
        return False
    try:
        strategy = federate(
            source, engine, replicated=table_floor(source, table, promoted=promoted) is not None
        )
    except UnreachableSource:
        return False
    return strategy is Strategy.MATERIALIZED


def has_live_attach(source: Any, engine: Any) -> bool:
    """Whether ``engine`` holds a live attach of ``source`` — a catalog, a view or a foreign table
    a statement could read the source through in place.

    False for a source the operator floors (it has no live attach at all, REQ-1912), for one the
    engine reaches only by replicating it, and for one it cannot reach. Asked by the admin
    discovery reads: with no live attach there is no engine catalog of the source to list, so its
    schemas, tables and columns are listed through the source's own driver. ``engine`` is the
    ``FederationEngine``."""
    from provisa.core.operator_floor import floor_setting
    from provisa.federation.engine import UnreachableSource
    from provisa.federation.strategy import Strategy, federate

    try:
        strategy = federate(source, engine, replicated=floor_setting(source) is not None)
    except UnreachableSource:
        return False
    return strategy is not Strategy.MATERIALIZED


def live_while_building(source: Any, table: Any, engine: Any) -> bool:
    """Whether ``table`` may be read live while its replica is being built (REQ-826): only a
    table that is replicated because it is busy — Default or Hot-N, promoted — on a source the
    engine holds a live attach of. Its promotion is best effort and never fails a read. Never
    for a table the operator floors outright (load_protected, Always: its reads come from the
    replica, and a failed build is an error) or one the engine cannot read in place (there is no
    live read to fall back on)."""
    from provisa.core.replicate import ALWAYS, resolved_load_protected, resolved_replicate

    if not has_live_attach(source, engine) or resolved_load_protected(source, table):
        return False
    return resolved_replicate(source, table) != ALWAYS


class UnknownRegisteredTable(LookupError):
    """A statement names a table that is not registered."""

    def __init__(self, table: str) -> None:
        self.table = table
        super().__init__(
            f"table {table!r} is not a registered table, so it has no name on the engine"
        )


async def registered_table_key(engine: Any, state: Any, table: "TableIdentity") -> TableKey:
    """The catalog-physical name the bound ``engine`` gives the registered ``table`` (its
    identity: source, schema, name) — ``(catalog, schema, table)``, the catalog folded into the
    schema on an engine whose SQL has none — exactly as a lowered statement names it.

    For a caller that holds a registered table rather than a statement (a join-pattern view's
    bound inputs, a Hot candidate): its identity is no engine address, live or replica, so it is
    resolved here first and then read through the address seam (``EngineRuntime.read_address``).
    Refused when no registered table has that identity. ``engine`` is the ``FederationEngine``."""
    from provisa.federation.registry_view import registered_tables

    found = [
        t
        for t in await registered_tables(state)
        if (t.source_id, t.schema_name, t.table_name)
        == (table.source_id, table.schema_name, table.table_name)
    ]
    if not found:
        raise UnknownRegisteredTable(table.label)
    reg = found[0]
    physical = getattr(state, "kafka_table_physical", None) or {}
    name = physical.get(reg.table_name, reg.table_name)
    # The last key is the form the engine executes: three-part, or folded where it has no catalog.
    return engine_table_keys(engine, state.catalog_for(reg.source_id), reg.schema_name, name)[-1]


def _data_columns(reg: dict) -> list[dict]:
    """The registered columns that hold data. A native-filter column is a synthetic query argument
    (a LIMIT, a path parameter), not replicated data; a table made only of those is a function of
    its arguments with nothing to replicate (REQ-1742)."""
    return [c for c in reg["columns"] if c["native_filter_type"] is None]


class _Registry(NamedTuple):
    """One read of the control plane: the registered tables, their sources by id, and where Hot
    replication stands (REQ-826) — ``promoted``: the tables past their threshold; ``serving``:
    those of them whose replica exists in this engine's store, the only ones whose reads go to
    their replica."""

    registered: list[dict]
    sources: dict[str, Any]
    serving: frozenset[tuple[str, str, str]]
    promoted: frozenset[tuple[str, str, str]]


async def _registry(state: Any) -> _Registry:
    """The registered tables, their sources by id and the Hot replication sets, read together
    from the control plane (REQ-1674): the names the compiler emits; a source created in the UI
    and a table registered at runtime count exactly like config-declared ones."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.federation.registry_view import registered_sources
    from provisa.federation.replica_builds import store_identity
    from provisa.federation.replica_state import promotion

    config = getattr(state, "config", None)
    mdb = getattr(state, "model_db", None)
    tdb = getattr(state, "tenant_db", None)
    if config is None or mdb is None or tdb is None:
        return _Registry([], {}, frozenset(), frozenset())
    # REQ-1922: the registry is the model (model store); what is promoted is this region's state.
    async with mdb.acquire() as conn:
        registered = await fetch_tables(conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}
    async with tdb.acquire() as conn:
        promoted, serving = await promotion(conn, lambda: store_identity(state))
    return _Registry(registered, sources, serving, promoted)


def _replica_key(reg: dict) -> tuple[str, str, str]:
    return (reg["source_id"], reg["schema_name"], reg["table_name"])


def _served_from_replica(
    engine: Any, registry: _Registry, busy: frozenset[tuple[str, str, str]]
) -> list[tuple[Any, dict]]:
    """The tables with a replica on ``engine`` by the one decision (``reads_replica``), where the
    tables replicated for being busy are ``busy``: the serving set for what reads go to, the
    promoted set for what has (or is to have) a replica."""
    registered, sources, _serving, _promoted = registry
    out: list[tuple[Any, dict]] = []
    for reg in registered:
        src = sources.get(reg["source_id"])
        if src is None:
            continue
        if not _data_columns(reg):
            continue
        if reads_replica(src, reg, engine, promoted=_replica_key(reg) in busy):
            out.append((src, reg))
    return out


def _floored(registry: _Registry) -> dict[int, tuple[str, str]]:
    registered, sources, serving, _promoted = registry
    floored: dict[int, tuple[str, str]] = {}
    for reg in registered:
        src = sources.get(reg["source_id"])
        if src is None or _source_type(src) in _NO_REPLICA_TYPES:
            continue
        setting = table_floor(src, reg, promoted=_replica_key(reg) in serving)
        if setting is not None:
            floored[reg["id"]] = (src.id, setting)
    return floored


async def replica_tables(engine: Any, state: Any) -> list[tuple[Any, dict]]:
    """Every registered table that has, or is to have, a replica on ``engine``, as
    ``(source, registry row)`` — by the one decision (``reads_replica``). A table promoted for
    being busy is here from its promotion, before its first build completes (REQ-826): this is
    what the replicator builds, keeps and retires by, so a promoted table is built and is not
    retired while its reads still go to the source. Where reads go is ``replica_routes``."""
    registry = await _registry(state)
    return _served_from_replica(engine, registry, registry.promoted)


async def floored_tables(state: Any) -> dict[int, tuple[str, str]]:
    """Every registered table whose reads the operator's settings put on its replica (REQ-030,
    REQ-826), as ``{registered table id: (source_id, setting)}`` — ``setting`` the one a refusal
    names (``table_floor``). Published with the routes and consulted per statement, so a
    statement is floored only by the tables it reads (``registry_view.operator_floor``)."""
    return _floored(await _registry(state))


async def export_view_addresses(state: Any) -> dict[tuple[str, str, str], tuple[str, str]]:
    """The export view of every table served from a replica on ``state``'s bound engine, keyed
    by the table's registered identity ``(source_id, schema_name, table_name)`` and giving the
    ``(schema, view)`` a store that publishes one creates it at (REQ-1912,
    ``EngineBackend.export_view_address``). The same tables, by the same decision, as the
    reconcile publishes views for — so a catalog export addresses exactly what was created."""
    engine = state.federation_engine.engine
    backend = engine.backend
    views: dict[tuple[str, str, str], tuple[str, str]] = {}
    for src, reg in await replica_tables(engine, state):
        address = backend.export_view_address(
            state,
            source_id=src.id,
            schema_name=reg["schema_name"],
            table_name=reg["table_name"],
        )
        views[(src.id, reg["schema_name"], reg["table_name"])] = (address.schema, address.table)
    return views


async def landing_worklist(
    engine: Any, state: Any
) -> list[tuple[Any, str, str, list[tuple[str, str]], list[str]]]:
    """The registered tables whose replica is reconciled, as
    ``(source, schema_name, table_name, columns, pk_columns)`` (REQ-846/932).

    The tables of :func:`replica_tables` whose data columns all have a resolved type. A registered
    data column with no type is a registration gap and the table is skipped (logged), never
    guessed. Shared by every backend's ``reconcile_landed_tables`` so the engines agree on exactly
    which tables are reconciled — they differ only in what they do with each entry."""
    from provisa.core.ir_types import to_ir

    work: list[tuple[Any, str, str, list[tuple[str, str]], list[str]]] = []
    for src, reg in await replica_tables(engine, state):
        data_cols = _data_columns(reg)
        if any(c["data_type"] is None for c in data_cols):
            _log.warning(
                "%s: skip eager reconcile of %s.%s — a registered column has no resolved type",
                engine.name,
                reg["schema_name"],
                reg["table_name"],
            )
            continue
        work.append(
            (
                src,
                reg["schema_name"],
                reg["table_name"],
                [(c["column_name"], to_ir(c["data_type"])) for c in data_cols],
                [c["column_name"] for c in data_cols if c["is_primary_key"]],
            )
        )
    return work


async def replica_routes(state: Any) -> ReplicaRoutes:
    """The published read map for ``state``'s org environment on its bound engine: every table
    served from a replica, keyed by the name a lowered statement gives it, with the address of
    its replica as the engine reads it.

    A name two sources share (possible on an engine with one catalog for every source) is
    recorded as ambiguous: a read of it is refused, never answered from one of the two."""
    engine_rt = state.federation_engine
    engine = engine_rt.engine
    backend = engine.backend
    physical = getattr(state, "kafka_table_physical", None) or {}
    read_catalog: str | None = None
    routes: dict[TableKey, ReplicaRoute] = {}
    ambiguous: dict[TableKey, tuple[str, ...]] = {}
    registry = await _registry(state)
    # Reads go to a replica only once it exists: a promoted table is served when its first
    # build has completed in this engine's store.
    tables = _served_from_replica(engine, registry, registry.serving)
    if tables:
        # The store's catalog dials no source, but naming it may open the store (a native engine
        # attaches it on first use): off the event loop, so a slow store holds no request on this
        # worker (REQ-1882).
        read_catalog = await asyncio.to_thread(backend.replica_read_catalog, state)
    for src, reg in tables:
        name = physical.get(reg["table_name"], reg["table_name"])
        keys = engine_table_keys(engine, state.source_catalogs[src.id], reg["schema_name"], name)
        address = backend.replica_address(
            state,
            source_id=src.id,
            schema_name=reg["schema_name"],
            table_name=reg["table_name"],
        )
        route = ReplicaRoute(
            source_id=src.id,
            table_name=reg["table_name"],
            target=(read_catalog, address.schema, address.table),
        )
        for key in keys:
            if key in ambiguous:
                ambiguous[key] = (*ambiguous[key], src.id)
            elif key in routes and routes[key].source_id != src.id:
                ambiguous[key] = (routes.pop(key).source_id, src.id)
            else:
                routes[key] = route
    # REQ-1922: a table the org keeps in another region is read from its replica there, always —
    # never live, never from a copy here. Whether that replica is built is asked on each read
    # (query_residency.require_home_replica); where it is read is the region's store, which the
    # engine attaches (backend.region_read_address).
    from provisa.federation.replica_converge import builds_here, home_region

    for reg in registry.registered:
        src = registry.sources.get(reg["source_id"])
        if src is None:
            continue
        home = home_region(reg)
        if builds_here(home):
            continue
        assert home is not None  # builds_here is True for a table naming no region
        region = state.foreign_regions[home]  # bound with the org: its selected regions
        name = physical.get(reg["table_name"], reg["table_name"])
        keys = engine_table_keys(engine, state.source_catalogs[src.id], reg["schema_name"], name)
        address = backend.replica_address(
            state, source_id=src.id, schema_name=reg["schema_name"], table_name=reg["table_name"]
        )
        route = ReplicaRoute(
            source_id=src.id,
            table_name=reg["table_name"],
            # Attaching that region's store may dial it: off the event loop (REQ-1882).
            target=await asyncio.to_thread(
                backend.region_read_address, state, region, address.schema, address.table
            ),
        )
        for key in keys:
            if key in ambiguous:
                ambiguous[key] = (*ambiguous[key], src.id)
            elif key in routes and routes[key].source_id != src.id:
                ambiguous[key] = (routes.pop(key).source_id, src.id)
            else:
                routes[key] = route
    return ReplicaRoutes(
        engine_name=engine.name,
        routes=routes,
        ambiguous=ambiguous,
        floored=(floored := _floored(registry)),
        unfloored={
            reg["id"]: reg["source_id"] for reg in registry.registered if reg["id"] not in floored
        },
        promoted=registry.promoted,
        serving=registry.serving,
        # The backend's own record, by reference: a later reconcile is seen without republishing.
        unreconciled=backend.unreconciled,
    )

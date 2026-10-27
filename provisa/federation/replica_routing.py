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

from collections.abc import Callable

import functools
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
#: ``data_profiler`` (REQ-1934): the same as ingest -- its runs append to its result relations in
#: the org's control plane, which a replica pass would drop and recreate.
#:
#: ``soda`` / ``great_expectations`` (REQ-1443): a checker's scans append to its result relation
#: in the org's control plane the same way, so each scan's rows stay beside the earlier ones.
NO_REPLICA_TYPES = frozenset({"ingest", "data_profiler", "soda", "great_expectations"})


def _source_type(source: Any) -> str:
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


def owns_replica(source: Any) -> bool:
    """Whether reads of ``source``'s tables can go through a replica at all: not for a type that
    writes its own rows (``NO_REPLICA_TYPES``), nor for a live-only type (REQ-2000)."""
    from provisa.core.source_registry import is_live_only

    stype = _source_type(source)
    return stype not in NO_REPLICA_TYPES and not is_live_only(stype)


#: The setting a never-expiring table's floor names (REQ-2031).
CACHE_TTL = "cache_ttl"


def resolved_cache_ttl(source: Any, table: Any) -> int | None:
    """The table's ``cache_ttl``: its own, else its source's. ``table`` a model or a registry row."""
    own = table["cache_ttl"] if isinstance(table, dict) else table.cache_ttl
    return own if own is not None else source.cache_ttl


def table_floor(source: Any, table: Any, *, promoted: bool) -> str | None:
    """The operator setting that puts ``table``'s reads on its replica (REQ-030, REQ-826), or
    None when they may be live: the floor of its source (``core.operator_floor.floor_setting`` —
    a floored source has no live attach, so every table of it is served from its replica), else
    a ``cache_ttl`` that never expires (REQ-2031), else the table's own resolved
    ``load_protected`` / ``replicate`` (``core.replicate.floor_of``).
    ``promoted``: the table passed its threshold AND its replica exists in this engine's store
    (``replica_state.promotion``'s serving set) — until then it is read live."""
    from provisa.core.cache_ttl import NEVER_EXPIRES
    from provisa.core.operator_floor import floor_setting
    from provisa.core.replicate import floor_of, resolved_load_protected, resolved_replicate

    of_source = floor_setting(source)
    if of_source is not None:
        return of_source
    if resolved_cache_ttl(source, table) == NEVER_EXPIRES:
        return CACHE_TTL  # REQ-2031: read only from its replica, whatever it says otherwise
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

    if not owns_replica(source):
        return False
    try:
        strategy = federate(
            source, engine, replicated=table_floor(source, table, promoted=promoted) is not None
        )
    except UnreachableSource:
        return False
    return strategy is Strategy.MATERIALIZED


def home_keeps_replica(source: Any, table: Any, region: Any) -> bool:
    """Whether ``region`` — another region of the org (``region_stores.ForeignRegion``), the one
    ``table`` names — keeps a replica of it (REQ-1921, REQ-1922): by the one decision
    (``reads_replica``) on that region's engine. A region whose engine reads the source in place
    keeps none, and a reader elsewhere then reads the source itself, under its own governance
    (RLS on the reader's attributes and region), never asking for a replica that was never meant
    to exist. A table it promotes for being busy is read live meanwhile, so it is judged
    unpromoted."""
    return reads_replica(source, table, _judging_engine(region.engine_kind), promoted=False)


def home_keeps_by_key(source: Any, table: Any, region: Any) -> bool:
    """Whether ``region`` — the other region of the org ``table`` names — keeps ``table``'s rows by
    key (REQ-1865, REQ-1921): it is row-level there, and that region's engine cannot read its
    source in place. Such a region holds only the rows its own reads fetched, so no other region
    reads the table from it."""
    from provisa.federation.strategy import engine_attaches

    return bool(getattr(table, "row_materialize", False)) and not engine_attaches(
        _judging_engine(region.engine_kind), _source_type(source)
    )


@functools.cache
def _judging_engine(kind: str) -> Any:
    """The engine of ``kind``, built once per process to judge another region's replicas by (it
    runs nothing)."""
    from provisa.federation.engine import build_engine

    return build_engine(kind)


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

    from provisa.core.cache_ttl import NEVER_EXPIRES

    if not has_live_attach(source, engine) or resolved_load_protected(source, table):
        return False
    if resolved_cache_ttl(source, table) == NEVER_EXPIRES:
        return False  # REQ-2031: read only from its replica
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
    #: REQ-1939: table id -> (dataset, store schema) of each generated synthetic dataset's table.
    synthetic: dict[int, tuple[str, str]]
    #: REQ-1942: the ids of the sources bound to a synthetic store.
    bound: frozenset[str] = frozenset()
    #: REQ-2031: the tables whose cache_ttl never expires and whose replica exists in this
    #: engine's store. Nothing asks their source for them again.
    kept_for_good: frozenset[tuple[str, str, str]] = frozenset()
    #: REQ-2031: the sources every registered table of which is kept for good.
    kept_sources: frozenset[str] = frozenset()


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
        return _Registry([], {}, frozenset(), frozenset(), {})
    from provisa.synthetic.datasets import generated_tables, synthetic_sources

    # REQ-1922: the registry is the model (model store); what is promoted is this region's state.
    async with mdb.acquire() as conn:
        registered = await fetch_tables(conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}
        synthetic = await generated_tables(conn)
        bound = frozenset(await synthetic_sources(conn))
    from provisa.core.cache_ttl import NEVER_EXPIRES
    from provisa.federation.replica_state import read_all

    never = {
        _replica_key(reg)
        for reg in registered
        if reg["source_id"] in sources
        and resolved_cache_ttl(sources[reg["source_id"]], reg) == NEVER_EXPIRES
    }
    async with tdb.acquire() as conn:
        promoted, serving = await promotion(conn, lambda: store_identity(state))
        kept: frozenset[tuple[str, str, str]] = frozenset()
        if never:  # the store is asked only then: a deployment with none may have no store
            here = store_identity(state)
            kept = frozenset(
                r.key for r in await read_all(conn) if r.key in never and r.exists_in(here)
            )
    by_source: dict[str, set[tuple[str, str, str]]] = {}
    for reg in registered:
        by_source.setdefault(reg["source_id"], set()).add(_replica_key(reg))
    kept_sources = frozenset(sid for sid, keys in by_source.items() if keys <= kept)
    return _Registry(registered, sources, serving, promoted, synthetic, bound, kept, kept_sources)


async def kept_sources(state: Any) -> frozenset[str]:
    """The sources every registered table of which never expires and has landed (REQ-2031):
    nothing contacts them, so they get no engine catalog, no pool and no credential. Read where
    a build of the org's runtime needs it before its routes are published."""
    return (await _registry(state)).kept_sources


def _replica_key(reg: dict) -> tuple[str, str, str]:
    return (reg["source_id"], reg["schema_name"], reg["table_name"])


def _served_from_replica(
    engine: Any, registry: _Registry, busy: frozenset[tuple[str, str, str]]
) -> list[tuple[Any, dict]]:
    """The tables with a replica on ``engine`` by the one decision (``reads_replica``), where the
    tables replicated for being busy are ``busy``: the serving set for what reads go to, the
    promoted set for what has (or is to have) a replica."""
    registered, sources, synthetic = registry.registered, registry.sources, registry.synthetic
    out: list[tuple[Any, dict]] = []
    for reg in registered:
        src = sources.get(reg["source_id"])
        if src is None or reg["id"] in synthetic:  # REQ-1939: served from its synthetic copy
            continue
        if not _data_columns(reg):
            continue
        if reads_replica(src, reg, engine, promoted=_replica_key(reg) in busy):
            out.append((src, reg))
    return out


def _unavailable(registry: _Registry) -> dict[int, str]:
    """REQ-1942: the tables of a source bound to a synthetic store that the store holds no
    generated copy of -- an API table that needs a required parameter, with no declared profile
    -- and why a read of each is refused. Nothing calls the API it was read from."""
    from provisa.synthetic.env_model import unavailable_reason

    return {
        reg["id"]: unavailable_reason(
            reg["table_name"],
            [c["column_name"] for c in reg["columns"] if c["native_filter_type"] is not None],
        )
        for reg in registry.registered
        if reg["source_id"] in registry.bound and reg["id"] not in registry.synthetic
    }


def _floored(registry: _Registry) -> dict[int, tuple[str, str]]:
    registered, sources, synthetic = registry.registered, registry.sources, registry.synthetic
    serving = registry.serving
    floored: dict[int, tuple[str, str]] = {}
    for reg in registered:
        src = sources.get(reg["source_id"])
        if src is None:
            continue
        if reg["id"] in synthetic:
            # REQ-1939: never read live -- its reads are its synthetic copy, through the engine.
            floored[reg["id"]] = (src.id, f"synthetic dataset {synthetic[reg['id']][0]!r}")
            continue
        if not owns_replica(src):
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


async def served_tables(
    state: Any,
) -> tuple[list[dict], dict[str, Any], frozenset[tuple[str, str, str]]]:
    """The registered tables, their sources by id, and the keys of the tables whose reads go to
    their replica now (REQ-2116) -- what ``replica_routes`` routes, read once."""
    registry = await _registry(state)
    engine = state.federation_engine.engine
    served = frozenset(
        _replica_key(reg) for _, reg in _served_from_replica(engine, registry, registry.serving)
    )
    return registry.registered, registry.sources, served


async def draft_replica_tables(engine: Any, state: Any) -> list[tuple[Any, dict]]:
    """Every DRAFT table that has, or may come to have, a replica on ``engine`` (REQ-1921), by
    the same decision as :func:`replica_tables`. A draft is offered to no reader, and it may hold
    a replica: its steward's preview lands it the way a read of a table in service does
    (admin/table_read.py), and it is kept for the table's release. What the replicator keeps
    by, and finds a requested build's table by; it asks for no build of a draft itself."""
    from provisa.api.admin.db_queries import fetch_tables

    registry = await _registry(state)
    if state.model_db is None:
        return []
    async with state.model_db.acquire() as conn:
        drafts = await fetch_tables(conn, draft=True)
    return _served_from_replica(engine, registry._replace(registered=drafts), frozenset())


def routes_with_draft(state: Any, draft: dict, source: Any) -> ReplicaRoutes:
    """The published routes of ``state`` and, where the bound engine reads ``draft`` from a
    replica, the draft's own route (REQ-1921). For the one read its steward's preview is: the
    published routes never hold a draft, so no other statement is addressed at its replica."""
    import dataclasses

    published: ReplicaRoutes = state.replica_routes
    engine = state.federation_engine.engine
    if not reads_replica(source, draft, engine, promoted=False):
        return dataclasses.replace(
            published, unfloored={**published.unfloored, draft["id"]: source.id}
        )
    backend = engine.backend
    physical = getattr(state, "kafka_table_physical", None) or {}
    name = physical.get(draft["table_name"], draft["table_name"])
    address = backend.replica_address(
        state,
        source_id=source.id,
        schema_name=draft["schema_name"],
        table_name=draft["table_name"],
    )
    route = ReplicaRoute(
        source_id=source.id,
        table_name=draft["table_name"],
        target=(backend.replica_read_catalog(state), address.schema, address.table),
        not_landed=_not_landed(source, draft, lambda: backend.dialect),  # REQ-2171
    )
    keys = engine_table_keys(engine, state.source_catalogs[source.id], draft["schema_name"], name)
    setting = table_floor(source, draft, promoted=False)
    floored, unfloored = dict(published.floored), dict(published.unfloored)
    if setting is not None:
        floored[draft["id"]] = (source.id, setting)
    else:
        unfloored[draft["id"]] = source.id
    return dataclasses.replace(
        published,
        engine_name=engine.name,
        routes={**published.routes, **dict.fromkeys(keys, route)},
        floored=floored,
        unfloored=unfloored,
    )


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
) -> list[tuple[Any, str, str, list[tuple[str, str]], list[str], str | None]]:
    """The registered tables whose replica is reconciled, as
    ``(source, schema_name, table_name, columns, pk_columns, landing_filter)`` (REQ-846/932,
    REQ-2062).

    The tables of :func:`replica_tables` whose data columns all have a resolved type. A registered
    data column with no type is a registration gap and the table is skipped (logged), never
    guessed. A table kept row by row (``replica_converge.row_level``) is not here: its row cache,
    at the same address, holds bookkeeping columns of its own, and reconciled to the data columns
    it was re-created, dropping the rows a read had just fetched. Shared by every backend's ``reconcile_landed_tables`` so the engines agree on exactly
    which tables are reconciled — they differ only in what they do with each entry."""
    return _worklist(engine, await replica_tables(engine, state))


async def draft_landing_worklist(
    engine: Any, state: Any
) -> list[tuple[Any, str, str, list[tuple[str, str]], list[str], str | None]]:
    """:func:`landing_worklist` for the DRAFT tables (:func:`draft_replica_tables`, REQ-1921):
    what the definition of a draft's replica is judged by."""
    return _worklist(engine, await draft_replica_tables(engine, state))


def _worklist(
    engine: Any, tables: list[tuple[Any, dict]]
) -> list[tuple[Any, str, str, list[tuple[str, str]], list[str], str | None]]:
    from provisa.core.ir_types import to_ir
    from provisa.federation.replica_converge import row_level

    work: list[tuple[Any, str, str, list[tuple[str, str]], list[str], str | None]] = []
    for src, reg in tables:
        if row_level(src, reg, engine):
            continue
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
                reg["landing_filter"],  # REQ-2062
            )
        )
    return work


def _not_landed(
    source: Any, reg: dict, platform_of: Callable[[], str]
) -> tuple[tuple[str, str, str], ...]:
    """REQ-2171: each column of ``reg`` its replica was built without -- a subtyped column no
    reader of ``source`` has a form to land in: (its name lowercased, subtype, reader)."""
    from types import SimpleNamespace

    from provisa.federation.residency import not_landed

    columns = [
        SimpleNamespace(name=c["column_name"], subtype=c["subtype"])
        for c in reg["columns"]
        if c["subtype"]
    ]
    if not columns:
        return ()
    return tuple(
        (name.lower(), subtype, reader)
        for name, subtype, reader in not_landed(source, columns, platform_of())
    )


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
    if tables or registry.synthetic:
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
            not_landed=_not_landed(src, reg, lambda: backend.dialect),  # REQ-2171
        )
        for key in keys:
            if key in ambiguous:
                ambiguous[key] = (*ambiguous[key], src.id)
            elif key in routes and routes[key].source_id != src.id:
                ambiguous[key] = (routes.pop(key).source_id, src.id)
            else:
                routes[key] = route
    # REQ-1922: a table the org keeps in another region is read from its replica there when that
    # region keeps one — never live, never from a copy here. Whether that replica is built is
    # asked on each read (query_residency.require_home_replica); where it is read is the region's
    # store, which the engine names (backend.region_read_address) and attaches when a read finds
    # it built. A region that keeps none (its engine reads the source in place) leaves the table
    # to be read in place here too (REQ-1921): it has no route.
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
        if not home_keeps_replica(src, reg, region):
            continue
        name = physical.get(reg["table_name"], reg["table_name"])
        keys = engine_table_keys(engine, state.source_catalogs[src.id], reg["schema_name"], name)
        address = backend.replica_address(  # where the home region wrote it (REQ-1922)
            state,
            source_id=src.id,
            schema_name=reg["schema_name"],
            table_name=reg["table_name"],
            region=home,
        )
        route = ReplicaRoute(
            source_id=src.id,
            table_name=reg["table_name"],
            # Attaching that region's store may dial it: off the event loop (REQ-1882).
            target=await asyncio.to_thread(
                backend.region_read_address, state, region, address.schema, address.table
            ),
            not_landed=_not_landed(src, reg, lambda: backend.dialect),  # REQ-2171
        )
        for key in keys:
            if key in ambiguous:
                ambiguous[key] = (*ambiguous[key], src.id)
            elif key in routes and routes[key].source_id != src.id:
                ambiguous[key] = (routes.pop(key).source_id, src.id)
            else:
                routes[key] = route
    # REQ-1939: each table of a generated synthetic dataset reads its copy in the dataset's schema.
    from provisa.federation.replica_address import replica_table_name

    for reg in registry.registered:
        found = registry.synthetic.get(reg["id"])
        src = registry.sources.get(reg["source_id"])
        if found is None or src is None:
            continue
        name = physical.get(reg["table_name"], reg["table_name"])
        target = (
            read_catalog,
            found[1],
            replica_table_name(src.id, reg["schema_name"], reg["table_name"]),
        )
        for key in engine_table_keys(
            engine, state.source_catalogs[src.id], reg["schema_name"], name
        ):
            routes[key] = ReplicaRoute(
                source_id=src.id, table_name=reg["table_name"], target=target
            )
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
        kept_for_good=registry.kept_for_good,
        kept_sources=registry.kept_sources,
        synthetic={tid: found[0] for tid, found in registry.synthetic.items()},
        unavailable=_unavailable(registry),
        # The backend's own record, by reference: a later reconcile is seen without republishing.
        unreconciled=backend.unreconciled,
    )

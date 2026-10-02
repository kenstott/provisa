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

# Requirements: REQ-826, REQ-1912, REQ-1141, REQ-030

from __future__ import annotations

import logging
from typing import Any

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


def _setting_on(source: Any, table: Any, setting: str) -> bool:
    """Whether ``setting`` is on for ``table``: the source's own value, or the table's override
    turning it on (a table value of None inherits the source's, REQ-826/REQ-1141)."""
    own = bool(getattr(source, setting))
    if own:
        return True
    override = table[setting] if isinstance(table, dict) else getattr(table, setting)
    return bool(override)


def floored(source: Any, table: Any) -> bool:
    """Whether the operator requires ``table``'s reads to come from its replica (REQ-030): the
    floor ``core.operator_floor.floor_setting`` names for its source, or a table override."""
    return _setting_on(source, table, "load_protected") or _setting_on(
        source, table, "prefer_materialized"
    )


def reads_replica(source: Any, table: Any, engine: Any) -> bool:
    """Whether reads of ``table`` on ``engine`` go to its replica.

    True when the engine cannot read the source in place (the replica is the only way to reach
    it) or the operator's setting floors the table. False for a table the engine reads live, for
    a source the engine cannot reach at all, and for a source type that owns no replica.
    ``engine`` is the ``FederationEngine``."""
    from provisa.federation.engine import UnreachableSource
    from provisa.federation.strategy import Strategy, federate

    if _source_type(source) in _NO_REPLICA_TYPES:
        return False
    try:
        strategy = federate(source, engine, prefer_materialized=floored(source, table))
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
        strategy = federate(source, engine, prefer_materialized=floor_setting(source) is not None)
    except UnreachableSource:
        return False
    return strategy is not Strategy.MATERIALIZED


def live_while_building(source: Any, table: Any, engine: Any) -> bool:
    """Whether ``table`` may be read live while its replica is being built. Never for a table the
    operator floors (its reads come from the replica, and a failed build is an error) or one the
    engine cannot read in place (there is no live read to fall back on)."""
    del source, table, engine
    return False


class UnknownRegisteredTable(LookupError):
    """A statement names a table that is not registered."""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        super().__init__(
            f"table {table_name!r} is not a registered table, so it has no name on the engine"
        )


class AmbiguousRegisteredTable(LookupError):
    """A statement names a table by a name more than one source registers."""

    def __init__(self, table_name: str, sources: list[str]) -> None:
        self.table_name = table_name
        self.sources = sources
        super().__init__(
            f"table {table_name!r} is registered by more than one source ({', '.join(sources)}), "
            "so the name alone does not say which is meant. Register the tables under different "
            "names, or define the view by SQL that names the source."
        )


async def registered_table_key(engine: Any, state: Any, table_name: str) -> TableKey:
    """The catalog-physical name the bound ``engine`` gives the table registered as
    ``table_name`` — ``(catalog, schema, table)``, the catalog folded into the schema on an
    engine whose SQL has none — exactly as a lowered statement names it.

    For a caller that holds only a registered table's name (a join-pattern materialized view):
    a bare name is no engine address, live or replica, so it is resolved here first and then
    read through the address seam (``EngineRuntime.read_address``). Refused when no registered
    table has that name, and when more than one source registers it. ``engine`` is the
    ``FederationEngine``."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.federation.registry_view import registered_tables

    wanted = {table_name, apply_sql_name(table_name)}
    found = [t for t in await registered_tables(state) if t.table_name in wanted]
    if not found:
        raise UnknownRegisteredTable(table_name)
    if len(found) > 1:
        raise AmbiguousRegisteredTable(table_name, sorted(t.source_id for t in found))
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


async def replica_tables(engine: Any, state: Any) -> list[tuple[Any, dict]]:
    """Every registered table served from a replica on ``engine``, as ``(source, registry row)``.

    Read from the control plane's REGISTERED tables (REQ-1674), which hold the names the compiler
    emits; a source created in the UI and a table registered at runtime count exactly like
    config-declared ones."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.federation.registry_view import registered_sources

    config = getattr(state, "config", None)
    tdb = getattr(state, "tenant_db", None)
    if config is None or tdb is None:
        return []
    async with tdb.acquire() as conn:
        registered = await fetch_tables(conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}  # REQ-1674
    out: list[tuple[Any, dict]] = []
    for reg in registered:
        src = sources.get(reg["source_id"])
        if src is None:
            continue
        if not _data_columns(reg):
            continue
        if reads_replica(src, reg, engine):
            out.append((src, reg))
    return out


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
    tables = await replica_tables(engine, state)
    if tables:
        read_catalog = backend.replica_read_catalog(state)
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
    return ReplicaRoutes(
        engine_name=engine.name,
        routes=routes,
        ambiguous=ambiguous,
        # The backend's own record, by reference: a later reconcile is seen without republishing.
        unreconciled=backend.unreconciled,
    )

# Copyright (c) 2026 Kenneth Stott
# Canary: 47954cc5-bf3c-4a7c-abe2-604009d3e5d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which code tables the federation engine attaches live (REQ-2100, REQ-2267).

A read across sources is the engine's. A code table of a BUILT source (its image on a remote
host, REQ-2098) is read by the engine attaching the host over pgwire as a Postgres catalog, its
parameters reaching the host as ``_nf_`` equalities; a table of a DEV-mode source (its code in the
server's worker) the same way, through the source's pgwire sidecar (REQ-2267 part 3,
provisa/codesource/pgwire_sidecar.py) -- unless it is landed: the source is floored or
replicated, the table is kept for good, or the table declares a landing TTL above 0 for its rows;
the response cache, a request's own opt-in, plays no part. A table on an engine that cannot attach
a code source is landed (it has no attach connector for code sources: a warehouse, or a Postgres
engine sharing no directory with Provisa for the identity it presents,
provisa/codesource/engine_identity.py); so is a built source's table on an engine that attaches
dev-mode sidecars alone, presenting no TLS identity (ClickHouse, ``CodeHost.attaches_built``).
"""

# Requirements: REQ-2100, REQ-2267
from __future__ import annotations

from typing import Any

from provisa.codesource.source import CODE_SOURCE_TYPES


def mapping_of(state: Any, source_id: str) -> dict:
    """A registered source's mapping, from the registry the engine's attach walk reads."""
    runtime = (getattr(state, "runtime_sources", None) or {}).get(source_id)
    if runtime is not None:
        return runtime.get("mapping") or {}
    config = getattr(state, "config", None)
    for source in getattr(config, "sources", None) or []:
        if source.id == source_id:
            return getattr(source, "mapping", None) or {}
    raise KeyError(f"source {source_id!r} is not registered")


def _connector(state: Any, source_type: str) -> Any:
    """The bound engine's attach connector for code sources of ``source_type``, or None."""
    engine = getattr(getattr(state, "federation_engine", None), "engine", None)
    # An opt-in reach (codehost_connectors): a code source asks for it by its mode.
    reaches = getattr(engine, "scan_connectors", {}) if engine is not None else {}
    return reaches.get(source_type)


def engine_attaches(state: Any, source_type: str) -> bool:
    """Whether the bound engine attaches a code source of ``source_type``: it has an attach
    connector for the type (its probe found what the attach needs)."""
    from provisa.federation.connector_base import Mechanism

    connector = _connector(state, source_type)
    return connector is not None and connector.mechanism in (
        Mechanism.ATTACH_R,
        Mechanism.ATTACH_RW,
    )


def attached(state: Any, table: dict) -> bool:
    """Whether the engine reads ``table`` (a ``state.tables`` row) by attaching its host."""
    from provisa.codesource.remote_sources import BUILT, mode_of

    source_type = (getattr(state, "source_types", None) or {}).get(table["source_id"], "")
    if source_type not in CODE_SOURCE_TYPES or not engine_attaches(state, source_type):
        return False
    mapping = mapping_of(state, table["source_id"])
    if mode_of(mapping) != BUILT:
        # REQ-2267: its sidecar serves the code as it is now, every table of it.
        return answered_live(state, table)
    if not _connector(state, source_type).attaches_built:
        # An engine that cannot present the host's TLS identity (ClickHouse) lands a built source.
        return False
    from provisa.codesource.manifest import manifest_from_json
    from provisa.codesource.remote_sources import remote_of
    from provisa.codesource.table import export_of

    # A function its kept manifest does not have is served by no pgwire catalog: it is landed.
    exports = manifest_from_json(remote_of(mapping)["manifest"]).exports
    if not any(e.name == export_of(table) for e in exports):
        return False
    return answered_live(state, table)


def answered_live(state: Any, table: dict) -> bool:
    """Whether a code table's reads are answered from its function, not its landed rows: the
    operator puts it on no replica and it declares no landing TTL above 0."""
    routes = state.replica_routes
    key = (table["source_id"], table["schema_name"], table["table_name"])
    # The operator's settings put the table on its replica (load_protected, replicate, a source
    # floored as a whole), or it never expires and has landed (REQ-2031).
    if table.get("id") in routes.floored or key in routes.kept_for_good:
        return False
    # Only the table's own declared landing TTL decides: the response cache is a request's opt-in
    # (REQ-544), never a reason to land a table.
    declared = table.get("cache_ttl")
    return declared is None or int(declared) <= 0


def parameters(state: Any, table_ids: Any) -> set[str]:
    """The parameter columns (``_nf_{name}``) of the attached tables a statement reads: their
    conditions stay in the statement the engine runs, which hands them to the host."""
    from provisa.core.parameters import NF_PREFIX, is_parameter

    ids = set(table_ids)
    return {
        c["column_name"]
        for t in getattr(state, "tables", None) or []
        if t.get("id") in ids and attached(state, t)
        for c in t["columns"]
        if is_parameter(c) and c["column_name"].startswith(NF_PREFIX)
    }


def _schemas(tables: list, source_ids: Any) -> dict[str, str]:
    """The schemas each of ``source_ids``' tables are registered under, as a comma list, by
    source: what its host or sidecar serves each function in (``provisa.schemas``)."""
    return {
        sid: ",".join(found)
        for sid in source_ids
        if (found := sorted({t["schema_name"] for t in tables if t["source_id"] == sid}))
    }


async def prepare(state: Any) -> None:
    """Write the organisation's remote-host identity, and each built source's host certificate,
    where the bound engine reads them, and tell its attach connectors where they are and which
    schemas each built source's tables are registered under; and name each dev-mode source to
    them with its prepared code and schemas (REQ-2267) (schema build, once ``state.tables`` is
    published)."""
    await _prepare_dev(state)
    await _prepare_built(state)


def _attaching(state: Any, rows: dict, *, built: bool) -> list:
    """The bound engine's attach connectors for the code source types of ``rows``: of built
    sources, only those that attach a built host (``CodeHost.attaches_built``)."""
    from provisa.federation.codehost_connectors import CodeHost

    return [
        c
        for t in sorted({row["type"] for row in rows.values()})
        if isinstance(c := _connector(state, t), CodeHost) and (c.attaches_built or not built)
    ]


async def _prepare_dev(state: Any) -> None:
    """REQ-2267: name each dev-mode source to the bound engine's attach connectors: its code as
    prepared now, whose sidecar the attach starts, and the schemas its tables are registered
    under. A sidecar serving other code than the source's now is stopped first, and the engine's
    catalog of it dropped (pgwire_sidecar.retire_changed); the engine drops its catalog of a
    forgotten source's too (:func:`_drop_on_engine`)."""
    from provisa.codesource import pgwire_sidecar
    from provisa.codesource.remote_sources import BUILT, mode_of
    from provisa.codesource.source import context_for
    from provisa.codesource.source import prepare as prepare_code
    from provisa.core.request_context import require_current_org
    from provisa.federation.codehost_connectors import DevSource

    rows = getattr(state, "runtime_sources", None) or {}
    dev = {
        sid: row
        for sid, row in rows.items()
        if row.get("type") in CODE_SOURCE_TYPES and mode_of(row.get("mapping") or {}) != BUILT
    }
    attaching = _attaching(state, dev, built=False)
    if not attaching:
        return
    pgwire_sidecar.on_stop(f"engine-{id(state)}", lambda org, sid: _drop_on_engine(state, sid))
    org_id = require_current_org()
    ctx = await context_for(state)
    tables = getattr(state, "tables", None) or []
    for connector in attaching:
        source_type = getattr(connector, "source_type")
        mine = {sid: row for sid, row in dev.items() if row["type"] == source_type}
        schemas = _schemas(tables, mine)
        named: dict[str, DevSource] = {}
        for sid in schemas:  # a source with no registered table has nothing to attach
            prepared = await prepare_code(sid, source_type, mine[sid]["mapping"], ctx)
            pgwire_sidecar.retire_changed(prepared)
            named[sid] = DevSource(prepared, schemas[sid])
        connector.remember_dev(org_id, named)
        if connector.files_for == "trino":
            # Trino's catalog of each dev-mode source is created now that it is named.
            _register_on_engine(state, list(named))


def _drop_on_engine(state: Any, source_id: str) -> None:
    """REQ-2267: the engine's catalog of a source whose sidecar stopped for good goes, so its
    next attach reads the catalog of the code as it is then. A source this state has no catalog
    for (deleted, its catalog dropped already) has nothing to drop."""
    catalogs = getattr(state, "source_catalogs", None) or {}
    if source_id not in catalogs:
        return
    state.federation_engine.drop_source(source_id, catalog_name=catalogs[source_id])


async def _prepare_built(state: Any) -> None:
    import dataclasses

    from provisa.codesource import engine_identity
    from provisa.codesource.remote_sources import BUILT, identity, mode_of
    from provisa.core.request_context import require_current_org

    rows = getattr(state, "runtime_sources", None) or {}
    built = {
        sid: row
        for sid, row in rows.items()
        if row.get("type") in CODE_SOURCE_TYPES and mode_of(row.get("mapping") or {}) == BUILT
    }
    attaching = _attaching(state, built, built=True)
    if not attaching:
        return
    org_id = require_current_org()
    me = await identity(org_id)
    tables = getattr(state, "tables", None) or []
    for connector in attaching:
        source_type = getattr(connector, "source_type")
        mine = {sid: row for sid, row in built.items() if row["type"] == source_type}
        # Each host's certificate, pinned when it was paired (remote_sources.connect): a source
        # connected before it was recorded has none, and is attached once connected again.
        roots = {
            sid: certificate
            for sid, row in mine.items()
            if (certificate := ((row.get("mapping") or {}).get("remote") or {}).get("certificate"))
        }
        schemas = _schemas(tables, mine)
        if connector.files_for == "trino":
            written = await engine_identity.for_trino(state, org_id, me, roots)
        elif connector.files_for == "postgres":
            written = engine_identity.for_postgres(org_id, me, roots)
        else:
            written = engine_identity.for_this_process(org_id, me, roots)
        connector.remember(org_id, dataclasses.replace(written, schemas=schemas))
        if connector.files_for == "trino":
            # Trino's catalog of each built source is created now that its identity is written.
            _register_on_engine(state, list(mine))


def _register_on_engine(state: Any, source_ids: list[str]) -> None:
    """Provision each of ``source_ids`` on the bound engine again (its catalog)."""
    config = getattr(state, "config", None)
    for source in getattr(config, "sources", None) or []:
        if source.id in source_ids:
            state.federation_engine.register_source(
                source, "", catalog_name=state.source_catalogs[source.id]
            )

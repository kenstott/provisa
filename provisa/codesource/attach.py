# Copyright (c) 2026 Kenneth Stott
# Canary: 47954cc5-bf3c-4a7c-abe2-604009d3e5d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which code tables the federation engine attaches live (REQ-2100).

A read across sources is the engine's. A code table of a BUILT source (its image on a remote
host, REQ-2098) is read by the engine attaching the host over pgwire as a Postgres catalog, its
parameters reaching the host as ``_nf_`` equalities -- unless it is landed: the source is floored
or replicated, the table is kept for good, or the table declares a landing TTL above 0 for its
rows; the response cache, a request's own opt-in, plays no part. A table of a dev-mode source (its code in the
server's worker) is always landed, and so is one on an engine that cannot attach a host (it has no
attach connector for code sources: a warehouse, or a Postgres engine sharing no directory with
Provisa for the identity it presents, provisa/codesource/engine_identity.py).
"""

# Requirements: REQ-2100
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


def engine_attaches(state: Any, source_type: str) -> bool:
    """Whether the bound engine attaches a built host of ``source_type``: it has an attach
    connector for the type (its probe found what the attach needs)."""
    from provisa.federation.connector_base import Mechanism

    engine = getattr(getattr(state, "federation_engine", None), "engine", None)
    connector = getattr(engine, "connectors", {}).get(source_type) if engine is not None else None
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
        return False
    from provisa.codesource.manifest import manifest_from_json
    from provisa.codesource.remote_sources import remote_of
    from provisa.codesource.table import export_of

    # A row type typed by a preview has no declared columns for the host to serve over pgwire
    # (names.name_tables): its rows are read through Provisa, and landed.
    export = next(
        (
            e
            for e in manifest_from_json(remote_of(mapping)["manifest"]).exports
            if e.name == export_of(table)
        ),
        None,
    )
    if export is None or export.preview:
        return False
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


async def prepare(state: Any) -> None:
    """Write the organisation's remote-host identity, and each built source's host certificate,
    where the bound engine reads them, and tell its attach connectors where they are and which
    schemas each built source's tables are registered under (schema build, once ``state.tables``
    is published)."""
    import dataclasses

    from provisa.codesource import engine_identity
    from provisa.codesource.remote_sources import BUILT, identity, mode_of
    from provisa.core.request_context import require_current_org
    from provisa.federation.codehost_connectors import CodeHost

    engine = getattr(getattr(state, "federation_engine", None), "engine", None)
    connectors = getattr(engine, "connectors", {}) if engine is not None else {}
    rows = getattr(state, "runtime_sources", None) or {}
    built = {
        sid: row
        for sid, row in rows.items()
        if row.get("type") in CODE_SOURCE_TYPES and mode_of(row.get("mapping") or {}) == BUILT
    }
    attaching = [
        c
        for t in sorted({row["type"] for row in built.values()})
        if isinstance(c := connectors.get(t), CodeHost)
    ]
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
        schemas = {
            sid: ",".join(found)
            for sid in mine
            if (found := sorted({t["schema_name"] for t in tables if t["source_id"] == sid}))
        }
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

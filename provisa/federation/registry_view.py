# Copyright (c) 2026 Kenneth Stott
# Canary: a2f607a5-46d3-4bee-a094-3aaaf9ed6483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registered sources and tables a landing path drives off (REQ-1674).

The config file is not the registry. After the seed the model store alone owns the model
(REQ-1919): a source or table is what its control-plane row says — whether a configuration seeded
it, the Sources page created it or Register Table registered it — and one the file declares that
the store does not hold does not exist. These two readers give every landing path the same view:
the control plane's rows.
"""

from __future__ import annotations

from types import SimpleNamespace
from collections.abc import Iterable
from typing import Any

from provisa.core.models import BUILT_IN_SOURCE_IDS, Source


def _source_from_row(row: dict) -> Source:
    """A Source model from a control-plane row: the row's columns that are model fields, set.

    REQ-1695: including its password, which the row carries as the ``password_ref`` reference.
    """
    from provisa.core.repositories.source import source_from_row

    return source_from_row(row)


async def registered_sources(state: Any, conn: Any | None = None) -> list[Source]:  # REQ-1674
    """Every registered source, as its control-plane row holds it — its settings and its password
    reference (REQ-1695, REQ-1919). Built-in sources (provisa-admin, provisa-otel, the
    derived-view source) are never landed and stay out.

    REQ-1892: cached (TTL + schema-generation-keyed) when called on the pool-acquire path
    (``conn`` unset) -- this is read 2-3 times per governed call (`ensure_rows_resident`,
    `ensure_resident`, the replica build) with no cache before this, the same shape of finding
    REQ-1882 already fixed for `registered_tables` in this file. A caller supplying its own
    ``conn`` (already inside an explicit transaction) bypasses the cache, unchanged from before."""
    from provisa.core.repositories import source as source_repo
    from provisa.core.request_context import current_org
    from provisa.federation.registered_sources_cache import get_cache_for

    db = getattr(state, "model_db", None)
    if db is None:
        return []
    if conn is not None:
        rows = await source_repo.list_all(conn)
        merged = _source_rows(rows)
        await _attach_file_glob_tables(merged, conn)
        return merged

    generation = (
        current_org.get(None),
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    rs_cache = get_cache_for(state)
    cached = rs_cache.get(generation)
    if cached is not None:
        return cached
    async with db.acquire() as _conn:
        rows = await source_repo.list_all(_conn)
        out = _source_rows(rows)
        await _attach_file_glob_tables(out, _conn)
    rs_cache.put(generation, out)
    return out


async def _attach_file_glob_tables(sources: list[Source], conn: Any) -> None:
    """REQ-788 (option c): attach each files source's file_glob tables to its Source object, so
    build_model_json can emit them as glob-url tables of the Calcite file adapter without the
    pgwire path needing the registry. A no-op unless a files-type source is present (the tables
    fetch is skipped otherwise)."""
    files = [s for s in sources if getattr(s.type, "value", None) in ("files", "csv", "parquet")]
    if not files:
        return
    from provisa.api.admin.db_queries import fetch_tables

    rows = await fetch_tables(conn)
    by_source: dict[str, list] = {}
    for row in rows:
        glob = row.get("file_glob")
        if glob:
            by_source.setdefault(row["source_id"], []).append(
                {
                    "name": row["table_name"],
                    "file_glob": glob,
                    "source_file_column": row.get("source_file_column"),
                }
            )
    for src in files:
        src.file_glob_tables = by_source.get(src.id, [])


def operator_floor(state: Any, table_ids: Iterable[int]) -> dict[str, str]:  # REQ-030, REQ-826
    """{source_id: operator setting} for the sources a statement may not read live, judged by the
    tables the STATEMENT reads (``table_ids``: the registered tables the pipeline resolved for
    it) — the floor ``decide_route`` enforces (REQ-030, amended 2026-09-30).

    A source is in the map when one of the statement's tables of it is put on its replica by the
    operator's settings (``replica_routing.table_floor``: load_protected, ``replicate: 0``, a
    promoted table, or a source floored as a whole). A statement that reads no such table keeps
    its direct route even when its source has other replicated tables. Read from the floored
    tables published with the replica routes at schema build, the same registry read the
    replicas are reconciled from, so routing and replication never disagree about a table."""
    from provisa.federation.replica_address import ReplicaRoutes

    # Published by the schema build on every runtime; a stand-in (a bare mock state) is refused
    # by name rather than unpacked into a confusing error deep in routing.
    routes = state.replica_routes
    if not isinstance(routes, ReplicaRoutes):
        raise TypeError(f"replica_routes is a {type(routes).__name__}, not ReplicaRoutes")
    from provisa.synthetic.datasets import refuse_mixed_data

    table_ids = list(table_ids)
    refuse_mixed_data(routes, table_ids)  # REQ-1487: never a synthetic table beside a real one
    floored = routes.floored
    floor: dict[str, str] = {}
    for table_id in table_ids:
        entry = floored.get(table_id)
        if entry is not None:
            source_id, setting = entry
            floor.setdefault(source_id, setting)
    return floor


def _source_rows(rows: list[dict]) -> list[Source]:
    """The control-plane rows as Sources, built-in sources left out. Factored out so both the
    cached (pool-acquire) and uncached (caller-supplied ``conn``) paths build identically."""
    return [_source_from_row(row) for row in rows if row["id"] not in BUILT_IN_SOURCE_IDS]


async def registered_tables(state: Any, conn: Any | None = None) -> list[Any]:  # REQ-1674
    """Every registered table, in the shape the landing paths read: the control plane's semantic
    sql name, resolved column types and landing settings (live block, change signal, watermark,
    cadence, probe) — the row's, never the file's (REQ-1919).

    REQ-1882: cached (TTL + schema-generation-keyed) when called on the pool-acquire path
    (``conn`` unset) — this is read on every single governed query's residency/pk-bounds step with
    no cache before this, and was one of the blocking-work sources a live py-spy dump caught
    running in-line on the shared event loop under concurrent load. A caller supplying its own
    ``conn`` (already inside an explicit transaction) bypasses the cache, unchanged from before."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.request_context import current_org
    from provisa.federation.registered_tables_cache import get_cache_for

    db = getattr(state, "model_db", None)
    if db is None:
        return []
    if conn is not None:
        registered = await fetch_tables(conn)
        return _build_registered_tables(registered)

    generation = (
        current_org.get(None),
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    rt_cache = get_cache_for(state)
    cached = rt_cache.get(generation)
    if cached is not None:
        return cached
    async with db.acquire() as _conn:
        registered = await fetch_tables(_conn)
    out = _build_registered_tables(registered)
    rt_cache.put(generation, out)
    return out


def _delta_of(raw: Any) -> Any:
    """Reconstruct a table's DeltaConfig from its stored dict (REQ-874); None when unset. A string
    is JSON (a backend that stores JSONB as text); a dict is used directly."""
    if not raw:
        return None
    import json

    from provisa.core.models import DeltaConfig

    # A JSON column stores Python None as the JSON string "null" (SQLAlchemy JSON type), and the
    # raw-SQL fetch returns JSON as text — so normalize to a dict or treat as "no delta".
    data = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, dict):
        return None
    return DeltaConfig(**data)


def _build_registered_tables(registered: list[dict]) -> list[Any]:
    """The SimpleNamespace-shaping loop `registered_tables` runs over `fetch_tables`' rows,
    factored out so both the cached (pool-acquire) and uncached (caller-supplied ``conn``) paths
    build identically-shaped rows."""
    from provisa.core.models import LiveDeliveryConfig
    from provisa.core.paging import stored_paging

    out: list[Any] = []
    for rt in registered:
        out.append(
            SimpleNamespace(
                id=rt["id"],
                source_id=rt["source_id"],
                schema_name=rt["schema_name"],
                table_name=rt["table_name"],
                columns=[
                    SimpleNamespace(
                        name=c["column_name"],
                        data_type=c["data_type"],
                        is_primary_key=c["is_primary_key"],
                        native_filter_type=c["native_filter_type"],
                    )
                    for c in rt["columns"]
                ],
                live=None if not rt["live"] else LiveDeliveryConfig.model_validate(rt["live"]),
                # REQ-929: the table's own change signal is on its row — saved there by the config
                # load and by the admin alike — so a table registered at runtime (no config entry)
                # is judged by its own signal, not its source's. NULL = it sets none.
                change_signal=rt["change_signal"],
                watermark_column=rt["watermark_column"],
                # REQ-1730: the Cache TTL the table's row holds — saved by the seed and by the
                # admin's TableEditForm alike (REQ-1919).
                cache_ttl=rt["cache_ttl"],
                # REQ-1907: the operator's per-role TTLs live on the registry row (config upsert
                # and the admin mutation both write it there).
                role_ttl=dict(rt["role_ttl"]),
                # REQ-318: how the table is read page by page (provisa.core.paging).
                pagination=stored_paging(rt["pagination"]),
                # REQ-826/REQ-1141 per-table settings (None = inherit the source's): a table they
                # put on its replica is read there even on an attach-capable engine.
                replicate=rt["replicate"],
                load_protected=rt["load_protected"],
                region=rt["region"],  # REQ-1921
                probe_type=rt["probe_type"],  # REQ-982
                # REQ-1443: a checker table's rows are the results of running its contract, so
                # the registered contract rides with the table into make_dq_loader.
                dq_contract=rt["dq_contract"],
                # REQ-1865: fetch_tables already SELECTs this column; it was simply never carried
                # onto the returned object here, so every getattr(t, "row_materialize", False)
                # check downstream (ensure_resident, the replica build,
                # row_materialized_tables_by_name) silently saw False for every table regardless of
                # its real registration -- the row-level cache could never actually engage through
                # the registry view. Confirmed live: a row_materialize=True neo4j table's residency
                # was still being decided entirely by the whole-source path.
                row_materialize=bool(rt.get("row_materialize", False)),
                file_glob=rt.get("file_glob"),  # REQ-788
                source_file_column=rt.get("source_file_column"),  # REQ-788
                delta=_delta_of(rt.get("delta")),  # REQ-874
                # REQ-1865: row_materialized_tables_by_name keys on this (apply_sql_name(t.alias or
                # t.table_name)) to match the SEMANTIC AST a query compiles to -- also never
                # surfaced here before. Dead code until row_materialize (above) actually started
                # returning True; would have raised AttributeError the moment it did.
                alias=rt.get("alias"),
            )
        )
    return out


async def connection_rows(state: Any, source_id: str, table_name: str) -> int:  # REQ-318
    """The most rows one read of ``source_id``'s connection table ``table_name`` takes: the
    table's own ``pagination.max_rows`` where it set one, else ``graphql_remote.max_rows``, the
    operator's default and ceiling (provisa.core.paging). A table the registry does not hold has
    no setting of its own, so the operator's bound applies."""
    from provisa.core.paging import connection_max_rows

    ceiling = state.config.graphql_remote.max_rows
    for table in await registered_tables(state):
        if table.source_id == source_id and table.table_name == table_name:
            return connection_max_rows(table.pagination, ceiling)
    return ceiling

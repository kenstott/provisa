# Copyright (c) 2026 Kenneth Stott
# Canary: e6f7a8b9-c0d1-2345-ef01-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source catalog cache: pre-index table+column metadata for fast NL search (REQ-464).

The cache is populated in the background after source registration.
The search endpoint reads from cache; falls back to live the engine if cache is cold.
"""

# Requirements: REQ-464
# complexity-gate: allow-ble=6 reason="best-effort catalog indexing over a pluggable engine backend: schema listing, table listing and column-cache writes each log and continue on any failure, so one source's metadata-indexing error never aborts source registration or the indexing of other sources"

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from provisa.federation.execution_auth import system_auth

from sqlalchemy import delete as _delete, func, select

from provisa.core.schema_org import source_catalog_cache

log = logging.getLogger(__name__)


async def ensure_table(pool) -> None:
    """Create the source catalog cache table from the portable metadata, in the org's own
    namespace: the one ``read_cache``/``write_cache`` reach through ``pool.acquire()``.

    A raw engine connection carries no org scope. On PostgreSQL the reconciliation scopes it to
    the org schema (``core.db.add_missing_columns``). Elsewhere the connection enters the org's
    namespace the way an acquired one does (``Capabilities.enter_org_sql``: MySQL's database,
    Oracle's schema) and a plane with no schemas at all (SQLite) has the one namespace — naming
    a schema there asks SQLite for a database that is not attached."""
    from sqlalchemy import text

    from provisa.core.db import add_missing_columns

    with pool.engine.begin() as conn:
        if pool.dialect == "postgresql":
            add_missing_columns(conn, [source_catalog_cache], pool.search_path)
            return
        if pool.search_path and (sql := pool.capabilities.enter_org_sql(pool.search_path)):
            conn.execute(text(sql))
        add_missing_columns(conn, [source_catalog_cache])


@dataclass
class CachedTable:
    schema_name: str
    table_name: str
    column_names: list[str]
    comment: str | None


async def read_cache(pool, source_id: str, schema_name: str) -> list[CachedTable] | None:  # REQ-464
    """Return cached tables for source+schema, or None if cache is cold."""
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(source_catalog_cache).where(
                (source_catalog_cache.c.source_id == source_id)
                & (source_catalog_cache.c.schema_name == schema_name)
            )
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
    if not rows:
        return None
    return [
        CachedTable(
            schema_name=r["schema_name"],
            table_name=r["table_name"],
            column_names=list(r["column_names"] or []),
            comment=r["comment"],
        )
        for r in rows
    ]


async def write_cache(
    pool,
    source_id: str,
    schema_name: str,
    tables: list[CachedTable],
) -> None:  # REQ-464
    if not tables:
        return
    async with pool.acquire() as conn, conn.transaction():
        # One transaction so a mid-batch failure rolls the whole write back (matches the original
        # single multi-row upsert's atomicity).
        for t in tables:
            await conn.upsert(
                source_catalog_cache,
                {
                    "source_id": source_id,
                    "schema_name": schema_name,
                    "table_name": t.table_name,
                    # JSON column takes a Python list directly.
                    "column_names": t.column_names,
                    "comment": t.comment,
                    "indexed_at": func.now(),
                },
                index_elements=["source_id", "schema_name", "table_name"],
                update_columns=["column_names", "comment", "indexed_at"],
            )


async def write_columns(
    pool, source_id: str, schema_name: str, columns: dict[str, list[str]]
) -> None:  # REQ-464
    """Record the column names of tables already in the cache; a table not there is left out."""
    from sqlalchemy import update

    async with pool.acquire() as conn, conn.transaction():
        for table_name, names in columns.items():
            await conn.execute_core(
                update(source_catalog_cache)
                .where(
                    (source_catalog_cache.c.source_id == source_id)
                    & (source_catalog_cache.c.schema_name == schema_name)
                    & (source_catalog_cache.c.table_name == table_name)
                )
                .values(column_names=list(names))
            )


async def record_table_columns(
    pool, source_id: str, schema_name: str, table_name: str, names: list[str]
) -> None:  # REQ-464
    """Keep the column names just fetched live for one table (a table selected in the Register
    Table form): written onto its cache row, or as a row of its own when the index has not
    listed the table yet. Its comment, if the index recorded one, is left as it is."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.upsert(
            source_catalog_cache,
            {
                "source_id": source_id,
                "schema_name": schema_name,
                "table_name": table_name,
                "column_names": list(names),
                "indexed_at": func.now(),
            },
            index_elements=["source_id", "schema_name", "table_name"],
            update_columns=["column_names"],
        )


async def invalidate_source(pool, source_id: str) -> None:  # REQ-464
    _new_index_generation(source_id)
    async with pool.acquire() as conn:
        await conn.execute_core(
            _delete(source_catalog_cache).where(source_catalog_cache.c.source_id == source_id)
        )


# -- column names for searching are loaded lazily (REQ-464) -------------------------------------
#
# The index of a source whose tables are listed through its adapter holds table names and
# comments only: listing columns there means a statement per table, hundreds for one source,
# for names most searches never read. A schema's column names are fetched the first time a
# search of it needs them — ONE statement for the whole schema, through the adapter, off the
# request, which answers with what is known — and kept in the cache from then on.

#: What a search answer says of the column names of the schema it searched.
COLUMNS_COMPLETE = "complete"  # every known column name was searched
COLUMNS_LOADING = "loading"  # still being loaded: a match on a column may be missing; ask again
COLUMNS_UNAVAILABLE = "unavailable"  # could not be loaded for this index of the source

#: Where each schema's column fill stands in the current index generation of its source, by
#: (org, source, schema): "filling" while in flight, "filled" once done, "failed" once it failed. A
#: schema is filled at most once per generation; a source still starting is left to a later
#: search. A generation ends when the source is indexed again or invalidated.
_COLUMN_FILLS: dict[tuple[str | None, str, str], str] = {}


def _fill_key(source_id: str, schema_name: str) -> tuple[str | None, str, str]:
    from provisa.core.request_context import current_org

    return (current_org.get(), source_id, schema_name)


def _new_index_generation(source_id: str) -> None:
    org = _fill_key(source_id, "")[0]
    for key in [k for k in _COLUMN_FILLS if k[0] == org and k[1] == source_id]:
        del _COLUMN_FILLS[key]


def _spawn_fill(coro, *, name: str) -> None:
    from provisa.core.connection_loop import spawn_background

    spawn_background(coro, name=name)


def loads_columns_lazily(source_type: str) -> bool:
    """Whether a source's column names are loaded on first search rather than by the index: a
    source listed through its bundled adapter (``pgwire_replica.PGWIRE_REPLICA_TYPES``)."""
    from provisa.federation.pgwire_replica import PGWIRE_REPLICA_TYPES

    return source_type in PGWIRE_REPLICA_TYPES


def request_column_fill(source_id: str, source_type: str, schema_name: str, tables, state) -> bool:
    """Start, off the request, the fill of ``schema_name``'s column names when a search has met
    tables without them; True when a fill was started. Never waits: the search answers with
    what is known."""
    if not loads_columns_lazily(source_type):
        return False
    if all(getattr(t, "column_names", None) or getattr(t, "columns", None) for t in tables):
        return False
    key = _fill_key(source_id, schema_name)
    if key in _COLUMN_FILLS:
        return False
    _COLUMN_FILLS[key] = "filling"
    _spawn_fill(
        _fill_columns(key, source_id, schema_name, state),
        name=f"catalog-columns:{source_id}/{schema_name}",
    )
    return True


def column_names_state(source_id: str, source_type: str, schema_name: str, tables) -> str:
    """What a search of ``schema_name`` can say of its column names, read after
    :func:`request_column_fill`: complete, still loading, or unavailable. Table names are
    complete from the first answer whatever this says."""
    if not loads_columns_lazily(source_type):
        return COLUMNS_COMPLETE
    stands = _COLUMN_FILLS.get(_fill_key(source_id, schema_name))
    if stands == "failed":
        return COLUMNS_UNAVAILABLE
    if stands == "filling":
        return COLUMNS_LOADING
    if all(getattr(t, "column_names", None) or getattr(t, "columns", None) for t in tables):
        return COLUMNS_COMPLETE
    # Filled this generation: what has no column names has none. Not filled (no index of the
    # source yet, or its server still starting): they are still to be loaded.
    return COLUMNS_COMPLETE if stands == "filled" else COLUMNS_LOADING


async def _fill_columns(key, source_id: str, schema_name: str, state) -> None:
    from provisa.api.admin.schema_query import _source_for_introspection
    from provisa.federation import pgwire_replica

    try:
        source = await _source_for_introspection(source_id)
        if source is None:
            raise LookupError(f"source {source_id!r} is not registered")
        columns = await pgwire_replica.schema_columns(source, schema_name)
    except pgwire_replica.SourceStillStartingError:
        _COLUMN_FILLS.pop(key, None)  # later: the next search of the schema asks again
        return
    except (pgwire_replica.ServerNotServing, LookupError) as exc:
        _COLUMN_FILLS[key] = "failed"  # not asked again this generation
        log.warning(
            "catalog_cache: the column names of %r/%r are not loaded: %s",
            source_id,
            schema_name,
            exc,
        )
        return
    await write_columns(state.tenant_db, source_id, schema_name, columns)
    _COLUMN_FILLS[key] = "filled"


#: How often, and for how long, the index asks again for a source whose own server is still
#: starting (``pgwire_replica.SourceStillStartingError``). Past the wait the source is reported
#: as not indexed, once; it is indexed the next time it is saved.
INDEX_STARTING_POLL_SECONDS = 10.0
INDEX_STARTING_WAIT_SECONDS = 900.0

#: The org-vault binding the attach seam runs inside (the source's credential is a reference
#: into the org's vault); resolved on first use.
_seam_bound = None


class _SeamSource:
    """The attach seam of the bound engine for one source: how the Register Table form lists a
    source the engine attaches (``engine.introspect_tables``), used here for the same listing.

    A native engine exposes an attached source as views of its registered tables, never as a
    catalog named after the source, so catalog SQL names a catalog that does not exist. ``None``
    from the seam means the engine has none (a federator), and its catalog SQL is the listing."""

    def __init__(self, engine, source_id: str) -> None:
        self._engine = engine
        self._source_id = source_id
        self._source = None

    async def tables(self, schema: str) -> list[str] | None:
        seam = getattr(self._engine, "introspect_tables", None)
        if seam is None:
            return None
        if self._source is None:
            from provisa.api.admin.schema_query import _source_for_introspection

            self._source = await _source_for_introspection(self._source_id)
            if self._source is None:
                return None
        global _seam_bound
        if _seam_bound is None:
            from provisa.core.secrets_store import bound_to_request_org

            _seam_bound = bound_to_request_org
        async with _seam_bound():
            return await asyncio.to_thread(seam, self._source, schema)


async def index_source(
    source_id: str, pool, engine, source_pools, source_types, state
) -> None:  # REQ-464
    """Background task: walk all schemas+tables for a source and populate cache.

    A source whose own server is still starting is not a failure: the index asks again
    (``INDEX_STARTING_POLL_SECONDS``) until it has started, and reports it once, by name, if it
    has not within ``INDEX_STARTING_WAIT_SECONDS``.
    """
    from provisa.federation.pgwire_replica import ServerNotServing, SourceStillStartingError

    waited = 0.0
    while True:
        try:
            await _index_source_once(source_id, pool, engine, source_pools, source_types, state)
            return
        except SourceStillStartingError:
            if waited >= INDEX_STARTING_WAIT_SECONDS:
                log.warning(
                    "catalog_cache: %r is not indexed: its server was still starting after %ds; "
                    "it is indexed when the source is next saved",
                    source_id,
                    int(waited),
                )
                return
            await asyncio.sleep(INDEX_STARTING_POLL_SECONDS)
            waited += INDEX_STARTING_POLL_SECONDS
        except ServerNotServing as exc:
            log.warning("catalog_cache: %r is not indexed: %s", source_id, exc)
            return


async def _index_source_once(
    source_id: str, pool, engine, source_pools, source_types, state
) -> None:  # REQ-464
    """One walk of all schemas+tables of a source into the cache.

    ``pool`` is the org's model store, which the source is listed through; the cache itself is
    a state table (``core.store_sides``) and is written through the org's state store,
    ``state.tenant_db``.

    Errors are logged and swallowed — cache miss is always safe (live fallback).
    """
    from provisa.api.admin.introspect import (
        native_columns,
        native_schemas,
        native_tables,
        unattached_source,
    )

    source_type = source_types.get(source_id, "")
    _new_index_generation(source_id)  # its schemas' columns are loaded afresh on first search
    # REQ-1912: a source's catalog is listed through the source's own driver. The engine's
    # catalog is asked only for what the driver cannot list, and only for a source the engine
    # holds a live attach of — a floored source has no engine catalog, so no query is sent.
    engine_lists = await unattached_source(state, source_id) is None
    seam = _SeamSource(engine, source_id)
    listed_through_attach: set[str] = set()
    try:
        async with pool.acquire() as config_conn:
            schemas = await native_schemas(source_id, source_type, source_pools, config_conn)
    except Exception as exc:
        log.warning("catalog_cache: schema list failed for %r: %s", source_id, exc)
        schemas = None

    if schemas is None:
        if not engine_lists:
            log.warning(
                "catalog_cache: %r is not indexed: its driver listed no schemas and the engine "
                "holds no live attach of it",
                source_id,
            )
            return
        # the engine fallback for schema list
        catalog = state.catalog_for(source_id)
        try:
            res = await engine.execute_engine(
                f'SELECT schema_name FROM "{catalog}".information_schema.schemata '
                f"ORDER BY schema_name",
                authorization=system_auth("catalog index"),
            )
            schemas = [row[0] for row in res.rows]
        except Exception as exc:
            log.warning("catalog_cache: the engine schema list failed for %r: %s", source_id, exc)
            return

    for schema in schemas:
        try:
            async with pool.acquire() as config_conn:
                tables = await native_tables(
                    source_id, source_type, schema, source_pools, config_conn, state
                )
        except Exception as exc:
            log.warning(
                "catalog_cache: the driver's table list failed for %r/%r: %s",
                source_id,
                schema,
                exc,
                exc_info=True,
            )
            tables = None

        if tables is None:
            if not engine_lists:
                log.warning(
                    "catalog_cache: %r/%r is not indexed: its driver listed no tables and the "
                    "engine holds no live attach of the source",
                    source_id,
                    schema,
                )
                continue
            # A source the engine attaches is listed through the attach seam, as the Register
            # Table form lists it. A server still starting raises here and is retried by
            # index_source.
            attached = await seam.tables(schema)
            if attached is not None:
                listed_through_attach.add(schema)
                tables_with_cols = [
                    CachedTable(schema_name=schema, table_name=t, column_names=[], comment=None)
                    for t in attached
                ]
            else:
                catalog = state.catalog_for(source_id)
                try:
                    res = await engine.execute_engine(
                        f'SELECT table_name FROM "{catalog}".information_schema.tables '
                        f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
                        f"ORDER BY table_name",
                        authorization=system_auth("catalog index"),
                    )
                    table_names = [row[0] for row in res.rows]
                    tables_with_cols = [
                        CachedTable(schema_name=schema, table_name=t, column_names=[], comment=None)
                        for t in table_names
                    ]
                except Exception as exc:
                    log.warning(
                        "catalog_cache: table list failed for %r/%r: %s", source_id, schema, exc
                    )
                    continue
        else:
            tables_with_cols = [
                CachedTable(
                    schema_name=schema,
                    table_name=t.name,
                    column_names=[],
                    comment=t.comment,
                )
                for t in tables
            ]

        # Enrich with column names: the source's own driver first, else the engine's catalog
        # where the engine holds a live attach of the source. Not for a source whose column
        # names are loaded on first search (one statement a schema, not one a table here).
        for cached in [] if loads_columns_lazily(source_type) else tables_with_cols:
            async with pool.acquire() as config_conn:
                native = await native_columns(
                    source_id, source_type, schema, cached.table_name, source_pools, config_conn
                )
            if native is not None:
                cached.column_names = [name for name, _dtype in native]
                continue
            if not engine_lists:
                continue  # neither lists this table's columns: it is indexed by name alone
            if schema in listed_through_attach:
                # No catalog of the engine is named after an attached source, and the seam
                # lists tables, not their columns: such a table is indexed by name alone.
                continue
            catalog = state.catalog_for(source_id)
            try:
                res = await engine.execute_engine(
                    f'SELECT column_name FROM "{catalog}".information_schema.columns '
                    f"WHERE table_schema = '{schema}' AND table_name = '{cached.table_name}' "
                    f"ORDER BY ordinal_position",
                    authorization=system_auth("catalog index"),
                )
                cached.column_names = [row[0] for row in res.rows]
            except Exception as exc:
                raise RuntimeError(
                    f"catalog_cache: column introspection failed for "
                    f"{source_id!r}/{schema!r}/{cached.table_name!r}: {exc}"
                ) from exc

        try:
            await write_cache(state.tenant_db, source_id, schema, tables_with_cols)
            log.debug(
                "catalog_cache: indexed %d tables for %r/%r",
                len(tables_with_cols),
                source_id,
                schema,
            )
        except Exception as exc:
            log.warning("catalog_cache: write failed for %r/%r: %s", source_id, schema, exc)

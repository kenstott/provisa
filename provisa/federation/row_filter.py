# Copyright (c) 2026 Kenneth Stott
# Canary: 9c87f0f3-08b0-4591-8de7-3c2773668df1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row-level table read through a filter that is not a key (REQ-1915).

A table replicated row by row is large, so it is never read whole: a read filters it. When the
filter names the table's key, the keyed path (``query_residency.ensure_rows_resident``) fetches
those rows. Any other filter is served here:

- The filter (``FilterBound.predicate``, canonical PostgreSQL over the table's own columns) is
  recorded in a table beside the replica (``<replica>__filters``) with the time it was fetched.
  While that fetch is within the reader's effective TTL, max(cache_ttl, role_ttl(role))
  (REQ-1907), the filter is served from the replica and the source is not read. Filters are
  matched exactly, as the parameter sets of an API source are.
- Otherwise, under the filter's lock, the filter is pushed to the source as a WHERE. The
  matching rows are read in bounded batches through the source's server-side cursor, each batch is
  admitted by this region's row rule where the platform declares regions (REQ-1922: the rule is
  pushed with the filter where it reads only the table's columns, ``region_rows.fetch_predicate``,
  and ``region_rows.admit_rows`` judges every fetched row: a row the region may not keep is never
  landed), and the batches are
  spooled to a local Arrow file (memory stays bounded, and no store lock is held while the source
  is read).
- Then, in ONE store transaction, the replica's rows the filter selects are deleted (a row that
  stopped matching, or was deleted at the source, goes), the fetched rows are upserted by key,
  and the filter's fetch time is recorded. A reader sees all of the filter's old rows or all of
  its new ones; a fetch cut by the request deadline commits nothing.

This module serves sources that evaluate SQL themselves: a direct-driver source read through its
own pool. Other source types translate the filter into their native form (per-type translation,
REQ-1915) or refuse it by name; until a type's translation is built, a non-key filter on it is
refused by name here.
"""

# Requirements: REQ-1915, REQ-1865, REQ-1907, REQ-1922

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

#: Rows per batch read from the source and written to the spool.
BATCH_ROWS = 50_000


def filter_hash(predicate: str) -> str:
    """The key a filter is recorded and locked under: a digest of its canonical text."""
    return hashlib.sha256(predicate.encode()).hexdigest()[:32]


def _store_dialect(dsn: str) -> str | None:
    """The sqlglot dialect of the store ``dsn`` names."""
    from sqlalchemy import make_url

    from provisa.core.source_registry import SOURCE_TO_DIALECT

    backend = make_url(dsn).get_backend_name()
    return SOURCE_TO_DIALECT.get({"mssql": "sqlserver"}.get(backend, backend))


def source_sql(table: Any, columns: list[str], predicate: str, dialect: str | None) -> str:
    """``SELECT <columns> FROM <schema>.<table> WHERE <predicate>`` in the source's ``dialect``."""
    import sqlglot
    import sqlglot.expressions as exp

    where = sqlglot.parse_one(predicate, read="postgres")
    return (
        exp.select(*[exp.column(c, quoted=True) for c in columns])
        .from_(exp.table_(table.table_name, db=table.schema_name, quoted=True))
        .where(where)
        .sql(dialect=dialect)
    )


def _refuse(table: Any, source: Any, why: str) -> NoReturn:
    from provisa.api.errors import RowLevelFilterNotPushable

    stype = source.type.value if hasattr(source.type, "value") else str(source.type)
    raise RowLevelFilterNotPushable(table.table_name, stype, why)


def _stamped(batch: Any, columns: list[tuple[str, str]], fetched_at: datetime, reap: int) -> Any:
    """``batch`` with the replica's own columns in order (a declared column the source did not
    return lands NULL) and the row cache's stamps: landed at ``fetched_at``, reapable after the
    longest TTL any reader accepts (``reap``)."""
    import pyarrow as pa

    from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT

    n = batch.num_rows
    names = batch.schema.names
    arrays = [batch.column(names.index(c)) if c in names else pa.nulls(n) for c, _ in columns]
    stamp = pa.timestamp("us")
    naive = fetched_at.replace(tzinfo=None)
    arrays += [
        pa.array([naive] * n, stamp),
        pa.array([naive + timedelta(seconds=reap)] * n, stamp),
    ]
    return pa.RecordBatch.from_arrays(
        arrays, names=[c for c, _ in columns] + [_ROW_CACHED_AT, _ROW_EXPIRES_AT]
    )


async def spool_filtered_rows(
    state: Any,
    source: Any,
    table: Any,
    predicate: str,
    columns: list[tuple[str, str]],
    *,
    fetched_at: datetime,
    reap: int,
) -> tuple[str, int]:
    """Read the rows ``predicate`` selects from ``source`` in bounded batches through its
    server-side cursor, admit each batch by the region's row rule, stamp it, and spool it to a
    local Arrow IPC file. Returns the file's path (the caller removes it) and the rows spooled.
    The request's deadline is checked between batches; a fetch it cuts leaves nothing behind."""
    import pyarrow as pa

    from provisa.core import request_deadline
    from provisa.executor.direct import open_direct_stream
    from provisa.executor.drivers.registry import has_driver
    from provisa.federation.region_rows import admit_rows, fetch_predicate
    from provisa.core.ir_arrow import arrow_schema
    from provisa.federation.replica_source import DirectTableSource

    stype = source.type.value if hasattr(source.type, "value") else str(source.type)
    pools = getattr(state, "source_pools", None)
    if not has_driver(stype) or pools is None or not pools.has(source.id):
        _refuse(table, source, "the filter is not pushed to this source type yet")
    assert pools is not None  # narrowed by the refusal above
    if not pools.supports_stream(source.id):
        _refuse(table, source, "its driver has no server-side cursor to read the filter's rows by")
    names = [c for c, _ in columns]
    # REQ-1922: the region's row rule travels with the filter where it can (a rule on this one
    # table's columns), so a row this region may not keep never leaves the source; admit_rows
    # below still judges every fetched row, for a rule no source fetch can carry.
    region = fetch_predicate(state, table)
    pushed = predicate if region is None else f"({predicate}) AND ({region})"
    sql = source_sql(
        table,
        names,
        pushed,
        state.source_dialects.get(source.id) or None,
    )
    reader = DirectTableSource(lambda: open_direct_stream(pools, source.id, sql, []), columns)

    from provisa.core import org_files

    # The org's rows, so in the org's folder (REQ-1992), never the node's temporary directory.
    descriptor, path = tempfile.mkstemp(
        prefix="provisa-row-filter-", suffix=".arrow", dir=org_files.org_tmp()
    )
    os.close(descriptor)
    writer = None
    rows = 0
    try:
        async for batch in reader.batches(BATCH_ROWS):
            request_deadline.check()
            admitted = admit_rows(state, table, pa.Table.from_batches([batch]))
            for part in admitted.to_batches():
                stamped = _stamped(part, columns, fetched_at, reap)
                if writer is None:
                    writer = pa.ipc.new_file(path, stamped.schema)
                writer.write_batch(stamped)
                rows += stamped.num_rows
        if writer is None:
            # No row matched: the file still carries the replica's columns, so the replace that
            # follows clears the filter's rows and records the fetch.
            empty = pa.RecordBatch.from_pylist([], schema=arrow_schema(columns))
            empty = _stamped(empty, columns, fetched_at, reap)
            writer = pa.ipc.new_file(path, empty.schema)
        writer.close()
        writer = None
    except BaseException:
        if writer is not None:
            writer.close()
        os.unlink(path)
        raise
    return path, rows


# -- the record and the replace, on either kind of store -----------------------------------------


def _record_table(schema: str, table: str, dialect: str | None) -> Any:
    from sqlalchemy import Column, DateTime, MetaData, String, Table

    from provisa.federation.store_connection import filter_record_table

    return Table(
        filter_record_table(table),
        MetaData(),
        Column("filter_hash", String(64), primary_key=True),
        Column("filter_sql", String(8000)),
        Column("fetched_at", DateTime(timezone=True)),
        schema=schema or None,
    )


async def _ensure_record_table(conn: Any, schema: str, record: Any) -> None:
    """The filter record beside the replica, in the replica's schema: a row-level table is read
    before anything is landed for it, so the schema may not exist yet."""
    from sqlalchemy.schema import CreateSchema, CreateTable

    if schema and conn.capabilities.schemas:
        await conn.execute_core(CreateSchema(schema, if_not_exists=True))
    await conn.execute_core(CreateTable(record, if_not_exists=True))


async def filter_fetched_at(
    state: Any, backend: Any, engine: Any, schema: str, table: str, digest: str
) -> datetime | None:
    """When the filter ``digest`` was last fetched into the replica ``schema.table`` (UTC), or
    None when it never was."""
    from provisa.federation.query_residency import _duckdb_runtime, _is_duckdb_store

    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()
        at = runtime._store_broker.filter_fetched_at(schema, table, digest)
    else:
        from sqlalchemy import select

        from provisa.federation import store_writer

        dsn = engine.engine.materialize_store()
        record = _record_table(schema, table, _store_dialect(dsn))
        async with store_writer.store_connection(dsn) as conn:
            await _ensure_record_table(conn, schema, record)
            row = (
                await conn.execute_core(
                    select(record.c.fetched_at).where(record.c.filter_hash == digest)
                )
            ).fetchone()
            at = None if row is None else row[0]
    if at is None:
        return None
    return at if at.tzinfo is not None else at.replace(tzinfo=UTC)


async def replace_filtered_rows(
    state: Any,
    backend: Any,
    engine: Any,
    *,
    schema: str,
    table: str,
    cache_table: Any,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    predicate: str,
    spool_path: str,
    fetched_at: datetime,
    reap: int,
) -> int:
    """Delete the replica's rows ``predicate`` selects, upsert the spooled rows, and record the
    filter's fetch, in ONE store transaction. Returns the rows landed."""
    import sqlglot

    from provisa.federation.materialize_exec import _ROW_CACHED_AT, _ROW_EXPIRES_AT
    from provisa.federation.query_residency import _duckdb_runtime, _is_duckdb_store

    digest = filter_hash(predicate)
    if _is_duckdb_store(backend, state):
        runtime = _duckdb_runtime(backend, state)
        runtime.ensure_materialize_attached()
        full = list(columns) + [(_ROW_CACHED_AT, "timestamp"), (_ROW_EXPIRES_AT, "timestamp")]
        return runtime._store_broker.replace_filtered_rows(
            schema,
            table,
            full,
            pk_columns,
            where_sql=sqlglot.transpile(predicate, read="postgres", write="duckdb")[0],
            spool_path=spool_path,
            filter_hash=digest,
            filter_sql=predicate,
            fetched_at=fetched_at.replace(tzinfo=None),
        )

    import pyarrow as pa
    from sqlalchemy import delete, text
    from sqlalchemy.schema import CreateTable

    from provisa.federation import store_writer
    from provisa.federation.materialize_exec import land_rows
    from provisa.federation.replica_guard import require_store_replica_table

    dsn = engine.engine.materialize_store()
    dialect = _store_dialect(dsn)
    record = _record_table(schema, table, dialect)
    where_sql = sqlglot.transpile(predicate, read="postgres", write=dialect)[0]
    stamps = {_ROW_CACHED_AT, _ROW_EXPIRES_AT}
    landed = 0
    async with store_writer.store_connection(dsn) as conn:
        await require_store_replica_table(conn, schema, table, action="write the row-level replica")
        async with conn.transaction():
            await conn.execute_core(CreateTable(cache_table, if_not_exists=True))
            await _ensure_record_table(conn, schema, record)
            await conn.execute_core(delete(cache_table).where(text(where_sql)))
            with pa.memory_map(spool_path) as source:
                reader = pa.ipc.open_file(source)
                for index in range(reader.num_record_batches):
                    rows = [
                        {k: v for k, v in row.items() if k not in stamps}
                        for row in reader.get_batch(index).to_pylist()
                    ]
                    await land_rows(
                        conn, cache_table, pk_columns, rows, resolved_cache_ttl=reap, now=fetched_at
                    )
                    landed += len(rows)
            await conn.execute_core(delete(record).where(record.c.filter_hash == digest))
            await conn.execute_core(
                record.insert().values(
                    filter_hash=digest, filter_sql=predicate, fetched_at=fetched_at
                )
            )
    return landed


# -- the entry point ------------------------------------------------------------------------------


async def ensure_filtered_rows_resident(
    state: Any, bounds: list[Any], *, reader_role: str | None
) -> list[tuple[str, str, int]]:
    """Make each filter of ``bounds`` (``FilterBound``) servable from its row-level replica:
    nothing to do while its last fetch is within the reader's effective TTL, else fetch the rows
    it selects from the source and replace them in the replica. Returns (source_id, table_name,
    rows fetched) per filter."""
    from provisa.events.row_lock import row_lock
    from provisa.federation.query_residency import (
        _ensure_row_cache_table,
        _node,
        _row_cache_ttls,
    )
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.residency import resolve_landing_args

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None or not bounds:
        return []
    sources_by_id = {s.id: s for s in await registered_sources(state)}
    tables_by_name = {t.table_name: t for t in await registered_tables(state)}
    results: list[tuple[str, str, int]] = []
    for bound in bounds:
        table = tables_by_name.get(bound.table_name)
        source = sources_by_id.get(bound.source_id)
        if table is None or source is None or not getattr(table, "row_materialize", False):
            continue
        args = resolve_landing_args(source, table, platform=backend.dialect)
        reader_ttl, reap = _row_cache_ttls(table, source, reader_role)
        address = backend.replica_address(
            state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
        )
        schema, name = address.schema, address.table
        digest = filter_hash(bound.predicate)

        def fresh(at: datetime | None) -> bool:
            # A change feed refreshes the rows already cached, never rows that start to match
            # a filter: a filter's freshness is its TTL alone.
            return at is not None and (datetime.now(UTC) - at).total_seconds() <= reader_ttl

        if fresh(await filter_fetched_at(state, backend, engine, schema, name, digest)):
            results.append((source.id, table.table_name, 0))
            continue
        landed = 0
        async with row_lock(_node(table), ("filter", digest)):
            if fresh(await filter_fetched_at(state, backend, engine, schema, name, digest)):
                results.append((source.id, table.table_name, 0))
                continue
            cache_table = await _ensure_row_cache_table(
                engine, backend, state, schema, name, args.columns
            )
            fetched_at = datetime.now(UTC)
            path, _spooled = await spool_filtered_rows(
                state,
                source,
                table,
                bound.predicate,
                args.columns,
                fetched_at=fetched_at,
                reap=reap,
            )
            try:
                landed = await replace_filtered_rows(
                    state,
                    backend,
                    engine,
                    schema=schema,
                    table=name,
                    cache_table=cache_table,
                    columns=args.columns,
                    pk_columns=list(bound.pk_columns),
                    predicate=bound.predicate,
                    spool_path=path,
                    fetched_at=fetched_at,
                    reap=reap,
                )
            finally:
                os.unlink(path)
        results.append((source.id, table.table_name, landed))
    return results

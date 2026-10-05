# Copyright (c) 2026 Kenneth Stott
# Canary: 2fa04ac4-80f1-4fd4-a965-6acfae9faca4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Real source-row loader for the event-loop landing path (REQ-941/846).

The engine is the reader: a MATERIALIZED source table's current rows are read through the federation
engine's SQL terminal — ``SELECT * FROM`` the source's engine-qualified table — and then landed by
the write face (the engine never writes). This is the same terminal the MV path uses
(``execute_engine``), so a SQL-federatable source needs no bespoke primitive.

Row-oriented API / push / stream sources (openapi, ingest, websocket, rss, grpc_remote, prometheus,
google_sheets) have no engine-scannable table — their current rows come from calling the adapter.
Those are served by injected per-type ``adapter_loaders`` (openapi is wired via
:func:`make_openapi_loader`); a type with no loader raises :class:`UnsupportedSourceFetch` rather
than silently returning nothing, so the boundary is explicit and the caller decides (the boot wiring
lands nothing for that node and logs; it never fabricates an empty snapshot).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import aclosing

from provisa.federation.execution_auth import system_auth

from typing import Any

from provisa.compiler.sql_literals import sql_literal

# Source types whose "current rows" are fetched by calling the adapter, not by an engine SQL scan.
# Everything else (RDBMS, cloud DW, OLAP, data lake, file, and connector-backed NoSQL/streaming/graph)
# is read through the engine terminal. Keep this the exclusion set — the scannable set is open-ended.
_ADAPTER_FETCH_ONLY: frozenset[str] = frozenset(
    {
        "openapi",
        "graphql_remote",
        "grpc_remote",
        "ingest",
        "websocket",
        "rss",
        "prometheus",
        "google_sheets",
        # REQ-1443: a data-quality checker source has no table to scan — its rows ARE the result of
        # running the contract, produced by the checker subprocess. See :func:`make_dq_loader`.
        "soda",
        "great_expectations",
        # REQ-1660: a sqlite file is read by its own connector (:func:`make_sqlite_loader`) and
        # landed in the materialize store like any other fetched source. An engine that attaches
        # the file in place (DuckDB) never materializes it, so the loader is never asked.
        "sqlite",
        # REQ-1730: neo4j/sparql rows are produced by running the registered Cypher/SPARQL query
        # against the source, not scanned from a relation Trino can reach directly — Trino has no
        # connector for either type (trino_connectors.py has no entry).
        "neo4j",
        "sparql",
        # REQ-1730: same gap as neo4j/sparql above — firebird/airport rows are produced by a
        # scratch DuckDB connection ATTACHed via their own community extension
        # (make_firebird_loader/make_airport_loader), not scanned from a relation Trino can reach
        # directly (no Trino connector for either type).
        "firebird",
        "airport",
    }
)


class UnsupportedSourceFetch(Exception):
    """A source type has no engine-scannable table; its adapter row-fetch is not yet wired."""


def _source_type(source: Any) -> str:
    """The source's type as a plain string (accepts an enum member or a bare string)."""
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


AdapterLoader = Any  # Callable[[source, table], Awaitable[list[dict]]] — a per-type row fetcher.
# Callable[[source, table, pk_columns, keys], Awaitable[list[dict]]] — a per-type KEYED row fetcher
# (REQ-1865). Distinct from AdapterLoader because a keyed fetch needs the extra pk_columns/keys
# args and, per design, must resolve its filter against the type's own verified projection
# (query_template's RETURN clause for neo4j/sparql) rather than any engine-facing naming.
AdapterKeyedLoader = Any


async def engine_table_rows(engine: Any, source: Any, table: Any) -> list[dict]:
    """Every row of ``table`` read through the engine terminal at its catalog-physical name — the
    default row fetch for an engine-scannable source."""
    from provisa.compiler.naming import source_to_catalog

    catalog = source_to_catalog(source.id)
    ref = f'"{catalog}"."{table.schema_name}"."{table.table_name}"'
    result = await engine.execute_engine(
        f"SELECT * FROM {ref}", authorization=system_auth("source row load")
    )
    return [dict(zip(result.column_names, row)) for row in result.rows]


def make_floored_direct_loader(state: Any, engine: Any) -> AdapterLoader:  # REQ-030, REQ-1141
    """The row fetch for a direct-driver source type: through the engine when the engine reads
    the source in place and the operator does not floor it, else through the source's own direct
    pool.

    A floored source (``core.operator_floor.floor_setting``) has no live relation on the engine —
    its catalog-physical name IS the landed copy, so reading it through the engine would land the
    replica onto itself. A source the engine cannot attach (Trino as a source on DuckDB) has no
    relation on the engine at all. The land is the one sanctioned pull from the source, so it reads
    the source directly, on the refresh policy the operator set; queries never do."""
    import sqlglot.expressions as exp

    from provisa.core.operator_floor import floor_setting
    from provisa.federation.strategy import engine_attaches

    async def _load(source: Any, table: Any) -> list[dict]:
        if floor_setting(source) is None and engine_attaches(engine, _source_type(source)):
            return await engine_table_rows(engine, source, table)
        dialect = state.source_dialects[source.id] or None
        sql = (
            exp.select("*")
            .from_(exp.table_(table.table_name, db=table.schema_name))
            .sql(dialect=dialect)
        )
        result = await engine.execute_native(state.source_pools, source.id, sql, [])
        return [dict(zip(result.column_names, row)) for row in result.rows]

    return _load


class SourceRowLoader:
    """Reads a MATERIALIZED source table's current rows (REQ-941/846).

    ``engine`` is the engine runtime wrapper (the one exposing ``execute_engine``) — the default
    reader for every SQL-federatable source. ``adapter_loaders`` maps a source type in
    ``_ADAPTER_FETCH_ONLY`` (openapi, ingest, …) to an ``async (source, table) -> list[dict]`` fetcher
    that calls the adapter instead of scanning a table; a type without one raises
    :class:`UnsupportedSourceFetch`. ``keyed_adapter_loaders`` is the ``load_keys`` counterpart —
    present only for the subset of adapter types that can translate a keyed fetch (REQ-1865); a
    type with no entry there still raises even if ``adapter_loaders`` has a whole-table one for it.
    ``load`` ignores the claimed events and returns a full snapshot; an incremental
    (watermark-filtered) read is a later refinement keyed off the change cursor."""

    def __init__(
        self,
        engine: Any,
        adapter_loaders: dict[str, AdapterLoader] | None = None,
        keyed_adapter_loaders: dict[str, AdapterKeyedLoader] | None = None,
        keyed_arrow_loaders: dict[str, AdapterKeyedLoader] | None = None,
    ) -> None:
        self._engine = engine
        self._adapter_loaders = adapter_loaders or {}
        self._keyed_adapter_loaders = keyed_adapter_loaders or {}
        self._keyed_arrow_loaders = keyed_arrow_loaders or {}

    async def load(self, source: Any, table: Any) -> list[dict]:
        # REQ-861: a file source may carry a producer command that refreshes the file IN PLACE.
        # The loader is invoked only after the REQ-860 gate reports stale (plan.prep is built from
        # sources needing a residency refresh), so this IS the on-stale point — run the producer
        # BEFORE the read so the freshened file is what gets scanned. Non-zero exit fails loud.
        from provisa.freshness.producer import has_producer, run_producer

        if has_producer(source):
            await run_producer(source)
        stype = _source_type(source)
        # A registered adapter loader always wins: it is the type's OWN row-fetch (openapi call,
        # connector pgwire replica SELECT, REQ-954), used in preference to the engine terminal even
        # for a type the engine could otherwise scan — the wiring registers one only when needed.
        loader = self._adapter_loaders.get(stype)
        if loader is not None:
            return await loader(source, table)
        if stype in _ADAPTER_FETCH_ONLY:
            raise UnsupportedSourceFetch(
                f"source type {stype!r} has no engine-scannable table and no adapter row-fetch "
                f"is wired (source {source.id!r})"
            )
        return await engine_table_rows(self._engine, source, table)

    def replica_source(
        self, state: Any, source: Any, table: Any, columns: list[tuple[str, str]]
    ) -> Any:
        """``table`` as a stream of Arrow record batches for a replica build (REQ-1915): the
        one place each source type's read is chosen, with the same precedence as :meth:`load`.

        - A source the operator floors, whose driver has a server-side cursor, is read through
          that cursor.
        - A type with a registered adapter loader is read by it: through the adapter's own
          cursor or Arrow stream where its client has one, else whole, as a single document.
        - Any other type is read by the engine, which streams Arrow batches; where the engine
          attaches the source in place it is also declared engine-reachable.

        A type with no engine-scannable table and no adapter raises
        :class:`UnsupportedSourceFetch`."""
        import sqlglot.expressions as exp

        from provisa.compiler.naming import source_to_catalog
        from provisa.core.operator_floor import floor_setting
        from provisa.executor.direct import open_direct_stream
        from provisa.federation.replica_source import (
            DirectTableSource,
            DocumentSource,
            EngineTableSource,
        )
        from provisa.federation.strategy import engine_attaches

        stype = _source_type(source)
        pools = getattr(state, "source_pools", None)
        if (
            floor_setting(source) is not None
            and pools is not None
            and pools.supports_stream(source.id)
        ):
            sql = (
                exp.select("*")
                .from_(exp.table_(table.table_name, db=table.schema_name))
                .sql(dialect=state.source_dialects[source.id] or None)
            )
            return DirectTableSource(lambda: open_direct_stream(pools, source.id, sql, []), columns)
        loader = self._adapter_loaders.get(stype)
        if loader is not None:
            # An adapter whose client has a cursor carries its own stream (``replica_source``,
            # set where the adapter is made); one without is read whole, as a single document.
            streamed = getattr(loader, "replica_source", None)
            if streamed is not None:
                return streamed(source, table, columns)
            return DocumentSource(lambda: loader(source, table), columns)
        if stype in _ADAPTER_FETCH_ONLY:
            raise UnsupportedSourceFetch(
                f"source type {stype!r} has no engine-scannable table and no adapter row-fetch "
                f"is wired (source {source.id!r})"
            )
        ref = f'"{source_to_catalog(source.id)}"."{table.schema_name}"."{table.table_name}"'
        return EngineTableSource(self._engine, ref, in_place=engine_attaches(self._engine, stype))

    async def load_keys(
        self,
        source: Any,
        table: Any,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
        *,
        admit: str | None = None,
    ) -> list[dict]:
        """Fetch exactly the rows whose ``pk_columns`` match one of ``keys``, full row, from the
        live source -- never a scan (REQ-1865, design doc section 3d).

        For an engine-scannable relational/warehouse source this is a bounded
        ``SELECT * FROM <physical> WHERE (pk...) IN (...)`` through the engine terminal, mirroring
        ``load``'s own catalog/ref resolution. A registered ``keyed_adapter_loaders`` entry always
        wins over that default, same precedence as ``load``'s ``adapter_loaders`` -- it is the
        type's own keyed-fetch translation (e.g. neo4j: wrap ``query_template`` with a filter on
        one of its own projected properties, ``provisa/cypher/query_template_filter.py``). A type
        in ``_ADAPTER_FETCH_ONLY`` with no keyed entry has no keyed-fetch translation at all and
        raises rather than falling back to a full ``load()`` per lookup, which would defeat the
        mechanism.

        ``admit`` (REQ-1921/1922): a row predicate, PostgreSQL text over the table's own columns,
        that the rows this fetch may return must satisfy (``region_rows.fetch_predicate``). The
        engine-terminal fetch carries it, so a row it rejects never leaves the source; a type's
        own keyed loader takes no predicate, and its rows are judged after the fetch instead
        (``region_rows.admit_rows``, the guard every fetch passes through either way).
        """
        if not keys:
            return []
        stype = _source_type(source)
        keyed_loader = self._keyed_adapter_loaders.get(stype)
        if keyed_loader is not None:
            return await keyed_loader(source, table, pk_columns, keys)
        if stype in _ADAPTER_FETCH_ONLY:
            raise UnsupportedSourceFetch(
                f"source type {stype!r} (source {source.id!r}) has no keyed-fetch translation "
                "wired for this table (REQ-1865)"
            )
        from provisa.compiler.naming import source_to_catalog

        catalog = source_to_catalog(source.id)
        ref = f'"{catalog}"."{table.schema_name}"."{table.table_name}"'
        where = _pk_in_clause(pk_columns, keys, self._engine.dialect)
        if admit is not None:
            import sqlglot

            rule = sqlglot.parse_one(admit, read="postgres").sql(dialect=self._engine.dialect)
            where = f"({where}) AND ({rule})"
        result = await self._engine.execute_engine(
            f"SELECT * FROM {ref} WHERE {where}", authorization=system_auth("source row load")
        )
        return [dict(zip(result.column_names, row)) for row in result.rows]

    async def load_keys_arrow(
        self,
        source: Any,
        table: Any,
        pk_columns: list[str],
        keys: list[tuple[Any, ...]],
        *,
        admit: str | None = None,
    ) -> Any:
        """``load_keys`` as a ``pyarrow.Table``. A type with a registered ``keyed_arrow_loaders``
        entry fetches columnar end to end (REQ-1865: millions of keyed rows never become Python
        objects); every other type is ``load_keys``'s rows, converted."""
        import pyarrow as pa

        arrow_loader = self._keyed_arrow_loaders.get(_source_type(source))
        if arrow_loader is not None:
            return await arrow_loader(source, table, pk_columns, keys)
        return pa.Table.from_pylist(
            await self.load_keys(source, table, pk_columns, keys, admit=admit)
        )


def _pk_in_clause(pk_columns: list[str], keys: list[tuple[Any, ...]], dialect: str) -> str:
    """A ``col IN (...)`` (single-column PK) or ``(col1, col2) IN ((...), (...))`` (composite PK)
    predicate naming exactly ``keys`` -- never a range, never unbounded. Each key value is a
    literal of ``dialect`` (the engine the statement runs on), by the dialect's one rule."""
    if len(pk_columns) == 1:
        col = pk_columns[0]
        values = ", ".join(sql_literal(k[0], dialect) for k in keys)
        return f'"{col}" IN ({values})'
    cols = ", ".join(f'"{c}"' for c in pk_columns)
    tuples = ", ".join("(" + ", ".join(sql_literal(v, dialect) for v in key) + ")" for key in keys)
    return f"({cols}) IN ({tuples})"


def _whole(source: Any, table: Any, rows: list[dict], cut: Any) -> list[dict]:
    """``rows`` when they are the whole answer. What a land writes is a replica's rows, and a
    replica is never cut: an answer that stopped at the endpoint's ``max_pages`` with more to
    read fails the land by name (``replication.page_limit_reached``), as a build does."""
    if cut is None:
        return rows
    from provisa.api_source.replica_read import PageLimitReached

    raise PageLimitReached(f"{source.id}.{table.table_name}", cut.max_pages, cut.rows)


def make_openapi_loader(state: Any) -> AdapterLoader:
    """Build the openapi adapter row-fetch (REQ-941/846): resolve the table's registered
    ``ApiEndpoint`` and its ``ApiSource`` (base_url + auth) from ``state`` at each read (a schema
    rebuild replaces both maps, and a table registered after startup is in the new ones), call
    the operation with
    its default params, and flatten the response pages into row dicts — the same call_api → flatten
    chain the API-cache path uses. The engine never touches this; the write face lands the result.

    A table with no registered endpoint, or a source with no api-source config, raises
    :class:`UnsupportedSourceFetch` (explicit — never a silent empty snapshot)."""

    def _registered(source: Any, table: Any) -> tuple[Any, Any]:
        endpoint = state.api_endpoints.get((source.id, table.table_name))
        api_source = state.api_sources.get(source.id)
        if endpoint is None or api_source is None:
            raise UnsupportedSourceFetch(
                f"openapi source {source.id!r} table {table.table_name!r}: no registered endpoint "
                f"or api-source config to fetch from"
            )
        return endpoint, api_source

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.api_source.caller import answer_rows, call_api

        endpoint, api_source = _registered(source, table)
        answer = await call_api(
            endpoint,
            dict(endpoint.default_params),
            base_url=api_source.base_url,
            auth=api_source.auth,
        )
        return _whole(source, table, *answer_rows(endpoint, answer))

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        # REQ-1915: a build reads the collection a page or a row at a time; only an answer
        # that has to be understood whole is read as one document in memory.
        from provisa.api_source.replica_read import replica_source
        from provisa.federation.replica_source import DocumentSource

        endpoint, api_source = _registered(source, table)
        streamed = replica_source(
            endpoint, api_source, columns, table=f"{source.id}.{table.table_name}"
        )
        if streamed is not None:
            return streamed
        return DocumentSource(lambda: _load(source, table), columns)

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_neo4j_keyed_loader(state: Any) -> AdapterKeyedLoader:
    """Build the neo4j keyed row-fetch (REQ-1865): wrap the table's registered ``query_template``
    with a ``WHERE <projected_pk_property> IN $keys`` filter (single-column PK only --
    ``provisa/cypher/query_template_filter.py`` raises loud on a composite one) and run it through
    the same ``call_api``/``neo4j_tx`` path ``make_openapi_loader`` already uses for the whole-table
    fetch -- the filter binds to a property the template itself already projects, never a name
    invented by the compiler's own SQL-facing convention (see that module's docstring)."""

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        from provisa.api_source.caller import answer_rows, call_api
        from provisa.cypher.query_template_filter import inject_keys_filter

        endpoint = state.api_endpoints.get((source.id, table.table_name))
        api_source = state.api_sources.get(source.id)
        if endpoint is None or api_source is None:
            raise UnsupportedSourceFetch(
                f"neo4j source {source.id!r} table {table.table_name!r}: no registered endpoint "
                f"or api-source config to fetch from"
            )
        if len(pk_columns) != 1:
            raise UnsupportedSourceFetch(
                f"neo4j source {source.id!r} table {table.table_name!r}: keyed fetch on a "
                f"composite PK {pk_columns!r} is not implemented (REQ-1865)"
            )
        wrapped_template = inject_keys_filter(endpoint.query_template, pk_columns[0])
        wrapped_endpoint = endpoint.model_copy(update={"query_template": wrapped_template})
        answer = await call_api(
            wrapped_endpoint,
            {**endpoint.default_params, "keys": [k[0] for k in keys]},
            base_url=api_source.base_url,
            auth=api_source.auth,
        )
        return _whole(source, table, *answer_rows(wrapped_endpoint, answer))

    return _load


def make_sqlite_loader() -> AdapterLoader:
    """Build the sqlite row-fetch (REQ-1660): the table's registered data columns, read straight
    from the file through the sqlite connector. The engine never sees the file; the rows it reads
    are the landed replica this loader feeds."""
    from provisa.federation import connector_sqlite

    async def _load(source: Any, table: Any) -> list[dict]:
        path = getattr(source, "path", None)
        if not path:
            raise ValueError(f"sqlite source {source.id!r} has no path")
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not columns:
            return []
        select = ", ".join(f'"{c}"' for c in columns)
        return await asyncio.to_thread(
            connector_sqlite.execute_sync, path, f'SELECT {select} FROM "{table.table_name}"'
        )

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource

        path = getattr(source, "path", None)
        if not path:
            raise ValueError(f"sqlite source {source.id!r} has no path")
        select = ", ".join(f'"{name}"' for name, _ in columns)
        sql = f'SELECT {select} FROM "{table.table_name}"'
        return BlockingCursorSource(
            lambda batch_rows: connector_sqlite.iter_row_batches(path, sql, batch_rows), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def _duckdb_extension_scratch_read(connector_details: dict, select_sql: str) -> list[dict]:
    """Read rows through a THROWAWAY in-memory DuckDB connection ATTACHed via a community
    extension's own ``details()`` DDL (REQ-1730) — for a source type DuckDB reaches only by
    extension (firebird, airport) and no other engine reaches at all, this is the only available
    reader: there is no separate Python client library for either wire protocol in this codebase
    (unlike sqlite's stdlib ``sqlite3``). Reusing the connector's own ``details()`` output (rather
    than re-deriving the ATTACH DSN here) means this can never drift from what the live federation
    engine actually runs. One-shot: opened, queried, closed — never shared with the app's own
    engine connection, so this can run regardless of which engine is currently active."""
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        install_from_community = connector_details.get("install_from_community", True)
        extension = connector_details["extension"]
        conn.execute(
            f"INSTALL {extension} FROM community"
            if install_from_community
            else f"INSTALL {extension}"
        )
        conn.execute(f"LOAD {extension}")
        conn.execute(connector_details["attach"])
        cursor = conn.execute(select_sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def _duckdb_extension_scratch_batches(
    connector_details: dict, select_sql: str, batch_rows: int
) -> Any:  # REQ-1915
    """:func:`_duckdb_extension_scratch_read` a batch at a time: the same throwaway connection,
    its cursor fetched ``batch_rows`` rows per step, closed when the iteration ends."""
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        install_from_community = connector_details.get("install_from_community", True)
        extension = connector_details["extension"]
        conn.execute(
            f"INSTALL {extension} FROM community"
            if install_from_community
            else f"INSTALL {extension}"
        )
        conn.execute(f"LOAD {extension}")
        conn.execute(connector_details["attach"])
        cursor = conn.execute(select_sql)
        names = [d[0] for d in cursor.description]
        while True:
            chunk = cursor.fetchmany(batch_rows)
            if not chunk:
                return
            yield [dict(zip(names, row)) for row in chunk]
    finally:
        conn.close()


def make_firebird_loader() -> AdapterLoader:
    """Build the firebird row-fetch (REQ-1730): no engine other than DuckDB reaches firebird at
    all (no Trino/pg connector exists), so landing is the ONLY way any other engine ever sees a
    firebird source's rows — read through a scratch DuckDB connection ATTACHed via the same
    `firebird` community extension DuckDBFirebirdConnector uses at query time."""
    from provisa.core.secrets import resolve_secrets
    from provisa.federation.connector_duckdb import DuckDBFirebirdConnector

    def _plan(source: Any, table: Any, columns: list[str]) -> tuple[dict, str]:
        """The scratch connection's ATTACH details and the SELECT of ``columns``."""
        connector = DuckDBFirebirdConnector()
        # registered_sources() (registry_view.py) returns the PERSISTED password reference
        # (REQ-1695), never plaintext — the live engine's own ATTACH resolves it separately before
        # registering (schema_common.py's _register_source_on_engine), but this scratch read
        # builds its own DSN straight from `source`, so it must resolve here too. Reproduced live:
        # DuckDBFirebirdConnector.details() embeds `source.password` directly in the ATTACH DSN
        # (`firebird://user:{password}@host:port/path`) — unresolved, the ATTACH authenticates
        # with the literal "${secret:...}" string, which fails and lands zero rows with no
        # surfaced error (airport has no such bug: its DSN carries no credential at all).
        resolved_source = (
            source.model_copy(update={"password": resolve_secrets(source.password)})
            if source.password
            else source
        )
        details = connector.details(resolved_source)
        select = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {select} FROM "{details["raw_alias"]}"."{table.schema_name}"."{table.table_name}"'
        details = {
            **details,
            "extension": connector.extension,
            "install_from_community": connector.install_from_community,
        }
        return details, sql

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not columns:
            return []
        details, sql = _plan(source, table, columns)
        return await asyncio.to_thread(_duckdb_extension_scratch_read, details, sql)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource

        details, sql = _plan(source, table, [name for name, _ in columns])
        return BlockingCursorSource(
            lambda batch_rows: _duckdb_extension_scratch_batches(details, sql, batch_rows), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_airport_loader() -> AdapterLoader:
    """Build the airport row-fetch (REQ-1730): same rationale as firebird's own loader above — no
    engine but DuckDB reaches an Arrow Flight (airport) source live, so landing through a scratch
    DuckDB connection ATTACHed via the `airport` extension is the only reader available to any
    other engine."""
    from provisa.federation.connector_duckdb import DuckDBAirportConnector

    def _plan(source: Any, table: Any, columns: list[str]) -> tuple[dict, str]:
        """The scratch connection's ATTACH details and the SELECT of ``columns``."""
        connector = DuckDBAirportConnector()
        details = connector.details(source)
        select = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {select} FROM "{details["raw_alias"]}"."{table.schema_name}"."{table.table_name}"'
        details = {
            **details,
            "extension": connector.extension,
            "install_from_community": connector.install_from_community,
        }
        return details, sql

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not columns:
            return []
        details, sql = _plan(source, table, columns)
        return await asyncio.to_thread(_duckdb_extension_scratch_read, details, sql)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource

        details, sql = _plan(source, table, [name for name, _ in columns])
        return BlockingCursorSource(
            lambda batch_rows: _duckdb_extension_scratch_batches(details, sql, batch_rows), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_pinot_loader() -> AdapterLoader:
    """Build the Pinot row-fetch (REQ-1730): a live SQL query over the broker's own query/sql
    endpoint, the same reader `native_tables`/discover-schema use for the picker. Wired only on an
    engine with no live Pinot connector (see build_adapter_loaders' `engine_attaches` gate) —
    Trino keeps scanning through TrinoPinotConnector."""
    from provisa.pinot.fetch import PinotConnection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        hints = getattr(source, "federation_hints", None) or {}
        conn = PinotConnection.build(source.host, source.port, hints.get("pinot_broker_url"))
        return await asyncio.to_thread(fetch_rows, conn, table.table_name, columns)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_spool import SpooledDocumentSource
        from provisa.pinot.fetch import iter_rows_spooled

        hints = getattr(source, "federation_hints", None) or {}
        conn = PinotConnection.build(source.host, source.port, hints.get("pinot_broker_url"))
        names = [name for name, _ in columns]
        return SpooledDocumentSource(
            lambda spooled: iter_rows_spooled(conn, table.table_name, names, spooled),
            columns,
            table=f"{source.id}.{table.table_name}",
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_pinot_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_pinot_loader` (REQ-1865): a bound
    ``WHERE pk IN (...)`` broker query instead of the whole-table scan."""
    from provisa.pinot.fetch import PinotConnection, fetch_rows_by_keys

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        hints = getattr(source, "federation_hints", None) or {}
        conn = PinotConnection.build(source.host, source.port, hints.get("pinot_broker_url"))
        return await asyncio.to_thread(
            fetch_rows_by_keys, conn, table.table_name, columns, pk_columns, keys
        )

    return _load


def make_druid_loader() -> AdapterLoader:
    """Build the Druid row-fetch (REQ-1730): a live SQL query over the broker's own /druid/v2/sql
    endpoint, the same reader `native_tables`/discover-schema use for the picker. Wired only on an
    engine with no live Druid connector — Trino keeps scanning through TrinoDruidConnector."""
    from provisa.druid.fetch import DruidConnection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        conn = DruidConnection.build(source.host, source.port)
        return await asyncio.to_thread(fetch_rows, conn, table.table_name, columns)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.druid.fetch import iter_rows_spooled
        from provisa.federation.replica_spool import SpooledDocumentSource

        conn = DruidConnection.build(source.host, source.port)
        names = [name for name, _ in columns]
        return SpooledDocumentSource(
            lambda spooled: iter_rows_spooled(conn, table.table_name, names, spooled),
            columns,
            table=f"{source.id}.{table.table_name}",
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_druid_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_druid_loader` (REQ-1865): a bound
    ``WHERE pk IN (...)`` broker query instead of the whole-datasource scan."""
    from provisa.druid.fetch import DruidConnection, fetch_rows_by_keys

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        conn = DruidConnection.build(source.host, source.port)
        return await asyncio.to_thread(
            fetch_rows_by_keys, conn, table.table_name, columns, pk_columns, keys
        )

    return _load


def make_hive_s3_loader() -> AdapterLoader:
    """Build the hive_s3 row-fetch (REQ-1730): a direct S3 Parquet read by Hive's own
    conventional table-directory layout — see provisa.hive.fetch's own module doc for why this is
    a documented narrowing, not a full Hive Metastore reader. Wired only on an engine with no live
    hive_s3 connector — Trino keeps scanning through TrinoHiveS3Connector."""
    from provisa.hive.fetch import HiveS3Connection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        conn = HiveS3Connection.build(getattr(source, "database", None), source.mapping or {})
        return await asyncio.to_thread(
            fetch_rows, conn, table.schema_name, table.table_name, columns
        )

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource
        from provisa.hive.fetch import iter_row_batches

        conn = HiveS3Connection.build(getattr(source, "database", None), source.mapping or {})
        names = [name for name, _ in columns]
        return BlockingCursorSource(
            lambda batch_rows: iter_row_batches(
                conn, table.schema_name, table.table_name, names, batch_rows
            ),
            columns,
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_hive_s3_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_hive_s3_loader` (REQ-1865): a bound
    ``WHERE pk IN (...)`` predicate over the same ``read_parquet`` files, instead of the full scan."""
    from provisa.hive.fetch import HiveS3Connection, fetch_rows_by_keys

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        conn = HiveS3Connection.build(getattr(source, "database", None), source.mapping or {})
        return await asyncio.to_thread(
            fetch_rows_by_keys, conn, table.schema_name, table.table_name, columns, pk_columns, keys
        )

    return _load


def make_elasticsearch_loader() -> AdapterLoader:
    """Build the Elasticsearch row-fetch (REQ-1672): the table's registered data columns, read from
    the index over HTTP (scroll) and landed like any other fetched source. Wired only on an engine
    with no Elasticsearch connector of its own; Trino keeps scanning through its connector."""
    from provisa.core.secrets import resolve_secrets
    from provisa.elasticsearch.fetch import ESConnection, fetch_rows, table_index_and_columns

    async def _load(source: Any, table: Any) -> list[dict]:
        mapping = getattr(source, "mapping", None) or {}
        conn = ESConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9200),
            tls=bool(mapping.get("tls", False)),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []

        def _read() -> list[dict]:
            index, columns = table_index_and_columns(conn, mapping, table.table_name, names)
            return fetch_rows(conn, index, columns)

        return await asyncio.to_thread(_read)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.elasticsearch.fetch import iter_row_batches
        from provisa.federation.replica_source import BlockingCursorSource

        mapping = getattr(source, "mapping", None) or {}
        conn = ESConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9200),
            tls=bool(mapping.get("tls", False)),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [name for name, _ in columns]

        def _batches(batch_rows: int) -> Any:
            index, paths = table_index_and_columns(conn, mapping, table.table_name, names)
            return iter_row_batches(conn, index, paths, batch_rows)

        return BlockingCursorSource(_batches, columns)

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_elasticsearch_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_elasticsearch_loader` (REQ-1865): a bound
    ``terms``/``bool`` query instead of the whole-index scroll."""
    from provisa.core.secrets import resolve_secrets
    from provisa.elasticsearch.fetch import (
        ESConnection,
        fetch_rows_by_keys,
        table_index_and_columns,
    )

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        mapping = getattr(source, "mapping", None) or {}
        conn = ESConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9200),
            tls=bool(mapping.get("tls", False)),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []

        def _read() -> list[dict]:
            index, columns = table_index_and_columns(conn, mapping, table.table_name, names)
            return fetch_rows_by_keys(conn, index, columns, pk_columns, keys)

        return await asyncio.to_thread(_read)

    return _load


def make_redis_loader() -> AdapterLoader:
    """Build the Redis row-fetch (REQ-1675): the table's keys (its prefix, or the mapping DSL's
    pattern) read as rows over redis-py and landed like any other fetched source. Wired only on an
    engine with no live Redis connector of its own; Trino keeps scanning through its connector."""
    from provisa.core.secrets import resolve_secrets
    from provisa.redis.fetch import RedisConnection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        mapping = getattr(source, "mapping", None) or {}
        conn = RedisConnection(
            host=resolve_secrets(getattr(source, "host", "") or "localhost"),
            port=int(getattr(source, "port", 0) or 6379),
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(fetch_rows, conn, mapping, table.table_name, names)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource
        from provisa.redis.fetch import iter_row_batches

        mapping = getattr(source, "mapping", None) or {}
        conn = RedisConnection(
            host=resolve_secrets(getattr(source, "host", "") or "localhost"),
            port=int(getattr(source, "port", 0) or 6379),
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [name for name, _ in columns]
        return BlockingCursorSource(
            lambda batch_rows: iter_row_batches(conn, mapping, table.table_name, names, batch_rows),
            columns,
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_redis_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_redis_loader` (REQ-1865). Redis has no server-
    side ``WHERE``-style predicate over a key-value table's rows, so this filters client-side after
    the same whole-table scan ``fetch_rows`` already runs — correct (never reads the self-
    referential landed replica :func:`make_redis_loader`'s absence used to force), but not a true
    pushdown: still one full key-scan per call, just not a full LAND. Redis tables are small demo/
    ops-key-space scale in every registered use so far; a true single-key ``GET``/``HGETALL`` per
    requested key would need the pattern's own placeholder position (not just its prefix) to
    reconstruct a real key from a bare column value, which the mapping DSL does not expose today."""
    from provisa.core.secrets import resolve_secrets
    from provisa.redis.fetch import RedisConnection, fetch_rows

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        mapping = getattr(source, "mapping", None) or {}
        conn = RedisConnection(
            host=resolve_secrets(getattr(source, "host", "") or "localhost"),
            port=int(getattr(source, "port", 0) or 6379),
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        rows = await asyncio.to_thread(fetch_rows, conn, mapping, table.table_name, names)
        wanted = {key for key in keys}
        return [row for row in rows if tuple(row.get(c) for c in pk_columns) in wanted]

    return _load


def make_clickhouse_loader() -> AdapterLoader:
    """Build the ClickHouse row-fetch (REQ-1730 gap): the table's registered data columns,
    SELECTed from ``<database>.<table>`` over the same ``ClickHouseDriver`` (HTTP via
    clickhouse-connect) the DIRECT route already uses, and landed like any other fetched source.
    Wired only on an engine with no live ClickHouse connector of its own (DuckDB has none — only
    the ClickHouse federation ENGINE itself reads it live).

    Before this loader existed, ClickHouse fell through to ``SourceRowLoader``'s generic
    ``SELECT * FROM {per_source_catalog}...`` engine-scan fallback, the same assumption
    make_mongodb_loader's docstring describes failing for Mongo on every self-only engine. For
    ClickHouse that fallback's catalog reference resolves to the LANDED REPLICA'S OWN address
    (there is no live attach for the fallback to read instead), so a land read back its own
    still-empty replica, inserted zero rows, and reported success — confirmed live (perf-bench
    federated_join: order_events landed with last_refresh_ok=True but the physical replica held
    0 rows against a real 60M-row ClickHouse source)."""
    from provisa.core.secrets import resolve_secrets
    from provisa.executor.drivers.clickhouse import ClickHouseDriver

    async def _load(source: Any, table: Any) -> list[dict]:
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        driver = ClickHouseDriver()
        driver.configure(getattr(source, "federation_hints", None) or {})
        await driver.connect(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 8123),
            getattr(source, "database", None) or table.schema_name,
            getattr(source, "username", None) or "default",
            resolve_secrets(getattr(source, "password", "") or ""),
        )
        try:
            cols = ", ".join(f'"{n}"' for n in names)
            result = await driver.execute(f'SELECT {cols} FROM "{table.table_name}"')
            return [dict(zip(result.column_names, row)) for row in result.rows]
        finally:
            await driver.close()

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import ArrowStreamSource

        cols = ", ".join(f'"{name}"' for name, _ in columns)
        sql = f'SELECT {cols} FROM "{table.table_name}"'

        async def _open() -> Any:
            # A driver of its own for the stream, opened here and closed when the stream ends.
            driver = ClickHouseDriver()
            driver.configure(getattr(source, "federation_hints", None) or {})
            await driver.connect(
                resolve_secrets(getattr(source, "host", "") or "localhost"),
                int(getattr(source, "port", 0) or 8123),
                getattr(source, "database", None) or table.schema_name,
                getattr(source, "username", None) or "default",
                resolve_secrets(getattr(source, "password", "") or ""),
            )
            return (lambda: driver.iter_arrow_batches(sql)), driver.close

        return ArrowStreamSource(_open)

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_clickhouse_keyed_loader() -> AdapterKeyedLoader:
    """Build the ClickHouse keyed row-fetch (REQ-1865): the ``load_keys`` counterpart to
    :func:`make_clickhouse_loader`, needed for a ``row_materialize`` ClickHouse table reached via
    key-pushdown. Without this, ``SourceRowLoader.load_keys``'s generic ``_ADAPTER_FETCH_ONLY``-else
    fallback (below) reads the SAME self-referential landed-replica address ``make_clickhouse_loader``
    was written to stop ``load`` from reading -- confirmed live: with only the whole-table loader
    fixed, opting ``order_events`` into ``row_materialize`` still landed zero rows, because
    ``pushdown_row_materialize`` calls ``load_keys``, not ``load``, and ``load_keys`` had the
    identical unfixed gap. The row dicts are the Arrow fetch's (:func:`make_clickhouse_keyed_arrow_loader`)."""
    fetch = make_clickhouse_keyed_arrow_loader()

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        return (await fetch(source, table, pk_columns, keys)).to_pylist()

    return _load


def make_clickhouse_keyed_arrow_loader() -> AdapterKeyedLoader:
    """The ClickHouse keyed fetch as one ``pyarrow.Table`` (REQ-1865): the table's registered data
    columns for exactly ``keys``, read in ClickHouse's native Arrow format -- no per-row Python
    objects. Confirmed live: large_federated_join's ~3M keyed order_events rows spent ~45s as
    Python rows (result tuples, dicts, a pandas frame) between ClickHouse and the store."""
    import pyarrow as pa

    from provisa.core.secrets import resolve_secrets
    from provisa.executor.drivers.clickhouse import ClickHouseDriver

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> Any:
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not keys or not names:
            return pa.table({n: pa.array([], pa.null()) for n in names})
        driver = ClickHouseDriver()
        driver.configure(getattr(source, "federation_hints", None) or {})
        await driver.connect(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 8123),
            getattr(source, "database", None) or table.schema_name,
            getattr(source, "username", None) or "default",
            resolve_secrets(getattr(source, "password", "") or ""),
        )
        try:
            cols = ", ".join(f'"{n}"' for n in names)
            parts = [
                await driver.execute_arrow(
                    f'SELECT {cols} FROM "{table.table_name}" WHERE {where}', bound
                )
                for where, bound in _pk_in_clauses_within(
                    pk_columns, keys, _CLICKHOUSE_MAX_IN_CLAUSE_CHARS
                )
            ]
            return _clickhouse_arrow_temporals(pa.concat_tables(parts), table.columns)
        finally:
            await driver.close()

    return _load


def _clickhouse_arrow_temporals(data: Any, columns: list[Any]) -> Any:
    """ClickHouse's Arrow output encodes ``DateTime`` as uint32 epoch seconds and ``Date`` as
    uint16 epoch days (no output setting changes this; ``DateTime64`` alone arrives as an Arrow
    timestamp) -- confirmed live: landing order_events.event_ts failed "Unimplemented type for cast
    (UINTEGER -> TIMESTAMP)". A column the registry declares temporal is re-typed to the Arrow
    temporal it encodes: naive UTC ``timestamp[s]``, or ``date32``."""
    import pyarrow as pa

    declared = {c.name: (c.data_type or "").lower() for c in columns}
    for i, field in enumerate(data.schema):
        kind = declared.get(field.name, "")
        if pa.types.is_uint32(field.type) and kind.startswith(("timestamp", "datetime")):
            via, target = pa.int64(), pa.timestamp("s")
        elif pa.types.is_uint16(field.type) and kind == "date":
            via, target = pa.int32(), pa.date32()
        else:
            continue
        data = data.set_column(i, field.name, data.column(i).cast(via).cast(target))
    return data


# ClickHouse rejects any statement longer than its server-side ``max_query_size`` (default 262144
# bytes) with "Max query size exceeded". Confirmed live: large_federated_join's 1..1M order_id key
# set (~7 MB of IN list) failed every keyed fetch. The margin leaves room for the SELECT list.
_CLICKHOUSE_MAX_IN_CLAUSE_CHARS = 200_000


def _bound_in_clause(
    pk_columns: list[str], keys: list[tuple[Any, ...]]
) -> tuple[str, dict[str, Any]]:
    """``_pk_in_clause`` with each key value a ``%(kN)s`` placeholder, and the values by name: the
    driver binds them, escaping each for the source, so no value is written into the text."""
    bound: dict[str, Any] = {}

    def _slot(value: Any) -> str:
        name = f"k{len(bound) + 1}"
        bound[name] = value
        return f"%({name})s"

    if len(pk_columns) == 1:
        values = ", ".join(_slot(k[0]) for k in keys)
        return f'"{pk_columns[0]}" IN ({values})', bound
    cols = ", ".join(f'"{c}"' for c in pk_columns)
    tuples = ", ".join("(" + ", ".join(_slot(v) for v in key) + ")" for key in keys)
    return f"({cols}) IN ({tuples})", bound


def _bound_chars(value: Any) -> int:
    """At most how many characters ``value`` takes once the driver has bound it: a number as
    written; any other value quoted, with every character possibly escaped."""
    if isinstance(value, (bool, int, float)) or value is None:
        return len(str(value)) + 2
    return 2 * len(str(value)) + 2


def _pk_in_clauses_within(
    pk_columns: list[str], keys: list[tuple[Any, ...]], max_chars: int
) -> list[tuple[str, dict[str, Any]]]:
    """Predicates that together name exactly ``keys``, each key once, each rendering to at most
    ``max_chars``. A run of three or more consecutive integers of a single-column key is one
    ``BETWEEN`` -- over integers it names exactly the run's members -- and the rest are
    ``_bound_in_clause`` batches. Confirmed live: large_federated_join's 1..1M order_id keys as IN
    batches were 35 statements of ~200 KB whose parameter-comment scan alone took ~20s; as a
    run they are one statement."""
    clauses: list[tuple[str, dict[str, Any]]] = []
    if len(pk_columns) == 1 and keys and all(type(k[0]) is int for k in keys):
        runs, singles = _integer_runs(sorted({k[0] for k in keys}))
        col = pk_columns[0]
        between = [f'"{col}" BETWEEN {lo} AND {hi}' for lo, hi in runs]
        part: list[str] = []
        size = 0
        for b in between:
            if part and size + len(b) + 4 > max_chars:
                clauses.append(("(" + " OR ".join(part) + ")", {}))
                part, size = [], 0
            part.append(b)
            size += len(b) + 4
        if part:
            clauses.append(("(" + " OR ".join(part) + ")", {}))
        keys = [(v,) for v in singles]
    batch: list[tuple[Any, ...]] = []
    size = 0
    for key in keys:
        key_chars = sum(_bound_chars(v) + 2 for v in key) + 4
        if batch and size + key_chars > max_chars:
            clauses.append(_bound_in_clause(pk_columns, batch))
            batch, size = [], 0
        batch.append(key)
        size += key_chars
    if batch:
        clauses.append(_bound_in_clause(pk_columns, batch))
    return clauses


def _integer_runs(values: list[int]) -> tuple[list[tuple[int, int]], list[int]]:
    """Split sorted distinct ``values`` into runs of three or more consecutive integers
    (``(first, last)``) and the values left over."""
    runs: list[tuple[int, int]] = []
    singles: list[int] = []
    i = 0
    while i < len(values):
        j = i
        while j + 1 < len(values) and values[j + 1] == values[j] + 1:
            j += 1
        if j - i >= 2:
            runs.append((values[i], values[j]))
        else:
            singles.extend(values[i : j + 1])
        i = j + 1
    return runs, singles


def make_mongodb_loader() -> AdapterLoader:
    """Build the MongoDB row-fetch (REQ-1730): the table's registered data columns, projected from
    ``<database>.<collection>`` over pymongo and landed like any other fetched source. Wired only on
    an engine with no live MongoDB connector of its own; Trino keeps scanning through its own
    connector (``TrinoMongoConnector``). Same gap REQ-1672/1675/1676 already closed for
    Elasticsearch/Redis/Cassandra — every self-only warehouse engine (Snowflake, BigQuery,
    Databricks, mssql/Fabric/Synapse) and DuckDB itself had no live MongoDB reach at all, so
    ``SourceRowLoader``'s generic ``SELECT * FROM {per_source_catalog}...`` engine-scan assumption
    (this module's own fallback, below) always failed for mongodb on any of them — unexercised
    until REQ-1730's engine-swap harness first queried a mongodb source under one live."""
    from provisa.core.secrets import resolve_secrets
    from provisa.mongodb.fetch import MongoConnection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        conn = MongoConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 27017),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        database = getattr(source, "database", None) or table.schema_name
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(fetch_rows, conn, database, table.table_name, names)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource
        from provisa.mongodb.fetch import iter_row_batches

        conn = MongoConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 27017),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        database = getattr(source, "database", None) or table.schema_name
        names = [name for name, _ in columns]
        return BlockingCursorSource(
            lambda batch_rows: iter_row_batches(
                conn, database, table.table_name, names, batch_rows
            ),
            columns,
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_mongodb_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_mongodb_loader` (REQ-1865): a bound
    ``$in``/``$or`` filter instead of the whole-collection scan."""
    from provisa.core.secrets import resolve_secrets
    from provisa.mongodb.fetch import MongoConnection, fetch_rows_by_keys

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        conn = MongoConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 27017),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        database = getattr(source, "database", None) or table.schema_name
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(
            fetch_rows_by_keys, conn, database, table.table_name, names, pk_columns, keys
        )

    return _load


def make_kafka_loader() -> AdapterLoader:
    """Build the Kafka row-fetch (REQ-1730): every message currently on the topic, drained via
    aiokafka and landed like any other fetched source. Wired only on an engine with no live Kafka
    connector of its own; Trino keeps scanning through its own connector (``TrinoKafkaConnector`,
    itself only reachable when a Confluent Schema Registry is configured). Same gap
    REQ-1672/1675/1676/(this REQ's own mongodb loader) already closed for
    Elasticsearch/Redis/Cassandra/MongoDB — every self-only warehouse engine and DuckDB itself had
    no live Kafka reach at all. ``kafka.fetch``'s own module doc explains the "current rows of an
    unbounded log" semantics this loader relies on."""
    from provisa.core.secrets import resolve_secrets
    from provisa.kafka.fetch import KafkaConnection, fetch_rows

    async def _load(source: Any, table: Any) -> list[dict]:
        conn = KafkaConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            getattr(source, "port", None),
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await fetch_rows(conn, table.table_name, names)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource
        from provisa.kafka.fetch import iter_row_batches

        conn = KafkaConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            getattr(source, "port", None),
        )
        names = [name for name, _ in columns]
        return CursorSource(
            lambda _batch_rows: iter_row_batches(conn, table.table_name, names), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_kafka_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_kafka_loader` (REQ-1865). A topic has no
    server-side ``WHERE``, and there is no cheaper access pattern than draining it — Kafka's own
    `fetch_rows` docstring calls this "current rows of an unbounded log" for the same reason
    :func:`make_kafka_loader` already has to drain the whole topic every whole-table call. Filters
    client-side after that same drain: correct (never reads the self-referential landed replica the
    absence of a keyed loader used to force), no worse than the whole-table load it replaces."""
    from provisa.core.secrets import resolve_secrets
    from provisa.kafka.fetch import KafkaConnection, fetch_rows

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        conn = KafkaConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            getattr(source, "port", None),
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        rows = await fetch_rows(conn, table.table_name, names)
        wanted = {key for key in keys}
        return [row for row in rows if tuple(row.get(c) for c in pk_columns) in wanted]

    return _load


def make_cassandra_loader() -> AdapterLoader:
    """Build the Cassandra row-fetch (REQ-1676): the table's registered data columns, SELECTed from
    ``<keyspace>.<table>`` over CQL and landed like any other fetched source. Wired only on an engine
    with no live Cassandra connector of its own; Trino keeps scanning through its connector."""
    from provisa.cassandra.fetch import CassandraConnection, fetch_rows
    from provisa.core.secrets import resolve_secrets

    async def _load(source: Any, table: Any) -> list[dict]:
        conn = CassandraConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9042),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(fetch_rows, conn, table.schema_name, table.table_name, names)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.cassandra.fetch import iter_row_batches
        from provisa.federation.replica_source import BlockingCursorSource

        conn = CassandraConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9042),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [name for name, _ in columns]
        return BlockingCursorSource(
            lambda batch_rows: iter_row_batches(
                conn, table.schema_name, table.table_name, names, batch_rows
            ),
            columns,
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_cassandra_keyed_loader() -> AdapterKeyedLoader:
    """The ``load_keys`` counterpart to :func:`make_cassandra_loader` (REQ-1865): a bound
    ``WHERE pk IN %s`` predicate instead of the whole-table page scan."""
    from provisa.cassandra.fetch import CassandraConnection, fetch_rows_by_keys
    from provisa.core.secrets import resolve_secrets

    async def _load(
        source: Any, table: Any, pk_columns: list[str], keys: list[tuple[Any, ...]]
    ) -> list[dict]:
        if not keys:
            return []
        conn = CassandraConnection.build(
            resolve_secrets(getattr(source, "host", "") or "localhost"),
            int(getattr(source, "port", 0) or 9042),
            username=getattr(source, "username", None) or None,
            password=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(
            fetch_rows_by_keys, conn, table.schema_name, table.table_name, names, pk_columns, keys
        )

    return _load


def make_prometheus_loader() -> AdapterLoader:
    """Build the Prometheus row-fetch (REQ-1689): the metric's samples over the table's range, read
    from the HTTP API and landed like any other fetched source. Wired only on an engine with no live
    Prometheus connector of its own; Trino keeps scanning through its connector."""
    from provisa.core.secrets import resolve_secrets
    from provisa.prometheus.fetch import PrometheusConnection, fetch_rows
    from provisa.prometheus.source import endpoint_url

    async def _load(source: Any, table: Any) -> list[dict]:
        mapping = getattr(source, "mapping", None) or {}
        conn = PrometheusConnection.build(
            resolve_secrets(
                endpoint_url(getattr(source, "host", None), getattr(source, "port", None), mapping)
            ),
            token=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not names:
            return []
        return await asyncio.to_thread(fetch_rows, conn, mapping, table.table_name, names)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_spool import SpooledDocumentSource
        from provisa.prometheus.fetch import iter_rows_spooled

        mapping = getattr(source, "mapping", None) or {}
        conn = PrometheusConnection.build(
            resolve_secrets(
                endpoint_url(getattr(source, "host", None), getattr(source, "port", None), mapping)
            ),
            token=resolve_secrets(getattr(source, "password", "") or "") or None,
        )
        names = [name for name, _ in columns]
        return SpooledDocumentSource(
            lambda spooled: iter_rows_spooled(conn, mapping, table.table_name, names, spooled),
            columns,
            table=f"{source.id}.{table.table_name}",
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_rss_loader() -> AdapterLoader:
    """Build the RSS/Atom row-fetch (REQ-342/1741): the feed's current items, read over HTTP and
    landed like any other fetched source.

    Unlike kafka/websocket (REQ-1733, a separate CDC-landing task per table via push_wiring.py),
    rss has no push transport to drain — it is a POLL source, so it goes through this generic
    adapter_loaders seam instead. Before this loader existed, "rss" was in ``_ADAPTER_FETCH_ONLY``
    with no entry in ``build_adapter_loaders``, so every poll of an rss table raised
    ``UnsupportedSourceFetch`` and nothing ever landed — the REQ-1739 form field had a mechanism
    (RSSNotificationProvider, unit-tested) that nothing at boot ever wired to the landing path,
    exactly the gap REQ-1733's own docstring called out for kafka/websocket before that fix.

    A fresh, watermark-less ``RSSNotificationProvider`` is built per call so ``poll_once`` returns
    the FULL current snapshot (matching ``SourceRowLoader.load``'s "ignores claimed events, returns
    a full snapshot" contract) rather than only items new since a prior call's in-memory watermark,
    which a stateless per-call provider could never carry between polls anyway."""
    from provisa.core.secrets import resolve_secrets
    from provisa.subscriptions.rss_provider import RSSNotificationProvider

    def _feed_url(source: Any) -> str:
        hints = getattr(source, "federation_hints", None) or {}
        feed_url = hints.get("feed_url")
        if feed_url:
            return resolve_secrets(feed_url)
        use_ssl = str(hints.get("use_ssl", "true")).lower() == "true"
        scheme = "https" if use_ssl else "http"
        path = getattr(source, "path", None) or "/"
        return f"{scheme}://{resolve_secrets(source.host)}:{source.port}{path}"

    async def _load(source: Any, table: Any) -> list[dict]:
        provider = RSSNotificationProvider(_feed_url(source))
        events = await provider.poll_once(table.table_name)
        return [event.row for event in events]

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_spool import SpooledDocumentSource
        from provisa.subscriptions.rss_provider import iter_rows_spooled

        url = _feed_url(source)
        return SpooledDocumentSource(
            lambda spooled: iter_rows_spooled(url, spooled),
            columns,
            table=f"{source.id}.{table.table_name}",
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load


def make_dq_loader(app_state: Any) -> AdapterLoader:
    """Build the data-quality checker row-fetch (REQ-1443).

    A checker table's "current rows" are one scan's results, so the load RUNS the contract: the
    checker subprocess verifies ``table.dq_contract`` against Provisa's own pgwire endpoint and its
    per-check results are the rows. That makes the poll loop the scan scheduler — a checker table
    refreshes on the same cadence machinery as any other table, with no separate scheduler.

    The endpoint the checker connects back through is ``source.mapping`` (the per-source
    type-specific DSL, REQ-251): host, port, database, user, password. The contract's dataset
    names the pgwire schema/table, resolved against ``app_state.contexts`` (read per load, since
    the property is org-routed) — a contract aimed at a table Provisa does not govern raises
    :class:`~provisa.dq.contract.ContractError` there, not here.
    """

    async def _load(source: Any, table: Any) -> list[dict]:
        import uuid
        from datetime import UTC, datetime

        from provisa.dq.contract import contract_dataset, resolve_contract_target
        from provisa.dq.runner import run_contract

        stype = _source_type(source)
        if not table.dq_contract:
            raise UnsupportedSourceFetch(
                f"{stype} source {source.id!r} table {table.table_name!r} carries no dq_contract; "
                f"a checker table's rows are the results of running one"
            )
        dataset = contract_dataset(table.dq_contract, stype)
        target = resolve_contract_target(dataset, app_state.contexts)
        mapping = source.mapping
        return await run_contract(
            checker=stype,
            contract_text=table.dq_contract,
            connection={
                "host": mapping["host"],
                "port": mapping["port"],
                "database": mapping["database"],
                "user": mapping["user"],
                "password": mapping["password"],
            },
            data_source_name=dataset.split("/")[0],
            scan_id=str(uuid.uuid4()),
            scan_time=datetime.now(UTC),
            target_table=f"{target.schema_name}.{target.table_name}",
        )

    return _load


def make_grpc_remote_loader(grpc_sources: dict[str, Any]) -> AdapterLoader:  # REQ-325, REQ-327
    """Build the grpc_remote adapter row-fetch (REQ-941/846): find the query method the table is
    registered from in ``state.grpc_remote_sources``, call it with no request fields on the
    source's channel for the running loop, and return its rows with the columns the table was
    registered with. A table no query method of the source registers as raises
    :class:`UnsupportedSourceFetch`."""

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.compiler.naming import apply_sql_name
        from provisa.grpc_remote.executor import channel_for, execute_query
        from provisa.grpc_remote.mapper import query_table_name

        reg = grpc_sources.get(source.id) or {}
        namespace = reg.get("namespace", "")
        names = {table.table_name, apply_sql_name(table.table_name)}
        query = next(
            (
                q
                for q in reg.get("queries") or []
                if {query_table_name(namespace, q), apply_sql_name(query_table_name(namespace, q))}
                & names
            ),
            None,
        )
        if query is None:
            raise UnsupportedSourceFetch(
                f"grpc_remote source {source.id!r} table {table.table_name!r}: no query method of "
                "the source registers as it"
            )
        rows = await execute_query(
            channel_for(reg),
            query.full_method_path,
            reg["pb2"],
            query.input_message,
            query.output_message,
            {},
            server_streaming=query.server_streaming,
        )
        registered = [c.name for c in table.columns if not c.name.startswith("_nf_")]
        return [{name: row.get(name) for name in registered} for row in rows]

    return _load


def make_graphql_remote_loader(gql_sources: dict[str, Any], max_rows: int) -> AdapterLoader:
    """Build the graphql_remote adapter row-fetch (REQ-941/846): resolve the table's registration in
    ``state.graphql_remote_sources`` (by ``sql_name``), forward a minimal GraphQL query to the remote
    endpoint via :func:`execute_remote`, and return the rows. Refreshes from the remote source — the
    materialized replica is landed by the write face, not read back from its stale cache.

    ``gql_sources`` maps source_id → a registration dict (``url``, ``auth``, ``tables``); each table
    carries its ``field_name``/``sql_name`` and ``columns`` (a column's ``gql_selection`` overrides
    its name for nested object fields). A table with no matching registration raises
    :class:`UnsupportedSourceFetch`."""

    def _request(source: Any, table: Any) -> dict:
        """The remote call for ``table``: url, auth, field name and column selections."""
        from provisa.compiler.naming import apply_gql_name, apply_sql_name
        from provisa.graphql_remote.executor import NO_POLICY

        normalised = apply_sql_name(table.table_name)
        for reg in gql_sources.values():
            for tbl in reg.get("tables", []):
                if tbl.get("sql_name") in (table.table_name, normalised):
                    cols = tbl.get("columns", [])

                    # The store lands under the semantic sql name; the remote keys the field by its
                    # GraphQL name. Both derive from the naming authority. When they differ, emit a
                    # GraphQL alias ``<sql_name>: <gqlField>`` so the outbound field matches the remote
                    # AND the response comes back keyed by the sql name the store expects; when they
                    # coincide, the bare field. gql_selection (nested object path) still wins.
                    def _selection(c: dict) -> str:
                        if c.get("gql_selection"):
                            return c["gql_selection"]
                        sql_name = apply_sql_name(c["name"])
                        gql_field = apply_gql_name(c["name"])
                        return gql_field if sql_name == gql_field else f"{sql_name}: {gql_field}"

                    col_selections = [_selection(c) for c in cols]
                    return {
                        "url": reg["url"],
                        "auth": reg.get("auth"),
                        "field_name": tbl.get("field_name") or tbl["name"],
                        "columns": col_selections,
                        "rows_path": tbl.get("rows_path"),
                        "error_policy": reg.get("error_policy") or NO_POLICY,
                    }
        raise UnsupportedSourceFetch(
            f"graphql_remote source {source.id!r} table {table.table_name!r}: no matching "
            f"registration in graphql_remote_sources"
        )

    def _pages(source: Any, table: Any) -> Any:
        """A connection table's whole collection, page by page; a read reaching max_rows with
        more to read fails by name (``replication.row_limit_reached``)."""
        from provisa.graphql_remote.executor import whole_connection

        from provisa.core.paging import connection_max_rows

        request = _request(source, table)
        return whole_connection(
            request["url"],
            request["auth"],
            request["field_name"],
            request["columns"],
            request["rows_path"],
            table=f"{source.id}.{table.table_name}",
            # REQ-318: the table's own bound where it set one (registry row), else the operator's.
            max_rows=connection_max_rows(table.pagination, max_rows),
            error_policy=request["error_policy"],
        )

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.graphql_remote.executor import execute_remote

        request = _request(source, table)
        if request["rows_path"]:
            # A land is the whole table: never one cut at max_rows (REQ-1915).
            async with aclosing(_pages(source, table)) as pages:
                return [row async for page in pages for row in page]
        return (await execute_remote(**request, max_rows=max_rows)).rows

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import CursorSource
        from provisa.federation.replica_spool import SpooledDocumentSource
        from provisa.graphql_remote.executor import iter_remote_rows_spooled

        request = _request(source, table)
        if request["rows_path"]:
            # REQ-1923: a connection table is read by cursor, a page per request: the build
            # holds one page at a time, and a read reaching max_rows with more fails by name.
            async def _row_batches(batch_rows: int) -> AsyncIterator[list[dict]]:
                async with aclosing(_pages(source, table)) as pages:
                    async for page in pages:
                        yield page

            return CursorSource(_row_batches, columns)
        return SpooledDocumentSource(
            lambda spooled: iter_remote_rows_spooled(
                request["url"],
                request["auth"],
                request["field_name"],
                request["columns"],
                spooled,
                request["error_policy"],
            ),
            columns,
            table=f"{source.id}.{table.table_name}",
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]

    return _load

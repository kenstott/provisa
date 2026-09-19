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

from typing import Any

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
        # connector for either type (trino_connectors.py has no entry). Missing from this set
        # left TrinoBackend.landing_target mangling their landing name with no engine-side redirect
        # view to expose it back under the registered address (see attach_landed_source's DuckDB
        # counterpart, which DOES create that redirect) — unexercised until a source of either type
        # was ever registered against a DuckDB backend and then queried under Trino.
        "neo4j",
        "sparql",
        # REQ-1730: same gap as neo4j/sparql above — firebird/airport rows are produced by a
        # scratch DuckDB connection ATTACHed via their own community extension
        # (make_firebird_loader/make_airport_loader), not scanned from a relation Trino can reach
        # directly (no Trino connector for either type). Missing from this set left
        # TrinoBackend.landing_target mangling their landing name while the query compiler expects
        # the raw registered address — reproduced live: SCHEMA_NOT_FOUND for the registered schema
        # after an engine-swap replay onto Trino, because the landed table was created under the
        # mangled name instead.
        "firebird",
        "airport",
    }
)


def is_adapter_fetched(source_type: Any) -> bool:
    """Whether a source type's rows are PRODUCED by its adapter rather than scanned from a relation
    the engine can already reach.

    The distinction decides where a landed replica has to live. An engine-scannable source has a
    mirror at its physical address the engine reads, so its landing table is an internal copy and
    can be named anything. An adapter-fetched source has no such mirror — the landed rows ARE the
    only copy, so on an engine that reads the store directly by physical name they must land at that
    name (see ``TrinoBackend.landing_target``). Accepts the enum member or the bare string."""
    stype = source_type.value if hasattr(source_type, "value") else str(source_type)
    return stype in _ADAPTER_FETCH_ONLY


class UnsupportedSourceFetch(Exception):
    """A source type has no engine-scannable table; its adapter row-fetch is not yet wired."""


def _source_type(source: Any) -> str:
    """The source's type as a plain string (accepts an enum member or a bare string)."""
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


AdapterLoader = Any  # Callable[[source, table], Awaitable[list[dict]]] — a per-type row fetcher.


class SourceRowLoader:
    """Reads a MATERIALIZED source table's current rows (REQ-941/846).

    ``engine`` is the engine runtime wrapper (the one exposing ``execute_engine``) — the default
    reader for every SQL-federatable source. ``adapter_loaders`` maps a source type in
    ``_ADAPTER_FETCH_ONLY`` (openapi, ingest, …) to an ``async (source, table) -> list[dict]`` fetcher
    that calls the adapter instead of scanning a table; a type without one raises
    :class:`UnsupportedSourceFetch`. ``load`` ignores the claimed events and returns a full snapshot;
    an incremental (watermark-filtered) read is a later refinement keyed off the change cursor."""

    def __init__(
        self, engine: Any, adapter_loaders: dict[str, AdapterLoader] | None = None
    ) -> None:
        self._engine = engine
        self._adapter_loaders = adapter_loaders or {}

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
        from provisa.compiler.naming import source_to_catalog

        catalog = source_to_catalog(source.id)
        ref = f'"{catalog}"."{table.schema_name}"."{table.table_name}"'
        result = await self._engine.execute_engine(f"SELECT * FROM {ref}")
        return [dict(zip(result.column_names, row)) for row in result.rows]


def make_openapi_loader(
    endpoints_by_table: dict[str, Any], sources_by_id: dict[str, Any]
) -> AdapterLoader:
    """Build the openapi adapter row-fetch (REQ-941/846): resolve the table's registered
    ``ApiEndpoint`` and its ``ApiSource`` (base_url + auth) from live state, call the operation with
    its default params, and flatten the response pages into row dicts — the same call_api → flatten
    chain the API-cache path uses. The engine never touches this; the write face lands the result.

    A table with no registered endpoint, or a source with no api-source config, raises
    :class:`UnsupportedSourceFetch` (explicit — never a silent empty snapshot)."""

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.api_source.caller import call_api
        from provisa.api_source.flattener import flatten_response

        endpoint = endpoints_by_table.get(table.table_name)
        api_source = sources_by_id.get(source.id)
        if endpoint is None or api_source is None:
            raise UnsupportedSourceFetch(
                f"openapi source {source.id!r} table {table.table_name!r}: no registered endpoint "
                f"or api-source config to fetch from"
            )
        pages = await call_api(
            endpoint,
            dict(endpoint.default_params),
            base_url=api_source.base_url,
            auth=api_source.auth,
        )
        rows: list[dict] = []
        for page in pages:
            rows.extend(
                flatten_response(
                    page, endpoint.response_root, endpoint.columns, endpoint.response_normalizer
                )
            )
        return rows

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


def make_firebird_loader() -> AdapterLoader:
    """Build the firebird row-fetch (REQ-1730): no engine other than DuckDB reaches firebird at
    all (no Trino/pg connector exists), so landing is the ONLY way any other engine ever sees a
    firebird source's rows — read through a scratch DuckDB connection ATTACHed via the same
    `firebird` community extension DuckDBFirebirdConnector uses at query time."""
    from provisa.core.secrets import resolve_secrets
    from provisa.federation.connector_duckdb import DuckDBFirebirdConnector

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not columns:
            return []
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
        return await asyncio.to_thread(_duckdb_extension_scratch_read, details, sql)

    return _load


def make_airport_loader() -> AdapterLoader:
    """Build the airport row-fetch (REQ-1730): same rationale as firebird's own loader above — no
    engine but DuckDB reaches an Arrow Flight (airport) source live, so landing through a scratch
    DuckDB connection ATTACHed via the `airport` extension is the only reader available to any
    other engine."""
    from provisa.federation.connector_duckdb import DuckDBAirportConnector

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]
        if not columns:
            return []
        connector = DuckDBAirportConnector()
        details = connector.details(source)
        select = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {select} FROM "{details["raw_alias"]}"."{table.schema_name}"."{table.table_name}"'
        details = {
            **details,
            "extension": connector.extension,
            "install_from_community": connector.install_from_community,
        }
        return await asyncio.to_thread(_duckdb_extension_scratch_read, details, sql)

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


def make_graphql_remote_loader(gql_sources: dict[str, Any]) -> AdapterLoader:
    """Build the graphql_remote adapter row-fetch (REQ-941/846): resolve the table's registration in
    ``state.graphql_remote_sources`` (by ``sql_name``), forward a minimal GraphQL query to the remote
    endpoint via :func:`execute_remote`, and return the rows. Refreshes from the remote source — the
    materialized replica is landed by the write face, not read back from its stale cache.

    ``gql_sources`` maps source_id → a registration dict (``url``, ``auth``, ``tables``); each table
    carries its ``field_name``/``sql_name`` and ``columns`` (a column's ``gql_selection`` overrides
    its name for nested object fields). A table with no matching registration raises
    :class:`UnsupportedSourceFetch`."""

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.compiler.naming import apply_gql_name, apply_sql_name
        from provisa.graphql_remote.executor import execute_remote

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
                    return await execute_remote(
                        url=reg["url"],
                        auth=reg.get("auth"),
                        field_name=tbl.get("field_name") or tbl["name"],
                        columns=col_selections,
                    )
        raise UnsupportedSourceFetch(
            f"graphql_remote source {source.id!r} table {table.table_name!r}: no matching "
            f"registration in graphql_remote_sources"
        )

    return _load

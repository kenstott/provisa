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
    }
)


def is_adapter_fetched(source_type: Any) -> bool:
    """Whether a source type's rows are PRODUCED by its adapter rather than scanned from a relation
    the engine can already reach.

    The distinction decides where a landed replica has to live. An engine-scannable source is
    mirrored at its physical address before the event loop ever runs (a sqlite file is migrated into
    Postgres at registration), so its landing table is an internal copy and can be named anything.
    An adapter-fetched source has no such mirror — the landed rows ARE the only copy, so on an engine
    that reads the store directly by physical name they must land at that name (see
    ``TrinoBackend.landing_target``). Accepts the enum member or the bare string."""
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


def make_dq_loader(tables: list) -> AdapterLoader:
    """Build the data-quality checker row-fetch (REQ-1443).

    A checker table's "current rows" are one scan's results, so the load RUNS the contract: the
    checker subprocess verifies ``table.dq_contract`` against Provisa's own pgwire endpoint and its
    per-check results are the rows. That makes the poll loop the scan scheduler — a checker table
    refreshes on the same cadence machinery as any other table, with no separate scheduler.

    The endpoint the checker connects back through is ``source.mapping`` (the per-source
    type-specific DSL, REQ-251): host, port, database, user, password. ``tables`` is the governed
    table list the contract's dataset resolves against — a contract aimed at a table Provisa does not
    govern raises :class:`~provisa.dq.contract.ContractError` there, not here.
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
        target = resolve_contract_target(dataset, tables)
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

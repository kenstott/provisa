# Copyright (c) 2026 Kenneth Stott
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
google_sheets) have no engine-scannable table — their current rows come from calling the adapter, a
per-adapter follow-up. For those the loader raises :class:`UnsupportedSourceFetch` rather than
silently returning nothing, so the boundary is explicit and the caller decides (the boot wiring skips
that node and logs; it never fabricates an empty snapshot).
"""

from __future__ import annotations

from typing import Any

# Source types whose "current rows" are fetched by calling the adapter, not by an engine SQL scan.
# Everything else (RDBMS, cloud DW, OLAP, data lake, file, and connector-backed NoSQL/streaming/graph)
# is read through the engine terminal. Keep this the exclusion set — the scannable set is open-ended.
_ADAPTER_FETCH_ONLY: frozenset[str] = frozenset(
    {
        "openapi",
        "grpc_remote",
        "ingest",
        "websocket",
        "rss",
        "prometheus",
        "google_sheets",
    }
)


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
        stype = _source_type(source)
        if stype in _ADAPTER_FETCH_ONLY:
            loader = self._adapter_loaders.get(stype)
            if loader is None:
                raise UnsupportedSourceFetch(
                    f"source type {stype!r} has no engine-scannable table and no adapter row-fetch "
                    f"is wired (source {source.id!r})"
                )
            return await loader(source, table)
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

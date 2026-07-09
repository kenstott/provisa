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


class SourceRowLoader:
    """Reads a MATERIALIZED source table's current rows via the federation engine (REQ-941/846).

    ``engine`` is the engine runtime wrapper (the one exposing ``execute_engine``). ``load`` ignores
    the claimed events and returns a full snapshot; an incremental (watermark-filtered) read is a
    later refinement keyed off the change event's cursor."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    async def load(self, source: Any, table: Any) -> list[dict]:
        stype = _source_type(source)
        if stype in _ADAPTER_FETCH_ONLY:
            raise UnsupportedSourceFetch(
                f"source type {stype!r} has no engine-scannable table — its adapter row-fetch is a "
                f"per-adapter follow-up (source {source.id!r})"
            )
        from provisa.compiler.naming import source_to_catalog

        catalog = source_to_catalog(source.id)
        ref = f'"{catalog}"."{table.schema_name}"."{table.table_name}"'
        result = await self._engine.execute_engine(f"SELECT * FROM {ref}")
        return [dict(zip(result.column_names, row)) for row in result.rows]

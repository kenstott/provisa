# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e1f27-9a3d-4b50-8e6f-2d7a1c9b5e30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Raw-SQL response-cache entries (REQ-1897): the payload kinds, and the write-through tee.

Every plan the one pipeline executes (pgwire, Flight SQL, /data/sql, gRPC, Bolt, REST, GraphQL)
reads and writes the one response cache under ``cache.key.raw_sql_cache_key``. An entry holds rows,
never a surface's own response shape, and is one of three explicitly tagged kinds:

* ``rows`` — ``{rows, column_names}`` plus the engine's declared ``column_types``, written by a
  terminal that drains rows (the buffered chokepoint, pgwire's ENGINE sink and DIRECT stream,
  gRPC's stream, Flight's Cypher and DIRECT terminals);
* ``arrow_ipc`` — the Arrow IPC stream (schema + batches) Flight SQL's Arrow terminal served,
  kept verbatim so a Flight miss -> hit is type-identical (e.g. ``decimal128(18, 2)``);
* ``pg_datarows`` — pgwire's Postgres passthrough (REQ-1863): the source's raw, wire-framed
  DataRow messages with the column names/types it advertised and the client's result format codes,
  replayed byte-for-byte by the passthrough terminal (a hit skips decode exactly as the miss did).
  Its bytes are specific to those format codes, so it lives under its own key (``wire_formats``)
  and only the passthrough terminal reads it.

Decoded readers handle ``rows`` and ``arrow_ipc``: a rows reader decodes ``arrow_ipc`` the way it
already decodes an Arrow-only engine's result (``runtime_support.arrow_type_name``); Flight builds
Arrow from a ``rows`` entry losslessly (``Decimal`` stays decimal, a timezone-aware timestamp keeps
its zone). A decoded reader handed a ``pg_datarows`` entry — or any reader handed an unknown kind —
raises; there is no defaulted read.

The tee (``ResponseCacheTee``) forwards every batch the moment it arrives — no added latency to the
first byte — and keeps a copy only while the result is within the bound: the org's large-result
threshold (``RedirectConfig.threshold``, the same inline-vs-redirect line GraphQL's buffered
transports use). Past it the copy is dropped at once and nothing is written; a stream that fails,
is closed early, or is cut off by the request deadline writes nothing either. Only a stream
drained to its end within the bound is stored.
"""

# Requirements: REQ-1897

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from provisa.executor.result import QueryResult, ResultStream, StreamingQueryResult

KIND_ROWS = "rows"
KIND_ARROW_IPC = "arrow_ipc"
KIND_PG_DATAROWS = "pg_datarows"


def rows_entry(rows: list[tuple], column_names: list[str]) -> dict:
    return {"kind": KIND_ROWS, "rows": rows, "column_names": column_names}


def arrow_entry(schema: Any, batches: list[Any]) -> dict:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return {"kind": KIND_ARROW_IPC, "ipc": sink.getvalue().to_pybytes()}


def datarows_entry(
    datarows: list[bytes],
    column_names: list[str],
    column_types: list[str] | None,
    wire_formats: list[int],
) -> dict:
    return {
        "kind": KIND_PG_DATAROWS,
        "datarows": [bytes(m) for m in datarows],
        "column_names": column_names,
        "column_types": column_types,
        "wire_formats": wire_formats,
    }


def entry_as_datarows(entry: dict, wire_formats: list[int]) -> QueryResult:
    """A ``pg_datarows`` entry as the passthrough result it replays: raw DataRow messages
    (``RawDataRowBytes``) that pgwire forwards without decoding, under the columns the miss
    advertised. Any other kind, or other format codes, raises."""
    from buenavista.core import RawDataRowBytes

    if entry["kind"] != KIND_PG_DATAROWS:
        raise ValueError(f"passthrough replay needs a pg_datarows entry, got {entry['kind']!r}")
    if entry["wire_formats"] != wire_formats:
        raise ValueError(
            f"pg_datarows entry was written for format codes {entry['wire_formats']!r}, "
            f"not {wire_formats!r}"
        )
    # A RawDataRowBytes stands in the row slot, as it does on the live passthrough stream
    # (FederationEngine._pg_passthrough_stream): buenavista's send_data_rows forwards it verbatim.
    datarows: list[Any] = [RawDataRowBytes(m) for m in entry["datarows"]]
    return QueryResult(
        rows=datarows,
        column_names=entry["column_names"],
        column_types=entry["column_types"],
    )


def _decoded_only(kind: str) -> ValueError:
    if kind == KIND_PG_DATAROWS:
        return ValueError(
            "a pg_datarows entry holds pgwire passthrough wire bytes; only the passthrough "
            "terminal replays it"
        )
    return ValueError(f"raw-SQL response-cache entry has unknown kind {kind!r}")


def _arrow_table(entry: dict) -> Any:
    import pyarrow as pa

    return pa.ipc.open_stream(entry["ipc"]).read_all()


def entry_as_result(entry: dict, column_types: list[str] | None) -> QueryResult:
    """A cached entry of either kind as the row result a live execution returns."""
    kind = entry["kind"]
    if kind == KIND_ROWS:
        # msgpack has no tuple type -- every row decodes back as a list.
        rows = [tuple(row) for row in entry["rows"]]
        return QueryResult(rows=rows, column_names=entry["column_names"], column_types=column_types)
    if kind == KIND_ARROW_IPC:
        from provisa.federation.runtime_support import arrow_type_name

        table = _arrow_table(entry)
        cols = [col.to_pylist() for col in table.columns]
        return QueryResult(
            rows=list(zip(*cols, strict=True)) if cols else [],
            column_names=list(table.schema.names),
            column_types=[arrow_type_name(f.type) for f in table.schema],
        )
    raise _decoded_only(kind)


def entry_as_arrow(entry: dict, column_types: list[str] | None) -> Any:
    """A cached entry of either kind as the Arrow table Flight serves."""
    import pyarrow as pa

    kind = entry["kind"]
    if kind == KIND_ARROW_IPC:
        return _arrow_table(entry)
    if kind == KIND_ROWS:
        names: list[str] = entry["column_names"]
        rows = entry["rows"]
        declared = column_types if column_types is not None else [None] * len(names)
        # The rows->Arrow typing Flight's live row stream uses, so a miss and its hit agree.
        from provisa.federation.runtime_support import arrow_array_for_rows

        arrays = [
            arrow_array_for_rows([row[i] for row in rows], declared[i]) for i in range(len(names))
        ]
        return pa.table(arrays, names=names)
    raise _decoded_only(kind)


def _releaser(stream: ResultStream) -> Callable[[], None] | None:
    """Closing the teed stream closes the source (its server-side cursor / pooled connection) —
    a live stream releases on close exactly as it did untee'd; a materialized result holds none."""
    return stream.close if isinstance(stream, StreamingQueryResult) else None


@dataclass
class _Target:
    """Where and how long a completed tee stores its entry."""

    store: Any
    key: str
    org_id: str | None
    ttl: int
    table_ids: set[int]


class ResponseCacheTee:
    """Write-through capture of one streamed result (see the module docstring for the contract).

    ``run`` stores the entry from a synchronous terminal (it runs the store coroutine on the
    terminal's own loop, e.g. pgwire's ``cl.run``); a terminal already on its loop passes ``None``
    and awaits :meth:`commit` after the drain."""

    def __init__(
        self,
        target: _Target,
        bound: int,
        run: Callable[[Coroutine[Any, Any, Any]], Any] | None,
    ) -> None:
        self._target = target
        self.bound = bound
        self._run = run
        self._buffer: list[Any] | None = []
        self._buffered_rows = 0
        self.peak_buffered_rows = 0
        self._complete = False
        self._entry: dict | None = None
        self._column_types: list[str] | None = None
        self._seen_rows = 0
        # REQ-1949: asked when the stream ends, with the rows it delivered: whether what was
        # captured is the statement's whole answer. A cut answer is not kept as the answer.
        self.whole: Callable[[int], bool] | None = None

    # -- capture -------------------------------------------------------------------------------

    def _keep(self, item: Any, n_rows: int) -> None:
        self._seen_rows += n_rows
        if self._buffer is None:
            return
        if self._buffered_rows + n_rows > self.bound:
            self._buffer = None  # over the bound: drop the copy, never write
            return
        self._buffer.append(item)
        self._buffered_rows += n_rows
        self.peak_buffered_rows = max(self.peak_buffered_rows, self._buffered_rows)

    def _finished(self) -> None:
        if self.whole is not None and not self.whole(self._seen_rows):
            self._buffer = self._entry = None
        self._complete = True
        if self._run is not None:
            self._run(self.commit())

    def rows(self, stream: ResultStream) -> ResultStream:
        """``stream`` with every row batch forwarded as it arrives and captured within the bound."""
        column_names = list(stream.column_names)
        self._column_types = stream.column_types

        def _batches() -> Iterator[list[tuple]]:
            for batch in stream.batches():
                self._keep(batch, len(batch))
                yield batch
            if self._buffer is not None:
                self._entry = rows_entry([r for b in self._buffer for r in b], column_names)
            self._finished()

        return StreamingQueryResult(
            _batches(),
            column_names=column_names,
            column_types=stream.column_types,
            on_release=_releaser(stream),
        )

    def datarows(self, stream: ResultStream, wire_formats: list[int]) -> ResultStream:
        """A passthrough ``stream`` of raw DataRow messages forwarded as each batch arrives and
        captured (still undecoded) within the bound — the ``pg_datarows`` entry."""
        column_names = list(stream.column_names)
        column_types = stream.column_types
        self._column_types = column_types

        def _batches() -> Iterator[list[tuple]]:
            for batch in stream.batches():
                self._keep(batch, len(batch))
                yield batch
            if self._buffer is not None:
                self._entry = datarows_entry(
                    [m for b in self._buffer for m in b],
                    column_names,
                    column_types,
                    wire_formats,
                )
            self._finished()

        return StreamingQueryResult(
            _batches(),
            column_names=column_names,
            column_types=column_types,
            on_release=_releaser(stream),
        )

    def arrow(self, schema: Any, batches: Iterable[Any]) -> Iterator[Any]:
        """``batches`` (Arrow record batches) forwarded as they arrive and captured within the
        bound."""
        for batch in batches:
            self._keep(batch, batch.num_rows)
            yield batch
        if self._buffer is not None:
            self._entry = arrow_entry(schema, self._buffer)
        self._finished()

    # -- store ---------------------------------------------------------------------------------

    @property
    def stored_entry(self) -> dict | None:
        """The entry a completed, in-bound drain produced (None otherwise)."""
        return self._entry if self._complete else None

    async def commit(self) -> None:
        """Store the entry — only after the stream drained to its end within the bound."""
        entry = self.stored_entry
        if entry is None:
            return
        from provisa.cache.middleware import store_result

        t = self._target
        await store_result(
            t.store,
            t.key,
            entry,
            ttl=t.ttl,
            table_ids=t.table_ids,
            org_id=t.org_id,
            column_types=self._column_types,
        )


def new_tee(
    store: Any,
    key: str,
    org_id: str | None,
    ttl: int,
    table_ids: set[int],
    bound: int,
    run: Callable[[Coroutine[Any, Any, Any]], Any] | None,
) -> ResponseCacheTee:
    return ResponseCacheTee(_Target(store, key, org_id, ttl, table_ids), bound, run)


__all__ = [
    "KIND_ARROW_IPC",
    "KIND_PG_DATAROWS",
    "KIND_ROWS",
    "ResponseCacheTee",
    "arrow_entry",
    "datarows_entry",
    "entry_as_datarows",
    "entry_as_arrow",
    "entry_as_result",
    "new_tee",
    "rows_entry",
]

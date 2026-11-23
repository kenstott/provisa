# Copyright (c) 2026 Kenneth Stott
# Canary: 9c41d7e2-5b08-4f3a-8e6d-1a2b3c4d5e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Edit a stored response-cache entry in place, to prove a HIT came from the cache (REQ-1897).

An entry is one Arrow IPC stream (``provisa.cache.raw_sql``); its schema metadata names its kind
and origin. These helpers read it, let a test replace its rows, and write it back under the same
schema and metadata, so the server reads the edit as the entry its MISS wrote.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa

from provisa.cache.raw_sql import KIND_PG_DATAROWS, decode_entry


def label(data: bytes) -> str:
    """``pg_datarows``, or for an ``arrow`` entry its origin: ``arrow`` (the engine's batches)
    or ``rows`` (a row stream's rows built into Arrow)."""
    entry = decode_entry(data)
    if entry.kind == KIND_PG_DATAROWS:
        return KIND_PG_DATAROWS
    assert entry.origin is not None
    return entry.origin


def rewrite(data: bytes, edit: Callable[[pa.Table], pa.Table]) -> bytes:
    """``data`` with its table replaced by ``edit(table)``, under the entry's own schema and
    metadata (its tags included)."""
    table = pa.ipc.open_stream(data).read_all()
    edited = edit(table)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in edited.to_batches():
            writer.write_batch(pa.RecordBatch.from_arrays(batch.columns, schema=table.schema))
    return sink.getvalue().to_pybytes()


def with_rows(rows: list[tuple]) -> Callable[[pa.Table], pa.Table]:
    """An edit that replaces every row with ``rows``, typed as the entry's columns."""

    def edit(table: pa.Table) -> pa.Table:
        names = table.schema.names
        return pa.Table.from_pylist(
            [dict(zip(names, row, strict=True)) for row in rows], schema=table.schema
        )

    return edit


def second_row_only(table: pa.Table) -> pa.Table:
    return table.slice(1, 1)

# Copyright (c) 2026 Kenneth Stott
# Canary: b95e2c07-1a4d-4f38-8e6b-0d3a7f19c528
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow record-batch framing + streaming preflight evaluation (REQ-1165, REQ-940).

The REQ-940 processor toolkit framed only NDJSON — one JSON object per row — which pays a
per-row parse/serialize cost that a large preflight check cannot afford. This module adds the
columnar alternative the REQ-1165 Python+Arrow transport requires: rows move as Arrow
``RecordBatch`` es, the same shape the engine already produces (``execute_engine_stream``), so a
non-SQL preflight consumes the produced dataset batch-by-batch and SHORT-CIRCUITS without ever
buffering it whole.

The streaming value for a preflight GATE is on the read side: :func:`rows_of` decodes batches
lazily and :func:`stream_preflight` feeds that lazy row stream to the compiled check, so a hook
whose body is ``any(P for r in rows)`` stops at the first violating row and memory stays bounded
to one batch. :func:`arrow_encode` / :func:`arrow_decode` are the wire framing (standard Arrow
IPC stream) parallel to ``ndjson_encode`` / ``ndjson_decode``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

import pyarrow as pa

from provisa.mv.preflight import Verdict, run_preflight

DEFAULT_BATCH_SIZE = 4096


def record_batches(
    rows: Iterable[dict],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    schema: pa.Schema | None = None,
) -> Iterator[pa.RecordBatch]:
    """Group ``rows`` into Arrow ``RecordBatch`` es of at most ``batch_size`` rows, lazily.

    Types are taken from ``schema`` when supplied, else inferred by pyarrow from the batch. An
    empty ``rows`` yields nothing (no empty batch)."""
    chunk: list[dict] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) >= batch_size:
            yield pa.RecordBatch.from_pylist(chunk, schema=schema)
            chunk = []
    if chunk:
        yield pa.RecordBatch.from_pylist(chunk, schema=schema)


def rows_of(batches: Iterable[pa.RecordBatch]) -> Iterator[dict]:
    """Decode Arrow batches back to row dicts, one batch at a time (lazy — the caller may stop
    early and the untouched batches are never converted)."""
    for batch in batches:
        yield from batch.to_pylist()


def arrow_encode(
    rows: Iterable[dict],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    schema: pa.Schema | None = None,
) -> bytes:
    """Encode ``rows`` as one Arrow IPC stream payload (schema + record batches).

    Returns ``b""`` for an empty, schema-less input (no batch → no schema to frame). This is the
    columnar wire framing for the REQ-940 Arrow transport; the batching is lazy but the IPC stream
    is a single self-describing payload."""
    sink = pa.BufferOutputStream()
    writer: pa.ipc.RecordBatchStreamWriter | None = None
    for batch in record_batches(rows, batch_size=batch_size, schema=schema):
        if writer is None:
            writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
    if writer is None:
        if schema is not None:
            writer = pa.ipc.new_stream(sink, schema)
            writer.close()
            return sink.getvalue().to_pybytes()
        return b""
    writer.close()
    return sink.getvalue().to_pybytes()


def arrow_decode(data: bytes) -> Iterator[dict]:
    """Decode an Arrow IPC stream payload (:func:`arrow_encode` output) back to row dicts, lazily
    per batch. Empty input yields nothing."""
    if not data:
        return
    reader = pa.ipc.open_stream(pa.py_buffer(data))
    for batch in reader:
        yield from batch.to_pylist()


async def stream_preflight(
    fn: Callable[..., Any] | None,
    batches: Iterable[pa.RecordBatch],
    ctx: Any,
) -> Verdict:
    """Evaluate a compiled preflight check over a stream of Arrow batches (REQ-1165).

    The batches are decoded lazily (:func:`rows_of`) and handed to the check as its ``rows``
    argument, so a quantified predicate (``any``/``all`` over ``rows``) short-circuits on the first
    decisive row and the full dataset is never materialized. ``fn`` None → continue."""
    return await run_preflight(fn, rows_of(batches), ctx)

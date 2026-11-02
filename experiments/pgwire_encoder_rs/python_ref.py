# Copyright (c) 2026 Kenneth Stott
# Canary: 94865ac8-f19a-4123-9c23-5cbb9324ab81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's own Python encode path, driven in isolation: Arrow batches -> stream_rows_from_arrow
(column_values per column, zip to row tuples) -> ProvisaQueryResult (types, coercers) -> buenavista
send_data_rows (BVTYPE_TO_PGTYPE converters, DataRow framing) into an in-memory writer.

Run with the worktree root and vendor/buenavista first on PYTHONPATH, and build/ for the Rust module.
"""

from __future__ import annotations

import io
from collections.abc import Iterable, Iterator
from typing import Any

import pyarrow as pa
from buenavista.postgres import BuenaVistaHandler

from provisa.federation.runtime_support import stream_rows_from_arrow
from provisa.pgwire.server import ProvisaQueryResult


class _Sink:
    """The part of a BuenaVistaHandler send_data_rows touches: its writer and chunk size."""

    _DATA_ROW_CHUNK_SIZE = BuenaVistaHandler._DATA_ROW_CHUNK_SIZE

    def __init__(self, wfile: Any) -> None:
        self.wfile = wfile


class _CountingWriter:
    """A writer that keeps only the byte count, so a large run never holds its output."""

    def __init__(self) -> None:
        self.nbytes = 0

    def write(self, b: bytes) -> None:
        self.nbytes += len(b)


def query_result(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None,
) -> ProvisaQueryResult:
    stream = stream_rows_from_arrow(schema, iter(batches), column_types=column_types)
    qr = ProvisaQueryResult(stream)
    qr.result_format = formats
    return qr


def bvtype_names(qr: ProvisaQueryResult) -> list[str]:
    """The BVType each column is encoded as, by member name: what the Rust encoder is told."""
    return [qr.column(i)[1].name for i in range(qr.column_count())]


def python_encode(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
) -> tuple[bytes, list[str]]:
    """The DataRow bytes the product sends for these batches, and the BVTypes it chose."""
    qr = query_result(schema, batches, column_types, formats)
    sink = _Sink(io.BytesIO())
    BuenaVistaHandler.send_data_rows(sink, qr)
    return sink.wfile.getvalue(), bvtype_names(qr)


def python_encode_count(
    schema: pa.Schema,
    batches: Iterator[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
) -> tuple[int, int]:
    """(rows, bytes) of a streamed encode whose output is counted, not kept."""
    qr = query_result(schema, batches, column_types, formats)
    writer = _CountingWriter()
    rows = BuenaVistaHandler.send_data_rows(_Sink(writer), qr)
    return rows, writer.nbytes

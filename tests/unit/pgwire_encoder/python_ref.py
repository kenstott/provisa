# Copyright (c) 2026 Kenneth Stott
# Canary: 94865ac8-f19a-4123-9c23-5cbb9324ab81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The two sides of the differential, driven in isolation into in-memory writers.

The wired path (``wired_encode``): Arrow batches as an engine hands them over (an
``ArrowStreamResult``) -> ``ProvisaQueryResult`` (wire types, coercers) -> buenavista's
``send_data_rows`` -> the Rust encoder (provisa._encoders.pgwire). This is what pgwire sends.

The reference (``python_encode``): the same batches read into Python rows as the product read them
before REQ-2194 (``column_values`` per column, each column's coercers applied), written by the
frozen Python writers in ``oracle.py``; ``pg_reference.pg17()`` corrects those to PostgreSQL 17's
answer. Both sides take their wire types and coercers from the one ``ProvisaQueryResult``.

Run with the worktree root and vendor/buenavista first on PYTHONPATH; rust/build-local.sh builds
the Rust module.
"""

from __future__ import annotations

import io
import re
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import tzinfo
from typing import Any

import pyarrow as pa
from buenavista.postgres import BuenaVistaHandler

from provisa.core import ir_arrow
from provisa.executor.result import ArrowStreamResult
from provisa.pgwire.server import ProvisaQueryResult
from tests.unit.pgwire_encoder import oracle


class _Sink:
    """The part of a BuenaVistaHandler send_data_rows touches: its writer."""

    def __init__(self, wfile: Any) -> None:
        self.wfile = wfile


class _CountingWriter:
    """A writer that keeps only the byte count, so a large run never holds its output."""

    def __init__(self) -> None:
        self.nbytes = 0

    def write(self, b: bytes) -> None:
        self.nbytes += len(b)


def _rows_of(batch: pa.RecordBatch) -> list[tuple]:
    return list(zip(*[ir_arrow.column_values(c) for c in batch.columns], strict=True))


def query_result(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None,
    *,
    sql: str = "",
    shape: list[tuple[str, str]] | None = None,
    read_shape: list[tuple[str, str]] | None = None,
    force_types: list[Any] | None = None,
    force_coerce: list[tuple[int, Any]] | None = None,
) -> ProvisaQueryResult:
    """The product's result over ``batches``, read as an Arrow engine hands them over.
    ``force_types``: each column's BVType set outright, for a type no engine type name reaches;
    ``force_coerce``: the (column, coercer) pairs set outright (a test of every writer and
    coercer)."""
    stream = ArrowStreamResult(schema, iter(batches), column_types, to_rows=_rows_of)
    qr = ProvisaQueryResult(stream, sql, shape=shape, read_shape=read_shape)
    if force_types is not None:
        qr._types = list(force_types)
    if force_coerce is not None:
        qr._coerce = list(force_coerce)
    qr.result_format = formats
    return qr


def bvtype_names(qr: ProvisaQueryResult) -> list[str]:
    """The wire type each column is encoded as, by member name ("xml[]" for xml's array)."""
    return [getattr(t, "name", t) for t in (qr.column(i)[1] for i in range(qr.column_count()))]


def coercer_names(qr: ProvisaQueryResult) -> list[list[str]]:
    """Each column's coercers, in the order ProvisaQueryResult applies them, by name."""
    return qr.coercers() or [[] for _ in range(qr.column_count())]


def coerce(qr: ProvisaQueryResult, row: tuple) -> list:
    """A row brought to its described types by the result's coercers (pg17 replaces this)."""
    out = list(row)
    for i, coercer in qr._coerce:
        if out[i] is not None:
            out[i] = coercer(out[i])
    return out


def _reference_rows(qr: ProvisaQueryResult, batches: Iterable[pa.RecordBatch]) -> Iterator[list]:
    for batch in batches:
        for row in zip(*[column_values(c) for c in batch.columns], strict=True):
            yield COERCE(qr, row)


#: The coercion the reference applies; pg_reference.pg17 swaps it for PostgreSQL's.
COERCE: Callable[[ProvisaQueryResult, tuple], list] = coerce
#: How the reference reads a column; named_zones swaps it.
column_values: Callable[[Any], list[Any]] = ir_arrow.column_values


def python_encode(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
    **described: Any,
) -> tuple[bytes, list[str], list[list[str]]]:
    """The reference's DataRow bytes for these batches, the wire types and the coercers
    (``described``: query_result's sql, shape, read_shape and forced types or coercers)."""
    batches = list(batches)
    qr = query_result(schema, [], column_types, formats, **described)
    out = io.BytesIO()
    oracle.send_data_rows(out, qr, _reference_rows(qr, batches))
    return out.getvalue(), bvtype_names(qr), coercer_names(qr)


def python_encode_count(
    schema: pa.Schema,
    batches: Iterator[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
) -> tuple[int, int]:
    """(rows, bytes) of the reference's streamed encode, its output counted, not kept."""
    qr = query_result(schema, [], column_types, formats)
    writer = _CountingWriter()
    rows = oracle.send_data_rows(writer, qr, _reference_rows(qr, batches))
    return rows, writer.nbytes


def wired_encode(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
    **described: Any,
) -> tuple[bytes, list[str], list[list[str]]]:
    """What pgwire sends for these batches: the product's send path, through the Rust encoder."""
    qr = query_result(schema, batches, column_types, formats, **described)
    sink = _Sink(io.BytesIO())
    BuenaVistaHandler.send_data_rows(sink, qr)  # type: ignore[arg-type]
    return sink.wfile.getvalue(), bvtype_names(qr), coercer_names(qr)


def wired_encode_count(
    schema: pa.Schema,
    batches: Iterator[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
) -> tuple[int, int]:
    """(rows, bytes) of the wired path's streamed encode, its output counted, not kept."""
    qr = query_result(schema, batches, column_types, formats)
    writer = _CountingWriter()
    rows = BuenaVistaHandler.send_data_rows(_Sink(writer), qr)  # type: ignore[arg-type]
    return rows, writer.nbytes


_FIXED = re.compile(r"UTC|[+-]\d\d:\d\d")


@contextmanager
def named_zones(zone: Callable[[str], tzinfo]) -> Iterator[None]:
    """Within: the reference reads a timestamp column in a named zone with its local times from
    ``zone(name)``, where pyarrow's to_pylist takes them from pytz. Its values are column_values
    of the column in UTC (infinities and all), each moved into the zone."""
    global column_values
    original = column_values

    def zoned(column: pa.Array | pa.ChunkedArray) -> list[Any]:
        t = column.type
        if not pa.types.is_timestamp(t) or t.tz is None or _FIXED.fullmatch(t.tz):
            return original(column)
        z = zone(t.tz)
        values = original(column.cast(pa.timestamp(t.unit, "UTC")))
        return [v if v is None or getattr(v, "pg_infinity", 0) else v.astimezone(z) for v in values]

    column_values = zoned
    try:
        yield
    finally:
        column_values = original

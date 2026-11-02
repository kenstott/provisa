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
import re
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import tzinfo
from typing import Any

import pyarrow as pa
from buenavista.postgres import BuenaVistaHandler

import provisa.pgwire.cast_types as cast_types
import provisa.pgwire.server as server
from provisa.federation import runtime_support
from provisa.federation.duckdb_pg_xml import xml_out
from provisa.federation.runtime_support import stream_rows_from_arrow
from provisa.pgwire.catalog_pg17 import PROC_NAMES, TYPES
from provisa.pgwire.server import ProvisaQueryResult
from provisa.transpiler.pg_forms_json import jsonb_out


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
    *,
    sql: str = "",
    shape: list[tuple[str, str]] | None = None,
    read_shape: list[tuple[str, str]] | None = None,
    force_types: list[Any] | None = None,
    force_coerce: list[tuple[int, Any]] | None = None,
) -> ProvisaQueryResult:
    """``force_types``: each column's BVType set outright, for a type no engine type name reaches;
    ``force_coerce``: the (column, coercer) pairs set outright (a test of every writer and
    coercer)."""
    stream = stream_rows_from_arrow(schema, iter(batches), column_types=column_types)
    qr = ProvisaQueryResult(stream, sql, shape=shape, read_shape=read_shape)
    if force_types is not None:
        qr._types = list(force_types)
    if force_coerce is not None:
        qr._coerce = list(force_coerce)
    qr.result_format = formats
    return qr


def bvtype_names(qr: ProvisaQueryResult) -> list[str]:
    """The BVType each column is encoded as, by member name ("xml[]" for xml's array, which
    BVType does not list): what the Rust encoder is told."""
    return [getattr(t, "name", t) for t in (qr.column(i)[1] for i in range(qr.column_count()))]


#: The coercers ProvisaQueryResult applies, by the name the Rust encoder knows each by.
_COERCER_NAMES: dict[Any, str] = {
    int: "int",
    float: "float",
    server._to_decimal: "decimal",
    server._to_date: "date",
    server._to_timestamp: "timestamp",
    server._to_list: "list",
    server._null_alone: "null_alone",
    server._duckdb_inet: "duckdb_inet",
    xml_out: "xml_out",
    jsonb_out: "jsonb_out",
    cast_types._to_int: "int",
    cast_types._to_float: "float",
    cast_types._to_decimal: "decimal",
    cast_types._to_bool: "bool",
    cast_types.record_text: "record",
    cast_types._clock_of: "clock",
    cast_types._of_no_zone: "no_zone",
}


def _coercer_name(coercer: Any) -> str:
    name = _COERCER_NAMES.get(coercer)
    if name is not None:
        return name
    if getattr(coercer, "__qualname__", "") == "_padded.<locals>.pad":
        (length,) = (c.cell_contents for c in coercer.__closure__)
        return f"pad:{length}"
    raise ValueError(f"no Rust coercer for {coercer!r}")


def coercer_names(qr: ProvisaQueryResult) -> list[list[str]]:
    """Each column's coercers, in the order ProvisaQueryResult applies them, by name."""
    names: list[list[str]] = [[] for _ in range(qr.column_count())]
    for i, coercer in qr._coerce:
        names[i].append(_coercer_name(coercer))
    return names


def register_catalog_names(module: Any) -> None:
    """Hands the reg* writers the catalog names server._register_reg_types reads."""
    module.register_catalog_names(
        list(PROC_NAMES.items()), [(oid, entry[-1]) for oid, entry in TYPES.items()]
    )


def python_encode(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    column_types: list[str],
    formats: list[int] | None = None,
    **described: Any,
) -> tuple[bytes, list[str], list[list[str]]]:
    """The DataRow bytes the product sends for these batches, the BVTypes it chose and the
    coercers it applies (``described``: query_result's sql, shape and read_shape)."""
    qr = query_result(schema, batches, column_types, formats, **described)
    sink = _Sink(io.BytesIO())
    BuenaVistaHandler.send_data_rows(sink, qr)
    return sink.wfile.getvalue(), bvtype_names(qr), coercer_names(qr)


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


_FIXED = re.compile(r"UTC|[+-]\d\d:\d\d")


@contextmanager
def named_zones(zone: Callable[[str], tzinfo]) -> Iterator[None]:
    """Within: a timestamp column in a named zone takes its local times from ``zone(name)``, where
    pyarrow's to_pylist takes them from pytz. The product's path is otherwise unchanged: its values
    are column_values of the column in UTC (infinities and all), each moved into the zone."""
    original = runtime_support.column_values

    def column_values(column: pa.Array | pa.ChunkedArray) -> list[Any]:
        t = column.type
        if not pa.types.is_timestamp(t) or t.tz is None or _FIXED.fullmatch(t.tz):
            return original(column)
        z = zone(t.tz)
        values = original(column.cast(pa.timestamp(t.unit, "UTC")))
        return [v if v is None or getattr(v, "pg_infinity", 0) else v.astimezone(z) for v in values]

    runtime_support.column_values = column_values  # type: ignore[assignment]
    try:
        yield
    finally:
        runtime_support.column_values = original  # type: ignore[assignment]

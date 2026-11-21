# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c2b7e-8a41-4f6e-9c3d-1b7a2e9f4c60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Trino array reaches pgwire as the Zaychik proxy sends it, an Arrow list of its element
type, and the wire writes PostgreSQL's array text (REQ-2194, REQ-2236). Run 44932379227 found
every array-returning function answering the protocol's JSON (``[1,2,3]``, ``["a","b"]``)
where PostgreSQL answers ``{1,2,3}``, ``{a,b}``: the proxy sent arrays as Utf8 JSON text and
the encoder wrote that text as it was."""

# Requirements: REQ-2194, REQ-2236

from __future__ import annotations

import json
import struct
from typing import Any

import pyarrow as pa
import pytest

from provisa._encoders import pgwire as encoder
from provisa.executor import trino_flight as trf
from provisa.executor.result import ArrowResult
from provisa.pgwire.server import ProvisaQueryResult
from buenavista.core import wire_type_name


def _type(raw: str, *args: dict) -> dict:
    return {"rawType": raw, "arguments": list(args)}


def _array(element: dict) -> dict:
    return _type("array", {"kind": "TYPE", "value": element})


_VARCHAR = _type("varchar", {"kind": "LONG", "value": 2147483647})


def _proxy_column(trino_type: str, signature: dict, arrow: pa.DataType, values: list[Any]):
    field = pa.field(
        "v",
        arrow,
        metadata={
            "trino.type": trino_type,
            "trino.type_signature": json.dumps(signature),
            "trino.encoding": "arrow",
        },
    )
    return pa.Table.from_arrays([pa.array(values, type=arrow)], schema=pa.schema([field]))


def _cells(message: bytes) -> list[bytes | None]:
    """The first cell of each DataRow message in ``message``."""
    cells: list[bytes | None] = []
    at = 0
    while at < len(message):
        (length,) = struct.unpack_from("!i", message, at + 1)
        (size,) = struct.unpack_from("!i", message, at + 7)
        cells.append(None if size < 0 else message[at + 11 : at + 11 + size])
        at += 1 + length
    return cells


def _wire_text(table: pa.Table) -> list[str | None]:
    """Each row's cell as pgwire writes it in text format from the engine's Arrow."""
    result = ProvisaQueryResult(
        ArrowResult(table, trf.column_types(table.schema), to_rows=trf.trino_rows), "SELECT v"
    )
    types = [wire_type_name(result.column(0)[1])]
    out: list[str | None] = []
    for batch in result.data().take(0):
        for cell in _cells(encoder.encode_batch(batch, types, [0], result.coercers())):
            out.append(None if cell is None else cell.decode("utf-8"))
    return out


@pytest.mark.parametrize(
    ("trino_type", "signature", "arrow", "values", "postgres"),
    [
        (
            "array(integer)",
            _array(_type("integer")),
            pa.list_(pa.int32()),
            [[1, 2, 3], [-7, None, 42], [], [None], None],
            ["{1,2,3}", "{-7,NULL,42}", "{}", "{NULL}", None],
        ),
        (
            "array(bigint)",
            _array(_type("bigint")),
            pa.list_(pa.int64()),
            [[4, 9], [2, 3], [2**40]],
            ["{4,9}", "{2,3}", "{1099511627776}"],
        ),
        (
            "array(double)",
            _array(_type("double")),
            pa.list_(pa.float64()),
            [[4.0, 6.5, 3.6875], [6.0, 12.0, 4.0], [-0.875, 0.5], [1.0, None]],
            ["{4,6.5,3.6875}", "{6,12,4}", "{-0.875,0.5}", "{1,NULL}"],
        ),
        (
            "array(varchar)",
            _array(_VARCHAR),
            pa.list_(pa.string()),
            [["cat", "fat"], ["abc"], ["a b", None, "", 'q"']],
            ["{cat,fat}", "{abc}", '{"a b",NULL,"","q\\""}'],
        ),
        (
            "array(array(integer))",
            _array(_array(_type("integer"))),
            pa.list_(pa.list_(pa.int32())),
            [[[1]], [[1, 2], [3, None]]],
            ["{{1}}", "{{1,2},{3,NULL}}"],
        ),
    ],
)
def test_a_trino_array_is_written_as_postgresqls_array_text(
    trino_type, signature, arrow, values, postgres
):
    table = _proxy_column(trino_type, signature, arrow, values)
    assert _wire_text(table) == postgres


def test_a_trino_array_reads_as_python_lists_for_a_row_consumer():
    table = _proxy_column(
        "array(real)", _array(_type("real")), pa.list_(pa.float32()), [[0.1, None], None]
    )
    assert trf.trino_rows(table) == [([0.1, None],), (None,)]

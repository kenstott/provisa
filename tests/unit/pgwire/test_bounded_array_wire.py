# Copyright (c) 2026 Kenneth Stott
# Canary: 0d7a4c93-2e5b-4f61-8a3d-b19e6c2f7084
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An array whose lower bounds are not 1 reaches the pgwire send as PostgreSQL's own text of it
("[0:2]={1,2,3}"; pg_runtime reads it so, J519). It is sent as PostgreSQL sends it: that text in
the text format, and in the binary format array_send's header with those lower bounds
(REQ-2167)."""

# Requirements: REQ-2167

from __future__ import annotations

import struct

import pytest
from buenavista.core import BVType

from provisa.executor.result import QueryResult
from provisa.pgwire.server import _SHAPE_COERCERS, ProvisaQueryResult, _sql_type_to_bvtype
from tests.unit.pgwire import wire_cells as wire
from tests.unit.pgwire.result_rows import rows_of


def _int4(*values: int) -> bytes:
    return b"".join(struct.pack("!ii", 4, v) for v in values)


#: Each array as PostgreSQL 17 writes it (array_out) and sends it (array_send).
_BOUNDED = [
    (
        "_int4",
        "[0:2]={1,2,3}",
        struct.pack("!iii", 1, 0, 23) + struct.pack("!ii", 3, 0) + _int4(1, 2, 3),
    ),
    (
        "_int4",
        "[-2:-1]={7,NULL}",
        struct.pack("!iii", 1, 1, 23)
        + struct.pack("!ii", 2, -2)
        + _int4(7)
        + struct.pack("!i", -1),
    ),
    (
        "_int4",
        "[1:1][2:3][3:5]={{{3,3,3},{3,3,3}}}",
        struct.pack("!iii", 3, 0, 23)
        + struct.pack("!iiiiii", 1, 1, 2, 2, 3, 3)
        + _int4(3, 3, 3, 3, 3, 3),
    ),
    (
        "_text",
        '[0:1]={a,"b c"}',
        struct.pack("!iii", 1, 0, 25)
        + struct.pack("!ii", 2, 0)
        + struct.pack("!i", 1)
        + b"a"
        + struct.pack("!i", 3)
        + b"b c",
    ),
]


@pytest.mark.parametrize(("declared", "text", "binary"), _BOUNDED)
def test_a_bounded_array_is_sent_as_postgresql_sends_it(declared: str, text: str, binary: bytes):
    _, to_text, to_binary = wire.entry(_sql_type_to_bvtype(declared))
    assert to_text(text) == text
    assert to_binary(text) == binary


def test_a_bounded_array_from_the_postgresql_runtime_reaches_the_encoder_as_its_text():
    result = ProvisaQueryResult(
        QueryResult(rows=[("[0:2]={1,2,3}",)], column_names=["v"], column_types=["_int4"]),
        "SELECT v",
    )
    assert result.column(0)[1] == BVType.INTEGERARRAY
    (row,) = rows_of(result)
    _, to_text, to_binary = wire.entry(BVType.INTEGERARRAY)
    assert to_text(row[0]) == "[0:2]={1,2,3}"
    assert to_binary(row[0]) == _BOUNDED[0][2]


def test_a_described_array_keeps_its_bounded_text():
    """A Describe said the column is int4[]; its value is brought to that type by the shape
    coercer, which reads a JSON array's text but leaves PostgreSQL's own array text alone."""
    coerce = _SHAPE_COERCERS[BVType.INTEGERARRAY]
    assert coerce("[0:2]={1,2,3}") == "[0:2]={1,2,3}"
    assert coerce("[1, 2]") == [1, 2]


def test_an_array_from_one_is_still_sent_from_its_list():
    _, to_text, to_binary = wire.entry(BVType.INTEGERARRAY)
    assert to_text([1, 2]) == "{1,2}"
    assert to_binary([1, 2]) == struct.pack("!iiiii", 1, 0, 23, 2, 1) + _int4(1, 2)


def test_an_element_type_with_no_reader_of_its_text_is_refused_by_name():
    _, _, to_binary = wire.entry(BVType.INTERVALARRAY)
    with pytest.raises(TypeError, match="INTERVAL"):
        to_binary("[0:0]={00:00:01}")

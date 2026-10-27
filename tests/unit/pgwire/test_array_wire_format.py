# Copyright (c) 2026 Kenneth Stott
# Canary: e52540b8-df5e-4489-8205-a769b04b512d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A one-dimensional array result is sent with its element type's array OID, in PostgreSQL's
array text form ({1,2,3}) and binary form (array_send) -- pgspec TestTypes/Arrays."""

from __future__ import annotations

import struct

import pytest
from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_PGTYPE

from provisa.pgwire.server import _sql_type_to_bvtype


@pytest.mark.parametrize(
    ("declared", "oid", "values", "text"),
    [
        ("INT[]", 1007, [1, 2, 3], "{1,2,3}"),
        ("INTEGER[]", 1007, [1, None, 3], "{1,NULL,3}"),
        ("BIGINT[]", 1016, [1, 2], "{1,2}"),
        ("TEXT[]", 1009, ["a", "b"], "{a,b}"),
        ("VARCHAR[]", 1009, ["a b", "", "NULL", 'q"x', "c,d"], '{"a b","","NULL","q\\"x","c,d"}'),
        ("DOUBLE PRECISION[]", 1022, [1.5, 2.5], "{1.5,2.5}"),
        ("BOOLEAN[]", 1000, [True, False], "{t,f}"),
        ("_int4", 1007, [1, 2, 3], "{1,2,3}"),
        ("_text", 1009, ["a"], "{a}"),
        ("_float8", 1022, [1.5], "{1.5}"),
    ],
)
def test_array_oid_and_text(declared, oid, values, text):
    bv = _sql_type_to_bvtype(declared)
    pg_oid, to_text, _ = BVTYPE_TO_PGTYPE[bv]
    assert pg_oid == oid
    assert to_text(values) == text


def test_int4_array_binary_is_array_send():
    _, _, to_binary = BVTYPE_TO_PGTYPE[_sql_type_to_bvtype("INT[]")]
    expected = struct.pack("!iiiii", 1, 0, 23, 3, 1) + b"".join(
        struct.pack("!ii", 4, v) for v in (1, 2, 3)
    )
    assert to_binary([1, 2, 3]) == expected


def test_text_array_binary_marks_nulls():
    _, _, to_binary = BVTYPE_TO_PGTYPE[_sql_type_to_bvtype("TEXT[]")]
    assert to_binary(["a", None]) == (
        struct.pack("!iiiii", 1, 1, 25, 2, 1) + struct.pack("!i", 1) + b"a" + struct.pack("!i", -1)
    )


def test_a_nested_array_stays_json():
    assert _sql_type_to_bvtype("INT[][]") is BVType.JSON

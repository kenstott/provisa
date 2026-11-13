# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b9d21-8c47-4e05-a1d3-7b2e9f6c5a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ClickHouse result is described as the type PostgreSQL returns (REQ-2167, REQ-2171).

Run 43361055823 found 76 calls whose ClickHouse result was described with another type than
PostgreSQL's, by (the oid described, PostgreSQL's): (25, 17) a send function's bytes, which
ClickHouse holds as a String and Arrow reads as text; (20, 23) a window function's or
percentile_disc's value of its argument's type, which no call type resolved; (17, 1022),
(17, 1016), (17, 1009) a NULL of an array type, which ClickHouse answers as a NULL of no type;
(25, 1007), (25, 1022), (114, 1007) an array ClickHouse answers as a Variant of it and NULL, or of
arrays of arrays; (25, 1083) time_in's text. Each case is described with the served type ClickHouse
gives it (``arrow_type_name`` of chdb's Arrow), and its value encoded as PostgreSQL's. Rows the run
found exact on ClickHouse for the same types are kept as they are."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import datetime
from typing import Any, Callable

import pyarrow as pa
import pytest
from buenavista.core import BVType

from provisa._encoders import pgwire as encoder
from provisa.federation.runtime_support import arrow_type_name
from provisa.pgwire import cast_types
from provisa.pgwire.server import (
    BVTYPE_TO_OID,
    _describe_casts,
    _sql_type_to_bvtype,
    _type_oid,
    coercer_names,
)


def _described(sql: str, served: str) -> tuple[Any, int, list[str]]:
    """How pgwire describes a one-column result ClickHouse serves as ``served`` (its Arrow type's
    name): the wire type, the oid and the coercers its values go through."""
    types: list[Any] = [_sql_type_to_bvtype(served)]
    oids: list[int | None] = [_type_oid(served)]
    typmods = [-1]
    coerce: list[tuple[int, Callable[[Any], Any]]] = []
    _describe_casts(sql, types, oids, typmods, coerce, [served])
    (names,) = coercer_names(coerce, 1)
    return types[0], oids[0] if oids[0] is not None else BVTYPE_TO_OID[types[0]], names


# --- (25, 17): a send function's bytes, served as ClickHouse's String ------------------------

_SENDS = [
    'SELECT pg_catalog."int4send"(NULL::integer) AS v',
    'SELECT pg_catalog."anyarray_send"(NULL::integer[]) AS v',
    'SELECT pg_catalog."byteain"(NULL::cstring) AS v',
    'SELECT pg_catalog."byteasend"(NULL::bytea) AS v',
    'SELECT pg_catalog."textsend"(NULL::text) AS v',
    'SELECT pg_catalog."uuid_send"(NULL::uuid) AS v',
    'SELECT pg_catalog."void_send"(NULL::void) AS v',
    "SELECT pg_catalog.int4send(1) AS v",
]


@pytest.mark.parametrize("sql", _SENDS)
def test_a_send_served_as_text_is_described_as_bytea(sql):
    assert _described(sql, "VARCHAR") == (BVType.BYTES, 17, ["text_bytes"])


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."sha224"(NULL::bytea) AS v',
        'SELECT pg_catalog."sha256"(NULL::bytea) AS v',
        "SELECT pg_catalog.sha512('\\x6162'::bytea) AS v",
    ],
)
def test_a_digest_served_as_bytes_keeps_its_bytea(sql):
    """Exact on ClickHouse in the run: a FixedString reaches Arrow as bytes."""
    assert _described(sql, "BLOB") == (BVType.BYTES, 17, [])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("value", [b"\x00\x00\x00\x01", b"abc", b"", "é".encode()])
def test_the_encoder_writes_the_strings_bytes_as_the_bytea(value, fmt):
    as_text = encoder.encode_rows([(value.decode(),)], ["BYTES"], [fmt], [["text_bytes"]])
    assert as_text == encoder.encode_rows([(value,)], ["BYTES"], [fmt])


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_the_encoder_writes_bytes_clickhouse_held_as_a_string_as_they_are(fmt):
    """int4send(255): bytes no text holds, read as bytes (clickhouse_runtime.bytes_where_not_text),
    which text_bytes leaves as they are: \\x000000ff (run 43382046085 failed XX000 here)."""
    sql = "SELECT pg_catalog.int4send(255) AS v"
    assert _described(sql, "VARCHAR") == (BVType.BYTES, 17, ["text_bytes"])
    sent = encoder.encode_rows([(b"\x00\x00\x00\xff",)], ["BYTES"], [fmt], [["text_bytes"]])
    assert sent == encoder.encode_rows([(b"\x00\x00\x00\xff",)], ["BYTES"], [fmt])
    if fmt == 0:
        assert sent.endswith(b"\\x000000ff")


def test_the_reference_reads_a_string_as_its_utf8_bytes():
    assert cast_types.bytes_of_text("\x00\x01é") == b"\x00\x01\xc3\xa9"
    assert cast_types.bytes_of_text(b"\x01") == b"\x01"
    assert cast_types.bytes_of_text(None) is None


def test_a_cast_to_bytea_served_as_text_is_still_text():
    """A cast's text is the engine's own form: only a call's is held as its bytes."""
    assert _described("SELECT CAST(x AS bytea) AS v FROM t", "VARCHAR")[1] == 25


# --- (25, 1083): time_in's text --------------------------------------------------------------


def test_time_in_served_as_text_is_described_as_time():
    sql = 'SELECT pg_catalog."time_in"(NULL::cstring, NULL::oid, NULL::integer) AS v'
    assert _described(sql, "VARCHAR") == (BVType.TIME, 1083, ["clock_text", "clock"])


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."time_larger"(NULL::time without time zone, '
        "NULL::time without time zone) AS v",
        "SELECT pg_catalog.time_smaller('01:02:03'::time, '04:05:06.5'::time) AS v",
    ],
)
def test_a_time_served_as_time64_keeps_its_clock(sql):
    """Exact on ClickHouse in the run: a Time64 reaches Arrow as the bytes of its count."""
    assert _described(sql, "BLOB")[:2] == (BVType.TIME, 1083)


@pytest.mark.parametrize(
    ("text", "clock"),
    [
        ("01:02:03", datetime.time(1, 2, 3)),
        ("23:59:59.999999", datetime.time(23, 59, 59, 999999)),
        ("00:00:00.5", datetime.time(0, 0, 0, 500000)),
    ],
)
def test_time_out_text_is_the_time_it_reads_as(text, clock):
    assert cast_types.clock_of_text(text) == clock


@pytest.mark.parametrize("text", ["", "1:02:03", "24:00:00", "01:60:00", "01:02:03.1234567"])
def test_a_text_time_out_does_not_write_is_refused(text):
    with pytest.raises(ValueError, match="invalid input syntax for type time"):
        cast_types.clock_of_text(text)


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_the_encoder_writes_the_text_as_the_time(fmt):
    as_text = encoder.encode_rows([("04:05:06.5",)], ["TIME"], [fmt], [["clock_text", "clock"]])
    as_time = encoder.encode_rows([(datetime.time(4, 5, 6, 500000),)], ["TIME"], [fmt])
    assert as_text == as_time


# --- (20, 23): a window function's and percentile_disc's value of its argument's type ---------


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."first_value"(NULL::integer) OVER () AS v',
        'SELECT pg_catalog."last_value"(NULL::integer) OVER () AS v',
        'SELECT pg_catalog."lag"(NULL::integer) OVER () AS v',
        'SELECT pg_catalog."lead"(NULL::integer) OVER () AS v',
        'SELECT pg_catalog."percentile_disc"(NULL::double precision) WITHIN GROUP '
        "(ORDER BY NULL::integer) AS v",
        "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY NULL::integer) AS v",
    ],
)
def test_a_value_of_its_arguments_type_is_described_as_that_type(sql):
    assert _described(sql, "BIGINT") == (BVType.INTEGER, 23, ["int"])


def test_lag_with_a_default_of_another_type_is_not_described_by_its_first():
    """lag(value, offset, default) returns the common type of value and default."""
    sql = "SELECT lag(NULL::integer, 1, NULL::bigint) OVER () AS v"
    assert _described(sql, "BIGINT") == (BVType.BIGINT, 20, [])


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."abs"(NULL::integer) AS v',
        'SELECT pg_catalog."ntile"(NULL::integer) OVER () AS v',
    ],
)
def test_an_integer_call_served_as_bigint_keeps_its_integer(sql):
    assert _described(sql, "BIGINT") == (BVType.INTEGER, 23, ["int"])


# --- (17, 1022), (17, 1016), (17, 1009): a NULL of an array type -----------------------------


@pytest.mark.parametrize(
    ("sql", "described"),
    [
        (
            'SELECT pg_catalog."float8_accum"(NULL::double precision[], NULL::double precision) '
            "AS v",
            (BVType.FLOATARRAY, 1022),
        ),
        (
            'SELECT pg_catalog."float8_combine"(NULL::double precision[], '
            "NULL::double precision[]) AS v",
            (BVType.FLOATARRAY, 1022),
        ),
        (
            'SELECT pg_catalog."int2_avg_accum"(NULL::bigint[], NULL::smallint) AS v',
            (BVType.BIGINTARRAY, 1016),
        ),
        (
            'SELECT pg_catalog."int4_avg_accum_inv"(NULL::bigint[], NULL::integer) AS v',
            (BVType.BIGINTARRAY, 1016),
        ),
        (
            'SELECT pg_catalog."tsvector_to_array"(NULL::tsvector) AS v',
            (BVType.STRINGARRAY, 1009),
        ),
        (
            'SELECT pg_catalog."ts_lexize"(NULL::regdictionary, NULL::text) AS v',
            (BVType.STRINGARRAY, 1009),
        ),
    ],
)
def test_a_null_of_no_type_is_described_as_the_array_the_call_returns(sql, described):
    assert _described(sql, "BLOB") == (*described, ["null_alone"])


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.\"int2_avg_accum\"('{3,6}'::bigint[], 3::smallint) AS v",
        "SELECT pg_catalog.\"int4_avg_accum_inv\"('{3,6}'::bigint[], 3::integer) AS v",
    ],
)
def test_a_bigint_array_served_as_integers_of_another_width_is_bigint(sql):
    """Run 43382046085: ClickHouse's Int64 array is named INTEGER[] (arrow_type_name names an array
    of integers by none of its widths); the call's bigint[] is what it is described as."""
    assert _described(sql, "INTEGER[]")[:2] == (BVType.BIGINTARRAY, 1016)


def test_an_engines_own_double_array_takes_no_coercer():
    sql = 'SELECT pg_catalog."float8_accum"(NULL::double precision[], NULL::double precision) AS v'
    assert _described(sql, "DOUBLE[]") == (BVType.FLOATARRAY, 1022, [])


# --- (25, 1007), (25, 1022), (114, 1007): ClickHouse's Variant of an array, and nested arrays --


_INT_LIST = pa.list_(pa.field("item", pa.int32()))


@pytest.mark.parametrize(
    ("arrow", "name"),
    [
        (pa.dense_union([pa.field("Array", _INT_LIST), pa.field("NULL", pa.null())]), "INTEGER[]"),
        (
            pa.dense_union(
                [pa.field("Array", pa.list_(pa.float64())), pa.field("NULL", pa.null())]
            ),
            "DOUBLE[]",
        ),
        (pa.list_(pa.field("item", _INT_LIST, nullable=False)), "INTEGER[]"),
        (pa.list_(pa.float64()), "DOUBLE[]"),
        (pa.list_(pa.float32()), "REAL[]"),
        (pa.list_(pa.int32()), "INTEGER[]"),
        (pa.list_(pa.string()), "VARCHAR[]"),
        (pa.list_(pa.struct([pa.field("a", pa.int32())])), "JSON"),
        (pa.dense_union([pa.field("a", pa.int32()), pa.field("b", pa.string())]), "VARCHAR"),
    ],
)
def test_an_arrow_type_is_named_as_the_type_it_holds(arrow, name):
    assert arrow_type_name(arrow) == name


@pytest.mark.parametrize(
    ("sql", "served", "oid"),
    [
        (
            "SELECT pg_catalog.\"array_larger\"('{1,2,3}'::integer[], '{1,2,3}'::integer[]) AS v",
            "INTEGER[]",
            1007,
        ),
        ("SELECT pg_catalog.\"array_agg\"('{1}'::integer[]) AS v", "INTEGER[]", 1007),
        (
            'SELECT pg_catalog."percentile_disc"(NULL::double precision[]) WITHIN GROUP '
            "(ORDER BY NULL::integer) AS v",
            "INTEGER[]",
            1007,
        ),
        (
            'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) WITHIN GROUP '
            "(ORDER BY NULL::double precision) AS v",
            "DOUBLE[]",
            1022,
        ),
        # Exact on ClickHouse in the run.
        (
            'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v',
            "INTEGER[]",
            1007,
        ),
    ],
)
def test_an_array_is_described_as_its_array_type(sql, served, oid):
    assert _described(sql, served)[1] == oid


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
def test_the_encoder_writes_an_array_of_arrays_as_one_array(fmt):
    written = encoder.encode_rows([([[1, 2], [3, None]],)], ["INTEGERARRAY"], [fmt])
    if fmt == 0:
        assert written.endswith(b"{{1,2},{3,NULL}}")
    else:
        assert written.startswith(b"D")

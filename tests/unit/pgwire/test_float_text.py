# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f0d3e-8c47-4a52-9e1d-72a5c0f4b8d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FLOAT TEXT (REQ-2167, LOCKS, FLOATS AND SERVER ANSWERS): float8out and float4out as PostgreSQL
writes them -- 2 not 2.0, NaN not nan, Infinity -- in the text format of a result."""

# Requirements: REQ-2167, REQ-2194
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import random
import struct
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest
from buenavista.core import BVType

from tests.unit.pgwire import wire_cells as wire

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

FLOAT8_TEXT = [
    (2.0, "2"),
    (-1.0, "-1"),
    (0.0, "0"),
    (-0.0, "-0"),
    (0.5, "0.5"),
    (100.25, "100.25"),
    (0.1, "0.1"),
    (1e14, "100000000000000"),
    (1e15, "1e+15"),
    (1.2345678901234568e17, "1.2345678901234568e+17"),
    (0.0001, "0.0001"),
    (1e-5, "1e-05"),
    (1e100, "1e+100"),
    (float("nan"), "NaN"),
    (float("inf"), "Infinity"),
    (float("-inf"), "-Infinity"),
]


@pytest.mark.parametrize(("value", "text"), FLOAT8_TEXT)
def test_float8_text_is_what_float8out_writes(value: float, text: str) -> None:
    assert wire.text(BVType.FLOAT, value) == text


def _as_float4(value: float) -> float:
    return struct.unpack("!f", struct.pack("!f", value))[0]


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (2.0, "2"),
        (-1.5, "-1.5"),
        (1.1, "1.1"),
        (0.1, "0.1"),
        # float4out goes to exponents at 10**6, not float8out's 10**15 (PostgreSQL 16.11 answered).
        (100000.0, "100000"),
        (123456.0, "123456"),
        (999999.0, "999999"),
        (1e6, "1e+06"),
        (1.5e6, "1.5e+06"),
        (1234567.0, "1.234567e+06"),
        (16777216.0, "1.6777216e+07"),
        (-1e6, "-1e+06"),
        (0.0001, "0.0001"),
        (1e-5, "1e-05"),
        (3.4028234663852886e38, "3.4028235e+38"),
        # PostgreSQL 17's float4out (Ryu built with STRICTLY_SHORTEST 0): a decimal on the edge of
        # the float's rounding interval is never taken, so 5.442255e+07 is not this float's text.
        (54422552.0, "5.4422552e+07"),
        (1 / 3, "0.33333334"),
        (123456.7, "123456.7"),
        (struct.unpack("!f", (1).to_bytes(4, "big"))[0], "1e-45"),
        (struct.unpack("!f", (0x00800000).to_bytes(4, "big"))[0], "1.1754944e-38"),
        (16777217.0, "1.6777216e+07"),
        (float("nan"), "NaN"),
        (float("-inf"), "-Infinity"),
    ],
)
def test_float4_text_is_the_shortest_form_that_reads_back(value: float, text: str) -> None:
    held = value if value != value or value in (float("inf"), float("-inf")) else _as_float4(value)
    assert wire.text(BVType.REAL, held) == text


def _named_float4(text: str) -> bytes:
    """The float4 a client reads ``text`` as (strtof, correctly rounded), as its 4 bytes: the
    float4 nearest the decimal, found exactly among the neighbours of a double's guess."""
    exact = Fraction(Decimal(text))
    guess = struct.unpack("!I", struct.pack("!f", float(text)))[0]
    sign = guess & 0x80000000
    near = [sign | m for m in (guess - 1, guess, guess + 1) if 0 <= (m & 0x7FFFFFFF) < 0x7F800000]
    distance = {
        m: abs(exact - Fraction(struct.unpack("!f", m.to_bytes(4, "big"))[0])) for m in near
    }
    best = min(distance.values())
    named = [m for m, d in distance.items() if d == best]
    assert len(named) == 1, f"{text} lies halfway between two float4 values"
    return named[0].to_bytes(4, "big")


@pytest.mark.parametrize(
    ("value", "text"),
    [
        # J492a defect 4: the old text, -3.57134089e-09, named a float4 other than the one binary sent.
        (-3.5713408890908175e-09, "-3.5713408e-09"),
        (3168.0175161495904, "3168.0176"),
        (5.295092292408126e-31, "5.2950924e-31"),
    ],
)
def test_a_double_described_as_real_is_written_as_the_float4_binary_sends(
    value: float, text: str
) -> None:
    assert wire.text(BVType.REAL, value) == text
    assert _named_float4(text) == wire.binary(BVType.REAL, value)


def test_real_text_and_binary_name_the_same_float4_for_random_doubles() -> None:
    """Text and binary of a double described as REAL name one float4, and the text is that float4's
    own float4out. The assertion is which float is named, so it is exact."""
    rng = random.Random(2167)
    values = [rng.uniform(-1, 1) * 10 ** rng.randint(-44, 38) for _ in range(100_000)]
    values = [v for v in values if abs(v) < 3.4e38]
    texts = [t.decode() for t in wire.cells(BVType.REAL, values, 0)]
    binaries = wire.cells(BVType.REAL, values, 1)
    own = [t.decode() for t in wire.cells(BVType.REAL, [_as_float4(v) for v in values], 0)]
    wrong = [
        v
        for v, text, binary, own_text in zip(values, texts, binaries, own, strict=True)
        if _named_float4(text) != binary or text != own_text
    ]
    assert wrong == []


def _text(conn: Any, sql: str) -> tuple[int, list[str]]:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    return res.ftype(0), [res.get_value(i, 0).decode() for i in range(res.ntuples)]


@pytest.mark.parametrize(
    ("sql", "oid", "text"),
    [
        ("SELECT 2.0::float8 AS v", 701, "2"),
        ("SELECT (-1.0)::float8 AS v", 701, "-1"),
        ("SELECT 'NaN'::float8 AS v", 701, "NaN"),
        ("SELECT 'Infinity'::float8 AS v", 701, "Infinity"),
        ("SELECT '-Infinity'::float8 AS v", 701, "-Infinity"),
        ("SELECT 2.0::float4 AS v", 700, "2"),
        ("SELECT 'NaN'::float4 AS v", 700, "NaN"),
        ("SELECT 1000000::float4 AS v", 700, "1e+06"),
        ("SELECT 16777216::float4 AS v", 700, "1.6777216e+07"),
        ("SELECT '{1000000}'::float4[] AS v", 1021, "{1e+06}"),
    ],
)
def test_a_result_carries_float_text_as_postgresql_writes_it(
    conn: Any, sql: str, oid: int, text: str
) -> None:
    assert _text(conn, sql) == (oid, [text])

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

# Requirements: REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import struct
from typing import Any

import pytest
from buenavista.postgres import BVTYPE_TO_PGTYPE, BVType, _float4_text, _float8_text

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
    assert _float8_text(value) == text
    assert BVTYPE_TO_PGTYPE[BVType.FLOAT][1](value) == text


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
        (float("nan"), "NaN"),
        (float("-inf"), "-Infinity"),
    ],
)
def test_float4_text_is_the_shortest_form_that_reads_back(value: float, text: str) -> None:
    held = value if value != value or value in (float("inf"), float("-inf")) else _as_float4(value)
    assert _float4_text(held) == text
    assert BVTYPE_TO_PGTYPE[BVType.REAL][1](held) == text


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

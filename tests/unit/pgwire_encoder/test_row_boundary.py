# Copyright (c) 2026 Kenneth Stott
# Canary: 47e61ca1-2b87-4dcd-a25c-7f93a9f740c8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The fetch boundary of a row-returning engine (REQ-2194, docs/arch/fetch-paths.md): rows of the
Python values its driver hands over become one Arrow batch (``ir_arrow.row_batch``), and what
pgwire sends of them is PostgreSQL 17's answer for those values -- the product's former Python
writers given the values themselves (oracle.py), each defect corrected (pg_reference.pg17).
Floats within one part in 10^14, as the differential compares them."""

from __future__ import annotations

import datetime as dt
import io
import ipaddress
import uuid
import zoneinfo
from decimal import Decimal
from typing import Any

import pytest
from buenavista.core import BVType
from buenavista.postgres import BuenaVistaHandler
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import infinite_date, infinite_timestamp
from provisa.executor.result import QueryResult
from provisa.pgwire.server import ProvisaQueryResult
from tests.unit.pgwire_encoder import oracle, python_ref
from tests.unit.pgwire_encoder.pg_reference import pg17
from tests.unit.pgwire_encoder.python_ref import _Sink
from tests.unit.pgwire_encoder.test_differential import _assert_same

T = BVType
UTC = dt.timezone.utc
PLUS2 = dt.timezone(dt.timedelta(hours=2))
MINUS5 = dt.timezone(dt.timedelta(hours=-5, minutes=-30))
NY = zoneinfo.ZoneInfo("America/New_York")

#: (case, wire type, the engine's type name or None, the column's values as its driver gives them)
CASES: list[tuple[str, Any, str | None, list[Any]]] = [
    ("ints", T.BIGINT, "BIGINT", [1, -(2**63), 2**63 - 1, None]),
    ("int4", T.INTEGER, "INTEGER", [0, -(2**31), 2**31 - 1]),
    ("bools", T.BOOL, "BOOLEAN", [True, False, None]),
    ("floats", T.FLOAT, "DOUBLE", [1.5, -0.0, float("inf"), float("-inf"), float("nan"), 1e300]),
    ("reals", T.REAL, "REAL", [1.5, 0.1, -3.25]),
    ("decimal_one_scale", T.DECIMAL, "NUMERIC", [Decimal("1.50"), Decimal("-2.25"), None]),
    ("decimal_scales", T.DECIMAL, "NUMERIC", [Decimal("1.5"), Decimal("1.25"), Decimal("100")]),
    (
        "decimal_exponents",
        T.DECIMAL,
        "NUMERIC",
        [Decimal("1E-7"), Decimal("1E+16"), Decimal("-0.00")],
    ),
    (
        "decimal_special",
        T.DECIMAL,
        "NUMERIC",
        [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
    ),
    ("decimal_wide", T.DECIMAL, "NUMERIC", [Decimal("1234567890123456789012345678901234567890.5")]),
    ("decimal_as_float", T.FLOAT, "DOUBLE", [Decimal("1.5"), Decimal("0.1")]),
    ("text", T.TEXT, "VARCHAR", ["a", "", "é ✓", None]),
    ("bytes", T.BYTES, "BYTEA", [b"\x00\xff", b"", None]),
    ("memoryview", T.BYTES, "BYTEA", [memoryview(b"ab")]),
    ("dates", T.DATE, "DATE", [dt.date(2024, 1, 2), dt.date(1, 1, 1), dt.date(9999, 12, 31)]),
    ("date_infinity", T.DATE, "DATE", [infinite_date(1), dt.date(2024, 1, 2), infinite_date(-1)]),
    (
        "timestamps",
        T.TIMESTAMP,
        "TIMESTAMP",
        [dt.datetime(2024, 1, 2, 3, 4, 5, 6), dt.datetime(1, 1, 1)],
    ),
    (
        "timestamp_infinity",
        T.TIMESTAMP,
        "TIMESTAMP",
        [
            infinite_timestamp(1, aware=False),
            dt.datetime(2024, 1, 2),
            infinite_timestamp(-1, aware=False),
        ],
    ),
    (
        "tstz_one_offset",
        T.TIMESTAMPTZ,
        "TIMESTAMPTZ",
        [dt.datetime(2024, 1, 2, 3, tzinfo=PLUS2), dt.datetime(2024, 7, 2, tzinfo=PLUS2)],
    ),
    ("tstz_utc", T.TIMESTAMPTZ, "TIMESTAMPTZ", [dt.datetime(2024, 1, 2, 3, tzinfo=UTC)]),
    (
        "tstz_infinity",
        T.TIMESTAMPTZ,
        "TIMESTAMPTZ",
        [infinite_timestamp(1, aware=True), dt.datetime(2024, 1, 2, tzinfo=UTC)],
    ),
    (
        "times",
        T.TIME,
        "TIME",
        [dt.time(12, 30, 45, 500000), dt.time(0, 0), dt.time(23, 59, 59, 999999)],
    ),
    (
        "timetz_one_offset",
        T.TIMETZ,
        "TIMETZ",
        [dt.time(12, 30, 45, tzinfo=PLUS2), dt.time(1, 2, tzinfo=PLUS2)],
    ),
    (
        "timetz_offsets",
        T.TIMETZ,
        "TIMETZ",
        [dt.time(12, 30, 45, tzinfo=PLUS2), dt.time(1, 2, 3, 4, tzinfo=MINUS5)],
    ),
    (
        "intervals",
        T.INTERVAL,
        "INTERVAL",
        [dt.timedelta(days=1, seconds=3), dt.timedelta(microseconds=-1), dt.timedelta(0)],
    ),
    (
        "interval_months",
        T.INTERVAL,
        "INTERVAL",
        [relativedelta(months=14, days=3, hours=4), dt.timedelta(days=2)],
    ),
    (
        "uuids",
        T.UUID,
        "UUID",
        [uuid.UUID(int=5), uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), None],
    ),
    (
        "inet",
        T.INET,
        "INET",
        [ipaddress.ip_interface("10.0.0.1/8"), ipaddress.ip_interface("::1/128")],
    ),
    (
        "inet_address",
        T.INET,
        "INET",
        [ipaddress.ip_address("10.0.0.1"), ipaddress.ip_address("::ffff:1.2.3.4")],
    ),
    (
        "cidr",
        T.CIDR,
        "CIDR",
        [ipaddress.ip_network("10.0.0.0/8"), ipaddress.ip_network("2001:db8::/32")],
    ),
    ("json_docs", T.JSON, "JSON", [{"a": 1, "b": [1, 2]}, {"c": None}, [1, "x"], "text", 3]),
    ("jsonb_docs", T.JSONB, "JSONB", [{"a": 1.5}, {"b": {"c": True}}]),
    ("json_same_keys", T.JSONB, "JSONB", [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
    ("int_array", T.INTEGERARRAY, "INTEGER[]", [[1, 2, None], [], None]),
    ("int_array_2d", T.INTEGERARRAY, "INTEGER[]", [[[1, 2], [3, 4]], [[5, None]]]),
    ("text_array", T.STRINGARRAY, "TEXT[]", [["a", "b c", None, '"q"'], []]),
    (
        "numeric_array",
        T.DECIMALARRAY,
        "NUMERIC[]",
        [[Decimal("1.5"), Decimal("1.25")], [Decimal("1E-7")]],
    ),
    ("float_array", T.FLOATARRAY, "DOUBLE[]", [[1.5, float("nan")], [0.1]]),
    ("bool_array", T.BOOLARRAY, "BOOLEAN[]", [[True, False, None]]),
    ("money", T.MONEY, "MONEY", [Decimal("1234.5"), Decimal("-0.01")]),
    ("inet_mapped", T.INET, "INET", [ipaddress.ip_interface("::ffff:1.2.3.4/128")]),
    ("cidr_of_interface", T.CIDR, "CIDR", [ipaddress.ip_interface("10.1.0.0/16")]),
    ("text_of_uuid", T.TEXT, "VARCHAR", [uuid.UUID(int=7)]),
    ("text_of_decimal", T.TEXT, "VARCHAR", [Decimal("1.50"), Decimal("1.5")]),
    ("text_of_dict", T.TEXT, "VARCHAR", [{"a": 1}, {"b": 2}]),
    ("xml", T.XML, "XML", ["<a>1</a>"]),
    ("oid", T.OID, "OID", [0, 2**32 - 1]),
]


def _rows(values: list[Any]) -> list[tuple]:
    return [(v,) for v in values]


def _reference(bvtype: Any, engine: str | None, values: list[Any], fmt: int) -> Any:
    qr = ProvisaQueryResult(QueryResult(rows=[], column_names=["c"], column_types=[engine]))
    qr._types = [bvtype]
    qr.result_format = [fmt]
    out = io.BytesIO()
    with pg17():
        try:
            oracle.send_data_rows(out, qr, (python_ref.COERCE(qr, r) for r in _rows(values)))
        except Exception as e:  # noqa: BLE001 -- a raise is an outcome to compare
            return e
    return out.getvalue()


def _wired(bvtype: Any, engine: str | None, values: list[Any], fmt: int) -> Any:
    qr = ProvisaQueryResult(
        QueryResult(rows=_rows(values), column_names=["c"], column_types=[engine])
    )
    qr._types = [bvtype]
    qr.result_format = [fmt]
    sink = _Sink(io.BytesIO())
    try:
        BuenaVistaHandler.send_data_rows(sink, qr)  # type: ignore[arg-type]
    except Exception as e:  # noqa: BLE001
        return e
    return sink.wfile.getvalue()


@pytest.mark.parametrize("fmt", [0, 1], ids=["text", "binary"])
@pytest.mark.parametrize("case", CASES, ids=[c[0] for c in CASES])
def test_driver_values(case: tuple[str, Any, str | None, list[Any]], fmt: int) -> None:
    _, bvtype, engine, values = case
    reference = _reference(bvtype, engine, values, fmt)
    wired = _wired(bvtype, engine, values, fmt)
    name = getattr(bvtype, "name", bvtype)
    _assert_same(reference, wired, [name], [fmt])

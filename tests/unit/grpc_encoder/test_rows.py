# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6f3d2a-9b41-4e8a-a7c5-3e2d81f40b96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row result is encoded from its rows (REQ-2195): the gRPC encoder reads a driver's cells as
they are (``Encoder.encode_rows``), never through Arrow. test_differential.py holds the rows form
to the Arrow form's bytes for every value both hold; this file covers the values a row source
hands back that Arrow holds otherwise or not at all."""

from __future__ import annotations

import datetime as dt
import ipaddress
import uuid
from decimal import Decimal
from typing import Any

import pytest
import pytz
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import infinite_date, infinite_timestamp, interval_value
from tests.unit.grpc_encoder.python_ref import Service, rows_encode


@pytest.fixture(scope="module")
def svc() -> Service:
    return Service()


def sent(svc: Service, column: str, value: Any, declared: str | None = None) -> Any:
    """The one Orders message a one-row, one-column row result is written as."""
    (m,) = rows_encode(svc.wire, "Orders", [[(value,)]], [column], types=[declared])
    return svc.wire.Orders.FromString(m)


def test_a_uuid_is_its_text(svc):
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert sent(svc, "tag", value).tag == str(value)


@pytest.mark.parametrize(
    "value,declared,text",
    [
        (ipaddress.ip_interface("10.0.0.1/32"), "inet", "10.0.0.1"),
        (ipaddress.ip_interface("10.0.0.1/24"), "inet", "10.0.0.1/24"),
        (ipaddress.ip_network("10.0.0.0/32"), "cidr", "10.0.0.0/32"),
        (ipaddress.ip_address("::ffff:1.2.3.4"), "inet", "::ffff:1.2.3.4"),
    ],
)
def test_an_address_is_postgres_text(svc, value, declared, text):
    assert sent(svc, "name", value, declared).name == text


@pytest.mark.parametrize("value", [b"\x00\xff", bytearray(b"\x00\xff"), memoryview(b"\x00\xff")])
def test_bytes_of_every_buffer_type(svc, value):
    assert sent(svc, "blob", value).blob == b"\x00\xff"
    assert sent(svc, "name", value).name == "\\x00ff"


def test_intervals(svc):
    assert sent(svc, "span", dt.timedelta(days=-1, seconds=5)).span == "-PT23H59M55S"
    assert sent(svc, "span", relativedelta(months=14, days=3)).span == "P1Y2M3D"
    assert sent(svc, "span", interval_value(0, 0, 26 * 3600 * 1_000_000)).span == "PT26H"


def test_infinities(svc):
    assert sent(svc, "day", infinite_date(1)).day == "infinity"
    assert sent(svc, "day", infinite_date(-1)).day == "-infinity"
    m = sent(svc, "created_tz", infinite_timestamp(1, aware=True))
    assert m.created_tz.seconds == 253402300799
    assert sent(svc, "name", infinite_timestamp(-1, aware=False)).name == "-infinity"


def test_an_int_past_i128(svc):
    big = 2**130
    assert sent(svc, "name", big).name == str(big)
    with pytest.raises(ValueError, match="out of range"):
        sent(svc, "id", big)


def test_decimals(svc):
    assert sent(svc, "name", Decimal("1.50")).name == "1.50"
    assert sent(svc, "name", Decimal("-0.00")).name == "0.00"
    assert sent(svc, "name", Decimal("1E+3")).name == "1E+3"
    assert sent(svc, "name", Decimal("NaN")).name == "NaN"
    assert sent(svc, "name", Decimal("-Infinity")).name == "-Infinity"
    assert sent(svc, "price", Decimal("2.5")).price == 2.5
    digits = "9" * 60
    assert sent(svc, "name", Decimal(digits)).name == digits


def test_a_dict_is_a_json_object(svc):
    assert sent(svc, "payload", {"a": 1, "b": [None, "x"]}).payload == '{"a":1,"b":[null,"x"]}'
    assert sent(svc, "payload", {}).payload == "{}"
    # a dict whose keys are not all text has no JSON object: json.dumps of it
    assert sent(svc, "payload", {1: "x"}, "json").payload == '{"1": "x"}'


def test_a_tuple_is_a_list(svc):
    assert sent(svc, "payload", (1, "a", None)).payload == '[1,"a",null]'


def test_a_time_with_a_zone_is_timetz_text(svc):
    value = dt.time(10, 30, 0, 500000, tzinfo=dt.timezone(dt.timedelta(hours=-3, minutes=-30)))
    assert sent(svc, "at", value).at == "10:30:00.5-03:30"


def test_a_pytz_datetime_is_read_by_zoneinfo(svc):
    # pytz has no daylight time after 2037 (12:00 at -05:00 is 17:00 UTC); zoneinfo, as
    # PostgreSQL, writes that instant in daylight time
    value = pytz.timezone("America/New_York").localize(dt.datetime(2040, 7, 1, 12, 0))
    assert sent(svc, "name", value).name == "2040-07-01 13:00:00-04:00"


def test_any_other_value_is_its_text(svc):
    class Point:
        def __str__(self) -> str:
            return "(1,2)"

    assert sent(svc, "name", Point()).name == "(1,2)"


def test_a_row_is_any_sequence(svc):
    class Record:
        """asyncpg's Record: indexed, sized, not a tuple."""

        def __init__(self, *cells: Any) -> None:
            self._cells = cells

        def __len__(self) -> int:
            return len(self._cells)

        def __getitem__(self, i: int) -> Any:
            return self._cells[i]

    (m,) = rows_encode(svc.wire, "Orders", [[Record(7, "x")]], ["id", "name"])
    msg = svc.wire.Orders.FromString(m)
    assert (msg.id, msg.name) == (7, "x")


def test_a_row_of_the_wrong_width_is_refused(svc):
    with pytest.raises(ValueError, match="2 columns named for a row of 1"):
        rows_encode(svc.wire, "Orders", [[(7,)]], ["id", "name"])


def test_nulls_are_absent(svc):
    (m,) = rows_encode(svc.wire, "Orders", [[(None, None)]], ["id", "name"])
    assert m == b""

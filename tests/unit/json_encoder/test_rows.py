# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8e1f6a-5c27-4b90-8a1e-c4f7d2b96e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row result is encoded from its rows (REQ-2195): the JSON encoders read a driver's cells as
they are (``RowBatches``), never through Arrow. encoders.py holds every differential case's rows
form to its Arrow form; this file covers the values a driver hands back, each held to what the
Arrow form answered of it (built by ir_arrow.row_batch), and ``result_batches``' choice of form."""

from __future__ import annotations

import datetime as dt
import ipaddress
import json
import uuid
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
import pytz
from dateutil.relativedelta import relativedelta

from provisa._encoders import json as encoders
from provisa.core.ir_arrow import row_batch
from provisa.core.ir_types import infinite_date, infinite_timestamp, interval_value
from provisa.executor import json_encoders as je
from provisa.executor.result import ArrowResult, QueryResult, StreamingQueryResult
from tests.unit.json_encoder.cases import C


class _Point:
    def __str__(self) -> str:
        return "(1,2)"


# (value, the engine's type name, column mode)
DRIVER_VALUES: list[tuple[Any, str | None, str]] = [
    (uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "uuid", "convert"),
    (ipaddress.ip_interface("10.0.0.1/32"), "inet", "convert"),
    (ipaddress.ip_network("10.0.0.0/32"), "cidr", "convert"),
    (memoryview(b"\x00\xff"), "bytea", "convert"),
    (bytearray(b"\x00\xff"), "bytea", "convert"),
    (dt.timedelta(days=-1, seconds=5), "interval", "convert"),
    (relativedelta(years=1, months=2, days=3, hours=4), "interval", "convert"),
    (interval_value(0, 1, 26 * 3600 * 1_000_000), "interval", "convert"),
    (infinite_date(1), "date", "convert"),
    (infinite_timestamp(-1, aware=True), "timestamptz", "convert"),
    (Decimal("NaN"), "numeric", "convert"),
    (Decimal("-0.00"), "numeric", "convert"),
    (Decimal("1E+3"), "numeric", "convert"),
    (Decimal("12345678901234567890.123456789012345678901234567890"), "numeric", "convert"),
    (7, "bigint", "integer"),
    (dt.datetime(2024, 3, 1, 12, 0, tzinfo=dt.timezone.utc), "timestamptz", "convert"),
    (
        dt.datetime(2024, 3, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=5))),
        "timestamptz",
        "convert",
    ),
    (
        pytz.timezone("America/New_York").localize(dt.datetime(2040, 7, 1, 12)),
        "timestamptz",
        "convert",
    ),
    (dt.datetime(2024, 3, 1, 12, 0, 0, 7), "timestamp", "convert"),
    (dt.date(1999, 12, 31), "date", "convert"),
    (dt.time(23, 59, 59, 999999), "time", "convert"),
    ([1, 2, None], "integer[]", "convert"),
    (["a", None, "b c"], "text[]", "convert"),
    ({"a": 1, "b": [True, None]}, "jsonb", "json"),
    ('{"a": 1}', "jsonb", "json"),
    (_Point(), "point", "convert"),
    (True, "boolean", "convert"),
    (1.5, "double precision", "convert"),
    ("xé", "text", "convert"),
]


def _columns(mode: str) -> list:
    return [C("v", json=mode == "json", integer=mode == "integer")]


@pytest.mark.parametrize(
    "value,kind,mode", DRIVER_VALUES, ids=[f"{k}-{i}" for i, (_, k, _) in enumerate(DRIVER_VALUES)]
)
def test_a_driver_value_answers_as_before(value, kind, mode):
    columns = _columns(mode)
    arrow = je.rest([row_batch([(value,)], ["v"], [kind])], columns)
    rows = je.rest(encoders.RowBatches([[(value,)]], [kind]), columns)
    assert rows == arrow


def test_a_zoned_time_row_is_its_time_of_day_with_no_date():
    """A row's timetz is a time of day (GraphQL's and REST's Time, REQ-2171): the date of the
    Arrow form ``row_batch`` holds it as (ir_arrow._clocks) is no part of the value."""
    value = dt.time(3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    answer = je.rest(encoders.RowBatches([[(value,)]], ["timetz"]), _columns("convert"))
    assert json.loads(answer)[0]["v"] == "03:04:05+02:00"


def test_an_int_past_arrows_is_a_number():
    # Arrow held an int past 128 bits as its text; a row result's int is a JSON number
    body, _ = je.rest(encoders.RowBatches([[(2**130,)]]), _columns("convert"))
    assert body == b'{"data":[{"v":%d}]}' % 2**130


def test_a_dict_is_an_object_whatever_its_neighbours_keys():
    # Arrow held dicts of differing keys as their repr's text; a row result's dict is itself
    body, _ = je.rest(encoders.RowBatches([[({"a": 1},), ({"b": 2},)]]), _columns("json"))
    assert json.loads(body) == {"data": [{"v": {"a": 1}}, {"v": {"b": 2}}]}


def test_extra_cells_past_the_plan_are_not_the_answer():
    body, _ = je.rest(encoders.RowBatches([[(1, "extra")]]), [C("n", integer=True)])
    assert json.loads(body) == {"data": [{"n": 1}]}


def test_a_row_shorter_than_the_plan_is_refused():
    with pytest.raises(je.EncoderFailed, match="the row has 1 columns, the plan 2"):
        je.rest(encoders.RowBatches([[(1,)]]), [C("n"), C("m")])


def test_result_batches_takes_each_result_in_the_form_it_has():
    table = pa.table({"n": [1, 2]})
    arrow = ArrowResult(table, ["bigint"], to_rows=lambda t: list(zip(*t.to_pydict().values())))
    assert isinstance(je.result_batches(arrow), list)
    rows = je.result_batches(
        QueryResult(rows=[(1,), (2,)], column_names=["n"], column_types=["bigint"])
    )
    assert isinstance(rows, encoders.RowBatches) and len(rows) == 2
    streamed = je.result_batches(StreamingQueryResult(iter([[(1,)], [], [(2,)]]), ["n"]))
    assert len(streamed) == 2
    assert not je.any_rows(je.result_batches(QueryResult(rows=[], column_names=["n"])))
    assert je.any_rows(arrow and je.result_batches(arrow))

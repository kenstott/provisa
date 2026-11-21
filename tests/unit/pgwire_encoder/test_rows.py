# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3a9c1e-2b84-4d06-9e5f-a1c8d3b27e40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row result is encoded from its rows (REQ-2195): pgwire's encoder reads a driver's cells as
they are (``encode_rows``), never through Arrow. test_differential.py holds the rows form to the
Arrow form's bytes for every value column_values gives; this file covers the values a driver hands
back (each held to what the Arrow form answered of it, built by ir_arrow.row_batch) and the
server's row form: its wire types, a cursor's rows and the session's zone."""

from __future__ import annotations

import datetime as dt
import io
import ipaddress
import uuid
from decimal import Decimal
from typing import Any

import pytest
import pytz
from buenavista.postgres import BuenaVistaHandler
from dateutil.relativedelta import relativedelta

from provisa._encoders import pgwire as encoder
from provisa.core.ir_arrow import row_batch
from provisa.core.ir_types import infinite_date, infinite_timestamp, interval_value
from provisa.executor.result import StreamingQueryResult
from provisa.pgwire.server import ProvisaQueryResult, _RowBuffer
from provisa.pgwire.session_zone import SessionZone, in_session_zone
from tests.unit.pgwire.result_rows import rows_of


class _Point:
    """A geometric value a driver hands back, which no rule reads: its text is str()."""

    def __str__(self) -> str:
        return "(1,2)"


# (value, wire type, the engine's type name)
DRIVER_VALUES: list[tuple[Any, str, str | None]] = [
    (uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "UUID", "uuid"),
    (ipaddress.ip_interface("10.0.0.1/32"), "INET", "inet"),
    (ipaddress.ip_interface("10.0.0.1/24"), "INET", "inet"),
    (ipaddress.ip_interface("::ffff:1.2.3.4/128"), "INET", "inet"),
    (ipaddress.ip_network("10.0.0.0/32"), "CIDR", "cidr"),
    (memoryview(b"\x00\xff"), "BYTES", "bytea"),
    (bytearray(b"\x00\xff"), "BYTES", "bytea"),
    (dt.timedelta(days=-1, seconds=5), "INTERVAL", "interval"),
    (relativedelta(years=1, months=2, days=3, hours=4), "INTERVAL", "interval"),
    (interval_value(0, 1, 26 * 3600 * 1_000_000), "INTERVAL", "interval"),
    (infinite_date(1), "DATE", "date"),
    (infinite_date(-1), "DATE", "date"),
    (infinite_timestamp(1, aware=True), "TIMESTAMPTZ", "timestamptz"),
    (infinite_timestamp(-1, aware=False), "TIMESTAMP", "timestamp"),
    (Decimal("NaN"), "DECIMAL", "numeric"),
    (Decimal("-Infinity"), "DECIMAL", "numeric"),
    (Decimal("-0.00"), "DECIMAL", "numeric"),
    (Decimal("1E+3"), "DECIMAL", "numeric"),
    (Decimal("12345678901234567890.123456789012345678901234567890"), "DECIMAL", "numeric"),
    (2**130, "TEXT", "numeric"),
    (
        dt.time(10, 30, 0, 500000, tzinfo=dt.timezone(dt.timedelta(hours=-3, minutes=-30))),
        "TIMETZ",
        "timetz",
    ),
    (dt.datetime(2024, 3, 1, 12, 0, tzinfo=dt.timezone.utc), "TIMESTAMPTZ", "timestamptz"),
    (
        dt.datetime(2024, 3, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=5))),
        "TIMESTAMPTZ",
        "timestamptz",
    ),
    (
        pytz.timezone("America/New_York").localize(dt.datetime(2040, 7, 1, 12)),
        "TIMESTAMPTZ",
        "timestamptz",
    ),
    (dt.datetime(2024, 3, 1, 12, 0, 0, 7), "TIMESTAMP", "timestamp"),
    (dt.date(1999, 12, 31), "DATE", "date"),
    (dt.time(23, 59, 59, 999999), "TIME", "time"),
    ([1, 2, None], "INTEGERARRAY", "integer[]"),
    (["a", None, "b c"], "STRINGARRAY", "text[]"),
    ({"a": 1, "b": [True, None]}, "JSON", "jsonb"),
    ('{"a": 1}', "JSON", "jsonb"),
    (_Point(), "TEXT", "point"),
    (True, "BOOL", "boolean"),
    (1.5, "FLOAT", "double precision"),
]


def _arrow_form(value: Any, bv: str, kind: str | None, fmt: int) -> bytes:
    """What the encoder answered of the value when a row result was built into Arrow first."""
    return encoder.encode_batch(row_batch([(value,)], ["c"], [kind]), [bv], [fmt])


@pytest.mark.parametrize("fmt", [0, 1])
@pytest.mark.parametrize(
    "value,bv,kind", DRIVER_VALUES, ids=[f"{t}-{i}" for i, (_, t, _) in enumerate(DRIVER_VALUES)]
)
def test_a_driver_value_answers_as_before(value, bv, kind, fmt):
    assert encoder.encode_rows([(value,)], [bv], [fmt], None, [kind]) == _arrow_form(
        value, bv, kind, fmt
    )


def _cell(message: bytes) -> bytes:
    (size,) = __import__("struct").unpack_from("!i", message, 7)
    return message[11 : 11 + size]


def test_a_dict_is_json_whatever_its_neighbours_keys():
    # Arrow held dicts of differing keys as their repr; a row result's dict is itself
    rows = [({"a": 1},), ({"b": 2},)]
    out = encoder.encode_rows(rows, ["JSON"], [0], None, ["jsonb"])
    first = out[: 1 + int.from_bytes(out[1:5], "big")]
    assert _cell(first) == b'{"a": 1}'


def test_decimals_of_differing_scales_keep_their_own():
    out = encoder.encode_rows([(Decimal("1.5"),), (Decimal("2.25"),)], ["DECIMAL"], [0])
    assert b"1.5" in out and b"2.25" in out


def test_a_row_is_any_sequence():
    class Record:
        """asyncpg's Record: indexed, sized, not a tuple."""

        def __init__(self, *cells: Any) -> None:
            self._cells = cells

        def __len__(self) -> int:
            return len(self._cells)

        def __getitem__(self, i: int) -> Any:
            return self._cells[i]

    assert encoder.encode_rows([Record(7, "x")], ["BIGINT", "TEXT"], [0, 0]) == encoder.encode_rows(
        [(7, "x")], ["BIGINT", "TEXT"], [0, 0]
    )


def test_a_row_of_the_wrong_width_is_refused():
    with pytest.raises(ValueError, match="2 types for a row of 1"):
        encoder.encode_rows([(7,)], ["BIGINT", "TEXT"], [0, 0])


def test_a_coercer_never_sees_a_null():
    out = encoder.encode_rows([(None,), (2.0,)], ["INTEGER"], [0], [["int"]])
    assert out == encoder.encode_batch(
        row_batch([(None,), (2.0,)], ["c"]), ["INTEGER"], [0], [["int"]]
    )


# ---- the server's row form ------------------------------------------------------------------


class _Sink:
    def __init__(self) -> None:
        self.wfile = io.BytesIO()


def _sent(result: Any) -> bytes:
    sink = _Sink()
    BuenaVistaHandler.send_data_rows(sink, result)  # type: ignore[arg-type]
    return sink.wfile.getvalue()


def test_a_row_result_reaches_the_encoder_as_rows():
    rows = [(1, "a", Decimal("1.50")), (2, None, Decimal("2.5"))]
    stream = StreamingQueryResult(iter([rows]), ["n", "s", "d"], ["bigint", "text", "numeric"])
    qr = ProvisaQueryResult(stream)
    assert qr.data().rows and not qr.data().raw
    assert _sent(qr) == encoder.encode_rows(rows, ["BIGINT", "TEXT", "DECIMAL"], None)


def test_a_row_results_untyped_columns_are_typed_from_its_rows():
    rows = [(None, None, None, None), (1, 1.5, dt.date(2024, 1, 1), [1, 2])]
    qr = ProvisaQueryResult(StreamingQueryResult(iter([rows]), ["a", "b", "c", "d"]))
    assert [qr.column(i)[1].name for i in range(4)] == ["BIGINT", "FLOAT", "DATE", "INTEGERARRAY"]
    assert rows_of(qr) == [list(r) for r in rows]


def test_a_cursor_holds_a_row_results_rows():
    buffer = _RowBuffer(False, True)
    buffer.add([(1,), (2,)])
    buffer.add([(3,)])
    assert buffer.rows(1, 3) == [[(2,)], [(3,)]]
    assert buffer.reversed_rows(0, 3) == [[(3,)], [(2,), (1,)]]


def test_a_row_results_timestamptz_is_written_in_the_session_zone():
    rows = [
        (dt.datetime(2024, 7, 1, 12, tzinfo=dt.timezone.utc),),
        (infinite_timestamp(1, aware=True),),
    ]
    qr = ProvisaQueryResult(StreamingQueryResult(iter([rows]), ["t"], ["timestamptz"]))
    zone = SessionZone("Europe/Paris")
    written = in_session_zone(qr, zone)
    out = _sent(written)
    assert b"2024-07-01 14:00:00+02" in out and b"infinity" in out

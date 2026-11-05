# Copyright (c) 2026 Kenneth Stott
# Canary: b991ea23-94d5-4f8b-af45-3178c904cba8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every ErrorResponse pgwire sends carries PostgreSQL's required fields: Severity (S and V),
Code (C) and Message (M) (REQ-2167, pgspec TestError/AllFields)."""

from __future__ import annotations

import struct

import duckdb
import pytest

from provisa.pgwire.server import ProvisaHandler


def _fields(exc: BaseException) -> dict[str, str]:
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    handler.send_error(exc)
    blob = b"".join(written)
    assert blob[:1] == b"E"
    (length,) = struct.unpack("!i", blob[1:5])
    body = blob[5 : 1 + length]
    fields: dict[str, str] = {}
    for part in body.rstrip(b"\x00").split(b"\x00"):
        fields[part[:1].decode()] = part[1:].decode()
    return fields


def test_an_error_carries_severity_code_and_message():
    fields = _fields(RuntimeError("boom"))
    assert fields["S"] == "ERROR"
    assert fields["V"] == "ERROR"
    assert fields["C"] == "XX000"
    assert fields["M"] == "boom"


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        ("SELEC 1", "42601"),
        ("SELECT * FROM no_such_table", "42P01"),
        ("SELECT no_such_column", "42703"),
        ("SELECT CAST('x' AS INTEGER)", "22P02"),
        ("SELECT 2147483647::INTEGER + 1::INTEGER", "22003"),
    ],
)
def test_an_engine_error_carries_postgres_sqlstate(sql, code):
    with pytest.raises(duckdb.Error) as caught:
        duckdb.connect().execute(sql)
    assert _fields(caught.value)["C"] == code
    # as the pipeline re-raises it
    try:
        raise RuntimeError(str(caught.value)) from caught.value
    except RuntimeError as wrapped:
        assert _fields(wrapped)["C"] == code


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (ZeroDivisionError("division by zero"), "22012"),
        (TimeoutError("request cancelled"), "57014"),
    ],
)
def test_division_by_zero_and_a_cancel_carry_their_sqlstate(exc, code):
    assert _fields(exc)["C"] == code


def test_a_source_error_keeps_its_own_sqlstate():
    import psycopg.errors

    assert _fields(psycopg.errors.DivisionByZero("division by zero"))["C"] == "22012"


def test_a_wrapped_source_error_keeps_its_sqlstate():
    import psycopg.errors

    try:
        try:
            raise psycopg.errors.DivisionByZero("division by zero")
        except psycopg.errors.DivisionByZero as inner:
            raise RuntimeError("the statement failed at its source") from inner
    except RuntimeError as outer:
        assert _fields(outer)["C"] == "22012"

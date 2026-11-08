# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b6d2e-91c4-4a7e-b05d-6e2a9c1f4d83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A text-format Bind value is read by the type its parameter has (REQ-589, REQ-2167): array_in's
text form is read as an array only for a parameter of an array type — the type the client
declared, or for an unspecified one (OID 0) the array type the statement casts it to. Any other
value starting with ``{`` stays the string it was sent as (psycopg's ``select %s::uuid`` with
``'{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}'``)."""

# Requirements: REQ-589, REQ-2167

from __future__ import annotations

import struct
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.pgwire.server import ProvisaHandler

_BRACED_UUID = "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}"


class _Ctx(SimpleNamespace):
    def mark_error(self):
        self.has_error = True

    def add_portal(self, portal, stmt, params, result_formats):
        self.portals[portal] = (stmt, params, result_formats)


def _bind_payload(values: list[str]) -> bytes:
    body = b"\x00\x00" + struct.pack("!h", 0) + struct.pack("!h", len(values))
    for value in values:
        raw = value.encode()
        body += struct.pack("!i", len(raw)) + raw
    return body + struct.pack("!h", 0)


def _bound(sql: str, oids: list[int], values: list[str]) -> list:
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler: Any = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    handler._open_request = lambda: None
    ctx: Any = _Ctx(session=SimpleNamespace(org_id=None), has_error=False, stmts={}, portals={})
    ctx.stmts[""] = (sql, oids)
    handler.handle_bind(ctx, _bind_payload(values))
    assert not ctx.has_error, b"".join(written)
    return ctx.portals[""][1]


@pytest.mark.parametrize(
    ("sql", "oid", "value"),
    [
        # Unspecified: the engine casts the string by context, as PostgreSQL resolves unknown.
        ("SELECT $1::uuid", 0, _BRACED_UUID),
        ("SELECT $1", 0, "{a,b}"),
        # A scalar type Bind has no decoder of keeps its text.
        ("SELECT $1", 25, "{a,b}"),
        ("SELECT $1", 1043, "{a,b}"),
        ("SELECT $1", 114, '{"a": 1, "b": 2}'),
        ("SELECT $1", 3802, '{"a": 1, "b": 2}'),
    ],
)
def test_a_braced_value_of_no_array_type_stays_its_string(sql: str, oid: int, value: str):
    assert _bound(sql, [oid], [value]) == [value]


def test_a_braced_uuid_of_the_uuid_type_is_that_uuid():
    assert _bound("SELECT $1", [2950], [_BRACED_UUID]) == [uuid.UUID(_BRACED_UUID)]


@pytest.mark.parametrize(
    ("oid", "value", "elements"),
    [
        (1009, "{foo,bar,baz}", ["foo", "bar", "baz"]),
        (1007, "{10,20}", ["10", "20"]),
        (1009, "{}", []),
        (1009, '{foo,null,"NULL","x,y"}', ["foo", None, "NULL", "x,y"]),
        (1009, '{{"fo{o","ba}r"},{"ba\\"z",qu\\\'x}}', [["fo{o", "ba}r"], ['ba"z', "qu'x"]]),
        (2951, "{" + _BRACED_UUID[1:-1] + "}", [_BRACED_UUID[1:-1]]),
        (1028, "{1,2}", ["1", "2"]),
    ],
)
def test_a_value_of_an_array_type_is_read_as_array_in_reads_it(
    oid: int, value: str, elements: list
):
    assert _bound("SELECT $1", [oid], [value]) == [elements]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT $1::text[] = $2::text[]",
        "SELECT $1 :: varchar(5) [] = $2::_text",
        "SELECT CAST($1 AS int[]) = CAST($2 AS character varying ARRAY)",
        'SELECT $1::pg_catalog."_text" = $2::pg_catalog.text[]',
    ],
)
def test_an_unspecified_parameter_the_statement_casts_to_an_array_is_an_array(sql: str):
    assert _bound(sql, [0, 0], ["{1,2}", "{3}"]) == [["1", "2"], ["3"]]


def test_only_the_parameter_cast_to_an_array_is_an_array():
    sql = "SELECT $1::text, $2::text[], $10::uuid, '$3::text[]', $3"
    oids = [0] * 10
    values = ["{a}", "{b}", "{c}"] + ["{d}"] * 6 + [_BRACED_UUID]
    assert _bound(sql, oids, values) == ["{a}", ["b"], "{c}"] + ["{d}"] * 6 + [_BRACED_UUID]

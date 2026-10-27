# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2b5dae-28c5-490e-a9be-01e855000e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DO block of RAISE statements answers as PostgreSQL answers it: notices at their levels, an
EXCEPTION as the error that ends the block; any other PL/pgSQL is refused by name (pgspec
TestError/Notice)."""

from __future__ import annotations

import struct
from types import SimpleNamespace

import pytest

from provisa.compiler.definitions import NotAvailableHere
from provisa.pgwire.do_block import Raised, raised
from provisa.pgwire.server import ProvisaHandler


def test_a_raise_notice_is_its_message():
    assert raised("DO $$ BEGIN RAISE NOTICE 'pgspec test notice'; END $$") == [
        Raised("NOTICE", "pgspec test notice")
    ]


def test_arguments_fill_the_format_and_a_bare_raise_is_an_exception():
    assert raised("DO $$ BEGIN RAISE WARNING 'a % b, 100%%', 'x'; RAISE 'boom %', 3; END $$") == [
        Raised("WARNING", "a x b, 100%"),
        Raised("EXCEPTION", "boom 3"),
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "DO $$ BEGIN PERFORM 1; END $$",
        "DO $$ DECLARE x int; BEGIN RAISE NOTICE 'x'; END $$",
        "DO $$ BEGIN RAISE NOTICE 'v %', now(); END $$",
    ],
)
def test_other_plpgsql_is_refused_by_name(sql):
    with pytest.raises(NotAvailableHere):
        raised(sql)


def _messages(sql: str) -> list[tuple[bytes, dict[str, str]]]:
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    ctx = SimpleNamespace(has_error=False)
    ctx.mark_error = lambda: setattr(ctx, "has_error", True)
    handler._run_do_block(ctx, sql)  # type: ignore[arg-type]
    blob, out, i = b"".join(written), [], 0
    while i < len(blob):
        kind = blob[i : i + 1]
        (length,) = struct.unpack("!i", blob[i + 1 : i + 5])
        body = blob[i + 5 : i + 1 + length]
        fields = {}
        if kind in (b"N", b"E"):
            for part in body.rstrip(b"\x00").split(b"\x00"):
                fields[part[:1].decode()] = part[1:].decode()
        out.append((kind, fields))
        i += 1 + length
    return out


def test_the_handler_sends_the_notice_then_completes():
    (notice, done) = _messages("DO $$ BEGIN RAISE NOTICE 'pgspec test notice'; END $$")
    assert notice == (b"N", {"S": "NOTICE", "V": "NOTICE", "C": "00000", "M": "pgspec test notice"})
    assert done[0] == b"C"


def test_an_exception_ends_the_block_with_its_error():
    (notice, error) = _messages("DO $$ BEGIN RAISE INFO 'one'; RAISE EXCEPTION 'two'; END $$")
    assert notice[1]["S"] == "INFO"
    assert error == (b"E", {"S": "ERROR", "V": "ERROR", "C": "P0001", "M": "two"})

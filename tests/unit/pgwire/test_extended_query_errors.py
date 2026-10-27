# Copyright (c) 2026 Kenneth Stott
# Canary: fd1f0e78-d109-41c4-b2e2-0fe2338b0441
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An error in an extended-query cycle is answered with an ErrorResponse, and the cycle's later
messages are discarded up to its Sync (pgspec TestExtended/ErrorDuringExtended,
TestExtended_ErrorDuringBind)."""

from __future__ import annotations

import struct
from types import SimpleNamespace

from provisa.pgwire.server import ProvisaHandler


def _parse_payload(sql: str, stmt: str = "") -> bytes:
    return stmt.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", 0)


def _bind_payload(n_params: int, stmt: str = "", portal: str = "") -> bytes:
    body = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!h", 0)
    body += struct.pack("!h", n_params)
    for _ in range(n_params):
        body += struct.pack("!i", 1) + b"1"
    return body + struct.pack("!h", 0)


class _Ctx(SimpleNamespace):
    def mark_error(self):
        self.has_error = True

    def add_statement(self, stmt, sql, param_oids):
        self.stmts[stmt] = (sql, param_oids)

    def add_portal(self, portal, stmt, params, result_formats):
        self.portals[portal] = (stmt, params, result_formats)


def _handler():
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = _W()
    handler._open_request = lambda: None
    return handler, written


def _codes(written: list[bytes]) -> list[bytes]:
    return [w[:1] for w in written if w[:1] in (b"E", b"1", b"2")]


def test_a_syntax_error_is_refused_at_parse():
    handler, written = _handler()
    ctx = _Ctx(session=SimpleNamespace(org_id=None), has_error=False, stmts={}, portals={})
    handler.handle_parse(ctx, _parse_payload("SELECTT BOGUS SYNTAX"))
    blob = b"".join(written)
    assert blob[:1] == b"E" and b"42601" in blob
    assert ctx.has_error


def test_messages_after_an_error_are_discarded_until_sync():
    handler, written = _handler()
    ctx = _Ctx(session=SimpleNamespace(org_id=None), has_error=True, stmts={}, portals={})
    handler.handle_parse(ctx, _parse_payload("SELECT 1"))
    handler.handle_bind(ctx, _bind_payload(0))
    assert written == []


def test_a_valid_statement_parses():
    handler, written = _handler()
    ctx = _Ctx(session=SimpleNamespace(org_id=None), has_error=False, stmts={}, portals={})
    handler.handle_parse(ctx, _parse_payload("SELECT $1::int AS val"))
    assert _codes(written) == [b"1"]
    assert ctx.stmts[""] == ("SELECT $1::int AS val", [])


def test_a_bind_with_the_wrong_parameter_count_is_refused():
    handler, written = _handler()
    ctx = _Ctx(
        session=SimpleNamespace(org_id=None),
        has_error=False,
        stmts={"": ("SELECT $1::int AS val", [])},
        portals={},
    )
    handler.handle_bind(ctx, _bind_payload(0))
    blob = b"".join(written)
    assert blob[:1] == b"E" and b"08P01" in blob
    assert b"supplies 0 parameters" in blob


def test_a_bind_with_the_right_parameter_count_binds():
    handler, written = _handler()
    ctx = _Ctx(
        session=SimpleNamespace(org_id=None),
        has_error=False,
        stmts={"": ("SELECT $1::int AS val, '$2'", [])},
        portals={},
    )
    handler.handle_bind(ctx, _bind_payload(1))
    assert _codes(written) == [b"2"]

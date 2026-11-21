# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e5c1a-94d2-4f60-8a1e-6c0d2f9b7e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement holding nothing but comments is PostgreSQL's empty query, on both protocols
(REQ-2167, REQ-2204): Grafana's health ping ``-- ping`` gets EmptyQueryResponse."""

# Requirements: REQ-2167, REQ-2204

from __future__ import annotations

import io
import struct
from types import SimpleNamespace

import pytest

from provisa.compiler.sql_rewrite import holds_no_statement, split_sql_statements
from provisa.pgwire.server import ProvisaHandler, ServerResponse

EMPTY = struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4)


@pytest.mark.parametrize(
    "sql", ["-- ping", "/* x */", "  -- a\n-- b\n", "/* a */ -- b", "-- a;\n;", "", " ; ; "]
)
def test_a_batch_of_only_comments_holds_no_statement(sql: str) -> None:
    assert split_sql_statements(sql) == []
    assert holds_no_statement(sql)


def test_a_statement_after_a_comment_still_runs() -> None:
    assert split_sql_statements("-- ping\nSELECT 1") == ["-- ping\nSELECT 1"]
    assert split_sql_statements("-- a\n; /* b */ ; SELECT 2; -- t") == ["SELECT 2"]
    assert not holds_no_statement("/* a */ SELECT 1")


def test_a_comment_holding_a_semicolon_is_not_a_statement() -> None:
    assert split_sql_statements("-- a; b") == []


def test_simple_query_of_a_comment_is_the_empty_query_response() -> None:
    handler = object.__new__(ProvisaHandler)
    handler.wfile = io.BytesIO()
    handler._open_request = lambda: None
    handler.send_ready_for_query = lambda ctx: handler.wfile.write(b"Z")
    handler.handle_query(SimpleNamespace(), b"-- ping\x00")
    assert handler.wfile.getvalue() == EMPTY + b"Z"


def test_execute_of_a_comment_is_the_empty_query_response() -> None:
    handler = object.__new__(ProvisaHandler)
    handler.wfile = io.BytesIO()
    handler._open_request = lambda: None
    ctx = SimpleNamespace(portals={"": ("s", (), ())}, stmts={"s": ("-- ping", ())})
    handler.handle_execute(ctx, b"\x00" + struct.pack("!i", 0))
    assert handler.wfile.getvalue() == EMPTY

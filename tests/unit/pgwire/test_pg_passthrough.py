# Copyright (c) 2026 Kenneth Stott
# Canary: bd0ce2d3-ff49-4901-9b8d-2103dc40643b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the raw-byte Postgres passthrough's wire exchange (REQ-1863), driven against a
scripted fake Postgres on the other end of a socketpair — no real database.

The exchange runs on ONE borrowed connection, as blocking I/O on the calling thread (amended
2026-10-01). A statement the connection has not seen is prepared on it under a name (Parse +
Describe(Statement) + Flush), which reads the source's own RowDescription and decides whether the
passthrough applies before anything executes; every run is then Bind + Execute + Sync — ONE round
trip — whose DataRow bytes are forwarded unmodified, read off the socket a batch at a time. A
completed exchange returns the connection to its pool; an abandoned or broken one discards it. A
source error raises PassthroughFailure — the request fails rather than falling back."""

from __future__ import annotations

import socket
import struct
import threading

import pytest

from provisa.pgwire.pg_passthrough import (
    BorrowedPgConnection,
    PassthroughError,
    PassthroughFailure,
    open_passthrough,
)

_INT32 = struct.Struct("!i")
_INT16 = struct.Struct("!h")

_PARSE_OK = b"1" + _INT32.pack(4)
_BIND_OK = b"2" + _INT32.pack(4)
_PARAMS_NONE = b"t" + _INT32.pack(6) + _INT16.pack(0)
_READY = b"Z" + _INT32.pack(5) + b"I"


def _msg(tag: bytes, payload: bytes = b"") -> bytes:
    return tag + _INT32.pack(4 + len(payload)) + payload


def _row_description(columns: list[tuple[str, int]]) -> bytes:
    body = bytearray(_INT16.pack(len(columns)))
    for name, oid in columns:
        body += name.encode() + b"\x00"
        body += struct.pack("!ihihih", 0, 0, oid, -1, -1, 0)
    return _msg(b"T", bytes(body))


def _data_row(ncols: int, values: list[bytes | None]) -> bytes:
    body = bytearray(_INT16.pack(ncols))
    for v in values:
        if v is None:
            body += b"\xff\xff\xff\xff"
        else:
            body += _INT32.pack(len(v)) + v
    return _msg(b"D", bytes(body))


class _FakePostgres:
    """The far end of the borrowed connection's socket. Each scripted step waits for the client to
    send exactly the named message tags, then answers; ``received`` is every tag it was sent. The
    connection's prepared-statement memory (``statements``) persists across borrows, as a pooled
    connection's does."""

    def __init__(self, steps: list[tuple[str, bytes]], *, hang_up: bool = False) -> None:
        self._hang_up = hang_up  # close the connection once the script has played out
        self.client, self._server = socket.socketpair()
        self.client.setblocking(False)  # as libpq leaves a pooled connection's socket
        self._steps = steps
        self.received = ""
        self.released: list[bool] = []
        self.cancelled = 0
        self.statements: dict = {}
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        buf = b""
        for tags, reply in self._steps:
            got = ""
            while got != tags:
                while len(buf) < 5 or len(buf) < 1 + _INT32.unpack_from(buf, 1)[0]:
                    chunk = self._server.recv(65536)
                    if not chunk:
                        return
                    buf += chunk
                length = _INT32.unpack_from(buf, 1)[0]
                got += buf[:1].decode()
                buf = buf[1 + length :]
            self.received += got
            if reply:
                self._server.sendall(reply)
        if self._hang_up:
            self._server.close()

    def borrow(self) -> BorrowedPgConnection:
        return BorrowedPgConnection(
            fileno=self.client.fileno(),
            ssl_in_use=False,
            cancel=lambda: setattr(self, "cancelled", self.cancelled + 1),
            release=self.released.append,
            statements=self.statements,
        )

    def finish(self) -> None:
        self._thread.join(timeout=5)
        self.client.close()
        self._server.close()


_TWO_COLUMNS = _PARSE_OK + _PARAMS_NONE + _row_description([("a", 23), ("b", 25)])
_DONE = _msg(b"C", b"SELECT 2\x00") + _READY


def test_fetch_forwards_data_row_bytes_unmodified_and_returns_the_connection():
    row1 = _data_row(2, [b"1", b"hello"])
    row2 = _data_row(2, [b"2", None])
    pg = _FakePostgres([("PDH", _TWO_COLUMNS), ("BES", _BIND_OK + row1 + row2 + _DONE)])
    try:
        result = open_passthrough(pg.borrow, "SELECT a, b FROM t", [], [0, 0], [23, 25])
        assert (result.column_names, result.column_types) == (["a", "b"], ["int4", "text"])
        assert result.cursor.fetch(10_000) == [row1, row2]
        assert pg.released == [False]  # back in its pool the moment the result was drained
        assert result.cursor.fetch(10_000) == []
        result.cursor.close()  # idempotent after the drain
    finally:
        pg.finish()
    assert pg.received == "PDHBES"
    assert pg.released == [False]


def test_a_statement_the_connection_has_prepared_runs_in_one_round_trip():
    """The connection remembers what was prepared on it: the second run of the same statement sends
    Bind + Execute + Sync and nothing else."""
    row = _data_row(2, [b"1", b"x"])
    pg = _FakePostgres(
        [
            ("PDH", _TWO_COLUMNS),
            ("BES", _BIND_OK + row + _DONE),
            ("BES", _BIND_OK + row + _DONE),
        ]
    )
    try:
        for _ in range(2):
            result = open_passthrough(
                pg.borrow, "SELECT a, b FROM t WHERE a = $1", [1], [0, 0], None
            )
            assert result.column_names == ["a", "b"]
            assert result.cursor.fetch(100) == [row]
    finally:
        pg.finish()
    assert pg.received == "PDH" + "BES" + "BES"
    assert pg.released == [False, False]


def test_rows_are_read_off_the_socket_a_batch_at_a_time():
    row1 = _data_row(1, [b"1"])
    row2 = _data_row(1, [b"2"])
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("a", 23)])),
            ("BES", _BIND_OK + row1 + row2 + _DONE),
        ]
    )
    try:
        cursor = open_passthrough(pg.borrow, "SELECT a FROM t", [], [0], None).cursor
        assert cursor.fetch(1) == [row1]
        assert pg.released == []  # the result is still streaming
        assert cursor.fetch(1) == [row2]
        assert cursor.fetch(1) == []
    finally:
        pg.finish()
    assert pg.received == "PDHBES"
    assert pg.released == [False]


def test_a_result_abandoned_mid_stream_discards_its_connection():
    row1 = _data_row(1, [b"1"])
    row2 = _data_row(1, [b"2"])
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("a", 23)])),
            ("BES", _BIND_OK + row1 + row2 + _DONE),
        ]
    )
    try:
        cursor = open_passthrough(pg.borrow, "SELECT a FROM t", [], [0], [23]).cursor
        assert cursor.fetch(1) == [row1]
        cursor.close()  # the client stopped reading: unread rows are still on the socket
    finally:
        pg.finish()
    assert pg.released == [True]


def test_a_cursor_closed_before_its_first_fetch_leaves_the_connection_idle():
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("a", 23)])),
            ("S", _READY),
        ]
    )
    try:
        open_passthrough(pg.borrow, "SELECT a FROM t", [], [0], [23]).cursor.close()
    finally:
        pg.finish()
    assert pg.received == "PDHS"
    assert pg.released == [False]


def test_a_source_error_while_fetching_fails_the_request_and_keeps_the_connection():
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("x", 23)])),
            (
                "BES",
                _BIND_OK + _msg(b"E", b"SERROR\x00C22012\x00Mdivision by zero\x00\x00") + _READY,
            ),
        ]
    )
    try:
        cursor = open_passthrough(pg.borrow, "SELECT 1/0", [], [0], [23]).cursor
        with pytest.raises(PassthroughFailure, match="division by zero"):
            cursor.fetch(10_000)
    finally:
        pg.finish()
    assert pg.released == [False]  # the exchange completed; the connection is idle
    assert "SELECT 1/0" in pg.statements  # an ordinary error leaves the prepared statement usable


def test_a_stale_prepared_statement_is_forgotten_so_the_next_run_prepares_again():
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("x", 23)])),
            (
                "BES",
                _msg(b"E", b"SERROR\x00C0A000\x00Mcached plan must not change result type\x00\x00")
                + _READY,
            ),
        ]
    )
    try:
        cursor = open_passthrough(pg.borrow, "SELECT x FROM t", [], [0], [23]).cursor
        with pytest.raises(PassthroughFailure, match="cached plan"):
            cursor.fetch(10)
    finally:
        pg.finish()
    assert pg.statements == {}


def test_a_source_error_at_prepare_fails_the_request():
    pg = _FakePostgres(
        [
            ("PDH", _msg(b"E", b'SERROR\x00C42P01\x00Mrelation "t" does not exist\x00\x00')),
            ("S", _READY),
        ]
    )
    try:
        with pytest.raises(PassthroughFailure, match="does not exist"):
            open_passthrough(pg.borrow, "SELECT a FROM t", [], [0], [23])
    finally:
        pg.finish()
    assert pg.released == [False]
    assert pg.statements == {}


def test_a_row_contradicting_the_described_column_count_fails_the_request():
    bad_row = _data_row(3, [b"1", b"2", b"3"])
    pg = _FakePostgres([("PDH", _TWO_COLUMNS), ("BES", _BIND_OK + bad_row + _DONE)])
    try:
        cursor = open_passthrough(pg.borrow, "SELECT * FROM t", [], [0, 0], [23, 25]).cursor
        with pytest.raises(PassthroughFailure, match="column count mismatch"):
            cursor.fetch(10_000)
    finally:
        pg.finish()
    assert pg.released == [True]  # unread rows may follow: never back to the pool


def test_a_layout_the_client_was_not_told_does_not_apply_and_nothing_executes():
    """The client was described int4; the source column is int8 — its binary bytes would be
    misread. Decided from the source's own RowDescription, before any Bind or Execute; the
    connection remembers that RowDescription, so the next attempt costs the source nothing."""
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("n", 20)])),
            ("S", _READY),
        ]
    )
    try:
        for _ in range(2):
            with pytest.raises(PassthroughError, match="20"):
                open_passthrough(pg.borrow, "SELECT n FROM t", [], [1], [23])
    finally:
        pg.finish()
    assert pg.received == "PDHS"  # never bound, never executed, never asked twice
    assert pg.released == [False, False]


def test_text_family_columns_share_a_layout():
    row = _data_row(1, [b"x"])
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("s", 1043)])),  # varchar
            ("BES", _BIND_OK + row + _DONE),
        ]
    )
    try:
        result = open_passthrough(pg.borrow, "SELECT s FROM t", [], [1], [25])  # described text
        assert result.cursor.fetch(10) == [row]
    finally:
        pg.finish()
    assert pg.released == [False]


def test_an_undescribed_statement_applies_only_to_types_pgwire_advertises_exactly():
    pg = _FakePostgres(
        [
            ("PDH", _PARSE_OK + _PARAMS_NONE + _row_description([("p", 600)])),  # point
            ("S", _READY),
        ]
    )
    try:
        with pytest.raises(PassthroughError, match="600"):
            open_passthrough(pg.borrow, "SELECT p FROM t", [], [1], None)
    finally:
        pg.finish()
    assert pg.released == [False]


def test_a_tls_connection_does_not_apply_and_is_returned_untouched():
    released: list[bool] = []
    borrowed = BorrowedPgConnection(
        fileno=-1, ssl_in_use=True, cancel=lambda: None, release=released.append
    )
    with pytest.raises(PassthroughError, match="TLS"):
        open_passthrough(lambda: borrowed, "SELECT 1", [], [0], None)
    assert released == [False]


def test_a_connection_that_drops_mid_exchange_is_discarded():
    # The source goes away with a DataRow half sent.
    pg = _FakePostgres([("PDH", _TWO_COLUMNS), ("BES", _BIND_OK + b"D\x00\x00")], hang_up=True)
    try:
        cursor = open_passthrough(pg.borrow, "SELECT a, b FROM t", [], [0, 0], [23, 25]).cursor
        with pytest.raises(PassthroughFailure, match="closed mid-message"):
            cursor.fetch(10_000)
    finally:
        pg.client.close()
    assert pg.released == [True]  # its protocol state is unknown: never back to the pool

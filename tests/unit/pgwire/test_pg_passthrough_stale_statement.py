# Copyright (c) 2026 Kenneth Stott
# Canary: 6f0c1c0e-3b6f-4b57-9a55-2d3b9f1f7a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement kept prepared on a pooled connection stays consistent with the server whatever error
its run raises (REQ-1863). The far end here keeps PostgreSQL's own prepared-statement table: Parse
under a name that is still prepared answers 42P05, Bind to a name that is not answers 26000. So a
passthrough that forgets a statement locally while it is still prepared on the server is caught by
the next Parse, as PostgreSQL catches it."""

from __future__ import annotations

import socket
import struct
import threading
from dataclasses import dataclass, field

import pytest

from provisa.pgwire.pg_passthrough import (
    BorrowedPgConnection,
    PassthroughFailure,
    open_passthrough,
)

_INT32 = struct.Struct("!i")
_INT16 = struct.Struct("!h")


def _msg(tag: bytes, payload: bytes = b"") -> bytes:
    return tag + _INT32.pack(4 + len(payload)) + payload


def _error(sqlstate: str, message: str, file: str, routine: str) -> bytes:
    """An ErrorResponse with the fields PostgreSQL sends: severity, SQLSTATE, message, source file,
    line and routine."""
    fields = [
        b"SERROR",
        b"VERROR",
        b"C" + sqlstate.encode(),
        b"M" + message.encode(),
        b"F" + file.encode(),
        b"L1",
        b"R" + routine.encode(),
    ]
    return _msg(b"E", b"\x00".join(fields) + b"\x00\x00")


def _row_description(columns: list[tuple[str, int]]) -> bytes:
    body = bytearray(_INT16.pack(len(columns)))
    for name, oid in columns:
        body += name.encode() + b"\x00"
        body += struct.pack("!ihihih", 0, 0, oid, -1, -1, 0)
    return _msg(b"T", bytes(body))


def _data_row(value: bytes) -> bytes:
    return _msg(b"D", _INT16.pack(1) + _INT32.pack(len(value)) + value)


def _cstring(buf: bytes, pos: int) -> tuple[str, int]:
    end = buf.index(b"\x00", pos)
    return buf[pos:end].decode(), end + 1


# PostgreSQL's own errors, as it raises them.
_NOT_SUPPORTED_BY_THE_FUNCTION = _error(
    "0A000",
    "searching for elements in multidimensional arrays is not supported",
    "array_userfuncs.c",
    "array_position_common",
)
_FUNCTION_MESSAGE = "searching for elements in multidimensional arrays is not supported"
_STALE_PLAN = _error(
    "0A000", "cached plan must not change result type", "plancache.c", "RevalidateCachedQuery"
)


@dataclass
class _Run:
    """One Bind/Execute of a statement: an error at Bind, an error at Execute, or rows."""

    rows: list[bytes] = field(default_factory=list)
    bind_error: bytes | None = None
    execute_error: bytes | None = None


class _StatefulPostgres:
    """The far end of the borrowed connection's socket, with a server's prepared-statement table.

    ``runs`` scripts each statement text's successive Bind/Execute outcomes (a statement not listed
    returns one row); ``columns`` its successive RowDescriptions at Parse (the last one repeats).
    ``log`` is every Parse, Close and Bind it was sent, with the statement's name."""

    def __init__(
        self,
        runs: dict[str, list[_Run]] | None = None,
        columns: dict[str, list[list[tuple[str, int]]]] | None = None,
    ) -> None:
        self.client, self._server = socket.socketpair()
        self.client.setblocking(False)  # as libpq leaves a pooled connection's socket
        self._runs = runs or {}
        self._columns = columns or {}
        self.prepared: dict[str, str] = {}  # name -> statement text, as the server holds it
        self.log: list[tuple[str, str]] = []
        self.released: list[bool] = []
        self.statements: dict = {}
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        buf = b""
        skipping = False  # after an error the server ignores messages until Sync
        portal: _Run | None = None
        while True:
            while len(buf) < 5 or len(buf) < 1 + _INT32.unpack_from(buf, 1)[0]:
                chunk = self._server.recv(65536)
                if not chunk:
                    return
                buf += chunk
            length = _INT32.unpack_from(buf, 1)[0]
            tag, body = buf[:1], buf[5 : 1 + length]
            buf = buf[1 + length :]
            if tag == b"S":
                skipping = False
                self._server.sendall(_msg(b"Z", b"I"))
                continue
            if skipping or tag == b"H":
                continue
            reply, failed, portal = self._answer(tag, body, portal)
            skipping = failed
            self._server.sendall(reply)

    def _answer(self, tag: bytes, body: bytes, portal: _Run | None):
        if tag == b"P":
            name, pos = _cstring(body, 0)
            sql, _ = _cstring(body, pos)
            self.log.append(("Parse", name))
            if name in self.prepared:
                already = _error(
                    "42P05",
                    f'prepared statement "{name}" already exists',
                    "prepare.c",
                    "StorePreparedStatement",
                )
                return already, True, portal
            self.prepared[name] = sql
            return _msg(b"1"), False, portal
        if tag == b"D":
            name, _ = _cstring(body, 1)
            shapes = self._columns.get(self.prepared[name], [[("x", 23)]])
            shape = shapes.pop(0) if len(shapes) > 1 else shapes[0]
            return _msg(b"t", _INT16.pack(0)) + _row_description(shape), False, portal
        if tag == b"C":
            name, _ = _cstring(body, 1)
            self.log.append(("Close", name))
            self.prepared.pop(name, None)  # closing a name that is not prepared is not an error
            return _msg(b"3"), False, portal
        if tag == b"B":
            _, pos = _cstring(body, 0)  # the portal's name
            name, _ = _cstring(body, pos)
            self.log.append(("Bind", name))
            if name not in self.prepared:
                gone = _error(
                    "26000",
                    f'prepared statement "{name}" does not exist',
                    "prepare.c",
                    "FetchPreparedStatement",
                )
                return gone, True, portal
            scripted = self._runs.get(self.prepared[name])
            run = scripted.pop(0) if scripted else _Run(rows=[_data_row(b"1")])
            if run.bind_error is not None:
                return run.bind_error, True, portal
            return _msg(b"2"), False, run
        assert tag == b"E" and portal is not None, tag
        if portal.execute_error is not None:
            return portal.execute_error, True, None
        done = _msg(b"C", f"SELECT {len(portal.rows)}".encode() + b"\x00")
        return b"".join(portal.rows) + done, False, None

    def borrow(self) -> BorrowedPgConnection:
        return BorrowedPgConnection(
            fileno=self.client.fileno(),
            ssl_in_use=False,
            cancel=lambda: None,
            release=self.released.append,
            statements=self.statements,
        )

    def finish(self) -> None:
        self._server.shutdown(socket.SHUT_RDWR)  # ends the server thread's read
        self._thread.join(timeout=5)
        self.client.close()
        self._server.close()

    def assert_no_name_prepared_twice(self) -> None:
        """Every Parse names a statement the server does not hold: one never prepared, or one
        closed since. And what this module remembers is exactly what the server holds."""
        held: set[str] = set()
        for message, name in self.log:
            if message == "Parse":
                assert name not in held, f"{name} prepared twice: {self.log}"
                held.add(name)
            elif message == "Close":
                held.discard(name)
        assert {p.name: sql for sql, p in self.statements.items()} == self.prepared


def _fetch_all(pg: _StatefulPostgres, sql: str) -> list[bytes]:
    return open_passthrough(pg.borrow, sql, [], [0], [23]).cursor.fetch(10_000)


_FUNCTION_SQL = "SELECT array_position('{{1,2},{3,4}}'::integer[], 3)"


@pytest.mark.parametrize(
    "run",
    [
        _Run(execute_error=_NOT_SUPPORTED_BY_THE_FUNCTION),
        # A constant call is folded when the statement is planned, which is at Bind.
        _Run(bind_error=_NOT_SUPPORTED_BY_THE_FUNCTION),
    ],
    ids=["at-execute", "at-bind"],
)
def test_a_function_s_own_0a000_reaches_the_client_once_and_the_connection_stays_usable(run):
    """A function that itself raises 0A000 is not a stale plan: the client gets PostgreSQL's error,
    the statement is run once, and statements new to the connection still prepare."""
    pg = _StatefulPostgres(runs={_FUNCTION_SQL: [run]})
    try:
        with pytest.raises(PassthroughFailure) as raised:
            _fetch_all(pg, _FUNCTION_SQL)
        assert (raised.value.sqlstate, str(raised.value)) == ("0A000", _FUNCTION_MESSAGE)
        assert pg.log.count(("Bind", "_provisa_pt_0")) == 1  # run once, not again
        for i in range(3):
            assert _fetch_all(pg, f"SELECT {i} AS x") == [_data_row(b"1")]
    finally:
        pg.finish()
    assert pg.released == [False, False, False, False]  # every exchange completed
    assert [name for message, name in pg.log if message == "Parse"] == [
        "_provisa_pt_0",
        "_provisa_pt_1",
        "_provisa_pt_2",
        "_provisa_pt_3",
    ]
    pg.assert_no_name_prepared_twice()


def test_a_stale_cached_plan_is_closed_prepared_again_and_run():
    """PostgreSQL refuses a kept plan whose result type changed (0A000, raised by its plan cache)
    and keeps the statement prepared. It is closed there before it is prepared again, and the
    client sees only the rows."""
    sql = "SELECT x FROM t"
    row = _data_row(b"7")
    pg = _StatefulPostgres(runs={sql: [_Run(rows=[row]), _Run(bind_error=_STALE_PLAN)]})
    try:
        assert _fetch_all(pg, sql) == [row]
        assert _fetch_all(pg, sql) == [_data_row(b"1")]  # stale, then prepared again and run
        assert _fetch_all(pg, "SELECT 2 AS x") == [_data_row(b"1")]
    finally:
        pg.finish()
    assert pg.released == [False, False, False]
    assert pg.log == [
        ("Parse", "_provisa_pt_0"),
        ("Bind", "_provisa_pt_0"),
        ("Bind", "_provisa_pt_0"),  # refused: the cached plan is stale
        ("Close", "_provisa_pt_0"),
        ("Parse", "_provisa_pt_0"),
        ("Bind", "_provisa_pt_0"),
        ("Parse", "_provisa_pt_1"),
        ("Bind", "_provisa_pt_1"),
    ]
    pg.assert_no_name_prepared_twice()


def test_a_stale_plan_that_now_describes_other_columns_is_refused_and_its_connection_closed():
    sql = "SELECT x FROM t"
    pg = _StatefulPostgres(
        runs={sql: [_Run(bind_error=_STALE_PLAN)]},
        columns={sql: [[("x", 23)], [("x", 20)]]},
    )
    try:
        with pytest.raises(PassthroughFailure, match="columns changed"):
            _fetch_all(pg, sql)
    finally:
        pg.finish()
    assert pg.released == [True]
    pg.assert_no_name_prepared_twice()


def test_a_statement_deallocated_behind_the_passthrough_is_prepared_again_under_a_free_name():
    """26000: the server no longer holds the statement (the driver's DEALLOCATE ALL)."""
    sql = "SELECT x FROM t"
    pg = _StatefulPostgres()
    try:
        assert _fetch_all(pg, sql) == [_data_row(b"1")]
        pg.prepared.clear()  # DEALLOCATE ALL
        assert _fetch_all(pg, sql) == [_data_row(b"1")]
        assert _fetch_all(pg, "SELECT 2 AS x") == [_data_row(b"1")]
    finally:
        pg.finish()
    assert pg.released == [False, False, False]
    pg.assert_no_name_prepared_twice()

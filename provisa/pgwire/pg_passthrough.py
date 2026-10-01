# Copyright (c) 2026 Kenneth Stott
# Canary: 473f4d02-8135-472d-a697-680875cc64cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Raw-byte DataRow passthrough for a pgwire route whose physical destination is itself Postgres —
either a DIRECT-route source (``PostgreSQLDriver``) or the ENGINE route when the bound federation
engine itself is Postgres (``PgFederationRuntime`` / REQ-904's ``PROVISA_ENGINE=pg``). The dispatch
decision of *when* to use it lives entirely in ``provisa/pgwire/server.py`` — never here.

Masking/RLS are always baked into ``governed.sql``'s text before this ever runs (see
``provisa/compiler/mask_inject.py``/``stage2.py``) — there is no post-execution, per-row Python
transform anywhere in the pipeline. When the physical destination is genuinely Postgres, the
DataRow bytes it sends back are therefore already the exact final answer: decoding them into
Python values and re-encoding them into pgwire's own DataRow format (``send_data_rows``) is pure
overhead. This module skips that round trip — it relays the source's own DataRow message bytes
straight to the downstream pgwire client, unmodified.

One connection, borrowed from the destination's EXISTING pool for the query's duration (REQ-1863,
amended 2026-10-01): the same pool the decoded path reads through, so a query opens no connection
and pays no handshake. The exchange is driven on that connection's own socket, on the request's
thread, as plain blocking I/O:

    Parse(named) + Describe(Statement) + Flush   -> the source's own RowDescription   (first use)
    Bind + Execute + Sync                        -> every DataRow, then ReadyForQuery

The statement is prepared under a name ON THAT CONNECTION and remembered with it (its name and
RowDescription), so every later run of the same statement text on the connection is the second
line alone: one round trip. The rows are read off the socket a batch at a time as the client
consumes them — TCP flow control holds the source back, so a large result never accumulates here.

The connection's driver (libpq) never sees the exchange and is idle before and after it, so a
connection whose exchange completed goes back to the pool; one whose exchange did not (it failed,
or the client stopped reading mid-result) is closed. The source's RowDescription decides whether
the passthrough APPLIES, before anything executes."""

from __future__ import annotations

import os
import socket
import struct
from collections.abc import Callable
from dataclasses import dataclass, field

from provisa.core import request_deadline

_INT16 = struct.Struct("!h")
_INT32 = struct.Struct("!i")

# A blocking read's own bound when the request carries no deadline: a source that stops answering
# surfaces as an error rather than a thread parked forever.
_IO_TIMEOUT_S = 600.0


class PassthroughError(Exception):
    """The passthrough does not APPLY to this statement — a result column whose advertised wire
    type does not have the source type's exact byte layout, or a destination connection whose
    socket carries TLS records rather than protocol messages. By design the statement then takes
    the decode/re-encode path; callers catch exactly this and route there. Raised before the
    statement executes. A passthrough that applies but fails raises :class:`PassthroughFailure`
    instead, which no caller catches."""


class PassthroughFailure(RuntimeError):
    """The passthrough applies but failed — the source returned an ErrorResponse, the connection
    closed, or its rows contradict the RowDescription the client was sent. The request fails with
    this error; it never falls back to the decode/re-encode path (REQ-1863)."""


class _ConnectionLost(PassthroughFailure):
    """The borrowed connection failed mid-exchange (closed, reset, or silent past its bound): its
    protocol state is unknown, so it is discarded rather than returned to its pool."""


@dataclass
class PreparedOnConnection:
    """A statement this module prepared on one connection: its name there and the source's
    RowDescription for it."""

    name: str
    column_names: list[str]
    type_oids: list[int]


# Statements kept prepared per connection (the source driver's own per-connection bound).
_PREPARED_MAX = 100


@dataclass
class BorrowedPgConnection:
    """One Postgres connection borrowed from its pool for a single query's raw exchange."""

    fileno: int  # the connection's socket descriptor
    ssl_in_use: bool
    cancel: Callable[[], None]  # cancel the statement in flight (safe from another thread)
    # Hand the connection back. ``discard=True`` closes it instead (its exchange did not complete,
    # so its protocol state is unknown) and lets the pool replace it.
    release: Callable[[bool], None]
    # statement text -> what this module prepared for it ON THIS CONNECTION. The lender keeps the
    # dict with the connection, so it lives exactly as long as the connection does.
    statements: dict[str, PreparedOnConnection] = field(default_factory=dict)


@dataclass
class PassthroughResult:
    """A ready-to-fetch passthrough query: the source's own column metadata (its RowDescription for
    this statement — never guessed), plus the cursor that streams raw DataRow bytes."""

    column_names: list[str]
    column_types: list[str]
    cursor: PassthroughCursor


# Source type OID -> the type name pgwire's _sql_type_to_bvtype (provisa/pgwire/server.py) maps to
# that SAME OID. Passthrough forwards the source's raw bytes verbatim in whatever format (text or
# binary) the client requested — safe only where the OID pgwire advertises for a column is the
# source column's own, since a binary-format client decodes strictly by OID-implied byte layout.
_OID_TYPE_NAMES = {
    16: "bool",
    21: "int2",
    700: "float4",
    3802: "jsonb",
    1184: "timestamptz",
    1266: "timetz",
    2950: "uuid",
    1186: "interval",
    17: "bytea",
    20: "int8",
    23: "int4",
    701: "float8",
    1700: "numeric",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    114: "json",
    25: "text",
    1043: "varchar",
    1042: "bpchar",
    19: "name",
    18: "char",
}

# The text family shares one wire layout (its bytes are the text), whichever of these OIDs the
# column is advertised under.
_TEXT_OIDS = frozenset({25, 1043, 1042, 19, 18})


def _same_layout(source_oid: int, advertised_oid: int) -> bool:
    if source_oid == advertised_oid:
        return True
    return source_oid in _TEXT_OIDS and advertised_oid in _TEXT_OIDS


def _applicable_types(source_oids: list[int], described_oids: list[int] | None) -> list[str]:
    """The source columns' type names when the passthrough applies; raises PassthroughError when it
    does not.

    ``described_oids`` are the type OIDs the client was already told at Describe: every source
    column must have that exact layout. With no prior Describe (``None``) the client is told the
    source's own types, so each must be one pgwire advertises under its own OID."""
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    from provisa.pgwire.server import _sql_type_to_bvtype

    names: list[str] = []
    for i, oid in enumerate(source_oids):
        name = _OID_TYPE_NAMES.get(oid)
        if name is None:
            raise PassthroughError(f"result column {i + 1} has source type OID {oid}")
        if described_oids is None:
            if not _same_layout(oid, BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(name)][0]):
                raise PassthroughError(f"source type {name!r} is not advertised under its own OID")
        names.append(name)
    if described_oids is not None:
        if len(described_oids) != len(source_oids):
            raise PassthroughFailure(
                f"column count mismatch: the source returns {len(source_oids)}, "
                f"the client was told {len(described_oids)}"
            )
        for i, (oid, told) in enumerate(zip(source_oids, described_oids)):
            if not _same_layout(oid, told):
                raise PassthroughError(
                    f"result column {i + 1}: the source type OID {oid} is not the described {told}"
                )
    return names


def _write_cstring(buf: bytearray, s: str) -> None:
    buf += s.encode("utf-8")
    buf += b"\x00"


def _encode_text_param(
    value: object,  # object-ok: a query param can be any bind value (str/int/float/Decimal/datetime/...); only bool needs special-casing, everything else round-trips through str()
) -> bytes:
    """Every parameter is sent as a text-format value (format code 0) — Postgres parses/casts
    text-format parameters against the statement's own inferred types, so this needs no
    per-type/OID-specific binary encoding at all. ``None`` (a SQL NULL) is the wire protocol's own
    -1-length marker, handled by the caller, not encoded here."""
    if isinstance(value, bool):
        return b"true" if value else b"false"
    return str(value).encode("utf-8")


_FLUSH = b"H" + _INT32.pack(4)
_SYNC = b"S" + _INT32.pack(4)
# Execute(unnamed portal, no row limit): the whole result, read off the socket as it is consumed.
_EXECUTE_ALL = b"E" + _INT32.pack(9) + b"\x00" + _INT32.pack(0)

# SQLSTATEs after which a kept prepared statement cannot be used again as it stands.
_STALE_STATEMENT_STATES = frozenset({"26000", "0A000"})


def _build_parse(name: str, sql: str) -> bytes:
    """Parse(named statement) with no parameter type OIDs — the source infers them."""
    body = bytearray()
    _write_cstring(body, name)
    _write_cstring(body, sql)
    body += _INT16.pack(0)
    return b"P" + _INT32.pack(4 + len(body)) + bytes(body)


def _build_describe_statement(name: str) -> bytes:
    body = bytearray(b"S")
    _write_cstring(body, name)
    return b"D" + _INT32.pack(4 + len(body)) + bytes(body)


def _build_close_statement(name: str) -> bytes:
    body = bytearray(b"S")
    _write_cstring(body, name)
    return b"C" + _INT32.pack(4 + len(body)) + bytes(body)


def _build_bind(statement: str, params: list, result_formats: list[int]) -> bytes:
    """Bind(unnamed portal, ``statement``): every parameter text-format, the result columns in the
    client's own requested formats."""
    out = bytearray()

    body = bytearray()
    _write_cstring(body, "")
    _write_cstring(body, statement)
    body += _INT16.pack(len(params))
    body += b"\x00\x00" * len(params)  # all parameter formats = text (0)
    body += _INT16.pack(len(params))
    for p in params:
        if p is None:
            body += _INT32.pack(-1)
        else:
            encoded = _encode_text_param(p)
            body += _INT32.pack(len(encoded))
            body += encoded
    body += _INT16.pack(len(result_formats))
    for fmt in result_formats:
        body += _INT16.pack(fmt)
    out += b"B" + _INT32.pack(4 + len(body)) + body

    return bytes(out)


def _error_sqlstate(payload: bytes) -> str:
    for part in payload.split(b"\x00"):
        if part[:1] == b"C":
            return part[1:].decode("ascii", errors="replace")
    return ""


def _parse_error_response(payload: bytes) -> str:
    fields = payload.split(b"\x00")
    parts = []
    for f in fields:
        if len(f) > 1:
            parts.append(f[1:].decode("utf-8", errors="replace"))
    return "; ".join(parts) if parts else "unknown Postgres error"


def _parse_row_description(payload: bytes) -> tuple[list[str], list[int]]:
    """(column names, type OIDs) from a RowDescription message body."""
    (count,) = _INT16.unpack_from(payload, 0)
    pos = 2
    names: list[str] = []
    oids: list[int] = []
    for _ in range(count):
        end = payload.index(b"\x00", pos)
        names.append(payload[pos:end].decode("utf-8"))
        pos = end + 1
        # table OID (4), column attnum (2), type OID (4), typlen (2), typmod (4), format (2)
        (oid,) = _INT32.unpack_from(payload, pos + 6)
        oids.append(oid)
        pos += 18
    return names, oids


class _RawExchange:
    """Blocking protocol I/O on a borrowed connection's socket, on the request's own thread.

    The socket object is built over a dup of the connection's descriptor and keeps the descriptor's
    non-blocking mode (libpq relies on it): a timeout makes each call wait in ``poll`` instead."""

    def __init__(self, borrowed: BorrowedPgConnection) -> None:
        self._borrowed = borrowed
        self._sock = socket.socket(fileno=os.dup(borrowed.fileno))
        budget = request_deadline.remaining()
        self._sock.settimeout(_IO_TIMEOUT_S if budget is None else min(_IO_TIMEOUT_S, budget))
        self._buf = bytearray()
        self._released = False

    def send(self, data: bytes) -> None:
        try:
            with request_deadline.cancel_on_deadline(self._borrowed.cancel):
                self._sock.sendall(data)
        except OSError as exc:
            raise _ConnectionLost(f"source connection failed while sending: {exc}") from exc

    def read_message(self) -> tuple[bytes, bytes]:
        header = self._recv_exact(5)
        length = _INT32.unpack_from(header, 1)[0]
        payload = self._recv_exact(length - 4) if length > 4 else b""
        return header[:1], payload

    def _recv_exact(self, n: int) -> bytes:
        buf = self._buf
        while len(buf) < n:
            # REQ-1882: the request's watchdog cancels the statement at its deadline; the source
            # then answers with an ErrorResponse, which ends this wait.
            try:
                with request_deadline.cancel_on_deadline(self._borrowed.cancel):
                    chunk = self._sock.recv(65536)
            except OSError as exc:
                raise _ConnectionLost(f"source connection failed while reading: {exc}") from exc
            if not chunk:
                raise _ConnectionLost("connection closed mid-message")
            buf += chunk
        out = bytes(buf[:n])
        del buf[:n]
        return out

    def sync(self) -> None:
        """End the exchange: Sync, then read to ReadyForQuery — the connection is idle again."""
        self.send(_SYNC)
        while True:
            tag, _payload = self.read_message()
            if tag == b"Z":
                return

    def release(self, *, discard: bool) -> None:
        if self._released:
            return
        self._released = True
        try:
            self._sock.close()
        finally:
            self._borrowed.release(discard)


def open_passthrough(
    borrow: Callable[[], BorrowedPgConnection],
    sql: str,
    params: list,
    result_formats: list[int],
    described_oids: list[int] | None,
) -> PassthroughResult:
    """Borrow ONE pooled connection (``borrow``) and return the cursor that fetches ``sql``'s raw
    DataRows on it. A statement the connection has not seen is prepared there first (one round
    trip, which reads the source's own RowDescription); one it has is not sent to the source at all
    until the first fetch. When the passthrough does not apply (:class:`PassthroughError`) the
    connection is back in its pool, idle, and nothing has executed."""
    borrowed = borrow()
    if borrowed.ssl_in_use:
        # The socket carries TLS records; the protocol messages are only readable through the
        # driver that holds the session keys.
        borrowed.release(False)
        raise PassthroughError("the destination connection is TLS-encrypted")
    exchange = _RawExchange(borrowed)
    clean = True  # nothing sent yet: the connection is still idle
    try:
        prepared = borrowed.statements.get(sql)
        newly_prepared = prepared is None
        if prepared is None:
            clean = False
            prepared = _prepare(exchange, borrowed, sql)
        try:
            column_types = _applicable_types(prepared.type_oids, described_oids)
        except (PassthroughError, PassthroughFailure):
            if newly_prepared:
                exchange.sync()  # closes the Parse/Describe exchange; nothing was bound or executed
            clean = True
            raise
    except BaseException:
        exchange.release(discard=not clean)
        raise
    cursor = PassthroughCursor(exchange, borrowed, sql, prepared, params, result_formats)
    return PassthroughResult(list(prepared.column_names), column_types, cursor)


def _prepare(
    exchange: _RawExchange, borrowed: BorrowedPgConnection, sql: str
) -> PreparedOnConnection:
    """Prepare ``sql`` under a name on this connection and read its RowDescription. The exchange is
    left open (Flushed, not Synced): the Bind/Execute/Sync that follows completes it."""
    statements = borrowed.statements
    request = bytearray()
    if len(statements) >= _PREPARED_MAX:
        oldest = next(iter(statements))
        request += _build_close_statement(statements.pop(oldest).name)
    used = {p.name for p in statements.values()}
    name = next(n for i in range(_PREPARED_MAX + 1) if (n := f"_provisa_pt_{i}") not in used)
    request += _build_parse(name, sql) + _build_describe_statement(name) + _FLUSH
    exchange.send(bytes(request))
    names: list[str] = []
    oids: list[int] = []
    while True:
        tag, payload = exchange.read_message()
        if tag == b"T":  # RowDescription
            names, oids = _parse_row_description(payload)
            break
        if tag == b"n":  # NoData — the statement returns no rows
            break
        if tag == b"E":  # ErrorResponse — the source ignores the rest until Sync
            message = _parse_error_response(payload)
            exchange.sync()
            exchange.release(discard=False)
            raise PassthroughFailure(message)
        # CloseComplete, ParseComplete, ParameterDescription, Notice, ParameterStatus: keep reading.
    prepared = PreparedOnConnection(name, names, oids)
    statements[sql] = prepared
    return prepared


class PassthroughCursor:
    """One run of a prepared statement on the borrowed connection: Bind + Execute + Sync on the
    first fetch, then the result's DataRows read off the socket ``fetch(limit)`` at a time."""

    def __init__(
        self,
        exchange: _RawExchange,
        borrowed: BorrowedPgConnection,
        sql: str,
        prepared: PreparedOnConnection,
        params: list,
        result_formats: list[int],
    ) -> None:
        self._exchange = exchange
        self._borrowed = borrowed
        self._sql = sql
        self._prepared = prepared
        self._params = params
        self._result_formats = result_formats
        self._expected_column_count = len(prepared.column_names)
        self._started = False
        self._done = False  # the connection has been handed back

    def fetch(self, limit: int) -> list[bytes]:
        """Return up to ``limit`` complete, wire-framed DataRow messages, or ``[]`` once the
        result is exhausted. Never decodes a single column value."""
        if self._done:
            return []
        exchange = self._exchange
        try:
            if not self._started:
                self._started = True
                exchange.send(
                    _build_bind(self._prepared.name, self._params, self._result_formats)
                    + _EXECUTE_ALL
                    + _SYNC
                )
            rows: list[bytes] = []
            failure: str | None = None
            while len(rows) < limit:
                tag, payload = exchange.read_message()
                if tag == b"D":  # DataRow — the whole point: forward unmodified
                    (ncols,) = _INT16.unpack_from(payload, 0)
                    if ncols != self._expected_column_count:
                        raise _ConnectionLost(
                            f"column count mismatch: source row has {ncols}, "
                            f"client was told {self._expected_column_count}"
                        )
                    rows.append(tag + _INT32.pack(4 + len(payload)) + payload)
                elif (
                    tag == b"E"
                ):  # ErrorResponse — ReadyForQuery follows (the Sync is already sent)
                    failure = _parse_error_response(payload)
                    if _error_sqlstate(payload) in _STALE_STATEMENT_STATES:
                        self._borrowed.statements.pop(self._sql, None)
                elif tag == b"Z":  # ReadyForQuery — the exchange is complete, the connection idle
                    self._done = True
                    exchange.release(discard=False)
                    if failure is not None:
                        raise PassthroughFailure(failure)
                    return rows
                # BindComplete, CommandComplete, EmptyQueryResponse, Notice, ParameterStatus.
            return rows
        except _ConnectionLost:
            self._done = True
            exchange.release(discard=True)
            raise
        except PassthroughFailure:
            raise
        except BaseException:
            # The exchange stopped mid-message: the connection's protocol state is unknown.
            if not self._done:
                self._done = True
                exchange.release(discard=True)
            raise

    def close(self) -> None:
        """Release the connection. Safe to call more than once. A result that was never started
        leaves the connection idle (a freshly prepared statement's exchange is Synced first); one
        abandoned mid-stream leaves unread rows on the socket, so that connection is closed."""
        if self._done:
            return
        self._done = True
        if self._started:
            self._exchange.release(discard=True)
            return
        try:
            self._exchange.sync()
        except BaseException:
            self._exchange.release(discard=True)
            raise
        self._exchange.release(discard=False)

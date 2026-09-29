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
engine itself is Postgres (``PgFederationRuntime`` / REQ-904's ``PROVISA_ENGINE=pg``). Same wire
mechanism either way: only the connect parameters differ (a ``SourcePool`` driver's stashed
``_connect_kwargs`` vs. the pg engine's own ``engine_dsn``), and the dispatch decision of *when* to
use it lives entirely in ``provisa/pgwire/server.py`` (and Flight's counterpart) — never here.

Masking/RLS are always baked into ``governed.sql``'s text before this ever runs (see
``provisa/compiler/mask_inject.py``/``stage2.py``) — there is no post-execution, per-row Python
transform anywhere in the pipeline. When the physical destination is genuinely Postgres, the
DataRow bytes it sends back are therefore already the exact final answer: decoding them into
``asyncpg.Record`` objects and re-encoding them into pgwire's own DataRow format
(``send_data_rows``) is pure overhead. This module skips that round trip — it relays the source's
own DataRow message bytes straight to the downstream pgwire client, unmodified.

No new authentication code: this module always connects with the SAME parameters (kwargs or a
DSN) the caller's own existing connection was already built from — never a new credential path.
Two short-lived dedicated connections are opened per query (one normal, to read real column
metadata via ``conn.prepare()``; one paused-transport, driven raw for the actual fetch) — neither
is ever pooled or handed back to shared use, so bypassing asyncpg's own row-decoding for the raw
one carries no cross-query state risk to reason about."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio
    import socket as socket_module

_INT16 = struct.Struct("!h")
_INT32 = struct.Struct("!i")


class PassthroughError(Exception):
    """Any condition that should fall back to the normal decode/re-encode DIRECT path — a real
    Postgres ErrorResponse, a column-count mismatch against what the client was already told,
    or a connection-level failure. Never a correctness risk: callers catch this and fall back."""


@dataclass
class RawPgConnection:
    """A dedicated, single-use asyncpg connection whose transport has been paused so its raw
    socket can be driven directly. Never returned to any pool — always closed after one query."""

    _asyncpg_conn: Any
    _loop: asyncio.AbstractEventLoop
    _sock: socket_module.socket

    async def close(self) -> None:
        # self._sock is a dup() of the transport's fd (see open_raw_connection) — a separate
        # descriptor this module owns outright and must close itself; the original transport's
        # own fd is unaffected and is released by self._asyncpg_conn.close() below.
        try:
            self._sock.close()
        except Exception:  # noqa: BLE001 - best-effort; the connection is being discarded either way
            pass
        try:
            self._asyncpg_conn._transport.resume_reading()
        except Exception:  # noqa: BLE001 - best-effort; the connection is being discarded either way
            pass
        await self._asyncpg_conn.close()


async def open_raw_connection(connect_kwargs: dict[str, Any]) -> RawPgConnection:
    """Open a fresh, dedicated connection using the exact parameters the caller's own existing
    connection (a ``SourcePool`` driver's pool, or the pg federation engine's ``engine_dsn``) was
    already built from (real ``asyncpg.connect()`` — the same auth/SCRAM handling, unchanged),
    then pause its transport so this module can read/write its raw socket directly instead of
    going through asyncpg's own Cython protocol."""
    import asyncio

    import asyncpg

    if not connect_kwargs:
        raise PassthroughError("no connect parameters given (connect() never called)")
    # uvloop's own transport socket, once dup()'d, is still a `uvloop.loop.PseudoSocket` (not a
    # genuine `socket.socket` the way the stdlib SelectorEventLoop's is) — its `.send()` is a
    # deliberate stub that raises `TypeError: transport sockets do not support send() method`.
    # 100% reproducible live: govern+execute both succeed (confirmed via [PGWIRE TIMING] logs),
    # then PassthroughCursor.fetch's loop.sock_sendall raises this on literally every call, which
    # propagates uncaught through the passthrough generator (nothing downstream of the initial
    # open_passthrough() call catches PassthroughError — see server.py) and kills the client
    # connection with zero visible signal outside ~/pgwire_debug.log. Detect and refuse up front
    # — matches this whole module's own fallback discipline (falls back to the ordinary
    # decode/re-encode path on ANY PassthroughError, never a correctness risk) — rather than ever
    # attempting a technique this event loop cannot support.
    _loop_for_check = asyncio.get_event_loop()
    if type(_loop_for_check).__module__.startswith("uvloop"):
        raise PassthroughError(
            "raw-socket passthrough is not supported under uvloop "
            "(PseudoSocket.send() unconditionally raises TypeError)"
        )
    conn = await asyncpg.connect(**connect_kwargs)
    # Everything below is real I/O/attribute-access that can raise (the AttributeError/
    # TransportSocket bugs this function's history is full of are exactly the kind of thing this
    # guards against) — on ANY failure past this point, `conn` is a live, connected asyncpg
    # connection against the REAL backend Postgres that nothing else will ever close. Confirmed:
    # during the period this function's bugs were live, every one of the (thousands of) failed
    # calls leaked one such connection — never explicitly `conn.close()`d, only reclaimed whenever
    # Python's GC/asyncpg's own __del__ got around to it, if ever, which can exhaust the backend's
    # max_connections and hang unrelated later callers (e.g. federated_join) with no clean error.
    try:
        # conn._transport (asyncpg.Connection's own slot, connection.py), NOT
        # conn._protocol.transport: CoreProtocol's `transport` is a plain `cdef object`
        # (coreproto.pxd) with no `public`/`readonly` modifier, so it is never exposed to Python at
        # all — reading it raises AttributeError, live-confirmed against asyncpg 0.31.0
        # ('Protocol' object has no attribute 'transport').
        transport = conn._transport
        transport.pause_reading()
        wrapped = transport.get_extra_info("socket")
        if wrapped is None:
            raise PassthroughError("transport exposes no raw socket (unexpected transport type)")
        # transport.get_extra_info("socket") is an asyncio.trsock.TransportSocket — a SAFE wrapper
        # around the real fd that deliberately does not implement send()/recv() (only metadata
        # calls like getsockname/fileno), specifically to stop code from doing what this module
        # needs to do. Live-confirmed: loop.sock_sendall() on the bare wrapper raises
        # AttributeError: 'TransportSocket' object has no attribute 'send'. `.dup()` returns a
        # genuine `socket.socket` sharing the same underlying fd — real send()/recv(), safe for
        # loop.sock_* while the original transport stays paused. Must be non-blocking for loop.sock_*.
        sock = wrapped.dup()
        sock.setblocking(False)
    except BaseException:
        # Live-confirmed: conn.close() can itself hang forever here if reads are still paused
        # (pause_reading() above already ran) — the same reason RawPgConnection.close() resumes
        # reading before closing. Best-effort; conn is being discarded either way.
        try:
            conn._transport.resume_reading()
        except Exception:  # noqa: BLE001
            pass
        await conn.close()
        raise
    return RawPgConnection(conn, asyncio.get_event_loop(), sock)


@dataclass
class PassthroughResult:
    """A ready-to-fetch passthrough query: real column metadata (from the pooled connection's own
    ``conn.prepare(sql)`` — never guessed), plus the cursor that streams raw DataRow bytes."""

    column_names: list[str]
    column_types: list[str]
    cursor: PassthroughCursor


# Column type names pgwire's own _sql_type_to_bvtype (provisa/pgwire/server.py) maps to a SPECIFIC
# real Postgres OID/wire layout (not a generic TEXT fallback). Passthrough forwards the source's
# raw bytes verbatim in whatever format (text/binary) the client requested — that is only safe if
# the RowDescription OID pgwire advertises for the column genuinely matches the real source type,
# since a binary-format client decodes strictly by OID-implied byte layout. An unrecognized type
# name falls through to a generic TEXT OID in the normal path; forwarded raw bytes for such a
# column could then be misdecoded, so passthrough is refused for it (falls back to decode/re-encode).
_TEXT_SAFE_TYPES = frozenset({"text", "varchar", "bpchar", "name", "char"})


def _column_types_are_passthrough_safe(column_types: list[str]) -> bool:
    from provisa.pgwire.server import _INT_TYPES, _TYPE_TO_BVTYPE

    recognized = {t.lower() for t in _TYPE_TO_BVTYPE} | {t.lower() for t in _INT_TYPES}
    return all(t.lower() in recognized or t.lower() in _TEXT_SAFE_TYPES for t in column_types)


async def open_passthrough(
    connect_kwargs: dict[str, Any], sql: str, params: list, result_formats: list[int]
) -> PassthroughResult:
    """Get real column metadata from a short-lived NORMAL connection (the same
    ``conn.prepare(sql)`` call ``_PgDirectStream._open`` already makes for the DIRECT route —
    closed right after, never pooled), then open a second, dedicated raw connection for the
    actual row fetch. ``connect_kwargs`` is whatever the caller's own connection was already
    built from — a ``SourcePool`` driver's stashed kwargs for DIRECT, or the pg federation
    engine's own DSN (as ``{"dsn": engine_dsn}``) for the ENGINE route."""
    import asyncpg

    if not connect_kwargs:
        raise PassthroughError("no connect parameters given (connect() never called)")
    conn = await asyncpg.connect(**connect_kwargs)
    try:
        stmt = await conn.prepare(sql)
        attrs = stmt.get_attributes()
        column_names = [a.name for a in attrs]
        column_types = [a.type.name for a in attrs]
    finally:
        await conn.close()

    if not _column_types_are_passthrough_safe(column_types):
        raise PassthroughError(f"unrecognized column type(s) in {column_types!r}")

    raw = await open_raw_connection(connect_kwargs)
    cursor = PassthroughCursor(raw, sql, params, result_formats, len(column_names))
    return PassthroughResult(column_names, column_types, cursor)


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


def _build_extended_query_messages(sql: str, params: list, result_formats: list[int]) -> bytes:
    """Parse(unnamed) + Bind(unnamed portal, unnamed statement) — sent once, before the
    Execute/Sync fetch loop begins. No Describe: the caller already has real column metadata
    from the SAME pooled connection's own ``conn.prepare(sql)`` (the well-tested path
    ``_PgDirectStream`` already uses), so a second round trip just to re-learn the column count
    would be pure overhead — the DataRow messages this yields carry their own ``ncols`` header,
    which is cheap to sanity-check per message instead."""
    out = bytearray()

    # Parse: statement="", query=sql, 0 parameter type OIDs (let the server infer them from the
    # text-format values Bind sends — the same "unspecified type is legal" contract this project's
    # own pgwire server relies on for asyncpg clients, REQ-883's case-insensitive counterpart).
    body = bytearray()
    _write_cstring(body, "")
    _write_cstring(body, sql)
    body += _INT16.pack(0)
    out += b"P" + _INT32.pack(4 + len(body)) + body

    # Bind: portal="", statement="", all params text-format, values, result_formats per column.
    body = bytearray()
    _write_cstring(body, "")
    _write_cstring(body, "")
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


def _build_execute_sync(limit: int) -> bytes:
    body = bytearray()
    _write_cstring(body, "")
    body += _INT32.pack(limit)
    execute = b"E" + _INT32.pack(4 + len(body)) + bytes(body)
    sync = b"S" + _INT32.pack(4)
    return execute + sync


async def _recv_exact(
    loop: "asyncio.AbstractEventLoop", sock: "socket_module.socket", n: int
) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = await loop.sock_recv(sock, n - len(buf))
        if not chunk:
            raise PassthroughError("connection closed mid-message")
        buf += chunk
    return buf


async def _read_message(
    loop: "asyncio.AbstractEventLoop", sock: "socket_module.socket"
) -> tuple[bytes, bytes]:
    header = await _recv_exact(loop, sock, 5)
    tag = header[:1]
    length = _INT32.unpack(header[1:])[0]
    payload = await _recv_exact(loop, sock, length - 4) if length > 4 else b""
    return tag, payload


def _parse_error_response(payload: bytes) -> str:
    fields = payload.split(b"\x00")
    parts = []
    for f in fields:
        if len(f) > 1:
            parts.append(f[1:].decode("utf-8", errors="replace"))
    return "; ".join(parts) if parts else "unknown Postgres error"


async def _simple_query(
    loop: "asyncio.AbstractEventLoop", sock: "socket_module.socket", sql: str
) -> None:
    """Send a Simple Query ('Q') message and drain the response up to ReadyForQuery. Used only
    for the cursor's own BEGIN/COMMIT bracket (never the governed statement itself, which always
    goes through the Extended Query Parse/Bind/Execute path — see ``_build_extended_query_messages``)."""
    body = sql.encode("utf-8") + b"\x00"
    await loop.sock_sendall(sock, b"Q" + _INT32.pack(4 + len(body)) + body)
    while True:
        tag, payload = await _read_message(loop, sock)
        if tag == b"E":  # ErrorResponse
            raise PassthroughError(_parse_error_response(payload))
        if tag == b"Z":  # ReadyForQuery
            return


class PassthroughCursor:
    """Drives one query's Extended Query Protocol exchange over a :class:`RawPgConnection`'s raw
    socket, ``fetch(limit)`` at a time — the same shape as ``_PgDirectStream`` (``postgresql.py``)
    so the existing ``execute_native_stream``-style ``run_coroutine_threadsafe`` batch pump can
    drive this one too, one whole batch per hop instead of one message per hop."""

    def __init__(
        self,
        raw: RawPgConnection,
        sql: str,
        params: list,
        result_formats: list[int],
        expected_column_count: int,
    ) -> None:
        self._raw = raw
        self._sql = sql
        self._params = params
        self._result_formats = result_formats
        self._expected_column_count = expected_column_count
        self._sent_parse_bind = False
        self._exhausted = False
        self._began_txn = False

    async def fetch(self, limit: int) -> list[bytes]:
        """Return up to ``limit`` complete, wire-framed DataRow messages, or ``[]`` once the
        result is exhausted. Never decodes a single column value."""
        if self._exhausted:
            return []
        sock, loop = self._raw._sock, self._raw._loop
        if not self._sent_parse_bind:
            # REQ-1863 multi-batch fix: an unnamed portal only survives a Sync while an explicit
            # transaction is open — outside one, Sync commits the implicit transaction Postgres
            # auto-starts and destroys the portal with it. `fetch()` sends Execute+Sync on every
            # call (see _build_execute_sync below), so a >1-batch result (limit < total rows,
            # e.g. large_scan) sent its SECOND Execute against an already-destroyed portal —
            # live-confirmed: `ERROR: portal "" does not exist`. Same bracket _PgDirectStream
            # (executor/drivers/postgresql.py) already holds for its own asyncpg-level cursor.
            self._began_txn = True
            await _simple_query(loop, sock, "BEGIN")
            self._sent_parse_bind = True
            await loop.sock_sendall(
                sock,
                _build_extended_query_messages(self._sql, self._params, self._result_formats),
            )

        rows: list[bytes] = []
        await loop.sock_sendall(sock, _build_execute_sync(limit))
        awaiting_ready = True
        while awaiting_ready:
            tag, payload = await _read_message(loop, sock)
            if tag == b"1" or tag == b"2":  # ParseComplete / BindComplete
                continue
            elif tag == b"D":  # DataRow — the whole point: forward unmodified
                (ncols,) = _INT16.unpack(payload[:2])
                if ncols != self._expected_column_count:
                    raise PassthroughError(
                        f"column count mismatch: source row has {ncols}, "
                        f"client was told {self._expected_column_count}"
                    )
                rows.append(tag + _INT32.pack(4 + len(payload)) + payload)
            elif tag == b"s":  # PortalSuspended — more rows to fetch on the next call
                pass
            elif tag == b"C" or tag == b"I":  # CommandComplete / EmptyQueryResponse
                self._exhausted = True
            elif tag == b"E":  # ErrorResponse
                raise PassthroughError(_parse_error_response(payload))
            elif tag == b"Z":  # ReadyForQuery — this fetch's round trip is done
                awaiting_ready = False
            # Any other tag (NoticeResponse, ParameterStatus, etc.): ignore and keep reading.
        return rows

    async def close(self) -> None:
        if self._began_txn:
            # Read-only statement (governed SQL never mutates on this route — see the module
            # docstring), so COMMIT vs ROLLBACK is immaterial; COMMIT mirrors _PgDirectStream's
            # own close (postgresql.py). Best-effort: the connection is discarded either way.
            try:
                await _simple_query(self._raw._loop, self._raw._sock, "COMMIT")
            except Exception:  # noqa: BLE001
                pass
        await self._raw.close()

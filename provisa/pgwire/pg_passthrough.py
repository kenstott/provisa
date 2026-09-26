# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Raw-byte DataRow passthrough for a DIRECT route whose source is itself Postgres.

Masking/RLS are always baked into ``governed.sql``'s text before this ever runs (see
``provisa/compiler/mask_inject.py``/``stage2.py``) — there is no post-execution, per-row Python
transform anywhere in the pipeline. When the physical source is genuinely Postgres, the DataRow
bytes it sends back are therefore already the exact final answer: decoding them into
``asyncpg.Record`` objects and re-encoding them into pgwire's own DataRow format
(``send_data_rows``) is pure overhead. This module skips that round trip — it relays the source's
own DataRow message bytes straight to the downstream pgwire client, unmodified.

No new authentication code: ``PostgreSQLDriver.connect()`` already stashes the exact
``asyncpg.connect()`` kwargs it used to build its pooled connection (``_connect_kwargs``); this
module reuses them to open ONE dedicated connection via the same call, outside any pool. That
connection's asyncio transport is paused (stopping asyncpg's own Cython protocol from processing
further bytes) and its raw socket is used directly — the connection is never returned to
``PostgreSQLDriver``'s shared pool afterward (bypassing asyncpg's own row-decoding for one query
and then handing the connection back to normal asyncpg use risks desyncing its cached
transaction/connection state), so there is no cross-query state risk to reason about."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio
    import socket as socket_module

    from provisa.executor.drivers.postgresql import PostgreSQLDriver

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
        try:
            self._asyncpg_conn.transport.resume_reading()
        except Exception:  # noqa: BLE001 - best-effort; the connection is being discarded either way
            pass
        await self._asyncpg_conn.close()


async def open_raw_connection(driver: "PostgreSQLDriver") -> RawPgConnection:
    """Open a fresh, dedicated connection using the exact kwargs ``driver``'s own pool was built
    with (real ``asyncpg.connect()`` — the same auth/SCRAM handling, unchanged), then pause its
    transport so this module can read/write its raw socket directly instead of going through
    asyncpg's own Cython protocol."""
    import asyncio

    import asyncpg

    if not driver._connect_kwargs:
        raise PassthroughError("driver has no stashed connect kwargs (connect() never called)")
    conn = await asyncpg.connect(**driver._connect_kwargs)
    transport = conn._protocol.transport
    transport.pause_reading()
    sock = transport.get_extra_info("socket")
    if sock is None:
        await conn.close()
        raise PassthroughError("transport exposes no raw socket (unexpected transport type)")
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
    driver: "PostgreSQLDriver", sql: str, params: list, result_formats: list[int]
) -> PassthroughResult:
    """Get real column metadata from the driver's existing pool (the same ``conn.prepare(sql)``
    call ``_PgDirectStream._open`` already makes — a short-lived pooled acquire, released before
    this returns), then open a dedicated raw connection for the actual row fetch."""
    pool = driver._pool
    if pool is None:
        raise PassthroughError("driver has no pool (connect() never called)")
    async with pool.acquire(timeout=driver._ACQUIRE_TIMEOUT) as conn:
        stmt = await conn.prepare(sql)
        attrs = stmt.get_attributes()
        column_names = [a.name for a in attrs]
        column_types = [a.type.name for a in attrs]

    if not _column_types_are_passthrough_safe(column_types):
        raise PassthroughError(f"unrecognized column type(s) in {column_types!r}")

    raw = await open_raw_connection(driver)
    cursor = PassthroughCursor(raw, sql, params, result_formats, len(column_names))
    return PassthroughResult(column_names, column_types, cursor)


def _write_cstring(buf: bytearray, s: str) -> None:
    buf += s.encode("utf-8")
    buf += b"\x00"


def _encode_text_param(
    value: object,
) -> bytes:  # object-ok: a query param can be any bind value (str/int/float/Decimal/datetime/...); only bool needs special-casing, everything else round-trips through str()
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

    async def fetch(self, limit: int) -> list[bytes]:
        """Return up to ``limit`` complete, wire-framed DataRow messages, or ``[]`` once the
        result is exhausted. Never decodes a single column value."""
        if self._exhausted:
            return []
        sock, loop = self._raw._sock, self._raw._loop
        if not self._sent_parse_bind:
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
        await self._raw.close()

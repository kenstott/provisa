# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL wire protocol server for Provisa.

Builds on buenavista's socketserver-based handler, adding:
- TLS via ssl.SSLContext wrap
- Cleartext password auth bridged to SimpleAuthProvider/bcrypt
- Catalog intercept (information_schema + pg_catalog via DuckDB)
- Full Provisa governance pipeline for user queries
- Multi-statement simple-query support
"""
# Requirements: REQ-001, REQ-002, REQ-120, REQ-124, REQ-125, REQ-266, REQ-273, REQ-1862
# complexity-gate: allow-ble=5 reason="wire-protocol request-handler boundary: an arbitrary user query / DDL / COPY / CTAS / describe can raise any exception type from the pluggable engine (DuckDB/buenavista/extensions) — each is caught and converted to a PostgreSQL SQLSTATE error response (send_error / _send_pg_error) so one bad statement returns a protocol error instead of crashing the connection handler; catching a narrower set would let an unmapped type kill the session"

from __future__ import annotations

import datetime
import decimal
import logging
import os
import re
import socketserver
import ssl
import struct
import threading
import time
from dataclasses import dataclass
from dataclasses import field as _dc_field
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Tuple

import jwt

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from provisa.core import request_deadline
from provisa.core.egress import CountingWriter
from provisa.core.limits import request_timeout_for  # REQ-1905: pgwire's own request timeout
from provisa.otel_compat import annotate_request as _annotate_request
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage
from provisa.otel_compat import HeldRequestSpan
from provisa.executor.result import ResultStream
from provisa.security.rights import can_act_cross_org, capabilities_for_claims

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

# REQ-1882 (amended 2026-09-29): the entire request runs on its connection thread. Each TCP
# connection is served by its own socketserver thread (ThreadingTCPServer); ProvisaHandler.handle
# checks a ConnectionLoop out for the connection's lifetime and every coroutine the connection runs
# — authentication, org resolution, governance, execution, audit, streamed-cursor pumps — executes
# on that loop via ``loop.run_until_complete`` on this same thread (provisa.core.connection_loop).
# No second thread, no hop to a shared loop: two connections govern and execute in parallel.


async def _run_with_org(org_id: str | None, coro):  # REQ-1266
    """Await ``coro`` with ``current_org`` bound to ``org_id``.

    The connection loop's task copies this thread's context when it starts, so an org bound on the
    thread is already visible; binding it here makes the coroutine's org explicit where the caller
    holds a session org rather than a thread binding (COPY, CTAS, audit finalization). ``None``
    (single-org / default) awaits unbound → the default runtime."""
    if org_id is None:
        return await coro
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await coro
    finally:
        reset_current_org(token)


async def _resolve_and_build_org(state_, identity, requested_org: str | None) -> str | None:
    """Resolve the org for an authenticated pgwire identity and materialize its runtime (REQ-1266).

    Runs on the connection's own loop (REQ-1882). Returns the org id to bind on the session, or
    None for a single-org deployment / default-org principal.

    REQ-1234: ``requested_org`` is the org the TLS SNI hostname named, when the client dialed one.
    It is a request and nothing more — ``resolve_session_org`` refuses an org the principal is not
    a member of, so dialing acme.provisa.dev does not put anyone inside acme."""
    from provisa.api.app import ensure_org_runtime
    from provisa.api.org_resolve import resolve_session_org

    # REQ-1337: resolve the claims to RIGHTS and test cross_org — never the role name.
    caps = capabilities_for_claims(
        getattr(identity, "roles", []) or [], getattr(state_, "roles", {})
    )
    org_id = await resolve_session_org(
        state_,
        user_id=getattr(identity, "user_id", None),
        can_act_any_org=can_act_cross_org(caps),
        requested_org=requested_org or getattr(identity, "active_org_id", None),
    )
    if org_id is not None:
        await ensure_org_runtime(org_id)
    return org_id


_TXN_TAG_RE = re.compile(
    r"^\s*(SET|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)
# CTAS: CREATE TABLE ... AS SELECT — a physical data move (REQ-996), NOT plain DDL. Routed to the
# CTAS handler ahead of _DDL_RE, whose column-def path cannot parse an AS-SELECT body.
_CTAS_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?.+?\bAS\b\s+(?:WITH\b|SELECT\b|\()",
    re.IGNORECASE | re.DOTALL,
)
_DDL_RE = re.compile(
    r"^\s*(CREATE\s+(TABLE|VIEW|INDEX|UNIQUE\s+INDEX|SEQUENCE|SCHEMA)"
    r"|ALTER\s+(TABLE|INDEX|SEQUENCE|VIEW)"
    r"|DROP\s+(TABLE|VIEW|INDEX|SEQUENCE|SCHEMA))\b",
    re.IGNORECASE,
)
# REQ-1862: session-scoped SQL cursors (no BEGIN/COMMIT machinery exists in pgwire today, so
# every DECLAREd cursor behaves as if WITH HOLD — it lives until CLOSE or disconnect).
_DECLARE_CURSOR_RE = re.compile(
    r"^\s*DECLARE\s+(?P<name>\"[^\"]+\"|[A-Za-z_][\w$]*)\s+"
    r"(?:BINARY\s+|INSENSITIVE\s+|ASENSITIVE\s+|(?P<scroll>NO\s+SCROLL|SCROLL)\s+)*"
    r"CURSOR\s*(?:(?:WITH|WITHOUT)\s+HOLD\s*)?FOR\s+(?P<sql>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_FETCH_RE = re.compile(r"^\s*FETCH\b", re.IGNORECASE)
_MOVE_RE = re.compile(r"^\s*MOVE\b", re.IGNORECASE)
_CLOSE_CURSOR_RE = re.compile(
    r"^\s*CLOSE\s+(?P<name>ALL|\"[^\"]+\"|[A-Za-z_][\w$]*)\s*;?\s*$", re.IGNORECASE
)


state = None  # module-level reference; replaced by tests via patch()

# PostgreSQL startup-message protocol codes (the uint32 following the length prefix). Magic values
# fixed by the wire protocol — named here so handle_startup reads as protocol dispatch, not integers.
_SSL_REQUEST_CODE = 80877103  # SSLRequest (1234 << 16 | 5679)
_CANCEL_REQUEST_CODE = 80877102  # CancelRequest (1234 << 16 | 5678)
_PROTOCOL_VERSION_3 = 196608  # StartupMessage protocol 3.0 (3 << 16)

if TYPE_CHECKING:  # REQ-1394 — the exchange is imported lazily at runtime, named here for typing.
    from provisa.auth.scram import ScramExchange

# REQ-890: bearer/JWT provider names whose cleartext password payload is an OIDC access token.
_OIDC_PROVIDERS = frozenset({"oidc", "oauth", "keycloak", "firebase"})

# REQ-1394: the authentication-request subcodes the SASL exchange uses. 3 is the cleartext request
# this server sent before SCRAM existed and still sends when SCRAM is off.
_AUTH_SASL = 10
_AUTH_SASL_CONTINUE = 11
_AUTH_SASL_FINAL = 12

# REQ-1394: the seed behind mock authentication. Per process and never persisted, so a username
# with no verifier gets a stable-looking salt within a connection and an unguessable one across
# deployments — the point being that an unknown user is answered exactly like a known one.
_MOCK_SEED = os.urandom(32)


def _pg_literal(v) -> str:
    """Render a Python value as a safe PG literal string."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return "E'\\\\x" + v.hex() + "'"
    if isinstance(v, (list, tuple)):
        return "'{" + ",".join(str(x) for x in v) + "}'"
    s = str(v)
    return "'" + s.replace("'", "''") + "'"


def _substitute_params(sql: str, params: list | None) -> str:
    """Replace $1, $2, ... with literal values (highest index first to avoid $1 matching $10)."""
    if not params:
        return sql
    result = sql
    for i in range(len(params), 0, -1):
        result = result.replace(f"${i}", _pg_literal(params[i - 1]))
    return result


def _tag_from_sql(sql: str) -> str:
    m = _TXN_TAG_RE.match(sql)
    if m:
        return m.group(1).upper().split()[0]
    return ""


_TYPE_TO_BVTYPE: dict[str, BVType] = {
    "INTEGER[]": BVType.INTEGERARRAY,
    "VARCHAR[]": BVType.STRINGARRAY,
    "BOOLEAN": BVType.BOOL,
    "FLOAT": BVType.FLOAT,
    "DOUBLE": BVType.FLOAT,
    "DECIMAL": BVType.DECIMAL,
    "TIMESTAMP": BVType.TIMESTAMP,
    "DATE": BVType.DATE,
    "TIME": BVType.TIME,
    # PostgreSQL result-type names (DIRECT sources now report real column types, REQ-883) —
    # without these, an int/float column would fall through to TEXT and mistype the client.
    "BOOL": BVType.BOOL,
    "FLOAT4": BVType.REAL,
    "FLOAT8": BVType.FLOAT,
    "NUMERIC": BVType.DECIMAL,
    "TIMESTAMPTZ": BVType.TIMESTAMPTZ,
    "TIMETZ": BVType.TIMETZ,
    "INT2": BVType.SMALLINT,
    "SMALLINT": BVType.SMALLINT,
    "TINYINT": BVType.SMALLINT,
    "UUID": BVType.UUID,
    # Declared engine types reported for every column (REQ-589): the Describe and the Execute use
    # these, never first-batch inference, so each value type must land on its own encoder.
    "TIMESTAMP WITH TIME ZONE": BVType.TIMESTAMPTZ,
    "TIMESTAMP_S": BVType.TIMESTAMP,
    "TIMESTAMP_MS": BVType.TIMESTAMP,
    "TIMESTAMP_NS": BVType.TIMESTAMP,
    "TIME WITH TIME ZONE": BVType.TIMETZ,
    "REAL": BVType.REAL,
    "DOUBLE PRECISION": BVType.FLOAT,
    "JSON": BVType.JSON,
    "JSONB": BVType.JSONB,
    "STRUCT": BVType.JSON,
    "MAP": BVType.JSON,
    "BLOB": BVType.BYTES,
    "BYTEA": BVType.BYTES,
    "INTERVAL": BVType.INTERVAL,
}
_INT_TYPES = {
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    # PostgreSQL integer type names
    "INT2",
    "INT4",
    "INT8",
}


def _sql_type_to_bvtype(type_str: str) -> BVType:
    # Case-normalize before lookup: _TYPE_TO_BVTYPE/_INT_TYPES are keyed uppercase (REQ-883), but
    # not every DIRECT-route driver reports type names that way — asyncpg's Type.name is lowercase
    # ("int4", not "INT4"). Confirmed live: an unrecognized-case int4 column fell through to
    # BVType.TEXT, whose BINARY-format converter (`lambda r: r.encode("utf-8")`) then crashed with
    # AttributeError on the raw int value. psycopg2 masked this — it requests TEXT wire format by
    # default, and TEXT's own converter is just `str`, tolerant of any Python type — so this was
    # never visible until a binary-preferring client (asyncpg) exercised the DIRECT route.
    type_str = type_str.upper()
    # A parameterized type name (DuckDB's "DECIMAL(18,2)", "NUMERIC(10,2)", "VARCHAR(20)",
    # "TIMESTAMP(3)") names the same wire type as its bare base: an exact lookup missed it and fell
    # to TEXT, whose binary converter then crashed on a Decimal. Array suffixes ("[]") are kept.
    type_str = re.sub(r"\s*\([^)]*\)", "", type_str).strip()
    if type_str in _TYPE_TO_BVTYPE:
        return _TYPE_TO_BVTYPE[type_str]
    # A 4-byte integer is advertised as int4 (OID 23), not widened to int8: a REQ-1863 passthrough
    # forwards the source's raw 4-byte int4 value, which a binary client would misread as int8.
    if type_str in ("INTEGER", "INT4", "INT"):
        return BVType.INTEGER
    if type_str in _INT_TYPES:
        return BVType.BIGINT
    if type_str.endswith("[]"):
        return BVType.JSON  # any other list type: its Python list value encodes as JSON
    return BVType.TEXT


def _infer_bvtype(rows: list[tuple], col_idx: int) -> BVType:
    for row in rows:
        v = row[col_idx] if col_idx < len(row) else None
        if v is None:
            continue
        if isinstance(v, bool):
            return BVType.BOOL
        if isinstance(v, int):
            return BVType.BIGINT
        if isinstance(v, float):
            return BVType.FLOAT
        if isinstance(v, decimal.Decimal):
            return BVType.DECIMAL
        if isinstance(v, datetime.datetime):
            return BVType.TIMESTAMP
        if isinstance(v, datetime.date):
            return BVType.DATE
        if isinstance(v, datetime.time):
            return BVType.TIME
        if isinstance(v, list):
            if v and isinstance(v[0], int):
                return BVType.INTEGERARRAY
            if v and isinstance(v[0], str):
                return BVType.STRINGARRAY
            return BVType.JSON
        if isinstance(v, dict):
            return BVType.JSON
        return BVType.TEXT
    return BVType.TEXT


def _to_decimal(v: Any) -> decimal.Decimal:
    return v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))


def _to_date(v: Any) -> Any:
    return v.date() if isinstance(v, datetime.datetime) else v


def _to_timestamp(v: Any) -> Any:
    if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
        return datetime.datetime(v.year, v.month, v.day)
    return v


# REQ-589: a prepared statement's rows are encoded as the types its Describe told the client. A
# source or engine may compute an expression in a wider or narrower type than the one described
# (Postgres averages an integer to numeric, DuckDB sums one to HUGEINT); these bring the value to
# the described type before its encoder runs. A type absent here encodes the value as it is.
_SHAPE_COERCERS: dict[BVType, Callable[[Any], Any]] = {
    BVType.INTEGER: int,
    BVType.BIGINT: int,
    BVType.SMALLINT: int,
    BVType.FLOAT: float,
    BVType.REAL: float,
    BVType.DECIMAL: _to_decimal,
    BVType.DATE: _to_date,
    BVType.TIMESTAMP: _to_timestamp,
    BVType.TIMESTAMPTZ: _to_timestamp,
}


class _StatementDescription(BVQueryResult):  # REQ-589
    """A Describe(Statement)'s answer: the statement's result columns, derived from registered
    metadata, with no rows — nothing ran. ``statement_shape`` is what every Execute of the statement
    is encoded to; ``prepared`` is the governed statement the next Execute continues from."""

    def __init__(self, shape: list[Tuple[str, str]], prepared: Any, sql: str) -> None:
        super().__init__()
        self.statement_shape = shape
        self.prepared = prepared
        self._types = [_sql_type_to_bvtype(t) for _, t in shape]
        self._status = _tag_from_sql(sql)

    def has_results(self) -> bool:
        return len(self.statement_shape) > 0

    def column_count(self) -> int:
        return len(self.statement_shape)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self.statement_shape[index][0], self._types[index])

    def rows(self) -> Iterator[list]:
        return iter(())

    def status(self) -> str:
        return self._status or "OK"


class ProvisaQueryResult(BVQueryResult):  # REQ-529, REQ-028
    """Adapts a :class:`ResultStream` (streaming ENGINE terminal, materialized DIRECT/admin
    result, or DuckDB catalog result) to the buenavista QueryResult ABC.

    Rows are pulled lazily: a streaming result's batches are drained only as buenavista emits
    DataRow messages, so a large user result set never fully materializes. The wire protocol
    needs column types up front (RowDescription precedes DataRow); when the engine supplies no
    per-column types, exactly ONE batch is buffered to infer them — a bounded peek, not the
    whole result."""

    def __init__(
        self,
        engine_result: ResultStream,
        original_sql: str = "",
        shape: list[Tuple[str, str]] | None = None,
        plan: Any = None,
    ):
        super().__init__()
        # Held so close() can release it (server-side cursor / pooled source connection): when a
        # portal is dropped without ever being executed (Describe with no Execute), or when a
        # DECLARE CURSOR built on this result is closed, without draining the remaining rows.
        self._engine_result = engine_result
        self._cols = engine_result.column_names
        self._status = _tag_from_sql(original_sql)
        self._batch_iter: Iterator[list] = engine_result.batches()  # type: ignore[assignment]
        if plan is not None:
            # REQ-074: the statement's audit row is written when this drain ends (audit_on_drain).
            from provisa.pgwire._pipeline import audit_on_drain

            self._batch_iter = audit_on_drain(plan, self._batch_iter)
        self._head: list | None = None
        # REQ-1863 large-result fix: a batch pulled from self._batch_iter but only PARTIALLY
        # forwarded when send_data_rows (vendor/buenavista) stops early at the client's own
        # Execute limit — see rows()'s own docstring for why that happens on every multi-batch
        # fetch. Persisted on self (not local to one .rows() call) so the NEXT .rows() call
        # resumes this exact batch at this exact offset instead of pulling a fresh one and
        # silently dropping the unforwarded tail.
        self._pending: list | None = None
        self._pending_pos: int = 0
        ctypes = engine_result.column_types
        # (column index, coercer) for each column whose rows must be brought to the described type.
        self._coerce: list[Tuple[int, Callable[[Any], Any]]] = []
        if shape is not None:
            # REQ-589: this statement was DESCRIBED — the client decodes its rows by that shape, so
            # the names and types are the described ones, never re-derived from the result.
            if len(shape) != len(self._cols):
                closer = getattr(engine_result, "close", None)
                if closer is not None:
                    closer()
                raise RuntimeError(
                    f"the statement was described with {len(shape)} result column(s) "
                    f"{[n for n, _ in shape]} but its execution returned {len(self._cols)} "
                    f"{list(self._cols)}"
                )
            self._cols = [name for name, _ in shape]
            self._types = [_sql_type_to_bvtype(t) for _, t in shape]
            for i, described in enumerate(self._types):
                actual = ctypes[i] if ctypes and i < len(ctypes) else None
                if actual is not None and _sql_type_to_bvtype(actual) == described:
                    continue
                coercer = _SHAPE_COERCERS.get(described)
                if coercer is not None:
                    self._coerce.append((i, coercer))
            return
        # A None entry (or absent types) means the type must be inferred from data, which
        # requires the first batch on hand before RowDescription is sent.
        if not ctypes or any(t is None for t in ctypes):
            self._head = next(self._batch_iter, [])
        if ctypes:
            self._types = [
                _sql_type_to_bvtype(t) if t else _infer_bvtype(self._head or [], i)
                for i, t in enumerate(ctypes)
            ]
        else:
            self._types = [_infer_bvtype(self._head or [], i) for i in range(len(self._cols))]

    def has_results(self) -> bool:
        return len(self._cols) > 0

    def column_count(self) -> int:
        return len(self._cols)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self._cols[index], self._types[index])

    def rows(self) -> Iterator[list]:
        # send_data_rows (vendor/buenavista) calls .rows() fresh on EVERY Execute message of a
        # paginated (portal-suspend) fetch, not once for the whole result — self._head must be
        # consumed exactly once across all of those calls, or every Execute after the first
        # re-yields the same peeked head batch before reaching new rows, so a multi-batch fetch
        # (any client with a row limit under the total result size — e.g. asyncpg's
        # cursor.fetch(n)) never advances past that first batch. Clearing it here, before the
        # first row is yielded, makes the peek genuinely one-time.
        if self._head is not None:
            head, self._head = self._head, None
            self._pending, self._pending_pos = head, 0
        # REQ-1863 large-result fix: send_data_rows stops calling next() on this generator as
        # soon as it has forwarded `limit` rows for THIS Execute — nearly always mid-batch when
        # the engine's own internal batch size (e.g. _STREAM_BATCH_ROWS, provisa/federation/
        # runtime_support.py) doesn't evenly divide the client's own fetch size (e.g. asyncpg
        # cursor.fetch(n)). This generator is simply abandoned at that point (a fresh one is
        # created for the NEXT Execute), so `for batch in self._batch_iter: yield from batch`
        # silently drops whatever of the current `batch` list was never reached — live-confirmed
        # against a real 2,000,000-row scan: only 310,000 (31 x asyncpg's own 10,000-row
        # cursor.fetch batch size) rows ever reached the client, with the remaining ~1.69M rows
        # pulled from the source and discarded without any error. self._pending/_pending_pos
        # track exactly how far into the current batch this call got, on `self` (not a local
        # generator frame), so the NEXT .rows() call resumes the SAME batch at the SAME offset
        # instead of pulling (and partially discarding) a new one.
        while True:
            if self._pending is not None:
                while self._pending_pos < len(self._pending):
                    row = self._pending[self._pending_pos]
                    self._pending_pos += 1
                    yield row
                self._pending = None
                self._pending_pos = 0
                continue
            batch = next(self._batch_iter, None)
            if batch is None:
                return
            if self._coerce and batch and not isinstance(batch[0], bytes):
                batch = [self._coerced(row) for row in batch]
            self._pending, self._pending_pos = batch, 0

    def _coerced(self, row: Any) -> list:
        out = list(row)
        for i, coercer in self._coerce:
            if out[i] is not None:
                out[i] = coercer(out[i])
        return out

    def status(self) -> str:
        return self._status or "OK"

    def close(self) -> None:
        """Release the underlying stream without draining remaining rows: used both for a
        cursor CLOSE and when a portal is dropped without ever being executed (Describe with
        no Execute).
        """
        # Materialized results (executor.result.QueryResult) have nothing to release; streaming
        # results (executor.result.StreamingQueryResult) do — duck-typed the same way
        # StreamingQueryResult.close() itself checks its source, rather than importing both
        # concrete result types here just to isinstance-check them.
        closer = getattr(self._engine_result, "close", None)
        if closer is not None:
            closer()
        # REQ-074: a result released before its drain ended is recorded with what it delivered.
        finish = getattr(self._batch_iter, "finish", None)
        if finish is not None:
            finish()


class _CursorRowsResult(BVQueryResult):  # REQ-1862
    """Adapts a batch of already-fetched cursor rows to buenavista's QueryResult ABC, for
    sending a FETCH response's RowDescription/DataRow pair."""

    def __init__(self, columns: list[Tuple[str, BVType]], fetched: list[list]):
        super().__init__()
        self._columns = columns
        self._fetched = fetched

    def has_results(self) -> bool:
        return True

    def column_count(self) -> int:
        return len(self._columns)

    def column(self, index: int) -> Tuple[str, BVType]:
        return self._columns[index]

    def rows(self) -> Iterator[list]:
        yield from self._fetched

    def status(self) -> str:
        return "OK"


class _CursorNotScrollableError(Exception):
    """Raised for PRIOR/BACKWARD/ABSOLUTE/LAST/negative-RELATIVE against a cursor that was not
    DECLAREd SCROLL — matches real PostgreSQL, which rejects backward navigation on a NO SCROLL
    cursor rather than silently returning wrong (or no) rows."""


@dataclass
class _CursorState:  # REQ-1862
    """A named SQL cursor's live state, owned by the pgwire session that DECLAREd it.

    ``source`` is the DECLARE SELECT's row generator (``ProvisaQueryResult.rows()``), drained
    forward-only and on demand.

    ``scrollable`` reflects whether the cursor was DECLAREd ``SCROLL`` (PostgreSQL's default is
    ``NO SCROLL`` when neither keyword is given — REQ-1862 amendment). A ``SCROLL`` cursor
    accumulates every row pulled from ``source`` into ``buffer`` forever, so PRIOR/BACKWARD/
    ABSOLUTE/LAST can be served without a rewindable engine cursor; that unbounded memory is the
    accepted, correct cost of an explicitly-scrollable cursor. A ``NO SCROLL`` cursor only ever
    moves forward, so rows behind ``pos`` can never be needed again: ``_cursor_move`` trims them
    out of ``buffer`` as it advances, keeping memory bounded to the current FETCH batch instead of
    the full result (e.g. an 80M-row FETCH FORWARD scan) — see REQ-1862 amendment 2026-09-25.

    ``pos`` is the 0-indexed boundary *within the current buffer*: the last row returned to the
    client is ``buffer[pos - 1]``. For a NO SCROLL cursor, ``pos`` is reset to 0 whenever consumed
    rows are trimmed from the front of ``buffer``, so it never grows with the amount already
    fetched.
    """

    name: str
    query_result: ProvisaQueryResult
    source: Iterator[list]
    scrollable: bool = False
    buffer: list = _dc_field(default_factory=list)
    pos: int = 0
    source_exhausted: bool = False


def _cursor_fill(cs: _CursorState, upto) -> None:
    """Pull from ``cs.source`` until ``cs.buffer`` has ``upto`` rows or the source is exhausted.

    ``upto`` may be ``float("inf")`` to fully drain the cursor (FETCH/MOVE ALL, or an ABSOLUTE/
    LAST position counted from the end)."""
    _sentinel = object()
    while not cs.source_exhausted and len(cs.buffer) < upto:
        row = next(cs.source, _sentinel)
        if row is _sentinel:
            cs.source_exhausted = True
            break
        cs.buffer.append(row)


def _cursor_move(cs: _CursorState, direction: str, count: int | None) -> list:
    """Advance ``cs`` per FETCH/MOVE semantics and return the rows the client should see
    (already in client-visible order — BACKWARD returns most-recent-first, like real PG).

    A NO SCROLL cursor (``cs.scrollable`` False) only permits forward movement — PRIOR, BACKWARD,
    ABSOLUTE, LAST, and RELATIVE with a negative count raise ``_CursorNotScrollableError``, same as
    real PostgreSQL rejecting those against a NO SCROLL cursor."""
    if not cs.scrollable and not (
        direction == "forward" or (direction == "relative" and (count or 0) >= 0)
    ):
        raise _CursorNotScrollableError(
            f'cursor "{cs.name}" can only scan forward — DECLARE it with SCROLL to fetch '
            f"{direction.upper()}"
        )
    if direction == "forward":
        if count is None:
            _cursor_fill(cs, float("inf"))
            end = len(cs.buffer)
        else:
            _cursor_fill(cs, cs.pos + count)
            end = min(cs.pos + count, len(cs.buffer))
        out = cs.buffer[cs.pos : end]
        cs.pos = end
        if not cs.scrollable:
            # Forward-only cursor: rows behind pos can never be needed again (BACKWARD/ABSOLUTE
            # are rejected above), so drop them instead of retaining the entire result in memory.
            del cs.buffer[: cs.pos]
            cs.pos = 0
        return out
    if direction == "backward":
        start = 0 if count is None else max(0, cs.pos - count)
        out = list(reversed(cs.buffer[start : cs.pos]))
        cs.pos = start
        return out
    if direction == "absolute":
        k = count or 0
        if k >= 1:
            _cursor_fill(cs, k)
            if k > len(cs.buffer):
                cs.pos = len(cs.buffer)
                return []
            cs.pos = k
            return [cs.buffer[k - 1]]
        if k == 0:
            cs.pos = 0
            return []
        _cursor_fill(cs, float("inf"))  # negative k counts from the end — needs the full extent
        idx = len(cs.buffer) + k
        if idx < 0 or idx >= len(cs.buffer):
            cs.pos = 0 if idx < 0 else len(cs.buffer)
            return []
        cs.pos = idx + 1
        return [cs.buffer[idx]]
    if direction == "relative":
        n = count or 0
        return _cursor_move(cs, "forward", n) if n >= 0 else _cursor_move(cs, "backward", -n)
    if direction == "first":
        return _cursor_move(cs, "absolute", 1)
    if direction == "last":
        return _cursor_move(cs, "absolute", -1)
    raise ValueError(f"unknown cursor direction: {direction!r}")


_CURSOR_FROM_IN_RE = re.compile(
    r"(?:\bFROM\b|\bIN\b)\s+(?P<name>\"[^\"]+\"|[A-Za-z_][\w$]*)\s*;?\s*$", re.IGNORECASE
)


def _parse_cursor_nav(stmt: str, keyword: str) -> tuple[str, str, int | None]:
    """Parse a FETCH/MOVE statement into (direction, raw cursor name, count).

    ``direction`` is one of forward/backward/absolute/relative/first/last. ``count`` is None for
    an unbounded ALL fetch, else the already sign-normalized row count.
    """
    body = re.sub(rf"^\s*{keyword}\b", "", stmt, flags=re.IGNORECASE).strip()
    if body.endswith(";"):
        body = body[:-1].strip()
    m = _CURSOR_FROM_IN_RE.search(body)
    if m:
        name = m.group("name")
        spec = body[: m.start()].strip()
    else:
        parts = body.rsplit(None, 1)
        if len(parts) == 2:
            spec, name = parts
        elif len(parts) == 1:
            spec, name = "", parts[0]
        else:
            raise ValueError(f"missing cursor name in {keyword}")
    su = spec.upper()
    if su in ("", "NEXT"):
        return "forward", name, 1
    if su == "PRIOR":
        return "backward", name, 1
    if su == "FIRST":
        return "first", name, None
    if su == "LAST":
        return "last", name, None
    if su in ("ALL", "FORWARD ALL"):
        return "forward", name, None
    if su == "BACKWARD ALL":
        return "backward", name, None
    m2 = re.match(r"^(FORWARD|BACKWARD)\s+(\d+)$", su)
    if m2:
        direction = "forward" if m2.group(1) == "FORWARD" else "backward"
        return direction, name, int(m2.group(2))
    m3 = re.match(r"^(ABSOLUTE|RELATIVE)\s+([-+]?\d+)$", su)
    if m3:
        return m3.group(1).lower(), name, int(m3.group(2))
    m4 = re.match(r"^([-+]?\d+)$", su)
    if m4:
        n = int(m4.group(1))
        return ("backward", name, -n) if n < 0 else ("forward", name, n)
    raise ValueError(f"unsupported {keyword} specification: {spec!r}")


def _normalize_cursor_name(raw: str) -> str:
    """Fold an unquoted cursor identifier like PostgreSQL does; keep a quoted one verbatim."""
    raw = raw.strip()
    if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw.lower()


class ProvisaSession(Session):  # REQ-001, REQ-002, REQ-266
    def __init__(self) -> None:
        super().__init__()
        self.role_id: str | None = None
        # REQ-074/REQ-1386: the authenticated principal this session acts as — the audit log's
        # user_id. Set wherever role_id is set (trust mode: the startup packet's user; secured
        # modes: the validated identity), so an authenticated session always has both.
        self.user_id: str | None = None
        # REQ-1266: the org this session is bound to (multitenant OIDC sessions only). None → the
        # single-org default runtime (trust/simple modes, or a platform admin with no single org).
        self.org_id: str | None = None
        # REQ-1862: named SQL cursors DECLAREd on this connection, keyed by normalized name.
        self.cursors: dict[str, _CursorState] = {}
        # REQ-1882: the connection thread that created this session and runs its loop.
        self._owner_thread: int | None = threading.get_ident()
        self._close_requested = False

    def cursor(self):
        return None

    def close(self):
        # REQ-1882: a cursor's stream is pumped on its connection's loop, which only the connection
        # thread may run. A CancelRequest arrives on ANOTHER connection and closes the session from
        # there; the owning thread releases the cursors when its handler exits (close_owned).
        owner = self._owner_thread
        if owner is not None and owner != threading.get_ident():
            self._close_requested = True
            return
        self.close_owned()

    def close_owned(self):
        """Release cursors on the owning connection thread (its loop is bound here)."""
        # REQ-1862: release every still-open cursor's underlying stream (server-side cursor /
        # pooled connection) — a client that disconnects mid-cursor must not leak it.
        for cs in self.cursors.values():
            try:
                cs.query_result.close()
            except Exception:
                log.debug("[PGWIRE] cursor cleanup failed for %r", cs.name, exc_info=True)
        self.cursors.clear()

    def in_transaction(self) -> bool:
        return False

    def load_df_function(self, table: str):
        del table
        return None

    def execute_sql(
        self, sql: str, params=None, result_fmt=None, *, prepared=None, shape=None
    ) -> ProvisaQueryResult:
        """``prepared`` is the governed statement this statement's Describe produced (the Execute
        continues from it instead of governing again); ``shape`` is the result shape that Describe
        told the client, which the rows are encoded to. Both are None for a statement that was
        never described (the simple protocol, or a Bind with no Describe(Statement))."""
        return self._with_org(
            lambda: self._execute_sql_bound(sql, params, result_fmt, prepared=prepared, shape=shape)
        )

    def describe_sql(self, sql: str, params=None) -> BVQueryResult:
        """Describe(Statement) (REQ-589, amended 2026-10-01): the statement is governed through
        the one pipeline — so a column the role cannot see is absent here too — and its result
        columns are derived from registered metadata. Nothing is routed, executed, or sent to a
        source or an engine, and no source connection is held for the Bind that follows. The
        Execute continues from the governed statement held here and encodes its rows to this
        shape, so describe and execute agree by construction and the statement is governed once.

        A catalog statement (INTERCEPT) is answered in-process from the catalog emulation, as its
        Execute is."""
        return self._with_org(lambda: self._describe_bound(sql, params))

    def _describe_bound(self, sql: str, params) -> BVQueryResult:
        from provisa.pgwire.catalog import classify

        stripped = sql.strip()
        if classify(stripped) == "INTERCEPT":
            return self._execute_sql_bound(sql, params, None)
        if self.role_id is None:
            raise RuntimeError("Not authenticated")
        if self.user_id is None:
            raise RuntimeError("Authenticated session has no principal")

        from provisa.audit.context import with_audit_identity
        from provisa.core.connection_loop import current_connection_loop
        from provisa.pgwire._pipeline import describe_pgwire_statement

        try:
            # REQ-074/REQ-1386: a refusal at Describe is audited under the acting principal.
            described = current_connection_loop().run(
                _run_with_org(
                    self.org_id,
                    with_audit_identity(
                        self.user_id, "pgwire", describe_pgwire_statement(stripped, self.role_id)
                    ),
                ),
                timeout=request_timeout_for("pgwire"),
            )
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            log.warning("[PGWIRE] DESCRIBE EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc
        return _StatementDescription(described.shape, described.governed, stripped)

    def _with_org(self, fn: Callable[[], Any]) -> Any:
        # REQ-1266: bind this session's org on the connection thread so the sync state.X reads
        # below (answer/INTERCEPT, execute_engine_sync, source_pools) route to its runtime; the
        # governance/execute coroutines run on this thread's loop and are bound again explicitly via
        # _run_with_org. None → default runtime (no bind).
        if self.org_id is None:
            return fn()
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(self.org_id)
        try:
            return fn()
        finally:
            reset_current_org(token)

    def _execute_sql_bound(
        self, sql: str, params=None, result_fmt=None, *, prepared=None, shape=None
    ) -> ProvisaQueryResult:
        from provisa.pgwire.catalog import answer, classify

        # REQ-589: the statement keeps its $N placeholders and the client's values stay BOUND all
        # the way to the engine/source (govern_pgwire_plan(params=...)) — never spliced in here, so
        # every value shares one SQL text (server-side prepares, SQL-text-keyed caches) and a value
        # can never change the governed shape. Only the INTERCEPT catalog emulation (not a governed
        # query) reads the values inline.
        stripped = sql.strip()
        bound = list(params) if params else None
        disposition = classify(stripped)
        if disposition == "INTERCEPT":
            from provisa.api.app import state

            stripped = _substitute_params(stripped, params)
            result = answer(stripped, self.role_id or "", state)
            log.debug(
                "[RESULT] cols=%r rows=%r",
                result.column_names,
                result.rows[:3] if result.rows else [],
            )
            return ProvisaQueryResult(result, stripped)

        if self.role_id is None:
            raise RuntimeError("Not authenticated")
        if self.user_id is None:
            # Both are set together at authentication; a role without a principal means the
            # session was admitted by a path that never identified its caller.
            raise RuntimeError("Authenticated session has no principal")

        from provisa.core.connection_loop import current_connection_loop

        cl = current_connection_loop()

        from provisa.api.app import state
        from provisa.pgwire._pipeline import (
            _execute_plan,
            _Plan,
            govern_pgwire_plan,
            governed_statement_is_current,
            plan_pgwire_statement,
            require_governed_plan,
        )

        # REQ-589: a statement its Describe already governed is only ROUTED here, with the Bind's
        # values — governance does not run a second time. A held statement governed under a schema
        # generation that has since been rebuilt is stale and is governed again.
        # REQ-1897: the Bind's result format codes go to the planner, which looks an opted-in
        # read up in the response cache BEFORE routing; a hit comes back as a Route.CACHE plan and
        # is served by the _execute_plan branch below (decoded rows, or the raw DataRow replay
        # written for these codes).
        _formats = list(result_fmt) if result_fmt else None
        if prepared is not None and governed_statement_is_current(prepared, state):
            to_plan = plan_pgwire_statement(prepared, bound, _formats)
        else:
            to_plan = govern_pgwire_plan(stripped, self.role_id, bound, _formats)

        # Govern on this connection's loop, then — for the ENGINE route — drain the engine's SYNC
        # streaming terminal on this same thread (REQ-028). Mirrors Flight SQL's govern-then-stream
        # split: the private engine cursor is created and drained here, and rows flow lazily as
        # buenavista emits DataRow. DIRECT/admin/govdata routes are async-native and materialize
        # via the connection loop — all on this one thread (REQ-1882).
        _t_govern0 = time.perf_counter()
        with _stage(_tracer, "pgwire.govern", name="govern"):  # REQ-1910
            try:
                # REQ-074/REQ-1386: the acting principal is bound inside the coroutine, so the
                # governor's audit/denial write records who ran the statement and that it arrived over
                # pgwire.
                from provisa.audit.context import with_audit_identity

                governed = cl.run(
                    _run_with_org(
                        self.org_id,
                        with_audit_identity(self.user_id, "pgwire", to_plan),
                    ),
                    timeout=request_timeout_for("pgwire"),
                )
            except PermissionError as exc:
                raise PermissionError(str(exc)) from exc
            except Exception as exc:
                log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
                raise RuntimeError(str(exc)) from exc
        # Parse/govern/route timing, isolated from physical execution below, so the pure-Python
        # compile-path cost (parse → govern_pgwire_plan → routing decision) can be measured
        # separately from engine/source execution time — logged at DEBUG so it's zero-cost in
        # normal operation.
        _t_govern1 = time.perf_counter()

        from buenavista.postgres import BVTYPE_TO_PGTYPE

        from provisa.transpiler.router import Route

        # The type OIDs the client was told at Describe (None when the statement was not described)
        # — a raw-DataRow passthrough forwards the source's bytes only where they have that layout.
        described_oids = (
            [BVTYPE_TO_PGTYPE[_sql_type_to_bvtype(t)][0] for _, t in shape]
            if shape is not None
            else None
        )

        from provisa.pgwire._pipeline import serve_stream_through_cache

        def cache_run(coro):  # REQ-1897: cache reads/writes on this connection's loop and org
            return cl.run(_run_with_org(self.org_id, coro), timeout=30)

        with _stage(_tracer, "pgwire.execute", name="execute"):  # REQ-1910
            try:
                if isinstance(governed, _Plan) and governed.route == Route.ENGINE:
                    # REQ-1176: this streaming sink runs physical_sql on the engine directly (like
                    # Flight SQL), so it MUST verify the governed-provenance stamp before the engine
                    # executes — the single-chokepoint guarantee is not satisfied by _execute_plan alone.
                    require_governed_plan(governed)
                    if governed.physical_sql is None:
                        raise RuntimeError("ENGINE plan missing physical_sql")
                    # REQ-1661: this streaming sink bypasses _execute_plan_in_org entirely (that's the
                    # whole point — the pgwire worker thread drains the engine terminal itself so a
                    # large result never materializes on the loop), so its own ensure_resident call is
                    # the ONLY place a MATERIALIZED source this plan reads gets landed before the
                    # engine executes. Confirmed live: a cross-engine federated_join touching a never-
                    # yet-landed ClickHouse table failed "Binder Error: Catalog ... does not exist" on
                    # its first run of a fresh boot — _attach_registered's own attach attempt for a LAND
                    # source is caught and logged, never raised, so nothing else would have surfaced it.
                    #
                    # REQ-1865: this streaming sink also never called ensure_rows_resident (it bypasses
                    # _execute_plan_in_org entirely, same reason ensure_resident is duplicated above) --
                    # a row_materialize table this plan's predicate DIRECTLY binds (e.g.
                    # bench_customer_node's own customer_id) was never keyed-fetched for this transport
                    # at all. Must run BEFORE the key-pushdown probe below: a join where every table is
                    # row_materialize needs the directly-bound ones populated first, or the probe's
                    # LEFT-preserved "known" side is itself still empty and resolves zero keys.
                    # REQ-1865 key pushdown: same reason this streaming sink needs its own
                    # ensure_resident call applies to pushdown_row_materialize -- it must run here too,
                    # not just in _execute_plan_in_org, or a JOIN-reached row_materialize table (e.g.
                    # cypher_cross_engine's bench_contains_edge) never gets landed for this transport at
                    # all (confirmed live: 0 rows, no [DIAG] trace, for every SQL-transport query here).
                    # REQ-1887: folded into one connection-loop run — see
                    # prepare_residency_and_check_cache (provisa/pgwire/_pipeline.py), shared with
                    # Flight SQL's identical ENGINE-route fold.
                    # REQ-1897: this streaming sink bypasses _execute_plan_in_org entirely, so it needs
                    # its own cache-HIT check too — folded into this SAME dispatch (not a second hop)
                    # via prepare_residency_and_check_cache, which checks the cache FIRST and skips
                    # residency prep entirely on a HIT (nothing to land if the engine is never dialled).
                    # A HIT is served as ordinary decoded rows -- the COPY-binary encoder downstream
                    # consumes any QueryResult-shaped `result` identically whether it came from the
                    # engine or the cache -- skipping BOTH the raw-wire-forwarding passthrough below
                    # and execute_engine_sync. check_response_cache itself audits/egress-accounts a HIT.
                    from provisa.pgwire._pipeline import prepare_residency_and_check_cache

                    result = cl.run(
                        prepare_residency_and_check_cache(governed, state),
                        timeout=request_timeout_for("pgwire"),
                    )
                    if result is None:
                        engine_plan = governed
                        # REQ-1897: the one read/write-through for this streaming sink — a raw
                        # DataRow (pg_datarows) HIT when the passthrough applies, else the decoded
                        # stream teed into the raw-SQL cache (the decoded HIT was checked above).
                        result = serve_stream_through_cache(
                            engine_plan,
                            state,
                            run=cache_run,
                            check_rows=False,
                            # REQ-1863 counterpart: when the bound federation ENGINE is itself
                            # Postgres (PROVISA_ENGINE=pg), pgwire and the engine both speak real
                            # Postgres wire protocol end to end — raw DataRows are forwarded; a
                            # PassthroughError falls through to execute_engine_sync below.
                            passthrough=(
                                (
                                    result_fmt,
                                    lambda: state.federation_engine.execute_pg_engine_passthrough(
                                        engine_plan.physical_sql,
                                        engine_plan.exec_params,
                                        result_fmt,
                                        described_oids=described_oids,
                                    ),
                                )
                                if result_fmt
                                and state.federation_engine.dialect in ("postgres", "postgresql")
                                else None
                            ),
                            open_rows=lambda: state.federation_engine.execute_engine_sync(
                                engine_plan.physical_sql,
                                engine_plan.exec_params,
                                session_hints=engine_plan.session_hints,
                            ),
                        )
                elif (
                    isinstance(governed, _Plan)
                    and governed.route == Route.DIRECT
                    and governed.source_id
                    and state.source_pools.has(governed.source_id)
                    and state.source_pools.supports_stream(governed.source_id)
                    and result_fmt
                    and state.source_pools.dialect_for(governed.source_id)
                    in ("postgres", "postgresql")
                ):
                    # REQ-1863: the DIRECT source is itself Postgres and the downstream client's own
                    # requested result_format is known (result_fmt is only populated for an
                    # Execute/Bind dispatch, never a bare Describe) — forward its DataRow bytes
                    # unmodified rather than decoding into asyncpg.Record and re-encoding. Masking/RLS
                    # need no separate check here: already baked into governed.sql's text regardless
                    # of route. Falls back to the decode/re-encode path below on ANY PassthroughError
                    # (never a correctness risk, purely a fast path).
                    require_governed_plan(governed)
                    direct_plan = governed
                    # REQ-1897: pg_datarows HIT replayed undecoded, else the passthrough teed; a
                    # PassthroughError falls back to the decoded DIRECT stream (rows HIT / teed).
                    result = serve_stream_through_cache(
                        direct_plan,
                        state,
                        run=cache_run,
                        check_rows=True,
                        passthrough=(
                            result_fmt,
                            lambda: state.federation_engine.execute_pg_passthrough(
                                state.source_pools,
                                direct_plan.source_id,
                                direct_plan.sql,
                                direct_plan.exec_params,
                                result_fmt,
                                described_oids=described_oids,
                            ),
                        ),
                        open_rows=lambda: state.federation_engine.execute_native_stream(
                            state.source_pools,
                            direct_plan.source_id,
                            direct_plan.sql,
                            direct_plan.exec_params,
                            run=cl.run,
                        ),
                    )
                elif (
                    isinstance(governed, _Plan)
                    and governed.route == Route.DIRECT
                    and governed.source_id
                    and state.source_pools.has(governed.source_id)
                    and state.source_pools.supports_stream(governed.source_id)
                ):
                    # REQ-1190: a single-reachable-source scan STREAMS via the source's server-side cursor,
                    # drained on this worker thread just like the ENGINE terminal — never materialized on the
                    # loop (streaming-uniformity Defect 1). REQ-1176: verify the stamp before the source runs.
                    require_governed_plan(governed)
                    direct_plan = governed
                    # REQ-1897: a decoded HIT served, else the source's stream teed into the cache.
                    result = serve_stream_through_cache(
                        direct_plan,
                        state,
                        run=cache_run,
                        check_rows=True,
                        passthrough=None,
                        open_rows=lambda: state.federation_engine.execute_native_stream(
                            state.source_pools,
                            direct_plan.source_id,
                            direct_plan.sql,
                            direct_plan.exec_params,
                            run=cl.run,
                        ),
                    )
                elif isinstance(governed, _Plan):
                    result = cl.run(
                        _run_with_org(self.org_id, _execute_plan(governed)),
                        timeout=request_timeout_for("pgwire"),
                    )
                else:
                    result = governed  # registered-function call: bounded, already materialized
            except PermissionError as exc:
                self._finalize_audit(governed, 500)
                raise PermissionError(str(exc)) from exc
            except Exception as exc:
                self._finalize_audit(governed, 500)
                log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
                raise RuntimeError(str(exc)) from exc
        _t_execute1 = time.perf_counter()
        log.debug(
            "[PGWIRE TIMING] govern=%.1fms execute=%.1fms sql=%r",
            (_t_govern1 - _t_govern0) * 1000,
            (_t_execute1 - _t_govern1) * 1000,
            stripped[:80],
        )
        _annotate_request(db__statement=stripped[:1000])  # recorded in debug detail only

        # REQ-074/REQ-1386: the ENGINE/DIRECT streaming terminals above never reach _execute_plan,
        # so the audit row is written here. Idempotent — the _execute_plan branch already wrote it.
        # The row itself is written when the client has drained the result (or stops reading it),
        # so it carries the rows delivered; a plan already recorded (cache hit, buffered chokepoint)
        # is not recorded again.
        self._finalize_audit(governed, 200, defer_to_drain=True)
        try:
            return ProvisaQueryResult(
                result, stripped, shape, plan=governed if isinstance(governed, _Plan) else None
            )
        except Exception:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise

    def _finalize_audit(self, governed, status_code: int, *, defer_to_drain: bool = False) -> None:
        """Write the governed plan's audit row on this connection's loop, under the session's org.
        ``defer_to_drain``: the result is a stream the client has not read yet — the row is
        written when its drain ends (see ``ProvisaQueryResult``), with the rows delivered."""
        from provisa.core.connection_loop import run_on_connection_loop
        from provisa.pgwire._pipeline import _Plan, finalize_audit

        if not isinstance(governed, _Plan):
            return  # a registered-function call carries no plan
        run_on_connection_loop(
            _run_with_org(
                self.org_id, finalize_audit(governed, status_code, defer_to_drain=defer_to_drain)
            ),
            timeout=30,
        )


class ProvisaConnection(Connection):  # REQ-529
    def new_session(self) -> ProvisaSession:
        return ProvisaSession()

    def parameters(self) -> dict[str, str]:
        # Startup ParameterStatus set. server_version declares PG 14, so we
        # report the full PG-14 hard-wired set (PG protocol §54.2), including
        # the PG-14 additions default_transaction_read_only and in_hot_standby.
        # Values are sourced from _KNOWN_SETTINGS so the handshake and
        # SHOW/current_setting stay consistent. Casing follows what PG sends.
        from provisa.pgwire.catalog_data import _KNOWN_SETTINGS as s

        return {
            "server_version": s["server_version"],
            "server_encoding": s["server_encoding"],
            "client_encoding": s["client_encoding"],
            "application_name": s["application_name"],
            "is_superuser": s["is_superuser"],
            "session_authorization": s["session_authorization"],
            "DateStyle": s["datestyle"],
            "IntervalStyle": s["intervalstyle"],
            "TimeZone": s["timezone"],
            "integer_datetimes": s["integer_datetimes"],
            "standard_conforming_strings": s["standard_conforming_strings"],
            "default_transaction_read_only": s["default_transaction_read_only"],
            "in_hot_standby": s["in_hot_standby"],
        }


class ProvisaHandler(BuenaVistaHandler):  # REQ-120, REQ-124, REQ-125, REQ-273
    """Extends BuenaVistaHandler with TLS, cleartext auth, and catalog intercept."""

    # REQ-1452: the metering writer, held apart from ``wfile`` because socketserver declares that
    # attribute as the plain stream it hands out. ``setup`` installs one before the first byte is
    # written and the TLS upgrade installs the next, so this always names the writer currently
    # counting -- and the org binding after auth reaches the one in force.
    _meter: CountingWriter

    # REQ-1394: what was advertised in the authentication request, and the exchange in flight.
    # Class-level so the state exists from the first byte — a connection that never reached
    # send_auth_request has been offered nothing and must not be read as mid-SASL.
    _sasl_offered: bool = False
    _sasl: "ScramExchange | None" = None
    # The session this connection's startup created (REQ-1882: closed on this thread).
    _session: "ProvisaSession | None" = None
    # REQ-1910: the request span of the statement cycle in flight — one simple Query, or one
    # extended-protocol cycle from its first Parse/Bind/Describe/Execute to the Sync that ends it.
    # Held, not a block: the cycle's messages are dispatched one by one by the vendor's loop.
    # Opened by each message handler below, closed at ReadyForQuery (and when the connection ends).
    _request: HeldRequestSpan
    # REQ-1905: the request deadline of that same cycle — pgwire's own request timeout, ONE for
    # the cycle: describe, governance, execution, the fetch and the row send all draw on it. Held
    # as the span is, and bound in this connection thread's context while the cycle is open, so
    # every run on the connection loop (``cl.run(..., timeout=...)`` keeps the tighter deadline)
    # and every stream this thread drains sees it.
    _deadline: "request_deadline.Deadline | None" = None

    def _open_request(self) -> None:
        self._request.open()
        if self._deadline is None:
            self._deadline = request_deadline.open_request("pgwire")
            request_deadline.bind(self._deadline)

    def _close_request(self) -> None:
        self._request.close()
        deadline, self._deadline = self._deadline, None
        if deadline is not None:
            request_deadline.unbind()
            deadline.stop()

    def _send_request_timeout(self, exc: TimeoutError) -> None:
        # 57014 query_canceled: what PostgreSQL itself reports for a statement its
        # statement_timeout ended. The message names the transport and the setting.
        self._send_pg_error("ERROR", "57014", str(exc))

    def handle_parse(self, ctx: BVContext, payload: bytes) -> None:
        self._open_request()
        super().handle_parse(ctx, payload)

    def handle_bind(self, ctx: BVContext, payload: bytes) -> None:
        self._open_request()
        super().handle_bind(ctx, payload)

    def send_ready_for_query(self, ctx: Optional[BVContext]) -> None:
        # ReadyForQuery ends the cycle: the Sync of an extended-protocol cycle, or a Query's end.
        try:
            super().send_ready_for_query(ctx)
        finally:
            self._close_request()

    def send_data_rows(self, query_result: BVQueryResult, limit: int = 0) -> int:
        # REQ-1905: the request's deadline covers the row send. A result that is ready only after
        # the deadline has passed is not sent; a stream is checked again at every batch it pulls
        # (provisa.executor.result). Either ends the statement with the timeout, raised to the
        # message handler below.
        request_deadline.check()
        # REQ-1910: rows are pulled from the result and encoded onto the socket here.
        with _stage(_tracer, "pgwire.encode", name="encode"):
            sent = super().send_data_rows(query_result, limit)
            _annotate_request(db__row_count=sent)
            return sent

    def handle(self) -> None:
        """Serve the connection with a ConnectionLoop bound to this thread for its whole life.

        REQ-1882 (amended 2026-09-29): every coroutine this connection runs — auth, org resolution,
        governance, execution, audit, cursor pumps — executes on this loop, on this thread."""
        from provisa.core.connection_loop import connection_loop

        with connection_loop():
            try:
                super().handle()
            finally:
                self._close_request()  # a connection that died mid-cycle still ends its span
                # A CancelRequest from another connection may have asked this session to close;
                # its cursors are released here, on the thread that owns their loop.
                session = self._session
                if session is not None and session._close_requested:
                    session.close_owned()

    def setup(self) -> None:
        # REQ-1452/REQ-1455: meter what this connection writes to its client. Wrapping the socket
        # writer is the only truthful place to count a pgwire result set — the rows are streamed
        # DataRow by DataRow long after the query was finalized, so the audit seam never sees the
        # byte total. Starts unattributed and is bound to an org once auth resolves one; the bytes
        # of the startup and auth exchange belong to no org and are dropped rather than guessed.
        super().setup()
        self._request = HeldRequestSpan(_tracer, "pgwire.query", transport="pgwire")  # REQ-1910
        self._meter = CountingWriter(self.wfile, None)
        self.wfile = self._meter

    def _send_pg_error(self, severity: str, sqlstate: str, message: str) -> None:
        buf = BVBuffer()
        for field, value in (
            (b"S", severity),
            (b"V", severity),
            (b"C", sqlstate),
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def _send_pg_notice(self, message: str) -> None:
        """Send a NoticeResponse (a non-fatal, out-of-band message) — never touches result rows."""
        buf = BVBuffer()
        for field, value in (
            (b"S", "NOTICE"),
            (b"V", "NOTICE"),
            (b"C", "01000"),  # SQLSTATE warning class
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.NOTICE_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def handle_post_auth(self, ctx):  # type: ignore[override]
        """After a successful auth, emit the REQ-1137 license nag once per connection as a
        NoticeResponse (out-of-band; the query results are never modified or gated)."""
        super().handle_post_auth(ctx)
        try:
            from provisa.licensing import emit as _lic_emit

            text = _lic_emit.nag_for_connection(f"pgwire:{getattr(ctx, 'process_id', id(ctx))}")
            if text:
                self._send_pg_notice(text.replace("\n", " "))
        except Exception:  # nag must never break a connection (REQ-1137)
            log.debug("pgwire license nag emission skipped", exc_info=True)

    def handle_startup(self, conn: Connection) -> Optional[BVContext]:  # type: ignore[override]
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == _SSL_REQUEST_CODE:
            ssl_ctx: ssl.SSLContext | None = getattr(self.server, "ssl_ctx", None)
            if ssl_ctx:
                self.wfile.write(b"S")
                self.wfile.flush()
                self.request = ssl_ctx.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile("rb")
                # Re-wrap: the TLS upgrade replaces the socket, and with it the writer setup()
                # metered. Leaving it unwrapped would silently stop metering every TLS pgwire
                # session, i.e. all of them on the hosted deployment.
                self._meter = CountingWriter(self.request.makefile("wb", 0), None)
                self.wfile = self._meter
                self.r = BVBuffer(self.rfile)
            else:
                self.wfile.write(b"N")
                self.wfile.flush()
            return self.handle_startup(conn)
        elif code == _CANCEL_REQUEST_CODE:
            process_id = self.r.read_uint32()
            secret_key = self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            return None
        elif code == _PROTOCOL_VERSION_3:
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(b"\x00")]
            params = dict(zip(msg[::2], msg[1::2]))
            log.info(
                "[PGWIRE] connect params: %s", {k: v for k, v in params.items() if k != "password"}
            )
            ctx = BVContext(conn.create_session(), None, params)
            self._session = ctx.session  # type: ignore[assignment]
            self.send_auth_request(ctx)
            return ctx
        else:
            raise Exception(f"Unsupported startup message code: {code}")

    def send_auth_request(self, ctx: BVContext) -> None:
        del ctx
        # REQ-1394: SCRAM when the deployment asked for it, cleartext-over-TLS otherwise. The
        # choice is made once, here, and the state machine below follows what was advertised.
        self._sasl_offered = self._scram_offered()
        self._sasl = None
        if self._sasl_offered:
            from provisa.auth.scram import MECHANISM

            # AuthenticationSASL carries the mechanism list as NUL-terminated names ended by an
            # empty one. Only SCRAM-SHA-256 is offered; -PLUS would promise channel binding that
            # the exchange does not implement.
            body = MECHANISM.encode("ascii") + b"\x00\x00"
            self.wfile.write(
                struct.pack(
                    "!cii", ServerResponse.AUTHENTICATION_REQUEST, 8 + len(body), _AUTH_SASL
                )
            )
            self.wfile.write(body)
        else:
            self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
        self.wfile.flush()

    def _app_state(self):
        """The running application state, whichever module holds it."""
        import provisa.pgwire.server as _m

        _state = _m.state
        if _state is None:
            from provisa.api.app import state as _state  # type: ignore[assignment]
        return _state

    def _scram_offered(self) -> bool:  # REQ-1394
        """Whether this connection is offered SASL rather than a cleartext password.

        SCRAM authenticates a local password and nothing else: it proves knowledge of a verifier
        this deployment derived, so it is offered only under the basic provider. A bearer provider
        or a personal access token arrives as an opaque secret in the password field, which SCRAM
        has no way to carry — those deployments keep the cleartext request, protected by TLS.
        """
        _state = self._app_state()
        auth_config = _state.auth_config
        if auth_config is None or not getattr(_state, "auth_middleware_active", False):
            return False
        if auth_config["provider"] != "basic":
            return False
        return bool(auth_config.get("scram"))

    def handle_md5_password(self, ctx: BVContext, payload: bytes) -> None:
        if self._sasl_offered:
            # REQ-1394: every SASL message arrives as a PASSWORD_MESSAGE, so the negotiation is
            # dispatched from here rather than from the vendored pre-auth loop.
            self._handle_sasl(ctx, payload)
            return
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")

        _state = self._app_state()

        auth_config = _state.auth_config
        if auth_config is None:
            if getattr(_state, "auth_middleware_active", False):
                # A real provider is active but its config is absent — misconfiguration.
                # Fail closed: never silently degrade a secured server to no-auth/trust.
                raise RuntimeError("pgwire auth_config not configured")
            # Explicit unsecured mode (provider: none / no auth section) — treat as trust mode.
            provider = "none"
        else:
            provider = auth_config["provider"]

        if provider == "none" or not _state.auth_middleware_active:
            # Trust mode: username maps directly to role_id, password ignored. The startup
            # packet's user is the only principal there is — it is what the audit row records.
            ctx.session.role_id = username  # type: ignore[attr-defined]
            ctx.session.user_id = username  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
            return

        assert auth_config is not None  # provider != "none" ⇒ auth_config is present

        from provisa.auth.throttle import LockedOut

        try:
            # REQ-1228: under PROVISA_MTLS_BIND_PRINCIPAL the client certificate's common name and
            # the startup packet's user must be the same person. Checked before the password is
            # examined — a mismatched certificate is not a credential question.
            self._assert_peer_binding(username)
        except PermissionError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
            return

        auth_provider = self._build_provider(_state, auth_config)
        if auth_provider is None:
            return
        try:
            identity = self._validate_credential(auth_provider, provider, username, password)
        except LockedOut as locked:
            # REQ-1393: a distinct answer from a wrong password. 28000 is invalid_authorization_
            # specification — the attempt was refused before the credential was examined at all.
            self._send_pg_error("FATAL", "28000", str(locked))
            return
        if identity is None:
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        self._complete_auth(ctx, identity, auth_config)

    def _send_auth_message(self, code: int, body: bytes) -> None:  # REQ-1394
        """One AuthenticationRequest message carrying a SASL payload."""
        self.wfile.write(
            struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8 + len(body), code)
        )
        self.wfile.write(body)
        self.wfile.flush()

    def _handle_sasl(self, ctx: BVContext, payload: bytes) -> None:  # REQ-1394
        """One step of the SCRAM exchange, driven by whichever message just arrived.

        Two round trips, and which one this is follows from whether an exchange already exists.
        The first message names the mechanism and carries client-first; the second carries the
        proof. A protocol error ends the connection with FATAL rather than being retried — SCRAM
        has no resynchronisation point, and a client that sent the wrong thing will send it again.
        """
        from provisa.auth.scram import MECHANISM, ScramError

        username = ctx.params.get("user", "")
        if self._sasl is None:
            mechanism, sep, rest = payload.partition(b"\x00")
            if not sep or mechanism.decode("utf-8") != MECHANISM:
                self._send_pg_error(
                    "FATAL", "28000", f"unsupported SASL mechanism: {mechanism.decode('utf-8')!r}"
                )
                return
            (length,) = struct.unpack("!i", rest[:4])
            if length < 0:
                # -1 means "no initial response". SCRAM's first message is not optional, so there
                # is nothing to answer with.
                self._send_pg_error("FATAL", "28000", "SASL initial response is required")
                return
            self._sasl_start(username, rest[4 : 4 + length].decode("utf-8"))
            return

        try:
            final = self._sasl.server_final(payload.decode("utf-8").rstrip("\x00"))
        except ScramError as exc:
            # REQ-1393: a failed proof is a failed password and counts against the account exactly
            # as a wrong one over any other surface.
            from provisa.auth.throttle import login_throttle, subject_key

            login_throttle().record_failure(subject_key(username, ""))
            log.info("[PGWIRE] SCRAM authentication failed for %r: %s", username, exc)
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return

        self._send_auth_message(_AUTH_SASL_FINAL, final.encode("utf-8"))
        self._sasl_complete(ctx, username)

    def _sasl_start(self, username: str, client_first: str) -> None:  # REQ-1394
        """Answer client-first with the account's salt, or a mock account's when it has none."""
        from provisa.auth.scram import ScramError, ScramExchange, mock_verifier
        from provisa.auth.scram_store import read_verifier
        from provisa.auth.throttle import LockedOut, login_throttle, subject_key

        from provisa.core.connection_loop import run_on_connection_loop

        try:
            # REQ-1393: the lockout is checked before any work is done on the account's behalf,
            # so a locked-out name cannot be used to make the server derive verifiers all day.
            login_throttle().check(subject_key(username, ""))
        except LockedOut as locked:
            self._send_pg_error("FATAL", "28000", str(locked))
            return

        _state = self._app_state()
        admin_db = _state.admin_db
        assert admin_db is not None  # the basic provider is DB-backed; _scram_offered required it
        verifier = run_on_connection_loop(read_verifier(admin_db, username), timeout=60)
        if verifier is None:
            # PostgreSQL's mock authentication. A user who has never set a password under SCRAM —
            # and a user who does not exist — gets a well-formed exchange that no proof satisfies,
            # so the handshake never becomes a name oracle.
            verifier = mock_verifier(username, _MOCK_SEED)

        exchange = ScramExchange(verifier)
        try:
            first = exchange.server_first(client_first)
        except ScramError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
            return
        self._sasl = exchange
        self._send_auth_message(_AUTH_SASL_CONTINUE, first.encode("utf-8"))

    def _sasl_complete(self, ctx: BVContext, username: str) -> None:  # REQ-1394
        """Turn a verified proof into a session.

        The proof says the password was right; it says nothing about whether the account is still
        active or what it may do. Both of those come from reading the account, which is why this
        goes through the provider rather than trusting the exchange.
        """
        from provisa.auth.throttle import login_throttle, subject_key

        from provisa.core.connection_loop import run_on_connection_loop

        _state = self._app_state()
        auth_config = _state.auth_config
        assert auth_config is not None  # _scram_offered required it
        auth_provider = self._build_provider(_state, auth_config)
        if auth_provider is None:
            return
        # REQ-1394: ``_scram_offered`` refuses SASL under any provider but ``basic``, so this
        # exchange only ever reaches a BasicAuthProvider -- the one that holds the verifier the
        # client just proved knowledge of, and the only one that can answer ``identity_for``
        # without a credential. The assert restates that guarantee where the narrow call is made.
        from provisa.auth.providers.basic import BasicAuthProvider

        assert isinstance(auth_provider, BasicAuthProvider)
        try:
            identity = run_on_connection_loop(auth_provider.identity_for(username), timeout=60)
        except ValueError:
            # The verifier matched but the account is gone or deactivated. Answered as a failed
            # password: a deactivated account must not be able to tell that its password is right.
            login_throttle().record_failure(subject_key(username, ""))
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        login_throttle().record_success(subject_key(username, ""))
        self._complete_auth(ctx, identity, auth_config)

    def _requested_org(self) -> str | None:  # REQ-1234
        """The org this connection's TLS SNI hostname named, or None.

        None on a plaintext connection and on one dialed by IP address, which is every connection
        on a single-org deployment — those resolve their org from the principal alone, unchanged.
        The socket is the wrapped one; ``handle_startup`` replaced ``self.request`` during the
        SSLRequest exchange, and the servername callback stashed the name on it during the
        handshake.
        """
        from provisa.security.sni import indicated_host, org_from_host

        return org_from_host(indicated_host(self.request))

    def _assert_peer_binding(self, username: str) -> None:  # REQ-1228
        """Bind the TLS client certificate to the startup packet's user, when configured.

        A plaintext connection has no peer certificate to inspect; ``resolve_client_auth`` returns
        None there because mTLS is only wired onto the context when a CA is configured, and the
        binding check is then a no-op. The socket is the wrapped one — ``handle_startup`` replaced
        ``self.request`` during the SSLRequest exchange.
        """
        from provisa.security.mtls import assert_principal_binding, resolve_client_auth

        auth = resolve_client_auth(
            "PROVISA_PGWIRE_CLIENT_CA",
            "PROVISA_PGWIRE_MTLS_MODE",
            "PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL",
        )
        if auth is None or not auth.bind_principal:
            return
        peer_cert = self.request.getpeercert() if isinstance(self.request, ssl.SSLSocket) else None
        assert_principal_binding(auth, peer_cert, username)

    def _build_provider(self, _state, auth_config: dict):  # REQ-124
        """The configured AuthProvider, or None after answering the client with FATAL.

        A provider that cannot be constructed — an unknown name, a missing signing key — can
        authenticate nobody. The client is told so on the wire; dropping the connection with an
        unhandled exception would leave it guessing.
        """
        from provisa.auth.wiring import build_auth_provider

        try:
            return build_auth_provider(auth_config, admin_pool=getattr(_state, "admin_db", None))
        except ValueError as exc:
            self._send_pg_error("FATAL", "28P01", f"pgwire auth provider unavailable: {exc}")
            return None

    def _validate_credential(  # REQ-124, REQ-890, REQ-1263
        self, auth_provider, provider_name: str, username: str, password: str
    ):
        """Validate the startup credential against the provider, or None.

        pgwire carries no scheme field — the startup packet holds a username and one cleartext
        secret — so the presentation is decided once, from what the secret is. A personal access
        token names itself by prefix and is a bearer credential (REQ-1263); a bearer/JWT provider
        is told to expect a token in the password field (REQ-890); everything else is a password,
        presented as ``basic``. One decision, one validator: a credential the chosen validator
        refuses is not retried against another, which would turn one rejection into a second guess.

        Validators run on this connection's loop (REQ-1882); the PAT store and DB-backed providers
        resolve their loop-bound handles per loop.
        """
        import base64

        from provisa.auth.models import validator_for_scheme
        from provisa.auth.pat import is_personal_access_token
        from provisa.auth.throttle import throttled

        if is_personal_access_token(password) or provider_name in _OIDC_PROVIDERS:
            scheme, token = "bearer", password
        else:
            scheme = "basic"
            token = base64.b64encode(f"{username}:{password}".encode()).decode()

        validator = validator_for_scheme(auth_provider, scheme)
        if validator is None:
            return None
        # REQ-1393: the startup packet names the account, so failed guesses count against it here
        # and on every other surface alike. LockedOut propagates — the caller answers 28000.
        attempt = throttled(validator, token, principal=username if scheme == "basic" else None)
        from provisa.core.connection_loop import run_on_connection_loop

        try:
            return run_on_connection_loop(attempt, timeout=60)
        except (ValueError, jwt.PyJWTError):
            return None

    def _complete_auth(  # REQ-273, REQ-551, REQ-890, REQ-1266
        self, ctx: BVContext, identity, auth_config: dict
    ) -> None:
        """Map the validated identity to a role, bind its org, and admit the connection."""
        from provisa.auth.role_mapping import resolve_role

        default_role = auth_config.get("default_role")
        if not default_role:
            # No admin default: an identity matching no mapping rule is refused, not escalated
            # onto whatever role the deployment happens to have named first.
            raise RuntimeError("pgwire auth requires auth.default_role to be configured")
        role = resolve_role(identity, auth_config.get("role_mapping", []), default_role)
        # REQ-1266: bind the session to the identity's org (multitenant) so its queries route to that
        # org's data-plane runtime. Resolution + build run on this connection's loop (REQ-1882); an
        # unresolvable principal fails the connection rather than silently landing on the default.
        import provisa.pgwire.server as _m

        _state = _m.state
        if _state is None:
            from provisa.api.app import state as _state  # type: ignore[assignment]
        if getattr(_state, "multitenancy", False):
            from provisa.api.org_resolve import OrgResolutionError

            from provisa.core.connection_loop import run_on_connection_loop

            try:
                ctx.session.org_id = run_on_connection_loop(  # type: ignore[attr-defined]
                    _resolve_and_build_org(_state, identity, self._requested_org()), timeout=60
                )
            except OrgResolutionError as exc:
                self._send_pg_error("FATAL", "28000", f"org selection failed: {exc}")
                return
        # REQ-1452: attribute this connection's writes from here on.
        self._meter.bind_org(getattr(ctx.session, "org_id", None))
        ctx.session.role_id = role  # type: ignore[attr-defined]
        ctx.session.user_id = identity.user_id  # type: ignore[attr-defined]  # REQ-074
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def handle_describe(self, ctx: BVContext, payload: bytes) -> None:
        self._open_request()
        ba = bytearray(payload)
        if ba[0] == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
            if stmt_name is not None and not ctx.stmts.get(stmt_name, ("x",))[0].strip():
                self.send_no_data()
                return
        elif ba[0] == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            sql = ctx.stmts[stmt][0]
            if not sql.strip():
                self.send_parameter_description([])
                self.send_no_data()
                return
            indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
            if "typeinfo_tree" in sql.lower() and indices:
                param_oids = [1028]
            elif "set_config" in sql.lower() and indices:
                param_oids = [25] * len(indices)
            else:
                stored_oids = ctx.stmts[stmt][1]
                if stored_oids:
                    param_oids = stored_oids
                elif indices:
                    _CAST_OID = {
                        "text": 25,
                        "varchar": 25,
                        "int": 23,
                        "int4": 23,
                        "int8": 20,
                        "bigint": 20,
                        "bool": 16,
                        "float8": 701,
                    }
                    cast_map = {
                        int(m): _CAST_OID.get(t.lower(), 25)
                        for m, t in re.findall(r"\$(\d+)::(\w+)", sql)
                    }
                    param_oids = [cast_map.get(i, 20) for i in range(1, max(indices) + 1)]
                else:
                    param_oids = []
            # Update stored param_oids so describe_statement substitutes example values
            # instead of executing the SQL with unresolved $N placeholders.
            ctx.stmts[stmt] = (sql, param_oids)
            try:
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            try:
                self.send_parameter_description(param_oids)
                if query_result.has_results():
                    self.send_row_description(query_result)
                else:
                    self.send_no_data()
            finally:
                # describe_statement HOLDS a parameterless statement's result for the Execute of
                # the Bind that follows (vendor buenavista add_portal), so it runs once; that held
                # result is closed by the context when used, replaced or dropped. Anything it does
                # not hold (a statement with parameters, a non-row result) is closed here —
                # asyncpg's prepare flow otherwise leaked one live cursor/source connection per
                # query (same leak class as the portal-describe one fixed in close_portal).
                if not ctx.holds_described(query_result):
                    closer = getattr(query_result, "close", None)
                    if closer is not None:
                        closer()
            return
        super().handle_describe(ctx, payload)

    def handle_execute(self, ctx: BVContext, payload: bytes) -> None:
        self._open_request()
        ba = bytearray(payload)
        portal_idx = ba.index(0)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
        if stmt_name is not None and not ctx.stmts.get(stmt_name, ("x",))[0].strip():
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            return
        try:
            super().handle_execute(ctx, payload)
        except request_deadline.RequestTimedOut as exc:
            # REQ-1905: the deadline passed while rows were being sent. The statement ends with
            # an ErrorResponse and the rest of the cycle is skipped up to its Sync.
            self._send_request_timeout(exc)
            ctx.mark_error()

    def handle_query(self, ctx: BVContext, payload: bytes) -> None:
        self._open_request()
        from provisa.compiler.sql_rewrite import split_sql_statements

        decoded = payload.decode("utf-8").rstrip("\x00")

        # Statement-aware split: a ';' inside a string literal / comment / dollar-quote must NOT
        # mis-split, so governance and execution see identical statement boundaries (no parser
        # differential — replaces the old naive decoded.split(';')).
        stmts = split_sql_statements(decoded)
        if not stmts:
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return

        # REQ-1266: bind the session's org on this worker thread so the DDL/COPY handlers' sync
        # state.X reads route to its runtime (SELECT re-binds inside execute_sql; nesting is safe).
        _org_id = ctx.session.org_id  # type: ignore[attr-defined]
        _org_token = None
        if _org_id is not None:
            from provisa.core.request_context import set_current_org

            _org_token = set_current_org(_org_id)
        try:
            self._process_query_stmts(ctx, stmts)
        except request_deadline.RequestTimedOut as exc:
            # REQ-1905: the deadline passed while rows were being sent — the Query ends with an
            # ErrorResponse and ReadyForQuery, as any failed statement does.
            self._send_request_timeout(exc)
            self.send_ready_for_query(ctx)
        finally:
            if _org_token is not None:
                from provisa.core.request_context import reset_current_org

                reset_current_org(_org_token)

    def _handle_declare_cursor(self, ctx: BVContext, stmt: str) -> None:  # REQ-1862
        """DECLARE name [...] CURSOR [...] FOR <select> — runs the SELECT through the normal
        governed pipeline (``ctx.execute_sql``, same as any SELECT), and stores its lazy row
        generator on the session for FETCH/MOVE/CLOSE to consume."""
        m = _DECLARE_CURSOR_RE.match(stmt)
        assert m is not None  # caller only dispatches here on a match
        name = _normalize_cursor_name(m.group("name"))
        inner_sql = m.group("sql")
        # PostgreSQL cursors are NO SCROLL (forward-only) unless SCROLL is given explicitly —
        # thread that through so _cursor_move can bound memory for the (overwhelmingly common)
        # forward-only case instead of buffering every row forever (REQ-1862 amendment 2026-09-25).
        scroll_kw = m.group("scroll")
        scrollable = scroll_kw is not None and not re.match(
            r"NO\s+SCROLL", scroll_kw, re.IGNORECASE
        )
        try:
            # ctx.execute_sql is typed to buenavista's base QueryResult, but every path through
            # ProvisaSession.execute_sql (the sole session implementation reachable here) returns
            # a ProvisaQueryResult — the only one that implements close().
            query_result: ProvisaQueryResult = ctx.execute_sql(inner_sql)  # type: ignore[assignment]
            cursors = ctx.session.cursors  # type: ignore[attr-defined]
            existing = cursors.pop(name, None)
            if existing is not None:
                existing.query_result.close()  # re-DECLARE of an open name replaces it
            cursors[name] = _CursorState(
                name=name,
                query_result=query_result,
                source=query_result.rows(),
                scrollable=scrollable,
            )
            self.send_command_complete("DECLARE CURSOR\x00")
        except PermissionError as exc:
            self._send_pg_error("ERROR", "42501", str(exc))
            ctx.mark_error()
        except Exception as exc:
            self._send_pg_error("ERROR", "42601", str(exc))
            ctx.mark_error()

    def _handle_fetch_move(self, ctx: BVContext, stmt: str) -> None:  # REQ-1862
        """FETCH [...] FROM cursor / MOVE [...] FROM cursor — advances the named cursor and, for
        FETCH, sends the rows it passed over as a normal RowDescription/DataRow pair."""
        is_fetch = _FETCH_RE.match(stmt) is not None
        keyword = "FETCH" if is_fetch else "MOVE"
        try:
            direction, raw_name, count = _parse_cursor_nav(stmt, keyword)
            name = _normalize_cursor_name(raw_name)
            cs = ctx.session.cursors.get(name)  # type: ignore[attr-defined]
            if cs is None:
                raise LookupError(f'cursor "{name}" does not exist')
            fetched = _cursor_move(cs, direction, count)
        except LookupError as exc:
            self._send_pg_error("ERROR", "34000", str(exc))
            ctx.mark_error()
            return
        except _CursorNotScrollableError as exc:
            # 55000 = object_not_in_prerequisite_state, PostgreSQL's SQLSTATE for exactly this
            # (backward/absolute FETCH against a NO SCROLL cursor).
            self._send_pg_error("ERROR", "55000", str(exc))
            ctx.mark_error()
            return
        except Exception as exc:
            self._send_pg_error("ERROR", "42601", str(exc))
            ctx.mark_error()
            return
        if is_fetch:
            columns = [cs.query_result.column(i) for i in range(cs.query_result.column_count())]
            result = _CursorRowsResult(columns, fetched)
            self.send_row_description(result)
            self.send_data_rows(result)
            self.send_command_complete(f"FETCH {len(fetched)}\x00")
        else:
            self.send_command_complete(f"MOVE {len(fetched)}\x00")

    def _handle_close_cursor(self, ctx: BVContext, stmt: str) -> None:  # REQ-1862
        """CLOSE cursor | CLOSE ALL — releases the underlying stream(s) and forgets the name(s)."""
        m = _CLOSE_CURSOR_RE.match(stmt)
        assert m is not None  # caller only dispatches here on a match
        raw_name = m.group("name")
        cursors = ctx.session.cursors  # type: ignore[attr-defined]
        try:
            if raw_name.upper() == "ALL":
                for cs in cursors.values():
                    cs.query_result.close()
                cursors.clear()
            else:
                cs = cursors.pop(_normalize_cursor_name(raw_name), None)
                if cs is not None:
                    cs.query_result.close()
            self.send_command_complete("CLOSE CURSOR\x00")
        except Exception as exc:
            self._send_pg_error("ERROR", "58000", str(exc))
            ctx.mark_error()

    def _process_query_stmts(self, ctx: BVContext, stmts: list[str]) -> None:
        for stmt in stmts:
            if _COPY_RE.match(stmt):
                from provisa.pgwire.copy_handler import CopyHandler

                try:
                    nrows = CopyHandler(self).handle(ctx, stmt)  # type: ignore[arg-type]
                    self.send_command_complete(f"COPY {nrows}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _CTAS_RE.match(stmt):
                from provisa.executor.ctas import run_ctas

                # role_id lives on the session, not the handler.
                role = ctx.session.role_id  # type: ignore[attr-defined]
                if not role:
                    self._send_pg_error("ERROR", "28000", "Not authenticated")
                    ctx.mark_error()
                    break
                user = ctx.session.user_id  # type: ignore[attr-defined]
                if not user:
                    self._send_pg_error("ERROR", "28000", "Not authenticated")
                    ctx.mark_error()
                    break
                from provisa.audit.context import with_audit_identity
                from provisa.core.connection_loop import run_on_connection_loop

                try:
                    tag = run_on_connection_loop(
                        _run_with_org(
                            ctx.session.org_id,  # type: ignore[attr-defined]
                            # REQ-074/REQ-1386: the CTAS SELECT runs through the governed pipeline;
                            # bind its principal inside the coroutine so the row is attributed.
                            with_audit_identity(user, "pgwire", run_ctas(stmt, role)),
                        ),
                        timeout=request_timeout_for("pgwire"),
                    )
                    self.send_command_complete(f"{tag}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _DDL_RE.match(stmt):
                from provisa.pgwire.ddl_handler import DdlHandler

                try:
                    tag = DdlHandler(self).handle(ctx, stmt)
                    self.send_command_complete(f"{tag}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _DECLARE_CURSOR_RE.match(stmt):
                self._handle_declare_cursor(ctx, stmt)
                break
            if _FETCH_RE.match(stmt) or _MOVE_RE.match(stmt):
                self._handle_fetch_move(ctx, stmt)
                break
            if _CLOSE_CURSOR_RE.match(stmt):
                self._handle_close_cursor(ctx, stmt)
                break
            try:
                from buenavista.core import Extension

                if req := Extension.check_json(stmt):
                    method = req.get("method")
                    extension = self.server.extensions.get(method)  # type: ignore[attr-defined]
                    if not extension:
                        raise Exception("Unknown method: " + str(method))
                    query_result = extension.apply(req.get("params"), ctx.session)
                else:
                    query_result = ctx.execute_sql(stmt)
            except PermissionError as exc:
                self._send_pg_error("ERROR", "42501", str(exc))
                ctx.mark_error()
                break
            except Exception as exc:
                self.send_error(exc, ctx)
                break

            if not query_result:
                raise Exception("No query result for: " + stmt)

            if query_result.has_results():
                self.send_row_description(query_result)
                row_count = self.send_data_rows(query_result)
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                status = query_result.status()
                self.send_command_complete(f"{status}\x00")

        self.send_ready_for_query(ctx)


class ProvisaServer(BuenaVistaServer):  # REQ-001, REQ-266
    allow_reuse_address = True

    def server_bind(self) -> None:
        # REQ-1900: allow_reuse_address (SO_REUSEADDR) only permits a quick rebind after this
        # socket closes -- it does NOT let two processes listen on the same port at once
        # (confirmed live: a second socketserver process still fails with "Address already in
        # use" with only allow_reuse_address set). SO_REUSEPORT is the option that actually
        # allows concurrent listeners, with the kernel load-balancing new connections between
        # them -- confirmed live the same way. Needed so multiple uvicorn `--workers N` processes
        # can each run their own pgwire listener on the same port instead of all but one crashing
        # on startup.
        import socket as _socket

        self.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        super().server_bind()

    def __init__(
        self,
        server_address: tuple[str, int],
        conn: ProvisaConnection,
        ssl_ctx: ssl.SSLContext | None = None,
    ) -> None:
        socketserver.ThreadingTCPServer.__init__(self, server_address, ProvisaHandler)  # type: ignore[arg-type]
        self.conn = conn
        self.rewriter = None
        self.extensions: dict = {}
        self.ctxts: dict = {}
        self.auth = None
        self.ssl_ctx = ssl_ctx

    def verify_request(self, request, client_address) -> bool:
        del request, client_address
        return True


def start_pgwire_server(  # REQ-527
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None,
) -> ProvisaServer:
    """Start the pgwire server in a daemon thread. Returns the server instance.

    Each TCP connection is served on its own thread with its own event loop (REQ-1882)."""
    conn = ProvisaConnection()
    server = ProvisaServer((host, port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[PGWIRE] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    return server

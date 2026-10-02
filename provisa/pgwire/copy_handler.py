# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-567890123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""COPY TO STDOUT for the pgwire server (COPY FROM STDIN is a write: refused, REQ-615).

COPY TO: runs governance pipeline, executes via Flight SQL (the engine) or direct,
         serialises result to PG COPY text/csv wire format.
COPY FROM: refused with SQLSTATE 0A000 before the client is asked for data — pgwire takes no
           writes (REQ-615).
"""

# Requirements: REQ-038, REQ-040, REQ-129, REQ-266, REQ-272

from __future__ import annotations

from provisa.core.connection_loop import run_on_connection_loop
from provisa.core.limits import request_timeout_for  # REQ-1905: pgwire's own request timeout
import csv
import io
import logging
import re
import struct
from typing import TYPE_CHECKING, Protocol

from provisa.pgwire.copy_binary import (
    _arrow_binary_tag,
    _rows_to_copy_binary,
    _sql_binary_tag,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa
    from buenavista.postgres import BVContext

    from provisa.executor.result import QueryResult
    from provisa.pgwire._pipeline import _Plan


class _ByteReader(Protocol):
    def read(self, size: int = ..., /) -> bytes: ...


class _ByteWriter(Protocol):
    def write(self, data: bytes, /) -> int: ...

    def flush(self) -> None: ...


class _CopyTransport(Protocol):
    """Subset of ProvisaHandler used by CopyHandler for wire I/O."""

    rfile: _ByteReader
    wfile: _ByteWriter


log = logging.getLogger(__name__)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)

_PARSE_TO_RE = re.compile(
    r"""^\s*COPY\s+
        (?:(?P<schema>[A-Za-z_][A-Za-z0-9_]*)\.)?
        (?P<table>[A-Za-z_][A-Za-z0-9_]*)
        \s+TO\s+STDOUT
        (?:\s+(?:WITH\s+)?\(?\s*FORMAT\s+["']?(?P<fmt>\w+)["']?\s*\)?)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

_PARSE_QUERY_TO_RE = re.compile(
    r"""^\s*COPY\s+\((?P<query>.+)\)\s+TO\s+STDOUT
        (?:\s+(?:WITH\s+)?\(?\s*FORMAT\s+["']?(?P<fmt>\w+)["']?\s*\)?)?
        \s*$
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

_PARSE_FROM_RE = re.compile(
    r"""^\s*COPY\s+
        (?:(?P<schema>[A-Za-z_][A-Za-z0-9_]*)\.)?
        (?P<table>[A-Za-z_][A-Za-z0-9_]*)
        (?:\s*\((?P<cols>[^)]+)\))?
        \s+FROM\s+STDIN
        (?:\s+(?:WITH\s+)?\(?\s*FORMAT\s+["']?(?P<fmt>\w+)["']?\s*\)?)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

# PG COPY wire message codes (appended to ServerResponse in postgres.py)
_COPY_OUT_RESPONSE = b"H"
_COPY_DATA = b"d"
_COPY_DONE = b"c"


state = None  # module-level reference; replaced by tests via patch()


def is_copy_sql(sql: str) -> bool:  # REQ-585
    return bool(_COPY_RE.match(sql))


def _value_to_copy_text(
    v: object,  # object-ok: genuinely opaque cell value from query result rows (str/int/float/bool/bytes/None union from any source)
) -> str:
    if v is None:
        return r"\N"
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (bytes, bytearray)):
        return "\\\\x" + v.hex()
    s = str(v)
    # Escape backslash, tab, newline, carriage return
    s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return s


def _rows_to_copy_text(rows: Sequence[Sequence[object]], col_count: int) -> bytes:
    out = io.StringIO()
    for row in rows:
        parts = [_value_to_copy_text(row[i] if i < len(row) else None) for i in range(col_count)]
        out.write("\t".join(parts))
        out.write("\n")
    return out.getvalue().encode("utf-8")


def _rows_to_copy_csv(rows: Sequence[Sequence[object]], col_count: int) -> bytes:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    for row in rows:
        w.writerow([row[i] if i < len(row) else None for i in range(col_count)])
    return out.getvalue().encode("utf-8")


def _arrow_table_to_copy_bytes(table: pa.Table, fmt: str) -> bytes:
    col_count = table.num_columns
    rows = table.to_pylist()
    col_names = table.column_names
    row_lists = [[row.get(n) for n in col_names] for row in rows]
    if fmt == "binary":
        tags = [_arrow_binary_tag(f.type) for f in table.schema]
        return _rows_to_copy_binary(row_lists, tags)
    if fmt == "csv":
        return _rows_to_copy_csv(row_lists, col_count)
    return _rows_to_copy_text(row_lists, col_count)


def _queryresult_to_copy_bytes(result: QueryResult, fmt: str) -> bytes:
    col_count = len(result.column_names)
    if fmt == "binary":
        types = result.column_types or []
        tags = [_sql_binary_tag(types[i] if i < len(types) else None) for i in range(col_count)]
        return _rows_to_copy_binary(result.rows, tags)
    if fmt == "csv":
        return _rows_to_copy_csv(result.rows, col_count)
    return _rows_to_copy_text(result.rows, col_count)


class CopyHandler:  # REQ-038, REQ-040, REQ-129, REQ-266, REQ-272
    """Handles COPY TO STDOUT for ProvisaHandler; COPY FROM STDIN is refused (REQ-615)."""

    def __init__(self, handler: _CopyTransport) -> None:
        self._h = handler  # ProvisaHandler instance

    def handle(self, ctx: BVContext, sql: str) -> int:
        """Dispatch COPY statement. Returns row count."""
        role_id = ctx.session.role_id  # type: ignore[attr-defined]

        m_query = _PARSE_QUERY_TO_RE.match(sql)
        if m_query:
            fmt = (m_query.group("fmt") or "text").lower()
            return self._handle_copy_to_query(ctx, m_query.group("query").strip(), fmt, role_id)

        m_to = _PARSE_TO_RE.match(sql)
        if m_to:
            schema = m_to.group("schema")
            table = m_to.group("table")
            fmt = (m_to.group("fmt") or "text").lower()
            query = f"SELECT * FROM {schema + '.' if schema else ''}{table}"  # noqa: S608  # schema/table matched against [A-Za-z_][A-Za-z0-9_]* regex identifier grammar
            return self._handle_copy_to_query(ctx, query, fmt, role_id)

        m_from = _PARSE_FROM_RE.match(sql)
        if m_from:
            # REQ-615: pgwire takes no writes. A bulk load is a write: refused like INSERT,
            # before the client is asked for any data.
            from provisa.pgwire._pipeline import WriteNotAvailableOverPgwire

            raise WriteNotAvailableOverPgwire("COPY ... FROM STDIN")

        raise ValueError(f"Cannot parse COPY statement: {sql!r}")

    # ------------------------------------------------------------------
    # COPY TO STDOUT
    # ------------------------------------------------------------------

    def _handle_copy_to_query(self, _ctx: BVContext, query: str, fmt: str, role_id: str) -> int:
        from provisa.audit.context import with_audit_identity
        from provisa.pgwire import server as _srv
        from provisa.pgwire._pipeline import plan_pgwire_sql
        from provisa.transpiler.router import Route

        user_id = _ctx.session.user_id  # type: ignore[attr-defined]
        if not user_id:
            raise RuntimeError("Not authenticated")
        org_id = _ctx.session.org_id  # type: ignore[attr-defined]
        # REQ-074/REQ-1386: bind the principal inside the coroutine so the governor's audit write
        # attributes this COPY; REQ-1266 binds the session's org there too — the row lands in its
        # tenant schema. Runs on this connection's own loop, on this thread (REQ-1882).
        plan = run_on_connection_loop(
            _srv._run_with_org(
                org_id, with_audit_identity(user_id, "pgwire", plan_pgwire_sql(query, role_id))
            ),
            timeout=60,
        )

        try:
            if plan.route == Route.ENGINE:
                data_bytes, nrows = self._exec_engine_flight(plan, fmt)
            else:
                data_bytes, nrows = self._exec_direct_plan(plan, fmt)
        except Exception:
            self._finalize_audit(plan, 500, org_id)
            raise
        plan.row_count = nrows
        self._finalize_audit(plan, 200, org_id)

        self._send_copy_out_response(fmt)
        self._send_copy_data(data_bytes)
        self._send_copy_done()
        return nrows

    def _finalize_audit(self, plan: _Plan, status_code: int, org_id: str | None) -> None:
        """Write the governed plan's audit row on this connection's loop (REQ-074/REQ-1386).

        COPY drains the engine/source terminal here, so the plan never reaches ``_execute_plan`` on
        the ENGINE route; ``finalize_audit`` is idempotent, so the DIRECT route stays single-write.
        The org is bound in the coroutine (REQ-1266) so the row lands in this session's tenant."""
        from provisa.pgwire import server as _srv
        from provisa.pgwire._pipeline import finalize_audit

        run_on_connection_loop(
            _srv._run_with_org(org_id, finalize_audit(plan, status_code)), timeout=30
        )

    def _exec_engine_flight(self, plan: _Plan, fmt: str) -> tuple[bytes, int]:
        from provisa.api.app import state
        from provisa.federation.query_residency import ensure_resident
        from provisa.pgwire._pipeline import require_governed_plan

        if plan.physical_sql is None:
            raise RuntimeError("the engine plan missing transpiled SQL")
        require_governed_plan(
            plan
        )  # REQ-1176: verify at the last moment, before the engine executes
        # REQ-1661: COPY drains the engine terminal here and never reaches _execute_plan (see
        # _finalize_audit's own comment above), so its own ensure_resident call is the ONLY place
        # a MATERIALIZED source this plan reads gets landed before the engine executes — mirrors
        # the identical ENGINE-route bypass fixes elsewhere (pgwire/server.py, api/flight/server.py,
        # api/airport/query.py).
        run_on_connection_loop(
            ensure_resident(
                state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids
            ),
            timeout=request_timeout_for("pgwire"),
        )
        # Arrow Flight is an advertised, engine-specific transport (REQ-825): route through the
        # bound engine, which fails closed if the engine lacks ARROW or the proxy is unconfigured.
        table = state.federation_engine.execute_engine_arrow(plan.physical_sql, plan.exec_params)
        data_bytes = _arrow_table_to_copy_bytes(table, fmt)
        return data_bytes, table.num_rows

    def _exec_direct_plan(self, plan: _Plan, fmt: str) -> tuple[bytes, int]:
        from provisa.pgwire._pipeline import _execute_plan

        result = run_on_connection_loop(_execute_plan(plan), timeout=request_timeout_for("pgwire"))
        data_bytes = _queryresult_to_copy_bytes(result, fmt)
        return data_bytes, len(result.rows)

    def _send_copy_out_response(self, fmt: str) -> None:
        # overall_format: 0=text, 1=binary; col_count 0 means unknown/variable (REQ-883)
        overall = 1 if fmt == "binary" else 0
        body = struct.pack("!bh", overall, 0)
        self._h.wfile.write(struct.pack("!ci", _COPY_OUT_RESPONSE, len(body) + 4))
        self._h.wfile.write(body)
        self._h.wfile.flush()

    def _send_copy_data(self, data: bytes) -> None:
        self._h.wfile.write(struct.pack("!ci", _COPY_DATA, len(data) + 4))
        self._h.wfile.write(data)
        self._h.wfile.flush()

    def _send_copy_done(self) -> None:
        self._h.wfile.write(struct.pack("!ci", _COPY_DONE, 4))
        self._h.wfile.flush()

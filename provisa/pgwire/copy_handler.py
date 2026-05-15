# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-567890123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""COPY TO STDOUT and COPY FROM STDIN for the pgwire server.

COPY TO: runs governance pipeline, executes via Flight SQL (Trino) or direct,
         serialises result to PG COPY text/csv wire format.
COPY FROM: receives PG COPY wire data, parses rows, inserts into writable
           SQL-backed sources only (postgresql / mysql / sqlite / mariadb).
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from buenavista.postgres import BVContext

log = logging.getLogger(__name__)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)

_PARSE_TO_RE = re.compile(
    r"""^\s*COPY\s+
        (?:(?P<schema>[A-Za-z_][A-Za-z0-9_]*)\.)?
        (?P<table>[A-Za-z_][A-Za-z0-9_]*)
        \s+TO\s+STDOUT
        (?:\s+WITH\s*\(?\s*FORMAT\s+(?P<fmt>\w+)\s*\)?)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

_PARSE_QUERY_TO_RE = re.compile(
    r"""^\s*COPY\s+\((?P<query>.+)\)\s+TO\s+STDOUT
        (?:\s+WITH\s*\(?\s*FORMAT\s+(?P<fmt>\w+)\s*\)?)?
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
        (?:\s+WITH\s*\(?\s*FORMAT\s+(?P<fmt>\w+)\s*\)?)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

_WRITABLE_SOURCE_TYPES = {"postgresql", "mysql", "sqlite", "mariadb"}

# PG COPY wire message codes (appended to ServerResponse in postgres.py)
_COPY_OUT_RESPONSE = b"H"
_COPY_IN_RESPONSE = b"G"
_COPY_DATA = b"d"
_COPY_DONE = b"c"
_COPY_FAIL = b"f"


def is_copy_sql(sql: str) -> bool:
    return bool(_COPY_RE.match(sql))


def _value_to_copy_text(v) -> str:
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


def _rows_to_copy_text(rows, col_count: int) -> bytes:
    out = io.StringIO()
    for row in rows:
        parts = [_value_to_copy_text(row[i] if i < len(row) else None) for i in range(col_count)]
        out.write("\t".join(parts))
        out.write("\n")
    return out.getvalue().encode("utf-8")


def _rows_to_copy_csv(rows, col_count: int) -> bytes:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    for row in rows:
        w.writerow([row[i] if i < len(row) else None for i in range(col_count)])
    return out.getvalue().encode("utf-8")


def _arrow_table_to_copy_bytes(table, fmt: str) -> bytes:

    col_count = table.num_columns
    rows = table.to_pylist()
    col_names = table.column_names
    if fmt == "csv":
        return _rows_to_copy_csv([[row.get(n) for n in col_names] for row in rows], col_count)
    return _rows_to_copy_text([[row.get(n) for n in col_names] for row in rows], col_count)


def _queryresult_to_copy_bytes(result, fmt: str) -> bytes:
    col_count = len(result.column_names)
    if fmt == "csv":
        return _rows_to_copy_csv(result.rows, col_count)
    return _rows_to_copy_text(result.rows, col_count)


def _unescape_copy_text(s: str) -> str | None:
    if s == r"\N":
        return None
    out = []
    i = 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            c = s[i + 1]
            if c == "t":
                out.append("\t")
            elif c == "n":
                out.append("\n")
            elif c == "r":
                out.append("\r")
            elif c == "\\":
                out.append("\\")
            else:
                out.append("\\" + c)
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def _parse_copy_data_text(data: bytes) -> list[list]:
    rows = []
    text = data.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        rows.append([_unescape_copy_text(f) for f in fields])
    return rows


def _parse_copy_data_csv(data: bytes) -> list[list]:
    text = data.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    return [list(row) for row in reader if row]


def _parse_copy_data(data: bytes, fmt: str) -> list[list]:
    if fmt == "csv":
        return _parse_copy_data_csv(data)
    return _parse_copy_data_text(data)


def _find_table_meta(schema: str | None, table: str, role_id: str):
    """Return (TableMeta, col_names) for the COPY target table."""
    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    table_lower = table.lower()
    schema_lower = schema.lower() if schema else None

    for type_name, tm in ctx.tables.items():
        tm_schema = domain_to_sql_name(tm.domain_id).lower()
        tm_table = (tm.table_name or tm.original_table_name or "").lower()
        if tm_table != table_lower:
            continue
        if schema_lower and tm_schema != schema_lower:
            continue
        col_names = [c for c, _ in ctx.aggregate_columns.get(tm.table_id, [])]
        return tm, col_names

    raise ValueError(f"Table {schema + '.' if schema else ''}{table!r} not found in role schema")


async def _insert_rows(
    source_id: str, schema: str, table: str, col_names: list[str], rows: list[list]
) -> int:
    """Bulk-insert *rows* into a writable source."""
    from provisa.api.app import state
    from provisa.executor.direct import execute_direct

    if not rows:
        return 0

    col_list = ", ".join(f'"{c}"' for c in col_names)
    placeholders = ", ".join(["?" for _ in col_names])
    sql = f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders})'

    inserted = 0
    for row in rows:
        params = list(row[: len(col_names)])
        # Pad with None if fewer values than columns
        while len(params) < len(col_names):
            params.append(None)
        await execute_direct(state.source_pools, source_id, sql, params)
        inserted += 1

    return inserted


class CopyHandler:
    """Handles COPY TO STDOUT and COPY FROM STDIN for ProvisaHandler."""

    def __init__(self, handler) -> None:
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
            query = f"SELECT * FROM {schema + '.' if schema else ''}{table}"
            return self._handle_copy_to_query(ctx, query, fmt, role_id)

        m_from = _PARSE_FROM_RE.match(sql)
        if m_from:
            schema = m_from.group("schema")
            table = m_from.group("table")
            cols_raw = m_from.group("cols")
            fmt = (m_from.group("fmt") or "text").lower()
            explicit_cols = [c.strip() for c in cols_raw.split(",")] if cols_raw else None
            return self._handle_copy_from(ctx, schema, table, explicit_cols, fmt, role_id)

        raise ValueError(f"Cannot parse COPY statement: {sql!r}")

    # ------------------------------------------------------------------
    # COPY TO STDOUT
    # ------------------------------------------------------------------

    def _handle_copy_to_query(self, ctx: BVContext, query: str, fmt: str, role_id: str) -> int:
        from provisa.pgwire._pipeline import plan_pgwire_sql
        from provisa.pgwire import server as _srv
        from provisa.transpiler.router import Route

        with _srv._loop_lock:
            loop = _srv._loop
        if loop is None:
            raise RuntimeError("Event loop not available")

        future = asyncio.run_coroutine_threadsafe(plan_pgwire_sql(query, role_id), loop)
        plan = future.result(timeout=60)

        if plan.route == Route.TRINO:
            data_bytes, nrows = self._exec_trino_flight(plan, fmt)
        else:
            data_bytes, nrows = self._exec_direct_plan(plan, loop, fmt)

        self._send_copy_out_response(fmt)
        self._send_copy_data(data_bytes)
        self._send_copy_done()
        return nrows

    def _exec_trino_flight(self, plan, fmt: str) -> tuple[bytes, int]:
        from provisa.api.app import state
        from provisa.executor.trino_flight import execute_trino_flight_arrow
        from provisa.executor.trino import execute_trino

        if state.flight_client is not None:
            try:
                table = execute_trino_flight_arrow(
                    state.flight_client, plan.trino_sql, plan.exec_params
                )
                data_bytes = _arrow_table_to_copy_bytes(table, fmt)
                return data_bytes, table.num_rows
            except Exception as exc:
                log.warning("[COPY TO] Flight SQL failed, falling back to Trino REST: %s", exc)

        if state.trino_conn is None:
            raise RuntimeError("Trino connection not available")
        result = execute_trino(state.trino_conn, plan.trino_sql, params=plan.exec_params)
        data_bytes = _queryresult_to_copy_bytes(result, fmt)
        return data_bytes, len(result.rows)

    def _exec_direct_plan(
        self, plan, loop: asyncio.AbstractEventLoop, fmt: str
    ) -> tuple[bytes, int]:
        from provisa.pgwire._pipeline import _execute_plan

        future = asyncio.run_coroutine_threadsafe(_execute_plan(plan), loop)
        result = future.result(timeout=120)
        data_bytes = _queryresult_to_copy_bytes(result, fmt)
        return data_bytes, len(result.rows)

    def _send_copy_out_response(self, fmt: str) -> None:
        # overall_format: 0=text, 1=binary; col_count 0 means unknown/variable
        overall = 0
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

    # ------------------------------------------------------------------
    # COPY FROM STDIN
    # ------------------------------------------------------------------

    def _handle_copy_from(
        self,
        ctx: BVContext,
        schema: str | None,
        table: str,
        explicit_cols: list[str] | None,
        fmt: str,
        role_id: str,
    ) -> int:
        from provisa.api.app import state

        tm, col_names = _find_table_meta(schema, table, role_id)

        source_type = state.source_types.get(tm.source_id, "")
        if source_type not in _WRITABLE_SOURCE_TYPES:
            raise PermissionError(
                f"COPY FROM is not supported for source type {source_type!r} (table {table!r})"
            )

        use_cols = explicit_cols if explicit_cols else col_names
        if not use_cols:
            raise ValueError(f"No columns discoverable for table {table!r}")

        self._send_copy_in_response(fmt)

        # Read all CopyData chunks until CopyDone or CopyFail
        data_chunks: list[bytes] = []
        while True:
            code = self._h.rfile.read(1)
            if not code:
                raise RuntimeError("Connection closed during COPY FROM")
            length_bytes = self._h.rfile.read(4)
            msglen = struct.unpack("!i", length_bytes)[0] - 4
            payload = self._h.rfile.read(msglen) if msglen > 0 else b""

            if code == _COPY_DATA:
                data_chunks.append(payload)
            elif code == _COPY_DONE:
                break
            elif code == _COPY_FAIL:
                msg = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
                raise RuntimeError(f"Client aborted COPY FROM: {msg}")
            else:
                log.warning("[COPY FROM] unexpected message code %r, ignoring", code)

        all_data = b"".join(data_chunks)
        rows = _parse_copy_data(all_data, fmt)

        from provisa.compiler.naming import domain_to_sql_name

        target_schema = schema or domain_to_sql_name(tm.domain_id)
        target_table = tm.table_name or tm.original_table_name or table

        from provisa.pgwire import server as _srv

        with _srv._loop_lock:
            loop = _srv._loop
        if loop is None:
            raise RuntimeError("Event loop not available")

        future = asyncio.run_coroutine_threadsafe(
            _insert_rows(tm.source_id, target_schema, target_table, use_cols, rows), loop
        )
        return future.result(timeout=120)

    def _send_copy_in_response(self, fmt: str) -> None:
        overall = 0
        body = struct.pack("!bh", overall, 0)
        self._h.wfile.write(struct.pack("!ci", _COPY_IN_RESPONSE, len(body) + 4))
        self._h.wfile.write(body)
        self._h.wfile.flush()

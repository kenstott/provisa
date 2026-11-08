# Copyright (c) 2026 Kenneth Stott
# Canary: b80c814d-a7b3-4891-8338-5572e74a63d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute queries via Arrow Flight SQL through the Zaychik proxy (REQ-045, REQ-2194).

Every Trino read reaches Provisa as Arrow: the ADBC Flight SQL driver talks to the Zaychik proxy,
which runs the statement through the Trino client protocol and streams the result as Arrow record
batches (``zaychik/trino-statement-protocol.patch``). What the proxy adds to the Flight exchange,
and how this module reads it:

* **Session** — the statement's Trino ``source`` (the org, REQ-056), catalog, schema and session
  properties (``query_max_execution_time``, the FTE hints) travel in the ``x-provisa-trino-context``
  call header (:class:`TrinoContext`); every statement is its own Trino session. An org on its
  own coordinator names it there; the proxy dials only a coordinator its deployment declared.
* **Types** — each Arrow field names its Trino type (``trino.type``, the ``column_types`` a
  result reports), its type signature and its encoding. A type Arrow has no type for arrives as
  Utf8 holding the protocol's text or JSON; :func:`trino_rows` reads it with the Trino client's own
  value mappers, so a row value is the one the Trino client gives.
* **Errors** — a statement Trino failed carries Trino's error name, type, code, message and query
  id in the Flight error's trailers (:func:`engine_error`); an unreachable proxy or coordinator is
  :class:`EngineUnreachable`.
* **Parameters** — bound as the Trino client binds them: ``EXECUTE IMMEDIATE '<sql>' USING
  <literals>`` (:func:`bound_statement`).
"""

# Requirements: REQ-028, REQ-045, REQ-051, REQ-143, REQ-144, REQ-145, REQ-146, REQ-271, REQ-277, REQ-278, REQ-279, REQ-302, REQ-303, REQ-2194

from __future__ import annotations

import base64
import datetime
import json
import logging
import math
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from provisa.core import request_deadline
from provisa.core.ir_arrow import column_values
from provisa.executor.result import ArrowResult, ArrowStreamResult
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

#: The call header the proxy reads a statement's Trino session from.
CONTEXT_HEADER = "x-provisa-trino-context"
_CALL_HEADER_OPTION = f"adbc.flight.sql.rpc.call_header.{CONTEXT_HEADER}"

#: Field metadata the proxy writes on every field.
TYPE_KEY = b"trino.type"
SIGNATURE_KEY = b"trino.type_signature"
ENCODING_KEY = b"trino.encoding"

#: Flight error trailers the proxy writes for a failed statement.
_ERROR_KIND = "x-trino-error-kind"
_ERROR_NAME = "x-trino-error-name"
_ERROR_TYPE = "x-trino-error-type"
_ERROR_CODE = "x-trino-error-code"
_QUERY_ID = "x-trino-query-id"
_ERROR_MESSAGE = "x-trino-error-message-bin"

#: Trino error names that mean the query ran out of memory: never retried.
_MEMORY_ERROR_NAMES = frozenset({"EXCEEDED_LOCAL_MEMORY_LIMIT", "EXCEEDED_GLOBAL_MEMORY_LIMIT"})


@dataclass(frozen=True)
class TrinoContext:
    """The Trino session a statement runs in: what a trino-client connection's kwargs and
    ``SET SESSION`` gave it before (REQ-056 source, REQ-1623 schema, REQ-302 session hints)."""

    source: str
    catalog: str | None = None
    schema: str | None = None
    properties: dict[str, str] = field(default_factory=dict)
    #: The coordinator the statement runs on (``scheme://host:port``) when it is not the one the
    #: proxy is configured with: an org's own engine. The proxy dials it only if the deployment
    #: declared it (``TF_TRINO_ALLOWED_SERVERS``), else refuses (:class:`CoordinatorRefused`).
    server: str | None = None

    def header(self) -> str:
        """The value of the ``x-provisa-trino-context`` call header: base64 of its JSON."""
        body: dict[str, Any] = {"source": self.source, "properties": dict(self.properties)}
        if self.server is not None:
            body["server"] = self.server
        if self.catalog is not None:
            body["catalog"] = self.catalog
        if self.schema is not None:
            body["schema"] = self.schema
        return base64.b64encode(json.dumps(body).encode("utf-8")).decode("ascii")


class TrinoFlightQueryError(Exception):
    """A statement Trino failed, as the proxy reports it: the attributes the Trino client's
    ``TrinoQueryError`` has, so :meth:`FederationError.from_engine_error` reads it the same way."""

    def __init__(
        self,
        message: str,
        *,
        error_name: str | None,
        error_type: str | None,
        error_code: int | None,
        query_id: str | None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_name = error_name
        self.error_type = error_type
        self.error_code = error_code
        self.query_id = query_id


class CoordinatorRefused(RuntimeError):
    """The statement named a coordinator the proxy is not declared to reach (its deployment's
    ``TF_TRINO_ALLOWED_SERVERS``): nothing was dialed. Never retried."""


class EngineUnreachable(ConnectionError):
    """The Zaychik proxy, or the coordinator behind it, could not be reached: no statement ran.
    A ConnectionError, so the engine read retries it inside its budget (coordinator loss)."""


def create_flight_connection(  # REQ-045, REQ-144, REQ-271
    host: str = "localhost",
    port: int = 8480,
    user: str = "provisa",
):
    """Create an ADBC Flight SQL connection to the Zaychik proxy.

    Returns an adbc_driver_flightsql.dbapi.Connection.
    Raises on connection failure — no fallback.
    """
    import adbc_driver_flightsql.dbapi as flight_sql

    conn = flight_sql.connect(
        uri=f"grpc://{host}:{port}",
        db_kwargs={
            "username": user,
            "password": "",
            "adbc.flight.sql.client_option.authority": f"{host}:{port}",
        },
        autocommit=True,
    )
    return conn


# -- parameters ---------------------------------------------------------------------------------


def render_param(param: Any) -> str:
    """``param`` as the literal an ``EXECUTE IMMEDIATE ... USING`` binds: the Trino client's own
    rendering (``trino.dbapi.Cursor._format_prepared_param``), so a value binds as it did there."""
    if param is None:
        return "NULL"
    if isinstance(param, bool):
        return "true" if param else "false"
    if isinstance(param, int):
        return "%d" % param
    if isinstance(param, float):
        if param == float("+inf"):
            return "infinity()"
        if param == float("-inf"):
            return "-infinity()"
        if math.isnan(param):
            return "nan()"
        return "DOUBLE '%s'" % param
    if isinstance(param, str):
        return "'%s'" % param.replace("'", "''")
    if isinstance(param, (bytes, bytearray)):
        return "X'%s'" % param.hex()
    if isinstance(param, datetime.datetime):
        datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
        if param.tzinfo is None:
            return "TIMESTAMP '%s'" % datetime_str
        if isinstance(param.tzinfo, ZoneInfo):
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.key)
        return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))
    if isinstance(param, datetime.time):
        time_str = param.strftime("%H:%M:%S.%f")
        if param.tzinfo is None:
            return "TIME '%s'" % time_str
        if isinstance(param.tzinfo, ZoneInfo):
            utc_offset = datetime.datetime.now(tz=param.tzinfo).strftime("%z")
            return "TIME '%s %s:%s'" % (time_str, utc_offset[:3], utc_offset[3:])
        return "TIME '%s %s'" % (time_str, param.strftime("%Z")[3:])
    if isinstance(param, datetime.date):
        return "DATE '%s'" % param.strftime("%Y-%m-%d")
    if isinstance(param, list):
        return "ARRAY[%s]" % ",".join(map(render_param, param))
    if isinstance(param, tuple):
        return "ROW(%s)" % ",".join(map(render_param, param))
    if isinstance(param, dict):
        keys = list(param.keys())
        return "MAP({}, {})".format(render_param(keys), render_param([param[k] for k in keys]))
    if isinstance(param, uuid.UUID):
        return "UUID '%s'" % param
    if isinstance(param, Decimal):
        return "DECIMAL '%s'" % format(param, "f")
    raise TypeError(f"query parameter of type {type(param).__name__!r} is not supported")


def bound_statement(sql: str, params: list | None) -> str:
    """The statement Trino runs for ``sql`` with ``params`` (or the values its embedded
    provisa-params comment carries, REQ-603): with values, ``EXECUTE IMMEDIATE`` binding them in
    placeholder order, exactly as the Trino client binds them; without, ``sql`` itself."""
    from provisa.compiler.params import bind_positionally, extract_params_comment

    exec_sql, embedded = extract_params_comment(sql)
    effective = params if params is not None else embedded
    if not effective:
        return exec_sql
    # A repeated or out-of-order $N (a pgwire client's SQL, REQ-589) binds the right value.
    exec_sql, ordered = bind_positionally(exec_sql, effective, "?")
    return (
        "EXECUTE IMMEDIATE '"
        + exec_sql.replace("'", "''")
        + "' USING "
        + ",".join(render_param(p) for p in ordered)
    )


# -- errors -------------------------------------------------------------------------------------


def _details(exc: BaseException) -> dict[str, bytes]:
    """The Flight error's header and trailer metadata, as the ADBC driver hands it on."""
    out: dict[str, bytes] = {}
    for name, value in getattr(exc, "details", None) or []:
        key = name.decode("ascii") if isinstance(name, bytes) else str(name)
        out.setdefault(key, value if isinstance(value, bytes) else str(value).encode("utf-8"))
    return out


def engine_error(exc: BaseException) -> BaseException:
    """The engine error a Flight failure stands for: :class:`FederationError` (by Trino's error
    name) for a statement Trino failed, :class:`EngineUnreachable` when nothing reached a
    coordinator; any other exception unchanged."""
    import adbc_driver_manager

    if not isinstance(exc, adbc_driver_manager.Error):
        return exc
    details = _details(exc)
    kind = details.get(_ERROR_KIND, b"").decode("utf-8")
    message = details[_ERROR_MESSAGE].decode("utf-8") if _ERROR_MESSAGE in details else str(exc)
    if kind == "query":
        from provisa.executor.errors import FederationError

        return FederationError.from_engine_error(
            TrinoFlightQueryError(
                message,
                error_name=details[_ERROR_NAME].decode("utf-8"),
                error_type=details[_ERROR_TYPE].decode("utf-8"),
                error_code=int(details[_ERROR_CODE]),
                query_id=details[_QUERY_ID].decode("utf-8"),
            )
        )
    if kind == "refused":
        return CoordinatorRefused(message)
    if kind == "connection" or exc.status_code == adbc_driver_manager.AdbcStatusCode.IO:
        # The proxy says the coordinator is unreachable, or the proxy itself is: the gRPC call
        # failed below any statement (UNAVAILABLE, a refused connection), which ADBC reports as IO.
        return EngineUnreachable(message)
    return exc


# -- rows ---------------------------------------------------------------------------------------


def column_types(schema: pa.Schema) -> list[str]:
    """Each column's Trino type, as the coordinator names it (``trino.type``)."""
    return [f.metadata[TYPE_KEY].decode("utf-8") for f in schema]


def _mapper(signature: bytes) -> Callable[[Any], Any]:
    """The Trino client's value mapper for a type signature: the one function that turns a
    protocol value into the Python value the Trino client gives."""
    from provisa.executor.trino import value_mapper

    return value_mapper(json.loads(signature))


def _field_values(f: pa.Field, column: pa.Array | pa.ChunkedArray) -> list[Any]:
    encoding = f.metadata[ENCODING_KEY]
    if encoding == b"text":
        mapper = _mapper(f.metadata[SIGNATURE_KEY])
        return [None if v is None else mapper(v) for v in column.to_pylist()]
    if encoding == b"json":
        mapper = _mapper(f.metadata[SIGNATURE_KEY])
        return [None if v is None else mapper(json.loads(v)) for v in column.to_pylist()]
    if pa.types.is_float32(f.type):
        # A REAL is the shortest decimal that names the float, as the protocol writes it and the
        # Trino client parses it; to_pylist would widen the float (0.1 -> 0.10000000149011612).
        import numpy as np

        return [None if v is None else float(str(np.float32(v))) for v in column.to_pylist()]
    if pa.types.is_timestamp(f.type) and f.type.tz is not None:
        # An instant is carried at UTC with its zone (REQ-2050), as datetime.timezone.utc.
        return [
            None if v is None else v.astimezone(datetime.timezone.utc)
            for v in column_values(column)
        ]
    return column_values(column)


def trino_rows(data: pa.RecordBatch | pa.Table) -> list[tuple]:
    """The rows of a Zaychik batch or table, each value the one the Trino client gives."""
    return list(zip(*(_field_values(f, c) for f, c in zip(data.schema, data.columns, strict=True))))


# -- execution ----------------------------------------------------------------------------------


def _open_cursor(conn, context: TrinoContext | None):
    """A cursor that runs its statement as a plain one (the proxy has no prepare step), with
    ``context`` in its call header."""
    import types

    cursor = conn.cursor()
    if context is not None:
        cursor.adbc_statement.set_options(**{_CALL_HEADER_OPTION: context.header()})

    def _prepare_execute_no_prepare(self, operation, *_):
        self._results = None
        if operation != self._last_query:
            self._last_query = operation
            self._stmt.set_sql_query(operation)

    cursor._prepare_execute = types.MethodType(_prepare_execute_no_prepare, cursor)
    return cursor


def _fetch_table(conn, exec_sql: str, context: TrinoContext | None) -> pa.Table:
    cursor = _open_cursor(conn, context)
    try:
        with request_deadline.cancel_on_deadline(cursor.adbc_cancel):
            cursor.execute(exec_sql)
            return cursor.fetch_arrow_table()
    except Exception as exc:
        raise engine_error(exc) from exc
    finally:
        cursor.close()


def _memory_error(exc: BaseException) -> MemoryError | None:
    from provisa.executor.errors import FederationError

    if isinstance(exc, FederationError) and exc.error_name in _MEMORY_ERROR_NAMES:
        return MemoryError(
            "Query exceeded Trino memory limit — add a limit clause or narrow your filter. "
            f"Detail: {exc.message[:300]}"
        )
    return None


def _with_retries(run: Callable[[], Any], span: Any) -> Any:
    """``run()``, retried with backoff inside the retry budget when it fails for coordinator loss
    (a retryable Trino error name, an unreachable coordinator); a memory error never is."""
    from provisa.executor.trino import _backoff_secs, _is_retryable, _retry_budget

    deadline = time.monotonic() + _retry_budget()
    attempt = 0
    while True:
        try:
            return run()
        except Exception as exc:
            memory = _memory_error(exc)
            if memory is not None:
                span.set_attribute("error", True)
                span.set_attribute("error.message", str(exc)[:500])
                raise memory from exc
            remaining = deadline - time.monotonic()
            if not _is_retryable(exc) or remaining <= 0:
                span.set_attribute("error", True)
                span.set_attribute("error.message", str(exc)[:500])
                raise
            attempt += 1
            delay = min(_backoff_secs(attempt), remaining)
            log.warning(
                "[EXEC TRINO FLIGHT] retry attempt=%d after %.1fs (%.1fs remaining) — %s",
                attempt,
                delay,
                remaining,
                exc,
            )
            time.sleep(delay)


def execute_trino_flight(  # REQ-028, REQ-277, REQ-278, REQ-279, REQ-302, REQ-303, REQ-2194
    conn,
    sql: str,
    params: list | None = None,
    *,
    context: TrinoContext | None = None,
    span_attrs: dict[str, str] | None = None,
    extra_table_attrs: list[dict[str, str]] | None = None,
) -> ArrowResult:
    """Run ``sql`` on Trino through the proxy and return the engine's Arrow table as the result,
    its rows built by :func:`trino_rows`; ``column_types`` are Trino's own type names. Coordinator
    loss is retried inside the retry budget."""
    exec_sql = bound_statement(sql, params)
    span_name = "provisa.query.trino" if span_attrs else "trino.execute"
    with _stage(_tracer, span_name, name="execute") as span:
        span.set_attribute("db.system", "trino")
        span.set_attribute("db.statement", exec_sql[:1000])
        for k, v in (span_attrs or {}).items():
            span.set_attribute(k, v)
        log.info("[EXEC TRINO FLIGHT] sql=%s", exec_sql[:200])
        table = _with_retries(lambda: _fetch_table(conn, exec_sql, context), span)
        span.set_attribute("db.row_count", table.num_rows)
        log.info("[EXEC TRINO FLIGHT] rows=%d", table.num_rows)
        for attrs in extra_table_attrs or []:
            with _tracer.start_as_current_span("provisa.query.trino") as child:
                for k, v in attrs.items():
                    child.set_attribute(k, v)
        # Rows are built here, on the calling (worker) thread: an awaiting consumer must do no
        # per-row work on the event loop. The table stays for a consumer that reads Arrow.
        return ArrowResult(table, column_types(table.schema), to_rows=trino_rows).with_rows()


def execute_trino_flight_sync(  # REQ-028, REQ-2194
    conn,
    sql: str,
    params: list | None = None,
    *,
    context: TrinoContext | None = None,
) -> ArrowStreamResult:
    """Run ``sql`` on Trino through the proxy and return its batches as they arrive. The statement
    is started (and coordinator loss retried) before the stream is returned; a failure while the
    batches are read raises the engine's error."""
    exec_sql = bound_statement(sql, params)
    with _stage(_tracer, "trino.execute", name="execute") as span:
        span.set_attribute("db.system", "trino")
        span.set_attribute("db.statement", exec_sql[:1000])
        log.info("[EXEC TRINO FLIGHT STREAM] sql=%s", exec_sql[:200])
        cursor, reader = _with_retries(lambda: _open_reader(conn, exec_sql, context), span)

    def _close(*_: Any) -> None:
        cursor.close()

    return ArrowStreamResult(
        reader.schema,
        _batches(reader),
        column_types(reader.schema),
        to_rows=trino_rows,
        on_close=_close,
    )


def _open_reader(conn, exec_sql: str, context: TrinoContext | None):
    cursor = _open_cursor(conn, context)
    try:
        with request_deadline.cancel_on_deadline(cursor.adbc_cancel):
            cursor.execute(exec_sql)
            reader = cursor.fetch_record_batch()
    except Exception as exc:
        cursor.close()
        raise engine_error(exc) from exc
    return cursor, reader


def _batches(reader) -> Iterator[pa.RecordBatch]:
    try:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                return
            yield batch
    except Exception as exc:
        raise engine_error(exc) from exc


def execute_trino_flight_arrow(  # REQ-051, REQ-143, REQ-144
    conn,
    sql: str,
    params: list | None = None,
    *,
    context: TrinoContext | None = None,
) -> pa.Table:
    """Execute SQL via Flight SQL, returning a native Arrow Table.

    Materializes the full result in memory. For unbounded streaming,
    use execute_trino_flight_stream() instead.
    """
    exec_sql = bound_statement(sql, params)
    log.info("[EXEC TRINO FLIGHT] sql=%s", exec_sql[:200])
    table = _fetch_table(conn, exec_sql, context)
    log.info("[EXEC TRINO FLIGHT] rows=%d", table.num_rows)
    return table


def execute_trino_flight_stream(  # REQ-145, REQ-271
    conn,
    sql: str,
    params: list | None = None,
    *,
    context: TrinoContext | None = None,
):
    """Execute SQL via Flight SQL, returning a streaming RecordBatchReader.

    Yields Arrow RecordBatches lazily — the full result is never held in
    memory. Use this for the Provisa Arrow Flight server to stream results
    to clients without materializing.

    Returns (schema, generator) where generator yields pa.RecordBatch.
    """
    exec_sql = bound_statement(sql, params)
    log.info("[EXEC TRINO FLIGHT STREAM] sql=%s", exec_sql[:200])
    cursor, reader = _open_reader(conn, exec_sql, context)

    def batch_generator():
        try:
            yield from _batches(reader)
        finally:
            cursor.close()

    return reader.schema, batch_generator()

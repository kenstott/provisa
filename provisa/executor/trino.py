# Copyright (c) 2026 Kenneth Stott
# Canary: 6f915d4b-6495-444e-8aa9-5436357ddb99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute transpiled SQL via Trino Python client.

Returns rows and column descriptions. Parameters substituted by Trino.
"""

# Requirements: REQ-028, REQ-054, REQ-277, REQ-278, REQ-279, REQ-302, REQ-303

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass

import trino
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

# Error names that indicate coordinator loss — safe to retry on read-only SQL.
_RETRYABLE_ERROR_NAMES: frozenset[str] = frozenset(
    {
        "COORDINATOR_NOT_AVAILABLE",
        "SERVER_SHUTTING_DOWN",
        "NO_NODES_AVAILABLE",
        "TOO_MANY_REQUESTS_FAILED",
        "REMOTE_TASK_FAILED",
    }
)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, ConnectionError):
        return True
    from provisa.executor.errors import FederationError

    if isinstance(exc, FederationError):
        return exc.error_name in _RETRYABLE_ERROR_NAMES
    return False


def _backoff_secs(attempt: int, cap: float = 30.0) -> float:
    """Full-jitter exponential backoff — spreads retries to avoid thundering herd."""
    return random.uniform(0, min(cap, 1.0 * (2**attempt)))


def _trino_query_timeout() -> int:
    try:
        from provisa.api.app import state

        return state.server_limits.get(
            "trino_query_timeout", int(os.environ.get("PROVISA_TRINO_QUERY_TIMEOUT", "120"))
        )
    except Exception:
        return int(os.environ.get("PROVISA_TRINO_QUERY_TIMEOUT", "120"))


def _retry_budget() -> float:
    try:
        from provisa.api.app import state

        return state.server_limits.get(
            "retry_budget_secs", float(os.environ.get("PROVISA_RETRY_BUDGET_SECS", "30"))
        )
    except Exception:
        return float(os.environ.get("PROVISA_RETRY_BUDGET_SECS", "30"))


@dataclass
class QueryResult:  # REQ-028
    """Result of executing a SQL query against Trino."""

    rows: list[tuple]
    column_names: list[str]
    column_types: list[str] | None = None


def _alive(conn: trino.dbapi.Connection) -> bool:
    """Probe liveness with a cheap no-op query."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return True
    except Exception:
        return False


def _close_quietly(conn: trino.dbapi.Connection | None) -> None:
    """Best-effort close of a connection execute_trino owns (opened from conn_kwargs)."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception:  # noqa: BLE001  # best-effort close in a finally must never raise
        pass


def execute_trino(  # REQ-028, REQ-054, REQ-277, REQ-278, REQ-279, REQ-302, REQ-303
    conn: trino.dbapi.Connection,
    sql: str,
    params: list | None = None,
    session_hints: dict[str, str] | None = None,
    conn_kwargs: dict | None = None,
    span_attrs: dict[str, str] | None = None,
    extra_table_attrs: list[dict[str, str]] | None = None,
) -> QueryResult:
    span_name = "provisa.query.trino" if span_attrs else "trino.execute"
    with _tracer.start_as_current_span(span_name) as span:
        _owned_conn = None
        if conn_kwargs is not None:
            conn = trino.dbapi.connect(**conn_kwargs)
            _owned_conn = conn  # execute_trino owns a conn it creates from kwargs; close it on exit
        elif not _alive(conn):
            log.warning("[EXEC TRINO] connection stale — reconnecting")
            try:
                from provisa.api.app import state

                conn = trino.dbapi.connect(**state.trino_conn_kwargs)
                state.trino_conn = conn
            except Exception as reconnect_exc:
                raise ConnectionError(f"Trino reconnect failed: {reconnect_exc}") from reconnect_exc
        # Extract embedded provisa-params comment if present; fall back to explicit params.
        from provisa.compiler.params import extract_params_comment

        exec_sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        # Trino Python client uses ? for parameter placeholders.
        # After SQLGlot transpilation, PG $N becomes Trino @N.
        # Replace both @N and $N with ? in reverse order to avoid $1 matching $10.
        if effective_params:
            for i in range(len(effective_params), 0, -1):
                exec_sql = exec_sql.replace(f"@{i}", "?")
                exec_sql = exec_sql.replace(f"${i}", "?")

        span.set_attribute("db.system", "trino")
        span.set_attribute("db.statement", exec_sql[:1000])
        if span_attrs:
            for k, v in span_attrs.items():
                span.set_attribute(k, v)

        # Inject session properties before the main query when hints are present.
        # Always inject query timeout so runaway queries don't starve workers.
        # FTE hints (retry_policy etc.) are merged in when FTE is enabled globally.
        effective_hints = dict(session_hints or {})
        effective_hints.setdefault("query_max_execution_time", f"{_trino_query_timeout()}s")
        try:
            from provisa.api.app import state as _app_state

            effective_hints = {**_app_state.trino_fte_hints, **effective_hints}
        except Exception:
            pass

        retry_budget = _retry_budget()
        deadline = time.monotonic() + retry_budget
        last_exc: Exception | None = None
        attempt = 0

        try:
            while True:
                if attempt > 0:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    delay = min(_backoff_secs(attempt), remaining)
                    log.warning(
                        "[EXEC TRINO] retry attempt=%d after %.1fs (%.1fs remaining) — %s",
                        attempt,
                        delay,
                        remaining,
                        last_exc,
                    )
                    time.sleep(delay)
                    try:
                        from provisa.api.app import state as _retry_state

                        conn = trino.dbapi.connect(**_retry_state.trino_conn_kwargs)
                        _retry_state.trino_conn = conn
                    except Exception as reconnect_exc:
                        last_exc = ConnectionError(
                            f"Trino reconnect on retry {attempt}: {reconnect_exc}"
                        )
                        attempt += 1
                        continue

                try:
                    cur = conn.cursor()
                    for key, value in effective_hints.items():
                        safe_key = key.replace("'", "")
                        safe_value = value.replace("'", "")
                        set_sql = f"SET SESSION {safe_key} = '{safe_value}'"
                        log.info("[EXEC TRINO] session hint: %s", set_sql)
                        cur.execute(set_sql)

                    log.info("[EXEC TRINO] sql=%s", exec_sql[:200])
                    if effective_params:
                        cur.execute(exec_sql, effective_params)
                    else:
                        cur.execute(exec_sql)
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description] if cur.description else []

                    span.set_attribute("db.row_count", len(rows))
                    log.info("[EXEC TRINO] rows=%d", len(rows))

                    if extra_table_attrs:
                        for _attrs in extra_table_attrs:
                            with _tracer.start_as_current_span("provisa.query.trino") as _child:
                                for k, v in _attrs.items():
                                    _child.set_attribute(k, v)

                    return QueryResult(rows=rows, column_names=column_names)

                except Exception as exc:
                    from provisa.executor.errors import FederationError

                    err_msg = str(exc)
                    # Memory errors are never retryable.
                    if any(
                        k in err_msg
                        for k in (
                            "EXCEEDED_LOCAL_MEMORY_LIMIT",
                            "EXCEEDED_GLOBAL_MEMORY_LIMIT",
                            "Query exceeded",
                        )
                    ):
                        span.set_attribute("error", True)
                        span.set_attribute("error.message", err_msg[:500])
                        raise MemoryError(
                            f"Query exceeded Trino memory limit — add a limit clause or narrow your filter. Detail: {err_msg[:300]}"
                        ) from exc

                    wrapped = (
                        FederationError.from_trino(exc)
                        if isinstance(exc, trino.exceptions.TrinoQueryError)
                        else exc
                    )

                    if _is_retryable(wrapped) and time.monotonic() < deadline:
                        last_exc = wrapped
                        attempt += 1
                        continue

                    span.set_attribute("error", True)
                    span.set_attribute("error.message", err_msg[:500])
                    if isinstance(exc, trino.exceptions.TrinoQueryError):
                        raise FederationError.from_trino(exc) from exc
                    raise

            span.set_attribute("error", True)
            span.set_attribute("error.message", str(last_exc)[:500])
            raise last_exc  # type: ignore[misc]  # loop always sets last_exc before exhausting
        finally:
            _close_quietly(_owned_conn)

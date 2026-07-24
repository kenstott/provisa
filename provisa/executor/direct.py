# Copyright (c) 2026 Kenneth Stott
# Canary: 1a5f3e92-8c7d-4b1a-9e3f-2d4c5b6a7e8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute transpiled SQL directly against source RDBMS via pluggable drivers.

Used when router decides single-source direct execution (REQ-027).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re

from provisa.executor.pool import SourcePool
from provisa.executor.result import QueryResult
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

# Requirements: REQ-027, REQ-031, REQ-052

_WRITE_RE = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|MERGE|CALL)\b",
    re.IGNORECASE,
)


def _backoff_secs(attempt: int, cap: float = 30.0) -> float:
    return random.uniform(0, min(cap, 1.0 * (2**attempt)))


def _retry_budget() -> float:
    try:
        from provisa.api.app import state

        return state.server_limits.get(
            "retry_budget_secs", float(os.environ.get("PROVISA_RETRY_BUDGET_SECS", "30"))
        )
    except Exception:
        return float(os.environ.get("PROVISA_RETRY_BUDGET_SECS", "30"))


async def execute_direct(  # REQ-027, REQ-031
    pool: SourcePool,
    source_id: str,
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Execute SQL directly against a source via its driver.

    Args:
        pool: SourcePool managing all source drivers.
        source_id: Target source identifier.
        sql: SQL in the source's native dialect (after SQLGlot transpilation).
        params: Positional parameter values.

    Returns:
        QueryResult with rows and column names.
    """
    with _tracer.start_as_current_span("direct.execute") as span:
        from provisa.compiler.params import extract_params_comment

        sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        span.set_attribute("db.source_id", source_id)
        span.set_attribute("db.statement", sql[:1000])

        is_write = bool(_WRITE_RE.match(sql))
        retry_budget = 0.0 if is_write else _retry_budget()
        deadline = asyncio.get_event_loop().time() + retry_budget
        last_exc: Exception | None = None
        attempt = 0

        while True:
            if attempt > 0:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                delay = min(_backoff_secs(attempt), remaining)
                log.warning(
                    "[EXEC DIRECT] retry attempt=%d after %.1fs (%.1fs remaining) — %s",
                    attempt,
                    delay,
                    remaining,
                    last_exc,
                )
                await asyncio.sleep(delay)

            try:
                log.info("[EXEC DIRECT] source=%s | sql=%s", source_id, sql[:200])
                result = await pool.execute(source_id, sql, effective_params)
                span.set_attribute("db.row_count", len(result.rows))
                log.info("[EXEC DIRECT] source=%s | rows=%d", source_id, len(result.rows))
                return result
            except ConnectionError as exc:
                remaining = deadline - asyncio.get_event_loop().time()
                if is_write or remaining <= 0:
                    span.set_attribute("error", True)
                    span.set_attribute("error.message", str(exc)[:500])
                    raise
                last_exc = exc
                attempt += 1

        span.set_attribute("error", True)
        span.set_attribute("error.message", str(last_exc)[:500])
        raise last_exc  # type: ignore[misc]


async def open_direct_stream(  # REQ-1190
    pool: SourcePool,
    source_id: str,
    sql: str,
    params: list | None = None,
):
    """Open a bounded server-side cursor on a single reachable source — the DIRECT streaming terminal.

    A large single-source passthrough scan (``SELECT * FROM one_big_source``) drains in fetch batches
    instead of materializing the whole result in Provisa, so DIRECT is memory-bounded identically to
    ENGINE (REQ-1190, streaming-uniformity-gap Defect 1). Reads only — a write never streams. No retry
    loop: a connection error surfaces at open (before any row has been served), unlike ``execute_direct``
    which can safely replay a whole buffered read.
    """
    with _tracer.start_as_current_span("direct.stream") as span:
        from provisa.compiler.params import extract_params_comment

        sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        span.set_attribute("db.source_id", source_id)
        span.set_attribute("db.statement", sql[:1000])
        log.info("[EXEC DIRECT STREAM] source=%s | sql=%s", source_id, sql[:200])
        return await pool.open_stream(source_id, sql, effective_params)

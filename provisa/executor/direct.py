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

import logging

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.pool import SourcePool
from provisa.executor.trino import QueryResult
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


async def execute_direct(
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
        span.set_attribute("db.source_id", source_id)
        span.set_attribute("db.statement", sql[:1000])
        log.info("[EXEC DIRECT] source=%s | sql=%s", source_id, sql[:200])
        result = await pool.execute(source_id, sql, params)
        span.set_attribute("db.row_count", len(result.rows))
        log.info("[EXEC DIRECT] source=%s | rows=%d", source_id, len(result.rows))
        return result

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

from __future__ import annotations

import logging
from dataclasses import dataclass

import trino
from provisa.otel_compat import get_tracer as _get_tracer

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)


@dataclass
class QueryResult:
    """Result of executing a SQL query against Trino."""

    rows: list[tuple]
    column_names: list[str]


def execute_trino(
    conn: trino.dbapi.Connection,
    sql: str,
    params: list | None = None,
    session_hints: dict[str, str] | None = None,
) -> QueryResult:
    """Execute SQL on Trino and return results.

    Args:
        conn: Active Trino connection.
        sql: Trino-dialect SQL string.
        params: Positional parameters (Trino uses ? placeholders internally,
                but we substitute $N -> ? before execution).
        session_hints: Optional Trino session properties to set before executing
                       the query (e.g. ``{"join_distribution_type": "BROADCAST"}``).
                       Each entry emits a ``SET SESSION key = 'value'`` statement.

    Returns:
        QueryResult with rows and column names.
    """
    with _tracer.start_as_current_span("trino.execute") as span:
        # Trino Python client uses ? for parameter placeholders.
        # After SQLGlot transpilation, PG $N becomes Trino @N.
        # Replace both @N and $N with ? in reverse order to avoid $1 matching $10.
        exec_sql = sql
        if params:
            for i in range(len(params), 0, -1):
                exec_sql = exec_sql.replace(f"@{i}", "?")
                exec_sql = exec_sql.replace(f"${i}", "?")

        span.set_attribute("db.system", "trino")
        span.set_attribute("db.statement", exec_sql[:1000])

        # Inject session properties before the main query when hints are present.
        if session_hints:
            cur = conn.cursor()
            for key, value in session_hints.items():
                safe_key = key.replace("'", "")
                safe_value = value.replace("'", "")
                set_sql = f"SET SESSION {safe_key} = '{safe_value}'"
                log.info("[EXEC TRINO] session hint: %s", set_sql)
                cur.execute(set_sql)

        log.info("[EXEC TRINO] sql=%s", exec_sql[:200])
        cur = conn.cursor()
        if params:
            cur.execute(exec_sql, params)
        else:
            cur.execute(exec_sql)

        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description] if cur.description else []

        span.set_attribute("db.row_count", len(rows))
        log.info("[EXEC TRINO] rows=%d", len(rows))
        return QueryResult(rows=rows, column_names=column_names)

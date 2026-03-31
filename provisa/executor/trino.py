# Copyright (c) 2025 Kenneth Stott
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

from dataclasses import dataclass

import trino


@dataclass
class QueryResult:
    """Result of executing a SQL query against Trino."""

    rows: list[tuple]
    column_names: list[str]


def execute_trino(
    conn: trino.dbapi.Connection,
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Execute SQL on Trino and return results.

    Args:
        conn: Active Trino connection.
        sql: Trino-dialect SQL string.
        params: Positional parameters (Trino uses ? placeholders internally,
                but we substitute $N -> ? before execution).

    Returns:
        QueryResult with rows and column names.
    """
    # Trino Python client uses ? for parameter placeholders.
    # After SQLGlot transpilation, PG $N becomes Trino @N.
    # Replace both @N and $N with ? in reverse order to avoid $1 matching $10.
    exec_sql = sql
    if params:
        for i in range(len(params), 0, -1):
            exec_sql = exec_sql.replace(f"@{i}", "?")
            exec_sql = exec_sql.replace(f"${i}", "?")

    cur = conn.cursor()
    if params:
        cur.execute(exec_sql, params)
    else:
        cur.execute(exec_sql)

    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description] if cur.description else []

    return QueryResult(rows=rows, column_names=column_names)

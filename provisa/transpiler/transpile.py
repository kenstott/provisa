# Copyright (c) 2026 Kenneth Stott
# Canary: 2af8ab62-fcda-4876-9364-1040f6919d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLGlot-based SQL transpilation (REQ-066, REQ-068).

Supports PG SQL → Trino, and PG SQL → any target dialect for direct execution.
"""

import sqlglot


# Valid SQLGlot write dialects for target sources
SUPPORTED_DIALECTS: set[str] = {
    "trino", "postgres", "mysql", "tsql", "duckdb", "snowflake", "bigquery",
}


def transpile_to_trino(pg_sql: str) -> str:
    """Transpile PostgreSQL-dialect SQL to Trino SQL."""
    return transpile(pg_sql, "trino")


def transpile(pg_sql: str, target_dialect: str) -> str:
    """Transpile PostgreSQL-dialect SQL to a target dialect.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.
        target_dialect: SQLGlot dialect name (e.g. "trino", "postgres", "mysql").

    Returns:
        SQL string in target dialect.
    """
    results = sqlglot.transpile(pg_sql, read="postgres", write=target_dialect)
    if not results:
        raise ValueError(f"SQLGlot produced no output for: {pg_sql!r}")
    return results[0]

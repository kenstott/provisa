# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLGlot-based SQL transpilation: PG SQL -> Trino SQL (REQ-066).

Phase D: all queries route through Trino. Phase E adds target dialect routing.
"""

import sqlglot


def transpile_to_trino(pg_sql: str) -> str:
    """Transpile PostgreSQL-dialect SQL to Trino SQL.

    Args:
        pg_sql: SQL string in PostgreSQL dialect with double-quoted identifiers.

    Returns:
        SQL string in Trino dialect.

    Raises:
        sqlglot.errors.ParseError: If the SQL cannot be parsed.
    """
    results = sqlglot.transpile(pg_sql, read="postgres", write="trino")
    if not results:
        raise ValueError(f"SQLGlot produced no output for: {pg_sql!r}")
    return results[0]

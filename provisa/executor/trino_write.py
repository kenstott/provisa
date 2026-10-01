# Copyright (c) 2026 Kenneth Stott
# Canary: 9fa82432-ea47-4f24-90c8-a4cd84b6386e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CTAS-based redirect: Trino writes results directly to S3 (REQ-045).

For formats Trino natively supports (Parquet, ORC), the query is wrapped in
CREATE TABLE AS SELECT so Trino workers write directly to MinIO/S3, avoiding
any serialization in Provisa.
"""

from __future__ import annotations

import logging
import threading
import uuid

import trino

# Requirements: REQ-029, REQ-044, REQ-138, REQ-141

log = logging.getLogger(__name__)

# Object-store / format helpers live in the neutral redirect module; re-export for impl callers.
from provisa.executor.redirect import (  # noqa: E402,F401  # re-export for impl back-compat
    ENGINE_NATIVE_FORMATS,
    is_engine_native_format,
    presign_ctas_result,
    schedule_s3_cleanup,
)

RESULTS_CATALOG = "results"
RESULTS_SCHEMA = "provisa_results"
RESULTS_BUCKET = "provisa-results"


def _iceberg_format(fmt: str) -> str:
    """Map output format name to Iceberg storage format."""
    return fmt.upper()  # PARQUET, ORC


_results_schema_ensured = False
_results_schema_lock = threading.Lock()


def ensure_results_schema(conn: trino.dbapi.Connection) -> None:
    """Create the results schema if it doesn't exist — once per process, by the first CTAS
    redirect (REQ-171), after the bucket its location names has been ensured.

    ``IF NOT EXISTS`` already makes an existing schema a success, so any error here is a real one
    (the catalog is missing, the location is unusable) and is raised to the redirect that needed
    the schema. It used to be logged at DEBUG at boot and the first CTAS failed later instead.
    """
    global _results_schema_ensured
    if _results_schema_ensured:
        return
    sql = (
        f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{RESULTS_SCHEMA} "
        f"WITH (location = 's3a://{RESULTS_BUCKET}/')"
    )
    with _results_schema_lock:
        if _results_schema_ensured:
            return
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()  # the statement's outcome arrives with its result, not with execute()
        log.info("Ensured results schema %s.%s exists", RESULTS_CATALOG, RESULTS_SCHEMA)
        _results_schema_ensured = True


def execute_ctas_redirect(  # REQ-029, REQ-044, REQ-138
    conn: trino.dbapi.Connection,
    select_sql: str,
    output_format: str = "parquet",
    params: list | None = None,
) -> dict:
    """Execute a query via CTAS, writing results directly to S3.

    Uses the Iceberg connector to write Parquet/ORC files to MinIO/S3.

    Args:
        conn: Trino connection.
        select_sql: The SELECT query to execute (already transpiled to Trino SQL).
        output_format: Target format (parquet, orc).
        params: The statement's bound values, in ``$N`` order (None: it binds none). Bound
            exactly as the row terminal binds them (``execute_trino``): the transpiled ``@N``
            placeholders become ``?`` in occurrence order, a repeated one repeating its value.

    Returns:
        {"table_name": "...", "s3_prefix": "...", "row_count": N}
    """
    result_id = uuid.uuid4().hex[:16]
    table_name = f"r_{result_id}"
    s3_prefix = f"s3a://{RESULTS_BUCKET}/results/{result_id}"
    iceberg_fmt = _iceberg_format(output_format)

    ctas_sql = (
        f'CREATE TABLE {RESULTS_CATALOG}.{RESULTS_SCHEMA}."{table_name}" '
        f"WITH (format = '{iceberg_fmt}', location = '{s3_prefix}') "
        f"AS {select_sql}"
    )

    log.info("[CTAS REDIRECT] table=%s format=%s", table_name, iceberg_fmt)
    log.debug("[CTAS REDIRECT] sql=%s", ctas_sql[:300])

    cur = conn.cursor()
    if params:
        from provisa.compiler.params import bind_positionally

        ctas_sql, bound = bind_positionally(ctas_sql, params, "?")
        cur.execute(ctas_sql, bound)
    else:
        cur.execute(ctas_sql)
    # CTAS returns the row count
    rows = cur.fetchall()
    row_count = rows[0][0] if rows and rows[0] else 0

    log.info("[CTAS REDIRECT] wrote %d rows to %s", row_count, s3_prefix)

    return {
        "table_name": table_name,
        "s3_prefix": s3_prefix,
        "row_count": row_count,
    }


def cleanup_result_table(conn: trino.dbapi.Connection, table_name: str) -> None:  # REQ-141
    """Drop a result table (metadata only — external table data stays on S3)."""
    sql = f'DROP TABLE IF EXISTS {RESULTS_CATALOG}.{RESULTS_SCHEMA}."{table_name}"'
    cur = conn.cursor()
    cur.execute(sql)
    log.info("[CTAS CLEANUP] dropped table %s", table_name)

# Copyright (c) 2026 Kenneth Stott
# Canary: b80c814d-a7b3-4891-8338-5572e74a63d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute queries via Arrow Flight SQL through Zaychik proxy (REQ-045).

Uses the ADBC Flight SQL driver to connect to the Zaychik Arrow Flight SQL
proxy, which translates Flight SQL requests to Trino JDBC queries and streams
results back as Arrow record batches.
"""

from __future__ import annotations

import logging

import pyarrow as pa

from provisa.executor.trino import QueryResult

log = logging.getLogger(__name__)


def create_flight_connection(
    host: str = "localhost",
    port: int = 8480,
    user: str = "provisa",
):
    """Create an ADBC Flight SQL connection to the Zaychik proxy.

    Returns an adbc_driver_flightsql.dbapi.Connection.

    Raises ConnectionError or ImportError when Zaychik is unavailable.
    Callers should catch these and fall back to execute_trino() (REQ-146).
    """
    import adbc_driver_flightsql.dbapi as flight_sql

    conn = flight_sql.connect(
        uri=f"grpc://{host}:{port}",
        db_kwargs={
            "username": user,
            "password": "",
            "adbc.flight.sql.client_option.authority": f"{host}:{port}",
        },
    )
    return conn


def _substitute_params(sql: str, params: list | None) -> str:
    """Substitute positional parameters inline into SQL.

    ADBC supports parameterized queries, but Trino's Flight SQL has
    limited prepared statement support. Inline substitution is consistent
    with the REST path.
    """
    if not params:
        return sql

    exec_sql = sql
    for i in range(len(params), 0, -1):
        exec_sql = exec_sql.replace(f"@{i}", "?")
        exec_sql = exec_sql.replace(f"${i}", "?")

    for param in params:
        if isinstance(param, str):
            safe = param.replace("'", "''")
            exec_sql = exec_sql.replace("?", f"'{safe}'", 1)
        elif param is None:
            exec_sql = exec_sql.replace("?", "NULL", 1)
        else:
            exec_sql = exec_sql.replace("?", str(param), 1)

    return exec_sql


def execute_trino_flight(
    conn,
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Execute SQL via Flight SQL, returning a QueryResult (tuple rows).

    Drop-in replacement for execute_trino() that uses Flight SQL transport
    but produces the same output type for the existing pipeline.
    """
    table = execute_trino_flight_arrow(conn, sql, params)
    column_names = table.column_names
    rows = [tuple(row.values()) for row in table.to_pylist()]
    return QueryResult(rows=rows, column_names=column_names)



def _skip_prepare(cursor) -> None:
    """Patch cursor to skip prepare() — Zaychik doesn't support it."""
    import types

    def _prepare_execute_no_prepare(self, operation, parameters=None):
        self._results = None
        if operation != self._last_query:
            self._last_query = operation
            self._stmt.set_sql_query(operation)

    cursor._prepare_execute = types.MethodType(
        _prepare_execute_no_prepare, cursor,
    )


def execute_trino_flight_arrow(
    conn,
    sql: str,
    params: list | None = None,
) -> pa.Table:
    """Execute SQL via Flight SQL, returning a native Arrow Table.

    Materializes the full result in memory. For unbounded streaming,
    use execute_trino_flight_stream() instead.
    """
    exec_sql = _substitute_params(sql, params)
    log.info("[EXEC TRINO FLIGHT] sql=%s", exec_sql[:200])

    cursor = conn.cursor()
    _skip_prepare(cursor)
    cursor.execute(exec_sql)
    table = cursor.fetch_arrow_table()
    cursor.close()

    log.info("[EXEC TRINO FLIGHT] rows=%d", table.num_rows)
    return table


def execute_trino_flight_stream(
    conn,
    sql: str,
    params: list | None = None,
):
    """Execute SQL via Flight SQL, returning a streaming RecordBatchReader.

    Yields Arrow RecordBatches lazily — the full result is never held in
    memory. Use this for the Provisa Arrow Flight server to stream results
    to clients without materializing.

    Returns (schema, generator) where generator yields pa.RecordBatch.
    """
    exec_sql = _substitute_params(sql, params)
    log.info("[EXEC TRINO FLIGHT STREAM] sql=%s", exec_sql[:200])

    cursor = conn.cursor()
    _skip_prepare(cursor)
    cursor.execute(exec_sql)
    reader = cursor.fetch_record_batch()
    schema = reader.schema

    def batch_generator():
        try:
            while True:
                batch = reader.read_next_batch()
                yield batch
        except StopIteration:
            pass
        finally:
            cursor.close()

    return schema, batch_generator()

# Copyright (c) 2026 Kenneth Stott
# Canary: 3a18df89-1f37-40d5-a1f0-12f184f5f305
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDBBackend — the DuckDB engine's in-process terminal. All lifecycle (persistent runtime, source
attach, execution, materialization-store cache terminal) lives in NativeEngineBackend; this subclass
supplies only the DuckDB runtime and the DuckDB driver error type."""

from __future__ import annotations

from typing import Any

import duckdb

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.federation.native_backend import NativeEngineBackend


class DuckDBBackend(NativeEngineBackend):
    """Every registered table ATTACHes into ONE persistent DuckDBFederationRuntime (postgres/sqlite/
    csv/parquet in place; non-attachable remote sources LAND into the materialization store). Governed
    physical SQL — already transpiled to the DuckDB dialect by transpile_physical — runs against its
    connection, whose catalog-physical views resolve the names the compiler emits."""

    _attach_errors = (duckdb.Error, KeyError)

    def _new_runtime(self) -> Any:
        return DuckDBFederationRuntime()


def local_catalog_connection() -> duckdb.DuckDBPyConnection:
    """Private in-memory DuckDB connection for pgwire's pg_catalog emulation (REQ-127,
    REQ-128, REQ-363). Independent of any federated engine's dialect — this connection
    only ever serves synthetic catalog tables, never physical source data."""
    db = duckdb.connect(":memory:")
    db.execute("CREATE MACRO pg_backend_pid() AS 0")
    db.execute("CREATE MACRO age(x) AS 0")
    db.execute("CREATE MACRO quote_ident(x) AS '\"' || replace(x, '\"', '\"\"') || '\"'")
    db.execute("""CREATE MACRO pg_available_extensions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS default_version,
               CAST(NULL AS VARCHAR) AS installed_version, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    db.execute("""CREATE MACRO pg_available_extension_versions() AS TABLE
        SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS VARCHAR) AS version,
               FALSE AS installed, FALSE AS superuser, FALSE AS trusted,
               FALSE AS relocatable, CAST(NULL AS VARCHAR) AS schema,
               CAST(NULL AS VARCHAR[]) AS requires, CAST(NULL AS VARCHAR) AS comment
        LIMIT 0""")
    return db

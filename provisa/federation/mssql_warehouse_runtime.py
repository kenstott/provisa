# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MssqlWarehouseRuntime — Microsoft Fabric Warehouse / Azure Synapse as a federation engine.

Both are T-SQL MPP warehouses reached over TDS/ODBC with Azure AD auth (no SQL password). A
partial-federator warehouse: object/lake sources on OneLake/ADLS ATTACH as zero-copy views over
``OPENROWSET`` (Fabric/Synapse's native external-data read); every other readable source LANDs into a
per-source schema. Governed SQL runs in the T-SQL dialect. Reads build Arrow from the ODBC cursor
(pyodbc has no native Arrow), surfaced through Arrow Flight.

Auth is an Azure AD access token (``azure-identity`` ``DefaultAzureCredential`` — picks up ``az login``
/ a managed identity / a service principal), passed to the driver via ``SQL_COPT_SS_ACCESS_TOKEN``. The
ODBC driver is the Microsoft ``ODBC Driver 18 for SQL Server`` (name, or a full dylib path via
``$PROVISA_MSSQL_ODBC_DRIVER`` on nonstandard installs). pyodbc/azure-identity are imported lazily so
this module loads where they are absent.

Physical naming: T-SQL is ``database.schema.table`` (a fixed warehouse database + per-source schema),
so the governed pipeline pins each source's catalog to the database (state.source_catalogs).
"""

from __future__ import annotations

import os
import struct
from typing import Any

from provisa.core.ir_types import to_ir
from provisa.executor.result import QueryResult
from provisa.federation.runtime_support import run_async

_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AAD_SCOPE = "https://database.windows.net/.default"

# Canonical IR name → T-SQL / Fabric-Warehouse type (Fabric supports a subset: VARCHAR, no TEXT).
_IR_TO_TSQL: dict[str, str] = {
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",
    "text": "VARCHAR(8000)",
    "boolean": "BIT",
    "float": "REAL",
    "double": "FLOAT",
    "numeric": "DECIMAL(38,9)",
    "date": "DATE",
    "timestamp": "DATETIME2",
    "time": "TIME",
    "uuid": "VARCHAR(64)",
    "bytea": "VARBINARY(8000)",
    "json": "VARCHAR(8000)",
}


def _tsql_type(ir_type: str) -> str:
    t = _IR_TO_TSQL.get(to_ir(ir_type))
    if t is None:
        raise ValueError(f"no T-SQL type mapping for IR type {ir_type!r}")
    return t


class MssqlWarehouseRuntime:  # Fabric / Synapse
    def __init__(self, *, server: str, database: str, engine_name: str = "fabric") -> None:
        if not server or not database:
            raise ValueError("Fabric/Synapse engine requires a SQL server host and database")
        self._server = server
        self._database = database
        self._engine_name = engine_name  # 'fabric' | 'synapse' — selects the connector set
        self._engine: Any = None
        self._conn = self._connect()

    def _connect(self) -> Any:
        import pyodbc
        from azure.identity import DefaultAzureCredential

        token = DefaultAzureCredential().get_token(_AAD_SCOPE).token
        raw = token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(raw)}s", len(raw), raw)
        driver = os.environ.get("PROVISA_MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
        # A registered driver is referenced by name in braces; a full dylib path is used bare.
        driver_clause = driver if driver.startswith("/") else f"{{{driver}}}"
        conn_str = (
            f"DRIVER={driver_clause};SERVER={self._server};DATABASE={self._database};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        return pyodbc.connect(
            conn_str, attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=60
        )

    @property
    def dialect(self) -> str:
        return "tsql"

    @property
    def connection(self):
        return self._conn

    def ensure_materialize_attached(self) -> str:
        return self._database

    def _engine_for(self) -> Any:
        if self._engine is None:
            from provisa.federation.engine import build_engine

            self._engine = build_engine(self._engine_name)
        return self._engine

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        """(database, schema, table) — the governed physical name (catalog pinned to the warehouse db)."""
        return self._database, source.schema_name, source.table_name

    def _ensure_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Object/lake sources attach as a ZERO-COPY view over ``OPENROWSET`` — the warehouse's native
        external-data read (SCAN); every other source LANDs, so attach is a no-op for it.

        For the FABRIC engine, external object storage (S3-compatible / R2) is reached through a OneLake
        **shortcut**: attach AUTO-PROVISIONS all its prerequisites — an S3-compatible connection, a
        lakehouse, and the shortcut — via the Fabric REST API (like the Databricks connector provisions
        its UC credential + external location), then OPENROWSETs the OneLake path. A direct ADLS/OneLake
        URL (Synapse, or Fabric with an already-onelake path) is OPENROWSET'd as-is. Validation reads a
        row so bad credentials / an unreachable object fail loud at attach time."""
        from provisa.federation.connector_base import LIVE_IN_PLACE

        entry = self._engine_for().resolve(source)
        if (
            entry.mechanism not in LIVE_IN_PLACE
        ):  # attach only what the engine reads in place (REQ-951)
            return None
        d = entry.details
        location = d.get("location")
        if not location:
            raise ValueError(f"external-link source {source.id!r} has no 'path' (object-store URL)")
        _database, schema, table = self._phys_parts(source)
        fq = f"[{schema}].[{table}]"
        bulk_path = self._resolve_bulk_path(source, location)
        cur = self._conn.cursor()
        try:
            self._ensure_schema(cur, schema)
            cur.execute(
                f"CREATE OR ALTER VIEW {fq} AS "
                f"SELECT * FROM OPENROWSET(BULK '{bulk_path}', FORMAT = '{d['format']}') AS r"
            )
            cur.execute(f"SELECT TOP 1 * FROM {fq}")  # validate the external attach
            cur.fetchall()
        finally:
            cur.close()
        return None

    def _resolve_bulk_path(self, source: Any, location: str) -> str:
        """The path OPENROWSET reads. An ``s3://`` (S3-compatible/R2) location on the Fabric engine is
        auto-provisioned as a OneLake shortcut and read via its OneLake path; any other URL (ADLS /
        OneLake) is read directly (Synapse, or an already-OneLake Fabric path)."""
        if self._engine_name != "fabric" or not location.startswith("s3://"):
            return location
        from provisa.federation.fabric_shortcuts import ensure_external_shortcut

        hints = getattr(source, "federation_hints", {}) or {}
        # Per-source override wins over the top-level env default, so two Fabric accounts/workspaces
        # can coexist — one source pins its own workspace via federation_hints.workspace_id.
        workspace_id = hints.get("workspace_id") or os.environ.get("FABRIC_WORKSPACE_ID")
        if not workspace_id:
            raise ValueError(
                "Fabric S3-compatible external link requires a workspace id "
                "(source federation_hints.workspace_id or $FABRIC_WORKSPACE_ID)"
            )
        # s3://<bucket>/<dir>/<file>
        rest = location[len("s3://") :]
        bucket, _, key = rest.partition("/")
        subpath, _, filename = key.rpartition("/")
        raw_name = f"{source.id}_{source.table_name}"
        shortcut_name = "".join(c if c.isalnum() else "_" for c in raw_name).strip("_")
        return ensure_external_shortcut(
            workspace_id=workspace_id,
            endpoint=hints.get("endpoint", ""),
            bucket=bucket,
            subpath="/" + subpath,
            filename=filename,
            access_key=hints.get("access_key_id", ""),
            secret=hints.get("secret_access_key", ""),
            name=shortcut_name,
        )

    async def materialize_source(
        self,
        source: Any,
        columns: list[tuple[str, str]],
        rows: list[dict],
        *,
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
    ) -> None:
        """LAND a source into a per-source schema at the compiler-physical name. A bulk multi-row
        INSERT (replace = TRUNCATE+insert, append = insert), never per-row."""
        del pk_columns
        import asyncio

        from provisa.core.change_signal import APPEND, select_landing_shape

        append = select_landing_shape(change_signal, watermark_column) == APPEND
        await asyncio.to_thread(self._land, source, columns, rows, append)

    def _land(self, source: Any, columns, rows: list[dict], append: bool) -> None:
        _database, schema, table = self._phys_parts(source)
        fq = f"[{schema}].[{table}]"
        cols_ddl = ", ".join(f"[{n}] {_tsql_type(t)}" for n, t in columns)
        cur = self._conn.cursor()
        try:
            self._ensure_schema(cur, schema)
            cur.execute(
                f"IF OBJECT_ID('{schema}.{table}') IS NULL EXEC('CREATE TABLE {fq} ({cols_ddl})')"
            )
            if not append:
                cur.execute(f"TRUNCATE TABLE {fq}")
            if rows:
                colnames = [n for n, _ in columns]
                collist = ", ".join(f"[{c}]" for c in colnames)
                ph = "(" + ", ".join("?" * len(colnames)) + ")"
                cur.fast_executemany = True
                cur.executemany(
                    f"INSERT INTO {fq} ({collist}) VALUES {ph}",
                    [tuple(r.get(c) for c in colnames) for r in rows],
                )
            self._conn.commit()
        finally:
            cur.close()

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute T-SQL (transpiled by the backend seam) and return rows."""
        del params  # SQL arrives fully substituted from the governed pipeline
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = [tuple(r) for r in cur.fetchall()] if cur.description else []
            return QueryResult(rows=rows, column_names=cols)
        finally:
            cur.close()

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async(self.run_sync, sql, params)

    # -- Arrow transport (built from the ODBC cursor — pyodbc has no native Arrow) --

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        import pyarrow as pa

        res = self.run_sync(sql, params)
        if not res.column_names:
            return pa.table({})
        cols = {name: [row[i] for row in res.rows] for i, name in enumerate(res.column_names)}
        return pa.table(cols)

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        table = self.run_arrow(sql, params)

        def _batches():
            yield from table.to_batches()

        return table.schema, _batches()

    def close(self) -> None:
        self._conn.close()

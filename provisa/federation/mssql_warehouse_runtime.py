# Copyright (c) 2026 Kenneth Stott
# Canary: b59c5b02-ffb5-4bd4-a968-4c667bb21a3e
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
from provisa.executor.result import QueryResult, ResultStream
from provisa.federation.runtime_support import run_async_materialized, stream_rows_from_arrow

_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AAD_SCOPE = "https://database.windows.net/.default"
_ARROW_CHUNK_ROWS = 10_000  # rows per lazy fetchmany chunk for the Arrow stream (REQ-1216)

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
    # REQ-1633: Fabric Warehouse rejects bare DATETIME2/TIME with "An integer precision value
    # between 0 and 6 must be specified" — unlike plain SQL Server (which defaults to 7
    # fractional-second digits when no precision is given), Fabric requires it explicit and caps
    # it at 6. Verified live (REQ-1730 engine-swap harness, 2026-09-21).
    "timestamp": "DATETIME2(6)",
    "time": "TIME(6)",
    "uuid": "VARCHAR(64)",
    "bytea": "VARBINARY(8000)",
    "json": "VARCHAR(8000)",
}


def _tsql_type(ir_type: str) -> str:
    t = _IR_TO_TSQL.get(to_ir(ir_type))
    if t is None:
        raise ValueError(f"no T-SQL type mapping for IR type {ir_type!r}")
    return t


def _coerce(value: Any, ir_type: str) -> Any:
    """A landed value, ODBC-bindable for its column's T-SQL type (REQ-1633/REQ-1730): a source's own
    adapter hands back its native string forms (Elasticsearch's own ``date`` fields are ISO-8601 with
    a trailing ``Z``, e.g. ``"2025-03-01T09:15:00Z"``), which pyodbc cannot implicitly cast to
    DATETIME2/DATE/TIME — verified live, ``Invalid character value for cast specification``. Parsed
    into a native ``datetime``/``date``/``time`` here so pyodbc binds a proper SQL_TIMESTAMP/
    SQL_DATE/SQL_TIME parameter instead of a raw string, the same class of coercion Snowflake's own
    ``_bind_value`` does for its driver."""
    if value is None:
        return None
    canonical = to_ir(ir_type)
    if canonical in ("timestamp", "date", "time") and isinstance(value, str):
        import datetime

        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if canonical == "date":
            return parsed.date()
        if canonical == "time":
            return parsed.timetz() if parsed.tzinfo else parsed.time()
        return parsed
    return value


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
        # pyodbc's connect(timeout=) is the LOGIN timeout. A serverless Synapse/Fabric SQL pool
        # auto-pauses and resumes on first connect, which can outlast a short window — make it
        # configurable (PROVISA_MSSQL_LOGIN_TIMEOUT, seconds) so a cold pool isn't a false failure.
        login_timeout = int(os.environ.get("PROVISA_MSSQL_LOGIN_TIMEOUT", "120"))
        return pyodbc.connect(
            conn_str, attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=login_timeout
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

    def _existing_columns(self, cur: Any, schema: str, table: str) -> list[str]:
        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        return [str(r[0]) for r in cur.fetchall()]

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

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """Eager reconcile (boot/registration, REQ-1632/REQ-1633): converge the landed table to
        ``columns`` (DDL only, no data), so the catalog is complete at startup and survives
        restart. REQ-1633: MssqlWarehouseRuntime (Fabric/Synapse) had neither this nor
        ``land_table`` before this — one of only two engines (with ClickHouse) implementing none
        of the three landing terminals at all."""
        import asyncio

        del pk_columns  # T-SQL PRIMARY KEY is not a landing concern here — REQ-1651 tracks it
        return await asyncio.to_thread(self._reconcile, source, columns)

    def _reconcile(self, source: Any, columns: list[tuple[str, str]]) -> str:
        _database, schema, table = self._phys_parts(source)
        fq = f"[{schema}].[{table}]"
        want = [name for name, _ in columns]
        cur = self._conn.cursor()
        try:
            self._ensure_schema(cur, schema)
            have = self._existing_columns(cur, schema, table)
            if not have:
                cols_ddl = ", ".join(f"[{n}] {_tsql_type(t)}" for n, t in columns)
                cur.execute(f"CREATE TABLE {fq} ({cols_ddl})")
                outcome = "created"
            elif have == want:
                outcome = "kept"
            else:
                cur.execute(f"DROP TABLE {fq}")
                cols_ddl = ", ".join(f"[{n}] {_tsql_type(t)}" for n, t in columns)
                cur.execute(f"CREATE TABLE {fq} ({cols_ddl})")
                outcome = "recreated"
            self._conn.commit()
            return outcome
        finally:
            cur.close()

    async def land_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """The ``NativeEngineBackend.land_source_table``/``hasattr(runtime, "land_table")`` seam
        every other native engine's runtime uses (REQ-1730) — before this,
        ``MssqlWarehouseRuntime`` had no method by this name (only ``materialize_source``, a
        different signature taking a full ``source`` object), so the seam silently fell through to
        the BASE ``EngineBackend`` default: landing through ``store_writer``'s async path against
        ``self.engine.materialize_store()`` — the shared PLATFORM Postgres by default
        (``_build_mssql_warehouse_engine``'s old ``_platform_db_materialize_default``, also fixed by
        this change), not Fabric/Synapse itself, which has no bridge reading from a separate
        Postgres landing table. Same class of bug as BigQuery's/Databricks'/ClickHouse's own.

        Adapts to ``materialize_source``'s own signature: ``_phys_parts`` (used internally) needs
        only ``schema_name``/``table_name`` on the source object — the catalog is the fixed
        warehouse database (``self._database``), never per-source (module doc), so unlike
        Databricks/ClickHouse this needs no catalog-folding through ``schema`` at all. CDC is not
        a shape ``_land`` implements (REPLACE/APPEND only) — raising loud on CDC rather than
        silently mishandling it."""
        from types import SimpleNamespace

        from provisa.core.change_signal import CDC, select_landing_shape

        del match_floor, pk_columns
        landing_shape = shape or select_landing_shape(change_signal, watermark_column)
        if landing_shape == CDC:
            raise NotImplementedError(
                "Fabric/Synapse native landing has no CDC shape; use replace or append"
            )
        source = SimpleNamespace(schema_name=schema, table_name=table)
        await self.materialize_source(
            source,
            columns,
            rows,
            change_signal=change_signal,
            watermark_column=watermark_column,
        )
        return f"{self._database}.{schema}.{table}"

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
                col_types = dict(columns)
                cur.executemany(
                    f"INSERT INTO {fq} ({collist}) VALUES {ph}",
                    [tuple(_coerce(r.get(c), col_types[c]) for c in colnames) for r in rows],
                )
            self._conn.commit()
        finally:
            cur.close()

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute T-SQL (transpiled by the backend seam) and STREAM it.

        Built on the lazy forward-only-cursor Arrow terminal (``run_arrow_stream``) so the pgwire ENGINE
        route stays memory-bounded — no full ``QueryResult`` materialization (REQ-1217, Defect 3)."""
        schema, batches = self.run_arrow_stream(sql, params)
        return stream_rows_from_arrow(schema, batches)

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async_materialized(self.run_sync, sql, params)

    # -- Arrow transport (built from the ODBC cursor — pyodbc has no native Arrow) --

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        import pyarrow as pa

        schema, batches = self.run_arrow_stream(sql, params)
        return pa.Table.from_batches(list(batches), schema=schema)

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Return ``(schema, batch_generator)`` for lazy record-batch streaming (REQ-1216, REQ-1217).

        pyodbc has no native Arrow, but a forward-only cursor pulls rows from the server incrementally:
        ``fetchmany`` drains one chunk at a time and each chunk is converted to an Arrow batch, so the
        full result never materializes — peak memory is bounded by one chunk. The schema is locked from
        the first chunk and later chunks are coerced to it, keeping a stable schema across the stream.
        The cursor closes when the generator drains or the consumer stops early."""
        import pyarrow as pa

        del params  # SQL arrives fully substituted from the governed pipeline
        cur = self._conn.cursor()
        cur.execute(sql)
        names = [c[0] for c in cur.description] if cur.description else []
        if not names:
            cur.close()
            return pa.table({}).schema, iter(())

        def _chunk_to_table(rows: list, schema: Any) -> Any:
            cols = {name: [row[i] for row in rows] for i, name in enumerate(names)}
            return pa.table(cols, schema=schema) if schema is not None else pa.table(cols)

        first_rows = cur.fetchmany(_ARROW_CHUNK_ROWS)
        if not first_rows:
            cur.close()
            return pa.table({name: [] for name in names}).schema, iter(())
        first_tbl = _chunk_to_table(first_rows, None)
        schema = first_tbl.schema

        def _batches():
            try:
                yield from first_tbl.to_batches()
                while True:
                    rows = cur.fetchmany(_ARROW_CHUNK_ROWS)
                    if not rows:
                        break
                    yield from _chunk_to_table(rows, schema).to_batches()
            finally:
                cur.close()

        return schema, _batches()

    def close(self) -> None:
        self._conn.close()

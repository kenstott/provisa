# Copyright (c) 2026 Kenneth Stott
# Canary: 2f3af646-81e4-4c24-a4d2-7a3b773d034b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a replica's rows are written in a cloud warehouse (REQ-1915, REQ-1912).

The write faces of the warehouses that are their own store: Microsoft Fabric Warehouse,
Snowflake, Databricks and BigQuery. Each fills a build beside the replica through the
warehouse's own bulk path and then replaces the replica's ROWS in one statement or one
transaction, keeping the replica's own table: its key, comments, tags and grants are put there
by the reconcile and the catalog export, and a swap of tables would hand them to the build.

No file is written on this host: a batch is encoded in memory and sent to the warehouse or to
a stage inside it, and what was staged is removed after the load.
"""

# Requirements: REQ-1915, REQ-1912

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa

from provisa.core import request_deadline
from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.federation.data_replicator import TargetCaps, TargetLoad, TargetWrite
from provisa.federation.replica_target import build_table_name

#: The Arrow type a ``numeric`` column is ingested as: Snowflake's NUMBER(38,9).
_SNOWFLAKE_DECIMAL = pa.decimal128(38, 9)
#: The Arrow type a ``numeric`` column is staged as: Delta's DECIMAL(38,9).
_DELTA_DECIMAL = pa.decimal128(38, 9)


class MssqlWarehouseStoreTarget:
    """A replica in a T-SQL warehouse (Microsoft Fabric Warehouse), written by bulk parameter
    arrays over ODBC.

    One connection of the build's own carries it. Each batch is inserted into a build table
    beside the replica and committed. The swap is one transaction that deletes the replica's
    rows and inserts the build table's: the warehouse commits both or neither, so a reader sees
    the previous rows until the commit, and the replica's own table is kept. The build table is
    dropped after the swap.

    ``transactional`` is the engine's declaration that its store commits such a transaction as
    one; a store that does not (Synapse serverless, which is read-only) declares no atomic swap
    and no method builds a replica in it."""

    def __init__(
        self,
        connect: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        transactional: bool,
    ) -> None:
        self._connect = connect
        self._schema = schema
        self._table = table
        self._columns = columns
        self._build = build_table_name(table)
        self._conn: Any = None
        self._begun = False
        self.caps = TargetCaps(
            frozenset({TargetWrite.BULK_BATCH}), atomic_swap=transactional, load=TargetLoad.ROW_COPY
        )

    def _ref(self, table: str) -> str:
        return f"[{self._schema}].[{table}]"

    def _ddl(self, table: str) -> str:
        from provisa.federation.mssql_warehouse_runtime import _tsql_type

        cols = ", ".join(f"[{name}] {_tsql_type(ir_type)}" for name, ir_type in self._columns)
        return f"CREATE TABLE {self._ref(table)} ({cols})"

    def _exists(self, cur: Any, table: str) -> bool:
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (self._schema, table),
        )
        return cur.fetchone() is not None

    def _begin(self) -> None:
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._conn = self._connect()
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", (self._schema,)
            )
            if cur.fetchone() is None:
                cur.execute(f"CREATE SCHEMA [{self._schema}]")
            if self._exists(cur, self._build):
                cur.execute(f"DROP TABLE {self._ref(self._build)}")
            cur.execute(self._ddl(self._build))
            self._conn.commit()
        finally:
            cur.close()
        self._begun = True

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        await asyncio.to_thread(self._begin)

    def _write(self, rows: list[dict]) -> None:
        from provisa.federation.mssql_warehouse_runtime import _coerce

        names = [name for name, _ in self._columns]
        types = dict(self._columns)
        collist = ", ".join(f"[{name}]" for name in names)
        marks = ", ".join("?" * len(names))
        cur = self._conn.cursor()
        try:
            cur.fast_executemany = True
            cur.executemany(
                f"INSERT INTO {self._ref(self._build)} ({collist}) VALUES ({marks})",
                [tuple(_coerce(row.get(name), types[name]) for name in names) for row in rows],
            )
            self._conn.commit()
        finally:
            cur.close()

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del batch  # this face writes rows
        if rows:
            await asyncio.to_thread(self._write, rows)

    def _swap(self) -> None:
        shield = request_deadline.shielded()
        conn = self._conn
        try:
            cur = conn.cursor()
            try:
                if not self._exists(cur, self._table):
                    cur.execute(self._ddl(self._table))
                    conn.commit()
                collist = ", ".join(f"[{name}]" for name, _ in self._columns)
                # One transaction (the connection is not in autocommit): both statements commit
                # together or not at all.
                cur.execute(f"DELETE FROM {self._ref(self._table)}")
                cur.execute(
                    f"INSERT INTO {self._ref(self._table)} ({collist}) "
                    f"SELECT {collist} FROM {self._ref(self._build)}"
                )
                conn.commit()
                cur.execute(f"DROP TABLE {self._ref(self._build)}")
                conn.commit()
                self._begun = False
            finally:
                cur.close()
        finally:
            with shield.lock:
                shield.settle()
                self._conn = None
                conn.close()

    async def swap(self) -> None:
        await asyncio.to_thread(self._swap)

    def _abort(self) -> None:
        shield = request_deadline.shielded()
        conn = self._conn
        if conn is None:
            return
        try:
            conn.rollback()
            if self._begun:
                self._begun = False
                cur = conn.cursor()
                try:
                    if self._exists(cur, self._build):
                        cur.execute(f"DROP TABLE {self._ref(self._build)}")
                    conn.commit()
                finally:
                    cur.close()
        finally:
            with shield.lock:
                shield.settle()
                self._conn = None
                conn.close()

    async def abort(self) -> None:
        await asyncio.to_thread(self._abort)


def _bigquery_arrow_type(ir_type: str) -> pa.DataType:
    """The Arrow type the Storage Write API takes for the BigQuery column of an IR type."""
    from provisa.core.ir_types import to_ir
    from provisa.federation.bigquery_store import bq_type

    by_column: dict[str, pa.DataType] = {
        "INT64": pa.int64(),
        "STRING": pa.string(),
        "BOOL": pa.bool_(),
        "FLOAT64": pa.float64(),
        "NUMERIC": pa.decimal128(38, 9),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "TIME": pa.time64("us"),
        "BYTES": pa.binary(),
        "JSON": pa.string(),
    }
    column = bq_type(ir_type)
    if column not in by_column:
        raise ValueError(f"no Arrow type for BigQuery column type {column!r} ({to_ir(ir_type)})")
    return by_column[column]


def _bigquery_value(value: Any, arrow_type: pa.DataType, *, is_json: bool) -> Any:
    """A row value as its BigQuery column's Arrow type takes it."""
    import datetime
    import json
    from decimal import Decimal

    if value is None:
        return None
    if is_json:
        return value if isinstance(value, str) else json.dumps(value)
    if pa.types.is_decimal(arrow_type):
        return Decimal(str(value))
    if isinstance(value, str):
        if pa.types.is_timestamp(arrow_type):
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if pa.types.is_date(arrow_type):
            return datetime.date.fromisoformat(value)
        if pa.types.is_time(arrow_type):
            return datetime.time.fromisoformat(value)
    elif pa.types.is_string(arrow_type):
        return str(value)  # a UUID
    return value


class BigQueryStoreTarget:
    """A replica in a BigQuery dataset, written as Arrow through the Storage Write API.

    The batches go into one PENDING write stream on a build table beside the replica: nothing
    is visible until the stream is committed, and a stream never committed is discarded by
    BigQuery. The swap is one ``MERGE`` that deletes the replica's rows and inserts the build
    table's, a single statement BigQuery applies atomically: a reader sees the previous rows
    until it commits, and the replica's own table (its key, descriptions and labels) is kept.
    The build table is deleted after the swap."""

    caps = TargetCaps(
        frozenset({TargetWrite.BULK_BATCH}), atomic_swap=True, load=TargetLoad.BULK_STREAM
    )

    def __init__(
        self,
        client: Any,
        *,
        project: str,
        dataset: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        from provisa.core.ir_types import to_ir

        self._client = client
        self._project = project
        self._dataset = dataset
        self._table = table
        self._columns = columns
        self._pk = list(pk_columns)
        self._build = build_table_name(table)
        self._json = frozenset(name for name, ir_type in columns if to_ir(ir_type) == "json")
        self._arrow = pa.schema(
            [(name, _bigquery_arrow_type(ir_type)) for name, ir_type in columns]
        )
        self._writer: Any = None
        self._stream: Any = None
        self._stream_name = ""
        self._begun = False

    def _ref(self, table: str) -> str:
        return f"`{self._project}.{self._dataset}.{table}`"

    def _begin(self) -> None:
        from google.cloud import bigquery, bigquery_storage_v1
        from google.cloud.bigquery_storage_v1 import types, writer

        from provisa.federation.bigquery_store import create_ddl

        self._client.create_dataset(
            bigquery.Dataset(f"{self._project}.{self._dataset}"), exists_ok=True
        )
        self._client.query(f"DROP TABLE IF EXISTS {self._ref(self._build)}").result()
        self._client.query(
            create_ddl((self._project, self._dataset, self._build), self._columns, None)
        ).result()
        self._begun = True
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._writer = bigquery_storage_v1.BigQueryWriteClient(
                credentials=self._client._credentials
            )
        parent = self._writer.table_path(self._project, self._dataset, self._build)
        pending = self._writer.create_write_stream(
            parent=parent, write_stream=types.WriteStream(type_=types.WriteStream.Type.PENDING)
        )
        self._stream_name = pending.name
        template = types.AppendRowsRequest()
        template.write_stream = pending.name
        arrow_rows = types.AppendRowsRequest.ArrowData()
        arrow_rows.writer_schema.serialized_schema = self._arrow.serialize().to_pybytes()
        template.arrow_rows = arrow_rows
        self._stream = writer.AppendRowsStream(self._writer, template)

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._dataset, self._table, action="build the replica at")
        await asyncio.to_thread(self._begin)

    def _write(self, rows: list[dict]) -> None:
        from google.cloud.bigquery_storage_v1 import types

        arrays = [
            pa.array(
                [
                    _bigquery_value(
                        row.get(field.name), field.type, is_json=field.name in self._json
                    )
                    for row in rows
                ],
                type=field.type,
            )
            for field in self._arrow
        ]
        batch = pa.RecordBatch.from_arrays(arrays, schema=self._arrow)
        request = types.AppendRowsRequest()
        arrow_rows = types.AppendRowsRequest.ArrowData()
        arrow_rows.rows.serialized_record_batch = batch.serialize().to_pybytes()
        request.arrow_rows = arrow_rows
        self._stream.send(request).result()  # raises what the service refused

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del batch
        if rows:
            await asyncio.to_thread(self._write, rows)

    def _close_writer(self) -> None:
        stream, self._stream = self._stream, None
        writer_client, self._writer = self._writer, None
        if stream is not None:
            stream.close()
        if writer_client is not None:
            writer_client.transport.close()

    def _swap(self) -> None:
        from google.cloud.bigquery_storage_v1 import types

        from provisa.federation.bigquery_store import create_ddl

        shield = request_deadline.shielded()
        try:
            self._stream.close()
            self._stream = None
            self._writer.finalize_write_stream(name=self._stream_name)
            commit = types.BatchCommitWriteStreamsRequest()
            commit.parent = self._writer.table_path(self._project, self._dataset, self._build)
            commit.write_streams = [self._stream_name]
            committed = self._writer.batch_commit_write_streams(commit)
            if committed.stream_errors:
                raise RuntimeError(
                    f"BigQuery refused the build of {self._ref(self._table)}: "
                    + "; ".join(str(error.error_message) for error in committed.stream_errors)
                )
            self._client.query(
                create_ddl((self._project, self._dataset, self._table), self._columns, self._pk)
            ).result()
            names = ", ".join(f"`{name}`" for name, _ in self._columns)
            self._client.query(
                f"MERGE {self._ref(self._table)} AS replica USING {self._ref(self._build)} AS build "
                "ON FALSE WHEN NOT MATCHED BY SOURCE THEN DELETE "
                f"WHEN NOT MATCHED BY TARGET THEN INSERT ({names}) VALUES ({names})"
            ).result()
            self._client.query(f"DROP TABLE IF EXISTS {self._ref(self._build)}").result()
            self._begun = False
        finally:
            with shield.lock:
                shield.settle()
                self._close_writer()

    async def swap(self) -> None:
        await asyncio.to_thread(self._swap)

    def _abort(self) -> None:
        shield = request_deadline.shielded()
        try:
            if self._begun:
                self._begun = False
                # The uncommitted stream goes with its table.
                self._client.query(f"DROP TABLE IF EXISTS {self._ref(self._build)}").result()
        finally:
            with shield.lock:
                shield.settle()
                self._close_writer()

    async def abort(self) -> None:
        await asyncio.to_thread(self._abort)


#: The Unity Catalog volume, in the replicas schema, that holds a build's staged batches.
DATABRICKS_BUILD_VOLUME = "provisa_replica_builds"


class DatabricksStoreTarget:
    """A replica in a Unity Catalog Delta table, written from Parquet staged in a volume.

    The build has no build table: its staging directory in the replicas schema's volume is the
    build. Each batch is encoded to Parquet in memory and uploaded there as a stream, never to a
    file on this host. The swap is one ``INSERT OVERWRITE`` of the replica from every staged
    file, a single Delta commit: a reader sees the previous rows until it lands, and the
    replica's own table (its key, comments, tags and grants) is kept. The staged files are
    removed after the swap, or on abort; files left by a build that died are removed by the
    next build of the same replica."""

    caps = TargetCaps(
        frozenset({TargetWrite.BULK_BATCH}), atomic_swap=True, load=TargetLoad.BULK_STREAM
    )

    def __init__(
        self,
        connect: Any,
        *,
        catalog: str,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        import secrets

        self._connect = connect
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._columns = columns
        self._pk = list(pk_columns)
        self._arrow = arrow_schema(columns, decimal=_DELTA_DECIMAL)
        self._root = (
            f"/Volumes/{catalog}/{schema}/{DATABRICKS_BUILD_VOLUME}/{build_table_name(table)}"
        )
        self._dir = f"{self._root}/{secrets.token_hex(8)}"
        self._conn: Any = None
        self._staged: list[str] = []

    def _execute(self, sql: str, **kwargs: Any) -> list:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, **kwargs)
            return cur.fetchall() if cur.description else []
        finally:
            cur.close()

    def _remove_under(self, directory: str) -> None:
        """Remove every file under ``directory`` of the volume (one level of build directories,
        each holding only files)."""
        for entry in self._execute(f"LIST '{directory}'"):
            path, name = str(entry[0]), str(entry[1])
            if name.endswith("/"):
                self._remove_under(path.rstrip("/"))
            else:
                self._execute(f"REMOVE '{path}'")

    def _begin(self) -> None:
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._conn = self._connect()
        self._execute(f"CREATE SCHEMA IF NOT EXISTS `{self._catalog}`.`{self._schema}`")
        self._execute(
            f"CREATE VOLUME IF NOT EXISTS "
            f"`{self._catalog}`.`{self._schema}`.`{DATABRICKS_BUILD_VOLUME}`"
        )
        volume_root = f"/Volumes/{self._catalog}/{self._schema}/{DATABRICKS_BUILD_VOLUME}"
        builds = {str(entry[1]).rstrip("/") for entry in self._execute(f"LIST '{volume_root}'")}
        if build_table_name(self._table) in builds:
            self._remove_under(self._root)  # left by a build of this replica that died

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        await asyncio.to_thread(self._begin)

    def _write(self, rows: list[dict]) -> None:
        import io

        import pyarrow.parquet as pq

        batch = rows_to_batch(rows, self._columns, self._arrow, decimal=_DELTA_DECIMAL)
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_batches([batch]), buffer)
        buffer.seek(0)
        path = f"{self._dir}/part-{len(self._staged):08d}.parquet"
        self._execute(f"PUT '__input_stream__' INTO '{path}' OVERWRITE", input_stream=buffer)
        self._staged.append(path)

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del batch
        if rows:
            await asyncio.to_thread(self._write, rows)

    def _unstage(self) -> None:
        staged, self._staged = self._staged, []
        for path in staged:
            self._execute(f"REMOVE '{path}'")

    def _swap(self) -> None:
        from provisa.federation.databricks_store import _create_ddl, _ddl_type, _qualified

        shield = request_deadline.shielded()
        try:
            replica = _qualified(self._catalog, self._schema, self._table)
            self._execute(
                _create_ddl(self._catalog, self._schema, self._table, self._columns, self._pk)
            )
            if self._staged:
                names = ", ".join(f"`{name}`" for name, _ in self._columns)
                projection = ", ".join(
                    f"CAST(`{name}` AS {_ddl_type(ir_type)})" for name, ir_type in self._columns
                )
                self._execute(
                    f"INSERT OVERWRITE {replica} ({names}) SELECT {projection} "
                    f"FROM read_files('{self._dir}/', format => 'parquet')"
                )
                self._unstage()
            else:
                self._execute(f"TRUNCATE TABLE {replica}")  # the source has no rows
        finally:
            with shield.lock:
                shield.settle()
                conn, self._conn = self._conn, None
                conn.close()

    async def swap(self) -> None:
        await asyncio.to_thread(self._swap)

    def _abort(self) -> None:
        shield = request_deadline.shielded()
        conn = self._conn
        if conn is None:
            return
        try:
            self._unstage()
        finally:
            with shield.lock:
                shield.settle()
                self._conn = None
                conn.close()

    async def abort(self) -> None:
        await asyncio.to_thread(self._abort)


def snowflake_adbc_connect(url: str, database: str) -> Any:
    """An ADBC connection to ``database`` in the Snowflake account of the engine URL
    (``snowflake://user:pass@account/db/schema?warehouse=WH&role=ROLE``), each statement its own
    transaction. ADBC is the Arrow write path: a batch is encoded to Parquet in memory and sent
    to a stage of the account, never to a file on this host."""
    from urllib.parse import parse_qs, unquote, urlparse

    import adbc_driver_snowflake.dbapi as adbc

    u = urlparse(url)
    if not u.hostname or not u.username:
        raise ValueError("snowflake engine URL requires account host and user")
    query = parse_qs(u.query)
    kwargs = {
        "username": unquote(u.username),
        "password": unquote(u.password) if u.password else "",
        "adbc.snowflake.sql.account": u.hostname,
        "adbc.snowflake.sql.db": database,
    }
    for option in ("warehouse", "role"):
        if option in query:
            kwargs[f"adbc.snowflake.sql.{option}"] = query[option][0]
    return adbc.connect(db_kwargs=kwargs, autocommit=True)


class SnowflakeStoreTarget:
    """A replica in a Snowflake database, written by ADBC Arrow ingest.

    Each batch is ingested into a build table beside the replica: the driver encodes it to
    Parquet in memory, sends it to a stage of the account and copies it in. The swap is one
    ``INSERT OVERWRITE`` of the replica from the build table, which Snowflake applies as a
    single statement: a reader sees the previous rows until it commits. The replica's own table
    is kept, so its key, comments, tags and grants stay on it — a table swap
    (``ALTER TABLE ... SWAP WITH``) hands those to the build table and was measured to leave the
    replica's name without them. The build table is dropped after the swap."""

    caps = TargetCaps(
        frozenset({TargetWrite.BULK_BATCH}), atomic_swap=True, load=TargetLoad.BULK_STREAM
    )

    def __init__(
        self,
        connect: Any,
        *,
        database: str,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        self._connect = connect
        self._database = database
        self._schema = schema
        self._table = table
        self._columns = columns
        self._pk = tuple(pk_columns)
        self._build = build_table_name(table)
        self._arrow = arrow_schema(columns, decimal=_SNOWFLAKE_DECIMAL)
        self._conn: Any = None
        self._ingested = False

    def _execute(self, sql: str) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def _begin(self) -> None:
        from provisa.federation.snowflake_store import qualified

        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._conn = self._connect()
        self._execute(f'CREATE SCHEMA IF NOT EXISTS "{self._database}"."{self._schema}"')
        # The driver's ingest names only the table: it lands in the session's schema.
        self._execute(f'USE SCHEMA "{self._database}"."{self._schema}"')
        self._execute(
            f"DROP TABLE IF EXISTS {qualified((self._database, self._schema, self._build))}"
        )

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        await asyncio.to_thread(self._begin)

    def _write(self, rows: list[dict]) -> None:
        # The source's batch is rebuilt to one schema per IR type, so the build table has the
        # same column types whichever driver produced the rows.
        batch = rows_to_batch(rows, self._columns, self._arrow, decimal=_SNOWFLAKE_DECIMAL)
        cur = self._conn.cursor()
        try:
            cur.adbc_ingest(
                self._build,
                batch,
                mode="append" if self._ingested else "create",
            )
        finally:
            cur.close()
        self._ingested = True

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del batch
        if rows:
            await asyncio.to_thread(self._write, rows)

    def _swap(self) -> None:
        from provisa.core.ir_types import to_ir
        from provisa.federation.snowflake_store import create_ddl, ddl_type, qualified

        shield = request_deadline.shielded()
        try:
            replica_parts = (self._database, self._schema, self._table)
            replica = qualified(replica_parts)
            build = qualified((self._database, self._schema, self._build))
            self._execute(create_ddl(replica_parts, self._columns, self._pk))
            if self._ingested:
                names = ", ".join(f'"{name}"' for name, _ in self._columns)
                projection = ", ".join(
                    f'PARSE_JSON("{name}")'
                    if to_ir(ir_type) == "json"
                    else f'CAST("{name}" AS {ddl_type(ir_type)})'
                    for name, ir_type in self._columns
                )
                self._execute(
                    f"INSERT OVERWRITE INTO {replica} ({names}) SELECT {projection} FROM {build}"
                )
                self._execute(f"DROP TABLE IF EXISTS {build}")
                self._ingested = False
            else:
                self._execute(f"DELETE FROM {replica}")  # the source has no rows
        finally:
            with shield.lock:
                shield.settle()
                conn, self._conn = self._conn, None
                conn.close()

    async def swap(self) -> None:
        await asyncio.to_thread(self._swap)

    def _abort(self) -> None:
        from provisa.federation.snowflake_store import qualified

        shield = request_deadline.shielded()
        conn = self._conn
        if conn is None:
            return
        try:
            if self._ingested:
                self._ingested = False
                self._execute(
                    f"DROP TABLE IF EXISTS {qualified((self._database, self._schema, self._build))}"
                )
        finally:
            with shield.lock:
                shield.settle()
                self._conn = None
                conn.close()

    async def abort(self) -> None:
        await asyncio.to_thread(self._abort)

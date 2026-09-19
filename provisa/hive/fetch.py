# Copyright (c) 2026 Kenneth Stott
# Canary: 6d3a8f52-1c94-47e0-b6a3-5f9d2c7b408e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hive-on-S3 (hive_s3) read directly off S3 storage, engine-independently (REQ-1730).

Trino's own connector (TrinoHiveS3Connector, trino_connectors.py) reads hive_s3 through a real
Hive Metastore Thrift client (table location, partitions, storage format) — no equivalent client
exists anywhere in this codebase or its dependencies (confirmed: no pyhive/hmsclient/thrift-
generated Hive Metastore stubs), and generating one is out of scope for this fix. This module
instead reads the table's data files DIRECTLY off S3 by Hive's own well-known unpartitioned-
external-table layout convention — ``<warehouse_path>/<schema>.db/<table>/*.parquet`` — using
DuckDB's own httpfs extension for the actual Parquet read (schema discovery via ``DESCRIBE``, a
live query with no separate metadata call needed, the same "one query answers both" shape
provisa.pinot.fetch uses) and boto3 for the one thing DuckDB's read_parquet glob cannot do:
listing which "directories" (Hive's flat-file convention for tables) exist under a schema prefix.

This is a genuine, documented narrowing versus a full Hive Metastore reader: a table with a
NON-default location, multiple storage formats, or Hive-style partitioning is not read correctly
by this reader. It is exactly what this project's own demo/test fixtures for hive_s3 produce
(a single unpartitioned Parquet table under the conventional path), and what a metastore-registered
external table commonly looks like for a project's own governed writes (REQ-1097's own hive_s3
write path already assumes non-managed/external tables at this same convention). A future Hive
Metastore Thrift client (see this session's own REQ-1730 report on `hive` — plain Hadoop-local
storage has no host-reachable path at all today, a strictly larger gap) would let both `hive` and
`hive_s3` drop this convention-based approach for a fully general one."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
from botocore.client import Config


@dataclass(frozen=True)
class HiveS3Connection:
    warehouse_path: str  # e.g. "s3a://bucket/warehouse"
    endpoint: str
    access_key: str
    secret_key: str
    region: str
    path_style: bool = True

    @classmethod
    def build(cls, database: str | None, mapping: dict) -> "HiveS3Connection":
        if not database:
            raise ValueError("hive_s3 source has no Warehouse Path (Source.database)")
        endpoint = mapping.get("s3_endpoint") or mapping.get("endpoint")
        access = mapping.get("access_key_id") or mapping.get("aws_access_key_id")
        secret = mapping.get("secret_access_key") or mapping.get("aws_secret_access_key")
        region = mapping.get("region") or mapping.get("s3_region")
        if not (endpoint and access and secret and region):
            raise ValueError(
                "hive_s3 source requires s3 endpoint, access_key_id, secret_access_key and "
                "region in its mapping"
            )
        return cls(
            warehouse_path=database,
            endpoint=str(endpoint),
            access_key=str(access),
            secret_key=str(secret),
            region=str(region),
            path_style=mapping.get("path_style") is not False,
        )

    def _bucket_and_prefix(self) -> tuple[str, str]:
        parsed = urlparse(self.warehouse_path)
        return parsed.netloc, parsed.path.lstrip("/")

    def _s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path" if self.path_style else "virtual"},
            ),
        )

    def table_files(self, schema: str, table: str) -> list[str]:
        """Every real data-file object (``s3://`` URLs, DuckDB's own httpfs scheme) under this
        table's conventional Hive directory — an explicit listing, not a glob: Trino's own hive
        connector writes Parquet files with NO ``.parquet`` extension (a bare query-id name), and
        the directory itself is a real zero-byte S3 object (a "folder" marker) that a `*` glob
        would also match and DuckDB's read_parquet would then fail to parse. Confirmed live: both
        of these are exactly what a real `CREATE TABLE ... WITH (format='PARQUET')` + `INSERT`
        through Trino produces against this project's own hive_s3 fixture."""
        bucket, prefix = self._bucket_and_prefix()
        table_prefix = f"{prefix.rstrip('/')}/{schema}.db/{table}/"
        s3 = self._s3_client()
        files: list[str] = []
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=table_prefix
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or obj.get("Size", 0) == 0:
                    continue
                files.append(f"s3://{bucket}/{key}")
        return files


def list_schemas(conn: HiveS3Connection) -> list[str]:
    """Every "<name>.db" directory directly under the warehouse root — Hive's own convention for
    a database/schema — with the ``.db`` suffix stripped, sorted."""
    bucket, prefix = conn._bucket_and_prefix()
    root_prefix = f"{prefix.rstrip('/')}/" if prefix else ""
    s3 = conn._s3_client()
    schemas: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=root_prefix, Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            name = common["Prefix"][len(root_prefix) :].rstrip("/")
            if name.endswith(".db"):
                schemas.add(name[: -len(".db")])
    return sorted(schemas)


def list_tables(conn: HiveS3Connection, schema: str) -> list[str]:
    """Every "directory" (Hive's own flat-file convention for a table) under the schema's
    warehouse prefix — an S3 ``ListObjectsV2`` with a ``/`` delimiter, sorted."""
    bucket, prefix = conn._bucket_and_prefix()
    schema_prefix = f"{prefix.rstrip('/')}/{schema}.db/"
    s3 = conn._s3_client()
    tables: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=schema_prefix, Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            name = common["Prefix"][len(schema_prefix) :].rstrip("/")
            if name:
                tables.add(name)
    return sorted(tables)


def _duckdb_s3_conn(conn: HiveS3Connection):
    """A scratch in-memory DuckDB connection with S3 access wired via ``CREATE SECRET`` — DuckDB
    1.x's own recommended mechanism; the older global ``SET s3_*`` variables were tried first and
    confirmed live NOT to take effect against a real MinIO endpoint (every read came back
    ``SignatureDoesNotMatch``/region "auto" regardless of the region actually SET)."""
    import duckdb

    def _q(s: str) -> str:
        return s.replace("'", "''")

    c = duckdb.connect(":memory:")
    c.execute("INSTALL httpfs")
    c.execute("LOAD httpfs")
    endpoint = conn.endpoint.split("://", 1)[-1]
    use_ssl = conn.endpoint.startswith("https://")
    c.execute(
        "CREATE SECRET ("
        "TYPE s3, "
        f"KEY_ID '{_q(conn.access_key)}', "
        f"SECRET '{_q(conn.secret_key)}', "
        f"ENDPOINT '{_q(endpoint)}', "
        f"REGION '{_q(conn.region)}', "
        f"URL_STYLE '{'path' if conn.path_style else 'vhost'}', "
        f"USE_SSL {'true' if use_ssl else 'false'}"
        ")"
    )
    return c


def _file_list_literal(files: list[str]) -> str:
    return "[" + ", ".join(f"'{f}'" for f in files) + "]"


def table_columns(conn: HiveS3Connection, schema: str, table: str) -> list[dict]:
    """``[{"name", "type"}]`` for `table`, from DuckDB's own DESCRIBE over a live read_parquet —
    no separate metadata call, the same "one query answers both" shape as provisa.pinot.fetch."""
    files = conn.table_files(schema, table)
    if not files:
        return []
    c = _duckdb_s3_conn(conn)
    try:
        rows = c.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_file_list_literal(files)})"
        ).fetchall()
        return [{"name": r[0], "type": r[1]} for r in rows]
    finally:
        c.close()


def fetch_rows(conn: HiveS3Connection, schema: str, table: str, columns: list[str]) -> list[dict]:
    """Every current row of `table`'s given columns (or every column when none are given)."""
    files = conn.table_files(schema, table)
    if not files:
        return []
    c = _duckdb_s3_conn(conn)
    try:
        select = ", ".join(f'"{col}"' for col in columns) if columns else "*"
        cur = c.execute(f"SELECT {select} FROM read_parquet({_file_list_literal(files)})")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]
    finally:
        c.close()

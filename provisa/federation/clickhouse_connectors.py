# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""ClickHouse connector definitions (REQ-909).

ClickHouse attaches external sources via native integration engines:
- Relational sources (PG/MySQL) mount as a DATABASE engine (auto-exposes every remote table).
- File sources (csv/parquet) and MongoDB mount as a per-table TABLE engine.
- Lakehouse sources (Iceberg/DeltaLake) mount via native lakehouse table engines (zero-copy).

All ATTACH_RW — referenced in place, nothing lands. ClickHouse is its own native store.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source

from provisa.federation.connector_base import Capability, Connector, Mechanism


class ClickHousePostgresConnector(Connector):
    """Mount a remote PostgreSQL source into ClickHouse via the PostgreSQL database engine.

    ``CREATE DATABASE ... ENGINE = PostgreSQL(...)`` exposes every remote table under a local
    database — the CREATE DATABASE analog of postgres_fdw's IMPORT FOREIGN SCHEMA. ClickHouse pushes
    WHERE predicates to PostgreSQL and can INSERT back through the engine.
    """

    engine = "clickhouse"
    source_type = "postgresql"
    materialized_store = True  # REQ-846: PG is the one proven materialized store today
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_postgres"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        local_schema = f"ch_{source.id}"
        # Remote schema override rides on federation_hints (Source has no `schema` field — and
        # ``source.schema`` would resolve to pydantic's BaseModel.schema method, never the default).
        remote_schema = source.federation_hints.get("schema") or "public"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" ENGINE = PostgreSQL('
                f"'{source.host}:{source.port}', '{source.database}', "
                f"'{source.username}', '{source.password}', '{remote_schema}')"
            ],
            "local_schema": local_schema,
        }


class ClickHouseMysqlConnector(Connector):
    """Mount a remote MySQL/MariaDB source into ClickHouse via the MySQL database engine.

    Same CREATE DATABASE shape as the PostgreSQL engine — every remote table is exposed under a
    local database. ClickHouse pushes predicates to MySQL and can INSERT back through the engine.
    """

    engine = "clickhouse"
    source_type = "mysql"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_mysql"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, write=True)

    def details(self, source: Source) -> dict:
        local_schema = f"ch_{source.id}"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" ENGINE = MySQL('
                f"'{source.host}:{source.port}', '{source.database}', "
                f"'{source.username}', '{source.password}')"
            ],
            "local_schema": local_schema,
        }


class ClickHouseSqliteConnector(Connector):  # REQ-1178
    """Mount a SQLite database file into ClickHouse via the SQLite database engine.

    ``CREATE DATABASE ... ENGINE = SQLite('<path>')`` exposes every table of a local SQLite file under
    a local database — the CREATE DATABASE auto-expose shape, like PostgreSQL/MySQL but over a file, so
    no server. ClickHouse parity with the SQLite reach the pg (sqlite_fdw) and duckdb (sqlite) engines
    already offer."""

    engine = "clickhouse"
    source_type = "sqlite"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_sqlite"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        if source.path is None:
            raise ValueError(f"sqlite source {source.id!r} has no path")
        local_schema = f"ch_{source.id}"
        return {
            "attach_ddl": [
                f'CREATE DATABASE IF NOT EXISTS "{local_schema}" ENGINE = SQLite(\'{source.path}\')'
            ],
            "local_schema": local_schema,
        }


class ClickHouseMongoConnector(Connector):
    """Mount a MongoDB collection into ClickHouse via the MongoDB table engine.

    MongoDB is a per-table engine (one collection per table) and cannot infer its schema, so the
    per-table ``CREATE TABLE`` column list is completed by the runtime from registry metadata; the
    ``engine_clause`` carries a ``{table}`` placeholder the runtime binds to the collection name.
    ClickHouse pushes simple predicates down to MongoDB.
    """

    engine = "clickhouse"
    source_type = "mongodb"
    mechanism = Mechanism.ATTACH_RW
    key = "clickhouse_mongo"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)

    def details(self, source: Source) -> dict:
        return {
            "engine_clause": (
                f"MongoDB('{source.host}:{source.port}', '{source.database}', "
                f"'{{table}}', '{source.username}', '{source.password}')"
            ),
            "requires_columns": True,
        }


def _clickhouse_s3_creds(source: Source) -> tuple[str | None, str | None]:
    """S3/S3-compatible (R2) access key + secret from federation_hints — accepts the ClickHouse
    ``aws_key``/``aws_secret`` or the generic ``access_key_id``/``secret_access_key`` spelling."""
    h = source.federation_hints
    return (
        h.get("aws_key") or h.get("access_key_id"),
        h.get("aws_secret") or h.get("secret_access_key"),
    )


def _clickhouse_file_engine(source: Source, fmt: str) -> str:
    """The ClickHouse table-engine clause for a file source, chosen by the path scheme: ``s3://`` (or
    an https S3-compatible endpoint carrying credentials — e.g. Cloudflare R2) → S3, a plain
    ``http(s)://`` → URL, otherwise a local File. Credentials, when the bucket is private, ride on
    federation_hints; absent means a public bucket."""
    path = source.path
    if path is None:
        raise ValueError(f"file source {source.id!r} has no path")
    key, secret = _clickhouse_s3_creds(source)
    if path.startswith("s3://") or (path.startswith("https://") and key):
        creds = f", '{key}', '{secret}'" if key else ""
        return f"S3('{path}'{creds}, '{fmt}')"
    if path.startswith(("http://", "https://")):
        return f"URL('{path}', '{fmt}')"
    # The File table engine is format-FIRST (unlike S3/URL, which are url-first).
    return f"File('{fmt}', '{path}')"


def _clickhouse_lake_engine(source: Source, engine: str) -> str:
    """A ClickHouse lakehouse table-engine clause (``IcebergS3`` / ``DeltaLake``) over an object-store
    URL + optional S3-compatible credentials. Reads the table's metadata + data in place — zero copy."""
    path = source.path
    if path is None:
        raise ValueError(f"lake source {source.id!r} has no path (object-store URL)")
    key, secret = _clickhouse_s3_creds(source)
    creds = f", '{key}', '{secret}'" if key else ""
    return f"{engine}('{path}'{creds})"


class _ClickHouseFileConnector(Connector):
    """Mount a file source into ClickHouse via an S3/URL/File table engine (chosen by path scheme).

    ClickHouse infers the column schema for these engines, so the runtime issues a bare
    ``CREATE TABLE ... ENGINE = <clause>`` with no column list. The data is read in place. ``validate``
    tells the runtime to probe the attached table (read one row) so bad credentials / an unreachable
    object fail loud at attach time, not at first query."""

    engine = "clickhouse"
    mechanism = (
        Mechanism.SCAN
    )  # S3/URL/File table engine reads the file in place — no copy (REQ-951)
    _format = ""  # ClickHouse input-format name

    def details(self, source: Source) -> dict:
        return {
            "engine_clause": _clickhouse_file_engine(source, self._format),
            "infer": True,
            "validate": True,
        }


class ClickHouseCsvConnector(_ClickHouseFileConnector):
    source_type = "csv"
    key = "clickhouse_csv"
    _format = "CSVWithNames"

    def capability(self) -> Capability:
        return Capability()  # CSV scan: no predicate pushdown


class ClickHouseParquetConnector(_ClickHouseFileConnector):
    source_type = "parquet"
    key = "clickhouse_parquet"
    _format = "Parquet"

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)  # column + row-group pruning


class _ClickHouseLakeConnector(Connector):
    """Mount a lakehouse table (Iceberg / Delta Lake) on object storage into ClickHouse via its native
    lakehouse table engine (``IcebergS3`` / ``DeltaLake``). Read in place (zero copy); ``validate``
    probes the attach so a bad credential / unreachable table fails loud at attach time."""

    engine = "clickhouse"
    mechanism = (
        Mechanism.SCAN
    )  # IcebergS3/DeltaLake engine reads the table in place — no copy (REQ-951)
    _engine_name = ""  # ClickHouse lakehouse table engine

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True)  # lakehouse manifest/stat pruning

    def details(self, source: Source) -> dict:
        return {
            "engine_clause": _clickhouse_lake_engine(source, self._engine_name),
            "infer": True,
            "validate": True,
        }


class ClickHouseIcebergConnector(_ClickHouseLakeConnector):
    source_type = "iceberg"
    key = "clickhouse_iceberg"
    _engine_name = "IcebergS3"


class ClickHouseDeltaLakeConnector(_ClickHouseLakeConnector):
    source_type = "delta_lake"
    key = "clickhouse_delta"
    _engine_name = "DeltaLake"


class ClickHouseHudiConnector(_ClickHouseLakeConnector):  # REQ-1178
    source_type = "hudi"
    key = "clickhouse_hudi"
    _engine_name = "Hudi"

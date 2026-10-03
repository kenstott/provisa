# Copyright (c) 2026 Kenneth Stott
# Canary: b5a41cd1-066c-4bc9-a9a4-d2b228228b8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An engine writes a statement's result straight to the results object store (REQ-1194).

Each engine that can reach an object store has its own statement for "run this query and put
the result there"; the rows never pass through Provisa. This module holds what is the same for
all of them -- where a result goes, and the statement each engine runs -- so a backend's
``ctas_redirect`` is: build the statement, run it with the statement's bound values, return
where the result is and how many rows it holds.

The address is the one the presigner reads (``executor.redirect.presign_ctas_result``):
``s3a://<bucket>/results/<id>``, the result's objects under that prefix.
"""

# Requirements: REQ-1194

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


class ResultFormatNotWritten(ValueError):
    """The engine does not write this format to an object store itself."""

    def __init__(self, engine: str, output_format: str, written: frozenset[str]) -> None:
        self.engine, self.output_format = engine, output_format
        super().__init__(
            f"engine {engine!r} does not write {output_format!r} results to an object store "
            f"(it writes: {', '.join(sorted(written))})"
        )


@dataclass(frozen=True)
class ResultTarget:
    """Where one statement's result is written: its own prefix in the results bucket."""

    bucket: str
    result_id: str

    @property
    def key_prefix(self) -> str:
        return f"results/{self.result_id}"

    @property
    def s3_prefix(self) -> str:
        """The address the presigner reads (the ``s3a`` spelling every engine returns)."""
        return f"s3a://{self.bucket}/{self.key_prefix}"

    @property
    def object_url(self) -> str:
        """The one object a single-file writer puts the result in."""
        return f"s3://{self.bucket}/{self.key_prefix}/data.parquet"

    @property
    def directory_url(self) -> str:
        """The prefix a writer that names its own files writes under."""
        return f"s3://{self.bucket}/{self.key_prefix}/"


def new_target(config: Any) -> ResultTarget:
    """A fresh prefix in the deployment's results bucket (``RedirectConfig``)."""
    return ResultTarget(bucket=config.bucket, result_id=uuid.uuid4().hex[:16])


def require_format(engine: str, output_format: str, written: frozenset[str]) -> None:
    if output_format.lower() not in written:
        raise ResultFormatNotWritten(engine, output_format, written)


def _quoted(text: str) -> str:
    """``text`` as a single-quoted SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


# -- DuckDB ------------------------------------------------------------------------------------------

DUCKDB_SECRET = "provisa_results"


def duckdb_secret(config: Any) -> str:
    """The S3 secret DuckDB's httpfs writes the results bucket with, scoped to its results
    prefix so it is never used for a source's own bucket. An endpoint that is not AWS's (MinIO,
    R2) is addressed path-style at the host given."""
    parts = [
        "TYPE S3",
        f"KEY_ID {_quoted(config.access_key)}",
        f"SECRET {_quoted(config.secret_key)}",
        f"REGION {_quoted(config.region)}",
        f"SCOPE {_quoted(f's3://{config.bucket}/results/')}",
    ]
    if config.endpoint_url:
        endpoint = urlparse(config.endpoint_url)
        parts += [
            f"ENDPOINT {_quoted(endpoint.netloc)}",
            "URL_STYLE 'path'",
            f"USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}",
        ]
    return f"CREATE OR REPLACE SECRET {DUCKDB_SECRET} ({', '.join(parts)})"


def duckdb_copy(select_sql: str, target: ResultTarget) -> str:
    """``COPY (query) TO``: DuckDB runs the query and writes one Parquet object. The statement
    keeps the query's placeholders, so it is executed with the query's bound values; its one
    result row is the number of rows written."""
    return f"COPY ({select_sql}) TO {_quoted(target.object_url)} (FORMAT PARQUET)"


# -- ClickHouse --------------------------------------------------------------------------------------

#: The ``s3`` table function's address and keys, sent as server-side parameters: ClickHouse keeps
#: a statement's text in its query log, and the text holds none of them.
_CH_S3 = (
    "s3({provisa_results_url:String}, {provisa_results_key:String}, "
    "{provisa_results_secret:String}, 'Parquet')"
)


def _clickhouse_s3(target: ResultTarget, config: Any) -> dict[str, str]:
    if config.endpoint_url:
        base = config.endpoint_url.rstrip("/")
    else:
        base = f"https://s3.{config.region}.amazonaws.com"
    return {
        "provisa_results_url": f"{base}/{target.bucket}/{target.key_prefix}/data.parquet",
        "provisa_results_key": config.access_key,
        "provisa_results_secret": config.secret_key,
    }


def clickhouse_insert(
    select_sql: str, target: ResultTarget, config: Any
) -> tuple[str, dict[str, str]]:
    """``INSERT INTO FUNCTION s3(...)``: ClickHouse runs the query and writes one Parquet object.
    Returns the statement and the parameters that address the object and carry the keys."""
    return f"INSERT INTO FUNCTION {_CH_S3} {select_sql}", _clickhouse_s3(target, config)


def clickhouse_count(target: ResultTarget, config: Any) -> tuple[str, dict[str, str]]:
    """The rows the written object holds, read back from its own footer."""
    return f"SELECT count() FROM {_CH_S3}", _clickhouse_s3(target, config)


# -- Snowflake ---------------------------------------------------------------------------------------


class ResultStoreNotGranted(ValueError):
    """The engine reaches the results store only through a grant the deployment has not named."""


def _identifier(name: str) -> str:
    """``name`` as a double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def snowflake_copy(select_sql: str, target: ResultTarget, config: Any) -> str:
    """``COPY INTO <location> FROM (query)``: Snowflake unloads the query's result as Parquet
    under the prefix, with the column names kept (``HEADER``). Its result row carries
    ``rows_unloaded``. Snowflake reaches the bucket through the storage integration the
    deployment names (``redirect.snowflake_storage_integration``), so no credential is in the
    statement, which Snowflake keeps in its query history. It writes to AWS S3; it cannot reach a
    private S3-compatible endpoint."""
    integration = config.snowflake_storage_integration
    if not integration:
        raise ResultStoreNotGranted(
            "Snowflake writes results to the object store only through a storage integration; "
            "none is named (redirect.snowflake_storage_integration)"
        )
    return (
        f"COPY INTO {_quoted(target.directory_url)} FROM ({select_sql}) "
        f"STORAGE_INTEGRATION = {_identifier(integration)} "
        "FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE"
    )


# -- Databricks --------------------------------------------------------------------------------------


def databricks_insert(select_sql: str, target: ResultTarget) -> str:
    """``INSERT OVERWRITE DIRECTORY``: Databricks writes the query's result as Parquet files
    under the prefix. The workspace reaches the bucket by its own grant (an external location or
    an instance profile); no credential is passed in the statement."""
    return f"INSERT OVERWRITE DIRECTORY {_quoted(target.directory_url)} USING PARQUET {select_sql}"


def databricks_count(target: ResultTarget) -> str:
    """The rows the written files hold, read back from the directory."""
    return f"SELECT count(*) FROM parquet.`{target.directory_url}`"

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


def clickhouse_insert(select_sql: str, target: ResultTarget, config: Any) -> str:
    """``INSERT INTO FUNCTION s3(...)``: ClickHouse runs the query and writes one Parquet
    object at the URL its ``s3`` table function is given."""
    if config.endpoint_url:
        base = config.endpoint_url.rstrip("/")
    else:
        base = f"https://s3.{config.region}.amazonaws.com"
    url = f"{base}/{target.bucket}/{target.key_prefix}/data.parquet"
    return (
        f"INSERT INTO FUNCTION s3({_quoted(url)}, {_quoted(config.access_key)}, "
        f"{_quoted(config.secret_key)}, 'Parquet') {select_sql}"
    )


def clickhouse_count(target: ResultTarget, config: Any) -> str:
    """The rows the written object holds, read back from its own footer."""
    if config.endpoint_url:
        base = config.endpoint_url.rstrip("/")
    else:
        base = f"https://s3.{config.region}.amazonaws.com"
    url = f"{base}/{target.bucket}/{target.key_prefix}/data.parquet"
    return (
        f"SELECT count() FROM s3({_quoted(url)}, {_quoted(config.access_key)}, "
        f"{_quoted(config.secret_key)}, 'Parquet')"
    )


# -- Snowflake ---------------------------------------------------------------------------------------


def snowflake_copy(select_sql: str, target: ResultTarget, config: Any) -> str:
    """``COPY INTO <location> FROM (query)``: Snowflake unloads the query's result as Parquet
    under the prefix, with the column names kept (``HEADER``). Its result row carries
    ``rows_unloaded``. Snowflake writes to AWS S3 with the keys given; it cannot reach a private
    S3-compatible endpoint."""
    return (
        f"COPY INTO {_quoted(target.directory_url)} FROM ({select_sql}) "
        "FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE "
        f"CREDENTIALS = (AWS_KEY_ID = {_quoted(config.access_key)} "
        f"AWS_SECRET_KEY = {_quoted(config.secret_key)})"
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

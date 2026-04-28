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
import uuid

import trino

log = logging.getLogger(__name__)

# Formats Trino can write natively via Hive/Iceberg CTAS
TRINO_NATIVE_FORMATS = {"parquet", "orc"}

RESULTS_CATALOG = "results"
RESULTS_SCHEMA = "provisa_results"
RESULTS_BUCKET = "provisa-results"


def is_trino_native_format(fmt: str) -> bool:
    """Check if a format can be written directly by Trino."""
    return fmt.lower() in TRINO_NATIVE_FORMATS


def _iceberg_format(fmt: str) -> str:
    """Map output format name to Iceberg storage format."""
    return fmt.upper()  # PARQUET, ORC


def ensure_results_schema(conn: trino.dbapi.Connection) -> None:
    """Create the results schema if it doesn't exist."""
    sql = (
        f'CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{RESULTS_SCHEMA} '
        f"WITH (location = 's3a://{RESULTS_BUCKET}/')"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        log.info("Ensured results schema %s.%s exists", RESULTS_CATALOG, RESULTS_SCHEMA)
    except Exception as e:
        # Schema may already exist
        log.debug("Results schema creation: %s", e)


def execute_ctas_redirect(
    conn: trino.dbapi.Connection,
    select_sql: str,
    output_format: str = "parquet",
) -> dict:
    """Execute a query via CTAS, writing results directly to S3.

    Uses the Iceberg connector to write Parquet/ORC files to MinIO/S3.

    Args:
        conn: Trino connection.
        select_sql: The SELECT query to execute (already transpiled to Trino SQL).
        output_format: Target format (parquet, orc).

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


def cleanup_result_table(conn: trino.dbapi.Connection, table_name: str) -> None:
    """Drop a result table (metadata only — external table data stays on S3)."""
    sql = f'DROP TABLE IF EXISTS {RESULTS_CATALOG}.{RESULTS_SCHEMA}."{table_name}"'
    cur = conn.cursor()
    cur.execute(sql)
    log.info("[CTAS CLEANUP] dropped table %s", table_name)


async def schedule_s3_cleanup(
    s3_prefix: str,
    redirect_config,
    delay_seconds: int | None = None,
) -> None:
    """Delete S3 objects under a CTAS result prefix after a delay.

    Called after the presigned URL TTL expires so the data doesn't
    accumulate indefinitely.
    """
    import asyncio
    import boto3
    from botocore.config import Config as BotoConfig

    ttl = delay_seconds if delay_seconds is not None else redirect_config.ttl

    await asyncio.sleep(ttl)

    s3 = boto3.client(
        "s3",
        endpoint_url=redirect_config.endpoint_url or None,
        aws_access_key_id=redirect_config.access_key,
        aws_secret_access_key=redirect_config.secret_key,
        region_name=redirect_config.region,
        config=BotoConfig(signature_version="s3v4"),
    )

    bucket = redirect_config.bucket
    prefix = s3_prefix.replace(f"s3a://{bucket}/", "")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    if contents:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
        )
        log.info("[CTAS CLEANUP] deleted %d S3 objects at %s", len(contents), prefix)


async def presign_ctas_result(
    s3_prefix: str,
    redirect_config,
) -> str:
    """Find the Parquet/ORC file(s) under the CTAS prefix and return a presigned URL.

    CTAS writes one or more files under the prefix. For a single result set,
    Trino typically writes a single file.
    """
    import boto3
    from botocore.config import Config as BotoConfig

    s3 = boto3.client(
        "s3",
        endpoint_url=redirect_config.endpoint_url or None,
        aws_access_key_id=redirect_config.access_key,
        aws_secret_access_key=redirect_config.secret_key,
        region_name=redirect_config.region,
        config=BotoConfig(signature_version="s3v4"),
    )

    # s3_prefix is like "s3a://provisa-results/results/abc123"
    # Strip the s3a://bucket/ to get the S3 key prefix
    bucket = redirect_config.bucket
    prefix = s3_prefix.replace(f"s3a://{bucket}/", "")

    # List objects under the prefix to find the data file(s)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    if not contents:
        raise FileNotFoundError(f"No files found at {s3_prefix}")

    # Use the first (and typically only) data file
    key = contents[0]["Key"]

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=redirect_config.ttl,
    )

    return url

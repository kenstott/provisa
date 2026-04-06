# Copyright (c) 2026 Kenneth Stott
# Canary: e826dc67-d83d-40db-a2df-0a4d7baad111
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Large result redirect to blob storage with presigned URL (REQ-029, REQ-044).

Results above a configurable row threshold are uploaded to S3-compatible storage
and a presigned URL with TTL is returned instead of inline data.
Pre-approved table queries cannot use redirect (REQ-006).
"""

from __future__ import annotations

import io
import json
import os
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from provisa.executor.trino import QueryResult

if TYPE_CHECKING:
    from provisa.compiler.sql_gen import ColumnRef


DEFAULT_THRESHOLD = 1000
DEFAULT_TTL = 3600  # seconds


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


@dataclass
class RedirectConfig:
    """Configuration for large result redirect."""

    enabled: bool
    threshold: int  # row count above which redirect kicks in
    bucket: str
    endpoint_url: str  # S3-compatible endpoint
    access_key: str
    secret_key: str
    ttl: int  # presigned URL TTL in seconds
    region: str = "us-east-1"
    default_format: str = "parquet"  # default S3 upload format

    @staticmethod
    def from_env() -> RedirectConfig:
        enabled = os.environ.get("PROVISA_REDIRECT_ENABLED", "false").lower() == "true"
        return RedirectConfig(
            enabled=enabled,
            threshold=int(os.environ.get("PROVISA_REDIRECT_THRESHOLD", str(DEFAULT_THRESHOLD))),
            bucket=os.environ.get("PROVISA_REDIRECT_BUCKET", "provisa-results"),
            endpoint_url=os.environ.get("PROVISA_REDIRECT_ENDPOINT", ""),
            access_key=os.environ.get("PROVISA_REDIRECT_ACCESS_KEY", ""),
            secret_key=os.environ.get("PROVISA_REDIRECT_SECRET_KEY", ""),
            ttl=int(os.environ.get("PROVISA_REDIRECT_TTL", str(DEFAULT_TTL))),
            region=os.environ.get("PROVISA_REDIRECT_REGION", "us-east-1"),
            default_format=os.environ.get("PROVISA_REDIRECT_FORMAT", "parquet"),
        )


def should_redirect(
    result: QueryResult,
    config: RedirectConfig,
    table_governance: dict[int, str] | None = None,
    target_table_ids: list[int] | None = None,
    *,
    force: bool = False,
) -> bool:
    """Check if a result should be redirected to blob storage.

    Returns False if:
    - Redirect is disabled
    - Row count is below threshold (unless force=True)
    - Any target table is pre-approved (REQ-006)
    """
    if not config.enabled:
        return False

    # Pre-approved tables cannot use redirect (REQ-006)
    if table_governance and target_table_ids:
        for tid in target_table_ids:
            if table_governance.get(tid) == "pre-approved":
                return False

    if force:
        return True

    if len(result.rows) <= config.threshold:
        return False

    return True


_FORMAT_META: dict[str, tuple[str, str]] = {
    "json": ("application/json", ".json"),
    "ndjson": ("application/x-ndjson", ".ndjson"),
    "csv": ("text/csv", ".csv"),
    "parquet": ("application/vnd.apache.parquet", ".parquet"),
    "arrow": ("application/vnd.apache.arrow.stream", ".arrow"),
}


def _serialize_for_redirect(
    result: QueryResult,
    columns: list[ColumnRef] | None,
    output_format: str,
) -> tuple[bytes, str, str]:
    """Serialize result in the requested format.

    Returns (body_bytes, content_type, file_extension).
    """
    content_type, ext = _FORMAT_META.get(output_format, _FORMAT_META["ndjson"])

    if output_format in ("parquet",) and columns is not None:
        from provisa.executor.formats.tabular import rows_to_parquet
        return rows_to_parquet(result.rows, columns), content_type, ext

    if output_format == "arrow" and columns is not None:
        from provisa.executor.formats.arrow import rows_to_arrow_ipc
        return rows_to_arrow_ipc(result.rows, columns), content_type, ext

    if output_format == "csv" and columns is not None:
        from provisa.executor.formats.tabular import rows_to_csv
        body = rows_to_csv(result.rows, columns)
        return body.encode("utf-8"), content_type, ext

    # JSON / NDJSON — works with plain column_names, no ColumnRef needed
    col_names = result.column_names
    rows_as_dicts = [
        {col_names[i]: v for i, v in enumerate(row)}
        for row in result.rows
    ]

    if output_format == "json":
        body = json.dumps(rows_as_dicts, cls=_Encoder)
        return body.encode("utf-8"), "application/json", ".json"

    # NDJSON (default)
    lines = [json.dumps(obj, cls=_Encoder) for obj in rows_as_dicts]
    body = "\n".join(lines)
    return body.encode("utf-8"), "application/x-ndjson", ".ndjson"


def _serialize_arrow_table(
    table,  # pa.Table
    output_format: str,
) -> tuple[bytes, str, str]:
    """Serialize a native Arrow Table for redirect upload.

    Avoids the round-trip through Python tuples when data is already in Arrow.
    """
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    content_type, ext = _FORMAT_META.get(output_format, _FORMAT_META["ndjson"])

    if output_format == "arrow":
        buf = io.BytesIO()
        writer = pa.ipc.new_stream(buf, table.schema)
        writer.write_table(table)
        writer.close()
        return buf.getvalue(), content_type, ext

    if output_format == "parquet":
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue(), content_type, ext

    if output_format == "csv":
        import pyarrow.csv as pcsv
        buf = io.BytesIO()
        pcsv.write_csv(table, buf)
        return buf.getvalue(), "text/csv", ".csv"

    # JSON/NDJSON — convert through Python dicts
    rows_as_dicts = table.to_pylist()
    if output_format == "json":
        body = json.dumps(rows_as_dicts, cls=_Encoder)
        return body.encode("utf-8"), "application/json", ".json"

    lines = [json.dumps(obj, cls=_Encoder) for obj in rows_as_dicts]
    body = "\n".join(lines)
    return body.encode("utf-8"), "application/x-ndjson", ".ndjson"


async def ensure_results_bucket(config: RedirectConfig) -> None:
    """Ensure the configured S3/MinIO results bucket exists, creating it if needed.

    Logs the outcome. Does not raise if MinIO is unavailable at startup.
    """
    import logging

    logger = logging.getLogger(__name__)

    if not config.endpoint_url:
        return

    try:
        import boto3
        from botocore.config import Config as BotoConfig

        s3 = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            config=BotoConfig(signature_version="s3v4"),
        )
        try:
            s3.head_bucket(Bucket=config.bucket)
            logger.info("S3 bucket %r already exists", config.bucket)
        except Exception:
            s3.create_bucket(Bucket=config.bucket)
            logger.info("Created S3 bucket %r", config.bucket)
    except Exception:
        logger.warning(
            "Could not ensure S3 bucket %r — MinIO may be unavailable at startup",
            config.bucket,
            exc_info=True,
        )


async def upload_and_presign(
    result: QueryResult,
    config: RedirectConfig,
    column_names: list[str] | None = None,
    *,
    output_format: str = "ndjson",
    columns: list[ColumnRef] | None = None,
    arrow_table=None,  # pa.Table | None
) -> dict:
    """Upload result to S3 and return presigned URL.

    If arrow_table is provided, serializes directly from Arrow (zero-copy for
    Arrow/Parquet formats). Otherwise falls back to row-based serialization.

    Returns {"redirect_url": "...", "row_count": N, "expires_in": TTL,
             "content_type": "..."}.
    """
    import boto3
    from botocore.config import Config as BotoConfig

    s3 = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url or None,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        config=BotoConfig(signature_version="s3v4"),
    )

    if arrow_table is not None:
        body_bytes, content_type, ext = _serialize_arrow_table(
            arrow_table, output_format,
        )
        row_count = arrow_table.num_rows
    else:
        body_bytes, content_type, ext = _serialize_for_redirect(
            result, columns, output_format,
        )
        row_count = len(result.rows)

    key = f"results/{uuid.uuid4()}{ext}"

    s3.put_object(
        Bucket=config.bucket,
        Key=key,
        Body=body_bytes,
        ContentType=content_type,
    )

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.bucket, "Key": key},
        ExpiresIn=config.ttl,
    )

    return {
        "redirect_url": url,
        "row_count": row_count,
        "expires_in": config.ttl,
        "content_type": content_type,
    }

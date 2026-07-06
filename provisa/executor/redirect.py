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

# Requirements: REQ-006, REQ-029, REQ-044, REQ-047, REQ-048, REQ-049, REQ-050, REQ-137, REQ-138, REQ-139, REQ-140, REQ-141, REQ-142

from __future__ import annotations

import io
import json
import os
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import logging

from provisa.executor.result import QueryResult

log = logging.getLogger(__name__)

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
class RedirectConfig:  # REQ-029, REQ-137, REQ-142
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
    encrypt: bool = False  # REQ-687: client-side envelope-encrypt the bulk payload before upload

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
            encrypt=os.environ.get("PROVISA_REDIRECT_ENCRYPT", "false").lower() == "true",
        )


def should_redirect(  # REQ-029, REQ-140
    result: QueryResult,
    config: RedirectConfig,
    _target_table_ids: list[int] | None = None,
    *,
    force: bool = False,
) -> bool:
    """Check if a result should be redirected to blob storage.

    Returns False if:
    - Redirect is disabled
    - Row count is below threshold (unless force=True)
    """
    if not config.enabled:
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


def _serialize_for_redirect(  # REQ-047, REQ-048, REQ-049, REQ-050, REQ-139
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
    rows_as_dicts = [{col_names[i]: v for i, v in enumerate(row)} for row in result.rows]

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
        pcsv.write_csv(table, buf)  # type: ignore[attr-defined]
        return buf.getvalue(), "text/csv", ".csv"

    # JSON/NDJSON — convert through Python dicts
    rows_as_dicts = table.to_pylist()
    if output_format == "json":
        body = json.dumps(rows_as_dicts, cls=_Encoder)
        return body.encode("utf-8"), "application/json", ".json"

    lines = [json.dumps(obj, cls=_Encoder) for obj in rows_as_dicts]
    body = "\n".join(lines)
    return body.encode("utf-8"), "application/x-ndjson", ".ndjson"


async def ensure_results_bucket(config: RedirectConfig) -> None:  # REQ-141
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


async def upload_and_presign(  # REQ-029, REQ-044, REQ-137, REQ-138, REQ-139, REQ-141
    result: QueryResult,
    config: RedirectConfig,
    _column_names: list[str] | None = None,
    *,
    output_format: str = "ndjson",
    columns: list[ColumnRef] | None = None,
    arrow_table=None,  # pa.Table | None
    role: str | None = None,  # REQ-687: creating role, bound into the encryption grant
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
            arrow_table,
            output_format,
        )
        row_count = arrow_table.num_rows
    else:
        body_bytes, content_type, ext = _serialize_for_redirect(
            result,
            columns,
            output_format,
        )
        row_count = len(result.rows)

    encryption_meta = None
    if config.encrypt:
        # REQ-687: envelope-encrypt the payload so the S3 object is ciphertext. The client
        # must open the role-bound grant via the authenticated /data/redirect/unwrap call to
        # get the DEK — a leaked presigned URL or the bucket admin alone cannot decrypt it,
        # and only the creating role (or an admin) can unwrap.
        body_bytes, encryption_meta = _encrypt_payload(body_bytes, role)
        content_type = "application/octet-stream"
        ext = ext + ".enc"

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

    response = {
        "redirect_url": url,
        "row_count": row_count,
        "expires_in": config.ttl,
        "content_type": content_type,
    }
    if encryption_meta is not None:
        response["encryption"] = encryption_meta
    return response


def _encrypt_payload(body_bytes: bytes, role: str | None) -> tuple[bytes, dict]:  # REQ-687
    """Envelope-encrypt a redirect body; return (ciphertext_blob, client encryption metadata).

    Fail-closed: requires a real envelope provider (NullEncryption cannot protect the
    payload). The metadata carries a role-bound ``grant`` — the raw DEK sealed (together
    with the creating role) under the master key. To read the payload the client presents
    the grant to the authenticated unwrap endpoint, which opens it, verifies the caller is
    the creating role (or an admin), and returns the DEK; the client then AES-256-GCM
    decrypts the ``iv``/ciphertext parsed from the self-describing blob. The grant is opaque
    to the client and integrity-protected, so it cannot be re-scoped to another role.
    """
    import base64
    import json

    from provisa.encryption import EnvelopeEncryption, encryption_service, split_envelope

    svc = encryption_service()
    if not isinstance(svc, EnvelopeEncryption):
        raise RuntimeError(
            "PROVISA_REDIRECT_ENCRYPT is on but no envelope encryption provider is "
            "configured; set encryption.provider so redirect payloads can be encrypted"
        )
    blob = svc.encrypt(body_bytes)
    dek = svc.unwrap(split_envelope(blob)[0])
    grant = svc.encrypt(
        json.dumps({"role": role, "dek": base64.b64encode(dek).decode("ascii")}).encode("utf-8")
    )
    return blob, {
        "scheme": "provisa-envelope-v1",
        "alg": "AES-256-GCM",
        "grant": base64.b64encode(grant).decode("ascii"),
        "unwrap_endpoint": "/data/redirect/unwrap",
    }


# Formats the engine can write natively via Hive/Iceberg CTAS (engine-neutral).
ENGINE_NATIVE_FORMATS = {"parquet", "orc"}


def is_engine_native_format(fmt: str) -> bool:  # REQ-138
    """Check if a format can be written directly by the engine (CTAS-to-object-store)."""
    return fmt.lower() in ENGINE_NATIVE_FORMATS


async def schedule_s3_cleanup(  # REQ-141
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


async def presign_ctas_result(  # REQ-044, REQ-029
    s3_prefix: str,
    redirect_config,
) -> str:
    """Find the Parquet/ORC file(s) under the CTAS prefix and return a presigned URL.

    CTAS writes one or more files under the prefix. For a single result set,
    the engine typically writes a single file.
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

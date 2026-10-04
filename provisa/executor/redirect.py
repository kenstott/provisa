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
import threading
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

import logging

from provisa.core import settings_registry
from provisa.core.operator_floor import OperatorFloorError
from provisa.executor.result import QueryResult

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from provisa.compiler.sql_gen import ColumnRef


# REQ-1913: the deployment's redirect settings are declared in provisa/core/settings_catalog.py.
# DEFAULT_THRESHOLD and DEFAULT_TTL (seconds) are their declared defaults, by the names the rest
# of the code knows them by — looked up when asked for, so importing this module does not load
# the settings declarations.
_DECLARED_DEFAULTS = {"DEFAULT_THRESHOLD": "redirect.threshold", "DEFAULT_TTL": "redirect.ttl"}


def __getattr__(name: str) -> Any:
    if name in _DECLARED_DEFAULTS:
        return settings_registry.setting(_DECLARED_DEFAULTS[name]).default
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# REQ-1349: the bound org's `redirect` overrides. `executor` cannot import API state, so the API
# layer installs a resolver at startup; an installed single-tenant deployment installs none and
# every field below resolves from the platform env alone.
_org_overrides_resolver: Callable[[], dict] | None = None


def set_org_overrides_resolver(resolver: "Callable[[], dict] | None") -> None:  # REQ-1349
    """Install the function returning the bound org's ``redirect`` override block."""
    global _org_overrides_resolver
    _org_overrides_resolver = resolver


def _org_redirect_overrides() -> dict:
    return _org_overrides_resolver() if _org_overrides_resolver is not None else {}


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
    # Desktop-dev fallback: when the object store is unreachable (or none is configured), the redirect
    # result is written here and served by /data/redirect-file/<name> instead of a presigned S3 URL —
    # so redirect can be exercised locally without standing up MinIO/S3.
    local_dir: str = ""
    # REQ-1194: the Snowflake storage integration a Snowflake engine unloads results through; None:
    # a Snowflake engine does not write results to the object store itself.
    snowflake_storage_integration: str | None = None

    @staticmethod
    def from_env() -> RedirectConfig:
        # REQ-1349: an org may narrow WHEN its own results redirect and how long the link lives —
        # enabled, threshold, ttl, default_format. WHERE they land (bucket, endpoint, credentials,
        # region, local_dir) is the deployment's object store and is never org-overridable.
        # REQ-1913: the deployment's value of every field is an operator setting, resolved through
        # the registry (stored, then environment, then the declared default).
        org = _org_redirect_overrides()
        deployment = settings_registry.value

        def _own(field: str) -> Any:
            return org[field] if field in org else deployment(f"redirect.{field}")

        def _text(key: str) -> str:
            # An unset endpoint or credential is the empty string in RedirectConfig: "no object
            # store configured" is a state the redirect path handles (it writes to local_dir).
            unset_or_value = deployment(key)
            return "" if unset_or_value is None else unset_or_value

        return RedirectConfig(
            enabled=bool(_own("enabled")),
            threshold=int(_own("threshold")),
            bucket=deployment("redirect.bucket"),
            endpoint_url=_text("redirect.endpoint"),
            access_key=_text("redirect.access_key"),
            secret_key=_text("redirect.secret_key"),
            ttl=int(_own("ttl")),
            region=deployment("redirect.region"),
            default_format=_own("default_format"),
            encrypt=deployment("redirect.encrypt"),
            local_dir=deployment("redirect.local_dir"),
            snowflake_storage_integration=deployment("redirect.snowflake_storage_integration"),
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


class RedirectStoreError(RuntimeError):
    """The configured results object store cannot take a redirect (unreachable, or its bucket
    cannot be seen or created)."""


# (endpoint, bucket) pairs this process has confirmed exist.
_ensured_buckets: set[tuple[str, str]] = set()
_ensure_bucket_lock = threading.Lock()


def ensure_results_bucket_sync(config: RedirectConfig) -> None:  # REQ-141, REQ-171, REQ-1900
    """Make sure the configured results bucket exists, creating it if it does not.

    Called by the first redirect that needs the bucket, not by boot: boot ran it in every worker
    process and spent 9-21s of each one's startup on botocore's retry backoff whenever nothing
    listened at the endpoint. Once per process per (endpoint, bucket) — a success is remembered, a
    failure is not. ONE attempt with a short connect timeout: the caller is a request.

    Raises :class:`RedirectStoreError` naming the endpoint when the store cannot be reached, the
    bucket cannot be seen, or it cannot be created — the redirect that asked for it fails with
    that, rather than a later write failing with less to say.
    """
    if not config.endpoint_url:
        return  # no object store configured: redirects materialize locally, there is no bucket
    key = (config.endpoint_url, config.bucket)
    if key in _ensured_buckets:
        return

    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import BotoCoreError, ClientError

    with _ensure_bucket_lock:
        if key in _ensured_buckets:
            return
        s3 = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            config=BotoConfig(
                signature_version="s3v4",
                retries={"total_max_attempts": 1},
                connect_timeout=3,
            ),
        )
        try:
            try:
                s3.head_bucket(Bucket=config.bucket)
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                    raise
                try:
                    s3.create_bucket(Bucket=config.bucket)
                    log.info("Created S3 bucket %r at %s", config.bucket, config.endpoint_url)
                except ClientError as create_exc:
                    # Another worker created it between this one's HEAD and CREATE: the bucket
                    # exists, which is the outcome asked for.
                    if create_exc.response["Error"]["Code"] not in (
                        "BucketAlreadyOwnedByYou",
                        "BucketAlreadyExists",
                    ):
                        raise
        except (BotoCoreError, ClientError) as exc:
            raise RedirectStoreError(
                f"results object store {config.endpoint_url} cannot provide bucket "
                f"{config.bucket!r}: {type(exc).__name__}: {exc}"
            ) from exc
        _ensured_buckets.add(key)


async def ensure_results_bucket(config: RedirectConfig) -> None:  # REQ-141
    """Awaitable form of :func:`ensure_results_bucket_sync`."""
    ensure_results_bucket_sync(config)


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
    from botocore.exceptions import (
        ConnectionError as BotoConnectionError,
        EndpointConnectionError,
    )

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

    name = f"{uuid.uuid4()}{ext}"
    key = f"results/{name}"

    def _store_local(reason: str) -> dict:
        """Write the payload to the local redirect dir and return a ``file://`` URL to it (desktop dev)."""
        from pathlib import Path

        results_dir = os.path.join(config.local_dir, "results")
        os.makedirs(results_dir, exist_ok=True)
        path = os.path.join(results_dir, name)
        with open(path, "wb") as fh:
            fh.write(body_bytes)
        resp = {
            # A fully-qualified file:// URL to the result on the local filesystem. Note: browsers block
            # navigating to file:// from an http page, so open it directly (Finder/terminal) or from a
            # desktop shell — it is not a clickable web download.
            "redirect_url": Path(path).resolve().as_uri(),
            "row_count": row_count,
            "expires_in": config.ttl,
            "content_type": content_type,
            "local": True,
            "note": (
                f"{reason} Saved locally as a file:// URL ({path}) — for desktop testing only (open it "
                "directly; browsers won't download file:// from a web page). For shared/production "
                "access, configure a reachable S3-compatible object store via PROVISA_REDIRECT_ENDPOINT "
                "(local dev: run MinIO with `docker compose -f docker-compose.core.yml up -d minio minio-init`)."
            ),
        }
        if encryption_meta is not None:
            resp["encryption"] = encryption_meta
        return resp

    # No object store configured at all → local fallback (desktop dev).
    if not config.endpoint_url:
        return _store_local("No S3-compatible object store is configured.")

    # The first redirect of this process makes sure its bucket exists (REQ-171); a store that
    # cannot provide it fails this request here, naming the endpoint.
    ensure_results_bucket_sync(config)

    try:
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
    except (EndpointConnectionError, ConnectionError, BotoConnectionError) as e:
        # The store is configured but unreachable (e.g. MinIO not running). Fall back to local so a
        # desktop dev can still exercise redirect, and say plainly what happened + how to get shared access.
        log.warning(
            "Redirect object store %s unreachable (%s); storing locally", config.endpoint_url, e
        )
        return _store_local(
            f"Object store {config.endpoint_url} is not reachable ({type(e).__name__})."
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


def schedule_s3_cleanup(  # REQ-141
    s3_prefix: str,
    redirect_config,
    delay_seconds: int | None = None,
) -> None:
    """Delete S3 objects under a CTAS result prefix once the presigned URL's TTL has passed, so
    the data doesn't accumulate indefinitely.

    REQ-1882: the delay is held by the background timer thread, not by a sleeping task; the
    deletion runs on a background worker at expiry."""
    from provisa.core.connection_loop import spawn_after

    ttl = delay_seconds if delay_seconds is not None else redirect_config.ttl
    spawn_after(ttl, cleanup_s3_prefix(s3_prefix, redirect_config), name=f"s3-cleanup:{s3_prefix}")


async def cleanup_s3_prefix(s3_prefix: str, redirect_config) -> None:  # REQ-141
    """Delete the S3 objects under a CTAS result prefix now (see :func:`schedule_s3_cleanup`)."""
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


# --------------------------------------------------------------------------- #
# The ONE materialize terminal (REQ-1194/REQ-1195).
#
# The redirect/materialize decision is an IR-level route directive attached to a governed plan by the
# top of the pipeline. Every transport inherits it because they all execute through _execute_plan —
# there is no transport-local "should I redirect?" branch anymore. This terminal is the single sink:
# it selects a sink tier and hands back an opaque delivery handle that each surface reports in its own
# envelope. Zero result rows flow through Provisa's memory on this path.
# --------------------------------------------------------------------------- #


@dataclass
class Delivery:  # REQ-1194, REQ-1195
    """A governed request to materialize a result to a sink instead of returning rows.

    Carried on ``_Plan.materialize`` and consumed by :func:`run_materialize`. ``output_format`` is the
    on-disk file format (parquet/orc); ``config`` is the resolved :class:`RedirectConfig`; ``role`` is
    the requesting role, threaded to the sink for per-username access scoping (REQ-1192).
    """

    output_format: str
    config: RedirectConfig
    role: str | None = None


class DeliveryFailed(RuntimeError):  # REQ-171, REQ-1194
    """A result the request was to receive in the results store could not be written there.

    ``forced``: the request asked for the delivery (``X-Provisa-Redirect`` and its equivalents);
    otherwise the buffered threshold chose it (REQ-1224). ``cause`` is the sink's or the engine's
    own error. A delivery that fails fails the request by name, never an inline answer instead."""

    def __init__(self, cause: Exception, *, forced: bool) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.forced = forced


async def deliver(
    state, physical_sql: str, delivery: Delivery, params: list | None, *, forced: bool
) -> dict:  # REQ-171, REQ-1194
    """:func:`run_materialize`, with its failure named (:class:`DeliveryFailed`). The request's
    deadline passing while the delivery runs is a timeout, not a delivery failure."""
    try:
        return await run_materialize(state, physical_sql, delivery, params)
    except TimeoutError:
        raise
    except Exception as exc:
        raise DeliveryFailed(exc, forced=forced) from exc


class RedirectFloorViolation(OperatorFloorError):
    """A request's redirect threshold is above the operator's (REQ-029, amended 2026-09-30).

    The operator's threshold is a floor that protects the platform: a request may lower it
    (redirect sooner), never raise it. A PermissionError so every transport's existing mapping
    carries it to the caller."""

    def __init__(self, requested: int, operator_threshold: int) -> None:
        super().__init__(
            f"redirect threshold {requested} is above the operator's floor: the operator set the "
            f"large-result redirect threshold to {operator_threshold} rows. Request a threshold at "
            "or under it, or omit it."
        )


def request_redirect_config(threshold: int | None) -> RedirectConfig:  # REQ-029, REQ-1194
    """The redirect config for a request that may carry its own ``threshold``.

    The one place a request's threshold meets the operator's: with redirect enabled the request
    may only lower it, and a higher one raises :class:`RedirectFloorViolation`. With redirect
    disabled a request threshold only makes this result redirect sooner than never."""
    from dataclasses import replace

    config = RedirectConfig.from_env()
    if threshold is None:
        return config
    if config.enabled and threshold > config.threshold:
        raise RedirectFloorViolation(threshold, config.threshold)
    return replace(config, enabled=True, threshold=threshold)


def delivery_from_request(  # REQ-1194, REQ-1195
    *,
    force_redirect: bool,
    redirect_format: str | None,
    threshold: int | None,
    role: str | None,
) -> Delivery | None:
    """Translate a buffered transport's caller redirect request into a ``_Plan.materialize`` directive.

    The buffered transports (GraphQL, JSON:API, Bolt) each read a redirect request from their own
    side-channel — HTTP ``X-Provisa-Redirect*`` headers, or Bolt transaction metadata — and call this
    to build the IR directive. Returns ``None`` when no redirect was asked for: that is the opt-out
    (``deliver=None``) the streaming transports also use, so the plan returns rows as usual.
    """
    if not force_redirect:
        return None
    config = request_redirect_config(threshold)
    fmt = redirect_format or config.default_format or "parquet"
    return Delivery(output_format=fmt, config=config, role=role)


def auto_delivery_for_buffered(role: str | None) -> Delivery | None:  # REQ-1224
    """The AUTOMATIC materialize policy for a buffered transport (GraphQL, JSON:API, Bolt).

    Unlike :func:`delivery_from_request` (caller-driven, an unconditional CTAS), this fires with NO
    caller side-channel: when large-result redirect is enabled in system config, every buffered-transport
    result is subject to the row-count threshold at the single terminal — the body is inlined below the
    threshold and landed as an engine-native CTAS above it (REQ-1224, streaming-uniformity Defect 4).
    The threshold decision lives in ``_execute_plan``; this only carries the resolved config/format/role.

    Returns ``None`` when redirect is disabled, so the terminal returns rows inline exactly as before —
    the automatic threshold is opt-in via the PROVISA_REDIRECT_* system configuration.
    """
    config = RedirectConfig.from_env()
    if not config.enabled:
        return None
    fmt = config.default_format or "parquet"
    return Delivery(output_format=fmt, config=config, role=role)


_CONTENT_TYPES = {
    "parquet": "application/vnd.apache.parquet",
    "orc": "application/x-orc",
}


async def run_materialize(
    state, physical_sql: str, delivery: Delivery, params: list | None
) -> dict:  # REQ-1194, REQ-1195
    """Materialize *physical_sql* to the selected sink and return the delivery handle.

    ``params`` are the statement's bound values (None when it binds none). Required, so no caller
    can hand the engine a statement whose placeholders have nothing bound to them.

    Sink-tier selection rule (REQ-1195): when the engine can write the requested format natively to a
    configured object store, the engine runs a CTAS straight to object storage (REQ-1194) and the
    client receives a presigned URL — zero rows transit Provisa. Otherwise the local/HTTP sink tier
    (REQ-1191) is used. The tiers are exclusive and chosen here, once, for every transport.

    Returns a handle dict: ``{sink, redirect_url, row_count, expires_in, content_type}``.
    """

    fmt = delivery.output_format.lower()
    config = delivery.config

    # Object-store tier (REQ-1194): the bound engine writes this format to an object store itself
    # (its backend's ``result_formats``). S3 creds/bucket come from config/env/IAM. An empty
    # endpoint_url means real AWS S3, not "no store", so it does NOT disqualify this tier.
    object_store_available = state.federation_engine.writes_result(fmt)

    if object_store_available:
        # Object-store tier: the engine CTAS-writes Parquet/ORC directly to S3-compatible storage.
        ctas_result = state.federation_engine.ctas_redirect(physical_sql, fmt, params)
        url = await presign_ctas_result(ctas_result["s3_prefix"], config)
        # Do NOT drop the Iceberg table here — DROP TABLE on the JDBC catalog purges S3 data files
        # immediately, invalidating the presigned URL. The background task deletes objects after TTL.
        schedule_s3_cleanup(ctas_result["s3_prefix"], config)
        return {
            "sink": "object-store",
            "redirect_url": url,
            "row_count": ctas_result["row_count"],
            "expires_in": config.ttl,
            "content_type": _CONTENT_TYPES.get(fmt, "application/octet-stream"),
        }

    # Local/HTTP sink tier (REQ-1191/REQ-1192/REQ-1193): served by the app's redirect-file endpoint
    # with per-username scoping and TTL reaping. Implemented as the follow-on increment.
    raise NotImplementedError(
        "local/HTTP materialization sink (REQ-1191) not yet wired — configure an object store "
        "(PROVISA_REDIRECT_ENDPOINT) with a native format (parquet/orc) to materialize results"
    )

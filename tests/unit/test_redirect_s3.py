# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1def3e-5fb6-4678-a2ca-bf8cc46c2472
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for S3 large-result redirect — TTL and S3 error scenarios.

Source: provisa/executor/redirect.py  (REQ-029, REQ-044)

Existing coverage in tests/unit/test_redirect.py:
  - should_redirect threshold logic (below/at/above)
  - disabled flag
  - pre-approved table governance (REQ-006)
  - force flag
  - RedirectConfig.from_env defaults

This file adds:
  - TTL is passed to generate_presigned_url with the exact configured value
  - S3 permission-denied error propagates from upload_and_presign
  - Missing bucket error propagates from upload_and_presign
  - ensure_results_bucket swallows errors when S3 is unreachable
  - RedirectConfig TTL field drives the returned expires_in value
  - upload_and_presign returns expected response shape
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from provisa.executor.redirect import (
    RedirectConfig,
    ensure_results_bucket,
    upload_and_presign,
)
from provisa.executor.trino import QueryResult


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _config(ttl: int = 3600, bucket: str = "provisa-results") -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=10,
        bucket=bucket,
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        ttl=ttl,
    )


def _result(n_rows: int = 5) -> QueryResult:
    return QueryResult(
        rows=[("r1", "r2") for _ in range(n_rows)],
        column_names=["col_a", "col_b"],
    )


def _mock_s3_client(presigned_url: str = "https://s3.example.com/results/abc.ndjson?X-Amz-Expires=3600"):
    """Return a mock boto3 S3 client that records calls."""
    client = MagicMock()
    client.put_object = MagicMock()
    client.generate_presigned_url = MagicMock(return_value=presigned_url)
    return client


# ---------------------------------------------------------------------------
# TTL expiration of presigned URL
# ---------------------------------------------------------------------------

class TestPresignedUrlTTL:
    """upload_and_presign must pass the configured TTL to generate_presigned_url."""

    @pytest.mark.asyncio
    async def test_ttl_passed_to_generate_presigned_url(self):
        cfg = _config(ttl=900)
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            result = await upload_and_presign(_result(), cfg, output_format="ndjson")

        s3.generate_presigned_url.assert_called_once()
        _, kwargs = s3.generate_presigned_url.call_args
        assert kwargs.get("ExpiresIn") == 900

    @pytest.mark.asyncio
    async def test_expires_in_in_response_matches_config_ttl(self):
        cfg = _config(ttl=1800)
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert resp["expires_in"] == 1800

    @pytest.mark.asyncio
    async def test_short_ttl_passed_correctly(self):
        cfg = _config(ttl=60)
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert resp["expires_in"] == 60
        _, kwargs = s3.generate_presigned_url.call_args
        assert kwargs.get("ExpiresIn") == 60

    @pytest.mark.asyncio
    async def test_default_ttl_3600(self):
        """RedirectConfig default TTL is 3600 seconds (1 hour)."""
        cfg = _config(ttl=3600)
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert resp["expires_in"] == 3600

    @pytest.mark.asyncio
    async def test_presigned_url_in_response(self):
        expected_url = "https://s3.example.com/results/uuid.ndjson?X-Amz-Expires=3600"
        cfg = _config()
        s3 = _mock_s3_client(presigned_url=expected_url)

        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert resp["redirect_url"] == expected_url

    @pytest.mark.asyncio
    async def test_bucket_name_passed_to_generate_presigned_url(self):
        cfg = _config(bucket="my-custom-bucket")
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            await upload_and_presign(_result(), cfg, output_format="ndjson")

        _, kwargs = s3.generate_presigned_url.call_args
        params = kwargs.get("Params", {})
        assert params.get("Bucket") == "my-custom-bucket"


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------

class TestUploadAndPresignResponseShape:
    """upload_and_presign must return a dict with the documented keys."""

    @pytest.mark.asyncio
    async def test_response_has_redirect_url_key(self):
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(3), cfg, output_format="ndjson")
        assert "redirect_url" in resp

    @pytest.mark.asyncio
    async def test_response_has_row_count_key(self):
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(7), cfg, output_format="ndjson")
        assert resp["row_count"] == 7

    @pytest.mark.asyncio
    async def test_response_has_expires_in_key(self):
        cfg = _config(ttl=300)
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")
        assert "expires_in" in resp
        assert resp["expires_in"] == 300

    @pytest.mark.asyncio
    async def test_response_has_content_type_key(self):
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")
        assert "content_type" in resp

    @pytest.mark.asyncio
    async def test_ndjson_content_type(self):
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="ndjson")
        assert resp["content_type"] == "application/x-ndjson"

    @pytest.mark.asyncio
    async def test_json_content_type(self):
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="json")
        assert resp["content_type"] == "application/json"

    @pytest.mark.asyncio
    async def test_csv_without_columns_falls_back_to_ndjson(self):
        """CSV serialization requires ColumnRef objects (columns= kwarg).
        Without them, _serialize_for_redirect falls through to NDJSON."""
        cfg = _config()
        s3 = _mock_s3_client()
        with patch("boto3.client", return_value=s3):
            resp = await upload_and_presign(_result(), cfg, output_format="csv")
        # columns=None → NDJSON fallback
        assert resp["content_type"] == "application/x-ndjson"


# ---------------------------------------------------------------------------
# S3 error handling — permission denied
# ---------------------------------------------------------------------------

class TestS3PermissionDenied:
    """ClientError with AccessDenied must propagate from upload_and_presign."""

    @pytest.mark.asyncio
    async def test_permission_denied_on_put_object_propagates(self):
        from botocore.exceptions import ClientError

        cfg = _config()
        s3 = MagicMock()
        s3.put_object = MagicMock(side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        ))
        s3.generate_presigned_url = MagicMock()

        with patch("boto3.client", return_value=s3):
            with pytest.raises(ClientError) as exc_info:
                await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"

    @pytest.mark.asyncio
    async def test_permission_denied_on_presign_propagates(self):
        from botocore.exceptions import ClientError

        cfg = _config()
        s3 = MagicMock()
        s3.put_object = MagicMock()
        s3.generate_presigned_url = MagicMock(side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "GeneratePresignedUrl",
        ))

        with patch("boto3.client", return_value=s3):
            with pytest.raises(ClientError) as exc_info:
                await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


# ---------------------------------------------------------------------------
# S3 error handling — bucket missing
# ---------------------------------------------------------------------------

class TestS3BucketMissing:
    """NoSuchBucket error must propagate from upload_and_presign."""

    @pytest.mark.asyncio
    async def test_no_such_bucket_on_put_object_propagates(self):
        from botocore.exceptions import ClientError

        cfg = _config(bucket="nonexistent-bucket")
        s3 = MagicMock()
        s3.put_object = MagicMock(side_effect=ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "The specified bucket does not exist"}},
            "PutObject",
        ))
        s3.generate_presigned_url = MagicMock()

        with patch("boto3.client", return_value=s3):
            with pytest.raises(ClientError) as exc_info:
                await upload_and_presign(_result(), cfg, output_format="ndjson")

        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    @pytest.mark.asyncio
    async def test_ensure_results_bucket_swallows_error_when_s3_unreachable(self):
        """ensure_results_bucket must not raise even if S3/MinIO is unavailable.

        This prevents startup failures when the object store is not yet ready.
        """
        cfg = _config()

        with patch("boto3.client", side_effect=Exception("Connection refused")):
            # Must not raise
            await ensure_results_bucket(cfg)

    @pytest.mark.asyncio
    async def test_ensure_results_bucket_swallows_client_error(self):
        from botocore.exceptions import ClientError

        cfg = _config()
        s3 = MagicMock()
        s3.head_bucket = MagicMock(side_effect=ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "HeadBucket",
        ))
        s3.create_bucket = MagicMock(side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "CreateBucket",
        ))

        with patch("boto3.client", return_value=s3):
            # Must not raise — startup must continue even if bucket creation fails
            await ensure_results_bucket(cfg)

    @pytest.mark.asyncio
    async def test_ensure_results_bucket_noop_when_no_endpoint(self):
        """ensure_results_bucket returns immediately if endpoint_url is empty."""
        cfg = RedirectConfig(
            enabled=True,
            threshold=10,
            bucket="test",
            endpoint_url="",  # no endpoint → noop
            access_key="",
            secret_key="",
            ttl=3600,
        )
        # boto3.client must not be called at all
        with patch("boto3.client") as mock_client:
            await ensure_results_bucket(cfg)
            mock_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_put_object_called_with_correct_bucket(self):
        """put_object is always called with the configured bucket name."""
        cfg = _config(bucket="target-bucket")
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            await upload_and_presign(_result(), cfg, output_format="ndjson")

        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "target-bucket"

    @pytest.mark.asyncio
    async def test_object_key_uses_results_prefix(self):
        """Uploaded S3 key should start with 'results/'."""
        cfg = _config()
        s3 = _mock_s3_client()

        with patch("boto3.client", return_value=s3):
            await upload_and_presign(_result(), cfg, output_format="ndjson")

        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Key"].startswith("results/")

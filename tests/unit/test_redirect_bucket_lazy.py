# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8a1f73-2d9e-4b40-a6c5-e7f3b0d2a918
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The results bucket is ensured by the first redirect that needs it, not by boot (REQ-171, REQ-1900).

Boot used to HEAD and then CREATE the bucket in every worker, and swallow both failures. Against
an endpoint nothing listens on, botocore's default retries spent 9-21s of each worker's startup
on it, the bucket was never created, and nothing said so. The bucket is now ensured once per
process by the first redirect, with one attempt, and a store that cannot be reached or a bucket
that cannot be made fails THAT request with an error naming the endpoint."""

# Requirements: REQ-171, REQ-1900

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from provisa.executor import redirect
from provisa.executor.redirect import (
    RedirectConfig,
    RedirectStoreError,
    ensure_results_bucket,
    upload_and_presign,
)
from provisa.executor.result import QueryResult


@pytest.fixture(autouse=True)
def _no_bucket_ensured_yet(monkeypatch):
    monkeypatch.setattr(redirect, "_ensured_buckets", set())


def _config(endpoint: str = "http://127.0.0.1:9") -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=10,
        bucket="results",
        endpoint_url=endpoint,
        access_key="k",
        secret_key="s",
        ttl=3600,
    )


def _result() -> QueryResult:
    return QueryResult(rows=[("a", "b")], column_names=["x", "y"])


def _s3() -> MagicMock:
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "https://s3.example/results/x"
    return s3


@pytest.mark.asyncio
async def test_the_first_redirect_ensures_the_bucket_and_later_ones_do_not():
    s3 = _s3()
    with patch("boto3.client", return_value=s3):
        await upload_and_presign(_result(), _config())
        await upload_and_presign(_result(), _config())
    s3.head_bucket.assert_called_once_with(Bucket="results")
    s3.create_bucket.assert_not_called()
    assert s3.put_object.call_count == 2


@pytest.mark.asyncio
async def test_a_missing_bucket_is_created_by_the_first_redirect():
    s3 = _s3()
    s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")
    with patch("boto3.client", return_value=s3):
        await upload_and_presign(_result(), _config())
    s3.create_bucket.assert_called_once_with(Bucket="results")
    s3.put_object.assert_called_once()


@pytest.mark.asyncio
async def test_the_bucket_check_makes_one_attempt_with_a_short_connect_timeout():
    s3 = _s3()
    with patch("boto3.client", return_value=s3) as client:
        await ensure_results_bucket(_config())
    boto_config = client.call_args.kwargs["config"]
    assert boto_config.retries == {"total_max_attempts": 1}
    assert boto_config.connect_timeout == 3


@pytest.mark.asyncio
async def test_an_unreachable_store_fails_the_redirect_naming_the_endpoint():
    started = time.monotonic()
    with pytest.raises(RedirectStoreError) as raised:
        await upload_and_presign(_result(), _config())  # nothing listens on port 9
    assert time.monotonic() - started < 3
    assert "http://127.0.0.1:9" in str(raised.value)
    assert "results" in str(raised.value)


@pytest.mark.asyncio
async def test_a_failed_check_is_not_remembered_as_done():
    with pytest.raises(RedirectStoreError):
        await ensure_results_bucket(_config())
    s3 = _s3()
    with patch("boto3.client", return_value=s3):
        await ensure_results_bucket(_config())
    s3.head_bucket.assert_called_once()


@pytest.mark.asyncio
async def test_a_bucket_this_identity_may_not_see_fails_the_redirect():
    s3 = _s3()
    s3.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadBucket"
    )
    with patch("boto3.client", return_value=s3), pytest.raises(RedirectStoreError) as raised:
        await upload_and_presign(_result(), _config())
    s3.create_bucket.assert_not_called()
    s3.put_object.assert_not_called()
    assert "http://127.0.0.1:9" in str(raised.value)


@pytest.mark.asyncio
async def test_a_bucket_that_cannot_be_created_fails_the_redirect():
    s3 = _s3()
    s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")
    s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "CreateBucket"
    )
    with patch("boto3.client", return_value=s3), pytest.raises(RedirectStoreError):
        await upload_and_presign(_result(), _config())
    s3.put_object.assert_not_called()


@pytest.mark.asyncio
async def test_a_bucket_another_worker_just_created_is_a_bucket_that_exists():
    s3 = _s3()
    s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")
    s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "CreateBucket"
    )
    with patch("boto3.client", return_value=s3):
        await upload_and_presign(_result(), _config())
    s3.put_object.assert_called_once()


@pytest.mark.asyncio
async def test_no_configured_store_ensures_nothing():
    with patch("boto3.client") as client:
        await ensure_results_bucket(_config(endpoint=""))
    client.assert_not_called()


@pytest.mark.asyncio
async def test_boot_does_not_touch_the_results_store():
    """Neither engine family's boot-time infra step opens an S3 client for the results bucket."""
    from provisa.federation.backend import EngineBackend

    with patch("boto3.client") as client:
        await EngineBackend.provision_infra(MagicMock(), SimpleNamespace())
    client.assert_not_called()

    import inspect

    from provisa.federation import trino_lifecycle

    boot_source = inspect.getsource(trino_lifecycle.connect_infra)
    assert "ensure_results_bucket(" not in boot_source
    assert "ensure_results_bucket_sync(" not in boot_source


def test_a_trino_ctas_redirect_ensures_its_bucket_first():
    from provisa.federation.backend import TrinoBackend

    order: list[str] = []
    state = SimpleNamespace(engine_conn="conn")
    with (
        patch(
            "provisa.executor.redirect.ensure_results_bucket_sync",
            side_effect=lambda _cfg: order.append("bucket"),
        ),
        patch(
            "provisa.executor.trino_write.execute_ctas_redirect",
            side_effect=lambda *_a: order.append("ctas") or {"ok": True},
        ),
    ):
        out = TrinoBackend.ctas_redirect(MagicMock(), state, "SELECT 1", "parquet")
    assert order == ["bucket", "ctas"]
    assert out == {"ok": True}

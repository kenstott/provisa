# Copyright (c) 2025 Kenneth Stott
# Canary: 1cb0f2b2-cfc4-4dfa-92a5-9f6dfe243b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for S3 blob upload + presigned URL via MinIO.

Requires MinIO running in Docker Compose (port 9000).
"""

import pytest

from provisa.executor.redirect import RedirectConfig, upload_and_presign
from provisa.executor.trino import QueryResult

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

MINIO_CONFIG = RedirectConfig(
    enabled=True,
    threshold=0,
    bucket="provisa-test-results",
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    ttl=300,
    region="us-east-1",
)


@pytest.fixture(autouse=True)
def _ensure_bucket():
    """Create the test bucket in MinIO if it doesn't exist."""
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_CONFIG.endpoint_url,
        aws_access_key_id=MINIO_CONFIG.access_key,
        aws_secret_access_key=MINIO_CONFIG.secret_key,
        region_name=MINIO_CONFIG.region,
        config=BotoConfig(signature_version="s3v4"),
    )
    try:
        s3.head_bucket(Bucket=MINIO_CONFIG.bucket)
    except ClientError:
        s3.create_bucket(Bucket=MINIO_CONFIG.bucket)


class TestBlobUpload:
    async def test_upload_and_presign(self):
        result = QueryResult(
            rows=[(1, "Alice", 19.99), (2, "Bob", 29.99)],
            column_names=["id", "name", "amount"],
        )
        response = await upload_and_presign(result, MINIO_CONFIG)
        assert "redirect_url" in response
        assert response["row_count"] == 2
        assert response["expires_in"] == 300
        assert "http" in response["redirect_url"]

    async def test_presigned_url_accessible(self):
        """Verify the presigned URL returns the uploaded data."""
        import httpx

        result = QueryResult(
            rows=[(10, "test", 42.0)],
            column_names=["id", "name", "val"],
        )
        response = await upload_and_presign(result, MINIO_CONFIG)
        url = response["redirect_url"]

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            assert resp.status_code == 200
            assert "test" in resp.text

    async def test_empty_result_uploads(self):
        result = QueryResult(rows=[], column_names=["id"])
        response = await upload_and_presign(result, MINIO_CONFIG)
        assert response["row_count"] == 0

    async def test_large_result_uploads(self):
        rows = [(i, f"name_{i}", float(i)) for i in range(500)]
        result = QueryResult(rows=rows, column_names=["id", "name", "val"])
        response = await upload_and_presign(result, MINIO_CONFIG)
        assert response["row_count"] == 500

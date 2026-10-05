# Copyright (c) 2026 Kenneth Stott
# Canary: 8218e944-bf12-41aa-b44e-0e4157f9c9c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The results bucket is created by the first redirect that needs it, not by startup (REQ-171,
amended 2026-10-01).

A real server (DuckDB engine) over the stack's Postgres, pointed at a results bucket on the stack's
MinIO that does not exist. Nothing provisions it: the server boots and answers without it, and the
bucket is still absent; the first forced redirect creates it and lands the rows there, and the
presigned link returns them.

Lands on the TEST instance only: a database the harness creates and a bucket of this module's own
name on the test stack's MinIO, removed at the end."""

from __future__ import annotations

import os

import pytest

from tests.integration.test_redirect_every_transport_e2e import (
    _IDS,
    _KEY,
    _PG_HOST,
    _PG_PORT,
    _SECRET,
    _delivered_ids,
    _http,
    _rest,
)
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_BUCKET = f"redirect-first-bucket-{os.getpid()}"


@pytest.fixture(scope="module")
def s3():
    import boto3
    from botocore.config import Config as BotoConfig

    client = boto3.client(
        "s3",
        endpoint_url=os.environ["PROVISA_REDIRECT_ENDPOINT"],
        aws_access_key_id=_KEY,
        aws_secret_access_key=_SECRET,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4"),
    )
    try:
        yield client
    finally:
        if _bucket_exists(client):
            for obj in client.list_objects_v2(Bucket=_BUCKET).get("Contents", []):
                client.delete_object(Bucket=_BUCKET, Key=obj["Key"])
            client.delete_bucket(Bucket=_BUCKET)


def _bucket_exists(client) -> bool:
    return any(b["Name"] == _BUCKET for b in client.list_buckets().get("Buckets", []))


@pytest.fixture(scope="module")
def server(s3):
    assert not _bucket_exists(s3), f"{_BUCKET} exists before the test; nothing may provision it"
    env = {
        "PROVISA_REDIRECT_ENDPOINT": os.environ["PROVISA_REDIRECT_ENDPOINT"],
        "PROVISA_REDIRECT_BUCKET": _BUCKET,
        "PROVISA_REDIRECT_ACCESS_KEY": _KEY,
        "PROVISA_REDIRECT_SECRET_KEY": _SECRET,
        "PROVISA_REDIRECT_REGION": "us-east-1",
        # Forced deliveries only: every other read answers inline.
        "PROVISA_REDIRECT_ENABLED": "false",
    }
    boot = WorkerBoot(1, pg_host=_PG_HOST, pg_port=_PG_PORT, env=env)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def test_the_first_redirect_creates_the_results_bucket(server, s3):
    # Startup does not create it, and an inline read does not need it.
    inline = _http(server, "GET", "/data/rest/sales/orders", None, {})
    assert {row["id"] for row in inline["data"]} == _IDS, inline
    assert not _bucket_exists(s3), "the bucket was created before any redirect needed it"

    handle = _rest(server)

    assert _bucket_exists(s3), "the first redirect did not create the results bucket"
    assert _BUCKET in handle["redirect_url"], handle
    assert _delivered_ids(handle) == _IDS

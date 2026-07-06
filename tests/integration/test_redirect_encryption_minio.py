# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3e7c21-5a48-4f0d-8e62-1c9a0f4d7b6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-687: bulk-result encryption proven against live MinIO.

Uploads an encrypted redirect payload to the real object store, then downloads it
via the presigned URL exactly as a client would. Asserts the stored object is
ciphertext (a bucket admin / URL holder cannot read it) and that the payload is
recoverable only after opening the role-bound grant with the master key and
AES-256-GCM decrypting with the returned DEK. Skips cleanly if MinIO is absent.
"""

from __future__ import annotations

import base64
import json
import os

import pytest
import urllib.request
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from provisa.executor.result import QueryResult
from provisa.encryption import (
    EnvelopeEncryption,
    configure_encryption,
    encryption_service,
    reset_encryption,
    split_envelope,
)
from provisa.executor.redirect import RedirectConfig, ensure_results_bucket, upload_and_presign

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_ENDPOINT = os.environ.get("PROVISA_REDIRECT_ENDPOINT", "http://localhost:9000")
_ACCESS = os.environ.get("PROVISA_REDIRECT_ACCESS_KEY", "minioadmin")
_SECRET = os.environ.get("PROVISA_REDIRECT_SECRET_KEY", "minioadmin")
_BUCKET = os.environ.get("PROVISA_REDIRECT_BUCKET", "provisa-results")

_SECRET_VALUE = "top-secret-cell-value-687"


@pytest.fixture(autouse=True)
def _enc():
    reset_encryption()
    os.environ["PROVISA_ENCRYPTION_KEY"] = base64.b64encode(bytes(range(32))).decode()
    svc = configure_encryption("local")
    assert isinstance(svc, EnvelopeEncryption)
    yield
    reset_encryption()


def _config() -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=0,
        bucket=_BUCKET,
        endpoint_url=_ENDPOINT,
        access_key=_ACCESS,
        secret_key=_SECRET,
        ttl=300,
        region="us-east-1",
        encrypt=True,
    )


def _reachable() -> bool:
    try:
        with urllib.request.urlopen(f"{_ENDPOINT}/minio/health/live", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


async def test_encrypted_redirect_roundtrip_via_minio():
    if not _reachable():
        pytest.skip(f"MinIO not reachable at {_ENDPOINT}")

    # Provision the results bucket (the app does this at startup via connect_infra).
    await ensure_results_bucket(_config())

    result = QueryResult(
        column_names=["id", "note"],
        rows=[(1, _SECRET_VALUE), (2, "plain")],
    )
    info = await upload_and_presign(result, _config(), output_format="ndjson", role="analyst")

    assert info["content_type"] == "application/octet-stream"
    meta = info["encryption"]
    assert meta["scheme"] == "provisa-envelope-v1"

    # Client downloads the object via the presigned URL — no auth headers, no key.
    with urllib.request.urlopen(info["redirect_url"], timeout=10) as r:
        blob = r.read()

    # What sits in the bucket (and travels over the presigned URL) is ciphertext.
    assert _SECRET_VALUE.encode() not in blob

    # Only the server (master key) can open the role-bound grant → DEK.
    payload = json.loads(encryption_service().decrypt(base64.b64decode(meta["grant"])))
    assert payload["role"] == "analyst"
    dek = base64.b64decode(payload["dek"])

    _wrapped, iv, ciphertext = split_envelope(blob)
    plaintext = AESGCM(dek).decrypt(iv, ciphertext, None)
    assert _SECRET_VALUE.encode() in plaintext
    assert plaintext.decode().count("\n") == 1  # two ndjson rows

# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0b6a2e-8c41-4d7a-9e55-1b2c7d9a4e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One results store per deployment, for every redirect format (REQ-1195, REQ-1191).

The results store is chosen by the deployment's configuration alone, never by the format asked
for or by which formats the bound engine writes itself:

- no object store configured: every format is written to the org's folder on this node and
  answered at the request's base URI + ``/data/redirect-file/<id>.<ext>``;
- an object store configured: every format is written there and answered with a presigned URL --
  by the engine itself where it writes the format, by Provisa from the rows otherwise.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.audit.context import audit_identity_scope
from provisa.core.request_context import current_org
from provisa.executor import local_results, redirect
from provisa.executor.redirect import Delivery, RedirectConfig, run_materialize
from provisa.executor.result import QueryResult

FORMATS = sorted(local_results.FILE_FORMATS)
BASE = "http://localhost:3000"
PRESIGNED = "https://store.example/results/signed"


@pytest.fixture
def org_folder(tmp_path, monkeypatch) -> Path:
    from provisa.core import org_files

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    folder = org_files.folder(current_org.get() or "default")
    folder.mkdir(parents=True)
    return folder


@pytest.fixture(autouse=True)
def _no_timers(monkeypatch):
    monkeypatch.setattr(local_results, "schedule_removal", lambda path, ttl: None)
    monkeypatch.setattr(redirect, "schedule_s3_cleanup", lambda *a, **k: None)
    monkeypatch.setattr(redirect, "_ensured_buckets", set())


class _ParquetEngine:
    """An engine like DuckDB: it writes Parquet to the object store itself, and nothing else."""

    def __init__(self) -> None:
        self.ctas: list[tuple] = []
        self.ran: list[str] = []

    def writes_result(self, fmt: str) -> bool:
        return fmt == "parquet"

    def writes_result_file(self, fmt: str) -> bool:
        return False

    def ctas_redirect(self, sql: str, fmt: str, params: list | None) -> dict:
        self.ctas.append((sql, fmt, params))
        return {"s3_prefix": "s3a://provisa-results/results/default/abc", "row_count": 3}

    def execute_engine_sync(self, sql: str, params: list | None = None) -> QueryResult:
        self.ran.append(sql)
        return QueryResult(rows=[(1, "a"), (2, "b"), (3, "c")], column_names=["id", "name"])


def _config(*, endpoint: str, key: str) -> RedirectConfig:
    return RedirectConfig(
        enabled=True,
        threshold=10,
        bucket="provisa-results",
        endpoint_url=endpoint,
        access_key=key,
        secret_key=key,
        ttl=600,
    )


async def _deliver(engine, fmt: str, config: RedirectConfig) -> dict:
    state = MagicMock()
    state.federation_engine = engine
    with audit_identity_scope("ada@example.com", "http"):
        return await run_materialize(
            state, "SELECT 1", Delivery(output_format=fmt, config=config), None
        )


# -- no object store configured: local disk, absolute URL -------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", FORMATS)
async def test_with_no_object_store_every_format_is_written_locally_at_the_request_base(
    org_folder, monkeypatch, fmt
):
    s3 = MagicMock()
    monkeypatch.setattr("boto3.client", lambda *a, **k: s3)
    engine = _ParquetEngine()
    with local_results.bound_request_base(BASE + "/"):
        handle = await _deliver(engine, fmt, _config(endpoint="", key=""))

    ext = local_results.FILE_FORMATS[fmt][1]
    match = re.fullmatch(
        rf"{re.escape(BASE)}/data/redirect-file/([0-9a-f]{{32}}{re.escape(ext)})",
        handle["redirect_url"],
    )
    assert match, handle["redirect_url"]
    assert handle["sink"] == "local"
    assert (org_folder / "results" / match.group(1)).is_file()
    assert engine.ctas == []
    s3.put_object.assert_not_called()


# -- an object store configured: every format goes there, presigned -----------------------------


def _store(monkeypatch) -> MagicMock:
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = PRESIGNED
    s3.list_objects_v2.side_effect = lambda Bucket, Prefix: {
        "Contents": [{"Key": f"{Prefix}/result"}]
    }
    monkeypatch.setattr("boto3.client", lambda *a, **k: s3)
    return s3


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize(
    "endpoint, key", [("http://minio:9000", "k"), ("", "aws-key")], ids=["endpoint", "aws"]
)
async def test_with_an_object_store_every_format_is_written_there_and_presigned(
    org_folder, monkeypatch, fmt, endpoint, key
):
    s3 = _store(monkeypatch)
    engine = _ParquetEngine()
    handle = await _deliver(engine, fmt, _config(endpoint=endpoint, key=key))

    assert handle["sink"] == "object-store"
    assert handle["redirect_url"] == PRESIGNED
    assert handle["row_count"] == 3
    assert handle["content_type"] == local_results.FILE_FORMATS[fmt][0]
    assert not (org_folder / "results").exists() or not any((org_folder / "results").iterdir())
    if fmt == "parquet":
        assert engine.ctas == [("SELECT 1", "parquet", None)]
        s3.put_object.assert_not_called()
    else:
        assert engine.ctas == []
        (call,) = s3.put_object.call_args_list
        assert call.kwargs["Bucket"] == "provisa-results"
        assert call.kwargs["Key"].endswith(local_results.FILE_FORMATS[fmt][1])
        assert call.kwargs["ContentType"] == local_results.FILE_FORMATS[fmt][0]
        assert call.kwargs["Body"]


@pytest.mark.asyncio
async def test_a_configured_store_that_cannot_be_reached_fails_every_format_by_name(
    org_folder, monkeypatch
):
    # The store the deployment configured is the results store for csv as for parquet: a store
    # that cannot provide its bucket fails the delivery, it never sends the result to local disk.
    from botocore.exceptions import EndpointConnectionError

    s3 = MagicMock()
    s3.head_bucket.side_effect = EndpointConnectionError(endpoint_url="http://localhost:9000")
    monkeypatch.setattr("boto3.client", lambda *a, **k: s3)
    with pytest.raises(redirect.RedirectStoreError, match="http://localhost:9000"):
        await _deliver(_ParquetEngine(), "csv", _config(endpoint="http://localhost:9000", key="k"))
    s3.put_object.assert_not_called()


# -- the base URI a local result is answered at ------------------------------------------------


def test_a_request_with_no_http_base_is_answered_at_the_public_url(org_folder, monkeypatch):
    from provisa.core import settings_registry

    real = settings_registry.value
    monkeypatch.setattr(
        settings_registry,
        "value",
        lambda k: "https://provisa.example.com/" if k == "server.public_url" else real(k),
    )
    with audit_identity_scope("ada@example.com", "pgwire"):
        handle = local_results.store_bytes(b"id\n1\n", "csv", 1, 60)
    assert handle["redirect_url"].startswith("https://provisa.example.com/data/redirect-file/")


def test_a_request_with_no_base_at_all_is_refused_by_name(org_folder, monkeypatch):
    from provisa.core import settings_registry

    real = settings_registry.value
    monkeypatch.setattr(
        settings_registry, "value", lambda k: None if k == "server.public_url" else real(k)
    )
    with audit_identity_scope("ada@example.com", "pgwire"):
        with pytest.raises(local_results.ResultAddressUnknown, match="server.public_url"):
            local_results.store_bytes(b"id\n1\n", "csv", 1, 60)
    assert not (org_folder / "results").exists() or not any((org_folder / "results").iterdir())

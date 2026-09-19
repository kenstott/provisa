# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8d1e6c-3a9f-4c2b-8d5e-1f7a3b6c9d2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.kaggle.client.

Kaggle datasets are ingested as plain csv/parquet Sources (Trino ATTACHes them live via
TrinoCsvConnector/TrinoParquetConnector, same as any other file source) — this client is the
only Kaggle-specific network surface: search, one dataset's file listing, and bundle download.
"""

# Requirements: REQ-1780, REQ-1781, REQ-1782, REQ-1783

import io
import zipfile

import httpx
import pytest
import respx

from provisa.kaggle.client import (
    KaggleApiError,
    download_dataset,
    get_dataset_metadata,
    search_datasets,
    validate_token,
)

TOKEN = "KGAT_test_token"


@respx.mock
async def test_search_datasets_sends_bearer_and_query():
    route = respx.get("https://www.kaggle.com/api/v1/datasets/list").mock(
        return_value=httpx.Response(200, json=[{"ref": "owner/ds", "title": "Ds"}])
    )
    result = await search_datasets(TOKEN, query="titanic", page=1)
    assert result == [{"ref": "owner/ds", "title": "Ds"}]
    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.url.params["search"] == "titanic"


@respx.mock
async def test_search_datasets_raises_on_error():
    respx.get("https://www.kaggle.com/api/v1/datasets/list").mock(
        return_value=httpx.Response(403, text="Forbidden")
    )
    with pytest.raises(KaggleApiError):
        await search_datasets(TOKEN)


@respx.mock
async def test_get_dataset_metadata():
    respx.get("https://www.kaggle.com/api/v1/datasets/list/owner/ds").mock(
        return_value=httpx.Response(200, json={"datasetFiles": [{"name": "a.csv"}]})
    )
    meta = await get_dataset_metadata(TOKEN, "owner", "ds")
    assert meta["datasetFiles"][0]["name"] == "a.csv"


@respx.mock
async def test_get_dataset_metadata_raises_on_error():
    respx.get("https://www.kaggle.com/api/v1/datasets/list/owner/missing").mock(
        return_value=httpx.Response(404, text="Not found")
    )
    with pytest.raises(KaggleApiError):
        await get_dataset_metadata(TOKEN, "owner", "missing")


@respx.mock
async def test_download_dataset_returns_bytes():
    respx.get("https://www.kaggle.com/api/v1/datasets/download/owner/ds").mock(
        return_value=httpx.Response(200, content=b"zip-bytes")
    )
    data = await download_dataset(TOKEN, "owner", "ds")
    assert data == b"zip-bytes"


@respx.mock
async def test_download_dataset_raises_on_error():
    respx.get("https://www.kaggle.com/api/v1/datasets/download/owner/ds").mock(
        return_value=httpx.Response(403, text="Forbidden")
    )
    with pytest.raises(KaggleApiError):
        await download_dataset(TOKEN, "owner", "ds")


@respx.mock
async def test_validate_token_true_past_auth():
    # datasets/list returns 200 for ANY token (verified live 2026-09-19 — it never rejects a bad
    # token), so validate_token uses datasets/create/new: a garbage token 401s before Kaggle even
    # looks at the body; a real token gets past auth to a payload-validation error instead.
    respx.post("https://www.kaggle.com/api/v1/datasets/create/new").mock(
        return_value=httpx.Response(200, json={"status": "Error", "error": "Invalid Owner Id"})
    )
    assert await validate_token(TOKEN) is True


@respx.mock
async def test_validate_token_false_on_401():
    respx.post("https://www.kaggle.com/api/v1/datasets/create/new").mock(
        return_value=httpx.Response(401, json={"code": 401, "message": "Unauthorized access"})
    )
    assert await validate_token("bad-token") is False


# -- downloader (REQ-1780/1781/1782, v1 csv/parquet only) -------------------


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


@respx.mock
async def test_stage_dataset_multi_csv(tmp_path, monkeypatch):
    from provisa.kaggle.downloader import stage_dataset

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    respx.get("https://www.kaggle.com/api/v1/datasets/list/acme/orders").mock(
        return_value=httpx.Response(
            200,
            json={"datasetFiles": [{"name": "orders.csv"}, {"name": "customers.csv"}]},
        )
    )
    bundle = _zip_bytes({"orders.csv": b"id\n1\n", "customers.csv": b"id\n2\n"})
    respx.get("https://www.kaggle.com/api/v1/datasets/download/acme/orders").mock(
        return_value=httpx.Response(200, content=bundle)
    )

    root = await stage_dataset(TOKEN, "acme", "orders")

    assert root == tmp_path / "kaggle" / "acme" / "orders"
    assert (root / "orders" / "orders.csv").read_bytes() == b"id\n1\n"
    assert (root / "customers" / "customers.csv").read_bytes() == b"id\n2\n"


@respx.mock
async def test_stage_dataset_rejects_sqlite_bundle(tmp_path, monkeypatch):
    from provisa.kaggle.downloader import UnsupportedKaggleDataset, stage_dataset

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    respx.get("https://www.kaggle.com/api/v1/datasets/list/acme/db").mock(
        return_value=httpx.Response(200, json={"datasetFiles": [{"name": "data.sqlite"}]})
    )
    with pytest.raises(UnsupportedKaggleDataset, match="SQLite"):
        await stage_dataset(TOKEN, "acme", "db")
    assert not (tmp_path / "kaggle").exists()


@respx.mock
async def test_stage_dataset_ignores_unsupported_non_data_files(tmp_path, monkeypatch):
    from provisa.kaggle.downloader import stage_dataset

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    respx.get("https://www.kaggle.com/api/v1/datasets/list/acme/mixed").mock(
        return_value=httpx.Response(
            200, json={"datasetFiles": [{"name": "readme.md"}, {"name": "data.csv"}]}
        )
    )
    bundle = _zip_bytes({"readme.md": b"# hi", "data.csv": b"x\n1\n"})
    respx.get("https://www.kaggle.com/api/v1/datasets/download/acme/mixed").mock(
        return_value=httpx.Response(200, content=bundle)
    )
    root = await stage_dataset(TOKEN, "acme", "mixed")
    assert (root / "data" / "data.csv").read_bytes() == b"x\n1\n"
    assert not (root / "readme").exists()


@respx.mock
async def test_stage_dataset_no_csv_parquet_raises(tmp_path, monkeypatch):
    from provisa.kaggle.downloader import UnsupportedKaggleDataset, stage_dataset

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    respx.get("https://www.kaggle.com/api/v1/datasets/list/acme/empty").mock(
        return_value=httpx.Response(200, json={"datasetFiles": [{"name": "readme.md"}]})
    )
    with pytest.raises(UnsupportedKaggleDataset, match="no CSV or Parquet"):
        await stage_dataset(TOKEN, "acme", "empty")

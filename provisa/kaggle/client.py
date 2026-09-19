# Copyright (c) 2026 Kenneth Stott
# Canary: 5d8e2a1b-4c6f-4a9d-9e3b-7f1c8a2d5e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kaggle REST API v1 client (REQ-1780).

Kaggle has no live query API — it is a file-download platform. This wraps the three
v1 endpoints the connector needs: dataset search, one dataset's file listing, and the
dataset bundle download. Auth is ``Authorization: Bearer <token>`` (verified against the
live API against ``datasets/list``).
"""

from __future__ import annotations

import httpx

# Requirements: REQ-1780, REQ-1781, REQ-1782, REQ-1783

BASE_URL = "https://www.kaggle.com/api/v1"


class KaggleApiError(Exception):
    """Kaggle API returned a non-2xx response."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"Kaggle API HTTP {status_code}: {body[:300]}")


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def search_datasets(  # REQ-1783
    token: str, query: str = "", page: int = 1, page_size: int = 20
) -> list[dict]:
    """Wraps ``GET /datasets/list`` — live full-catalog search backing the dataset picker."""
    params: dict[str, str | int] = {"page": page, "pageSize": page_size}
    if query:
        params["search"] = query
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{BASE_URL}/datasets/list", params=params, headers=_headers(token))
        if resp.status_code != 200:
            raise KaggleApiError(resp.status_code, resp.text)
        return resp.json()


async def get_dataset_metadata(token: str, owner: str, ref: str) -> dict:  # REQ-1781
    """Wraps ``GET /datasets/list/{owner}/{ref}`` — the per-file listing for one bundle
    (``{"datasetFiles": [{"name": ...}, ...]}``).

    Not ``GET /datasets/view/{owner}/{ref}`` — verified live 2026-09-19: that endpoint's own
    ``files`` field is always empty; ``datasets/list/{owner}/{ref}`` is the one that actually
    returns the file listing (confirmed against real Kaggle datasets, e.g. uciml/iris)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{BASE_URL}/datasets/list/{owner}/{ref}", headers=_headers(token))
        if resp.status_code != 200:
            raise KaggleApiError(resp.status_code, resp.text)
        return resp.json()


async def download_dataset(token: str, owner: str, ref: str) -> bytes:  # REQ-1782
    """Wraps ``GET /datasets/download/{owner}/{ref}`` — the bundle archive (zip, or a bare
    SQLite file for a single-sqlite-file dataset)."""
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(
            f"{BASE_URL}/datasets/download/{owner}/{ref}", headers=_headers(token)
        )
        if resp.status_code != 200:
            raise KaggleApiError(resp.status_code, resp.text)
        return resp.content


async def validate_token(token: str) -> bool:  # REQ-1783
    """Lightweight live check for the token-gated UI step.

    NOT ``datasets/list`` (what REQ-1783 originally proposed, and what an early ``search_datasets``
    smoke test seemed to confirm): verified live 2026-09-19 that ``GET /datasets/list`` returns
    HTTP 200 for public search results with ANY token — malformed, garbage, or even no
    ``Authorization`` header at all. It never rejects a bad token, so it cannot gate anything.

    ``POST /datasets/create/new`` DOES require real auth (verified live: a garbage/invalid token
    gets ``401 {"message": "Unauthorized access"}``; a valid token gets past auth straight to a
    payload-validation error, ``200 {"status": "Error", "error": "Invalid Owner Id"}`` for the
    empty body this sends) — no dataset is ever created because the intentionally-empty body
    fails Kaggle's own validation before anything is persisted. This is the actual live token
    check; returns True/False rather than raising, since the UI step's whole purpose is a clear
    invalid-token message rather than a propagated raw HTTP failure.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{BASE_URL}/datasets/create/new",
            json={},
            headers=_headers(token),
        )
        return resp.status_code != 401

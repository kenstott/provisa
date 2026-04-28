# Copyright (c) 2026 Kenneth Stott
# Canary: c2d3e4f5-a6b7-8901-2345-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""POST /admin/sources/crawl — directory crawl endpoint (Issue #28)."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources", tags=["admin", "sources"])


class CrawlRequest(BaseModel):
    path: str
    depth: int | None = None  # None = unlimited


class CrawlResponse(BaseModel):
    path: str
    total_files: int
    total_tables: int
    discovered: list[dict]


@router.post("/crawl", response_model=CrawlResponse)
async def crawl_directory_endpoint(body: CrawlRequest) -> CrawlResponse:
    """Crawl *path* recursively and return discovered file-based tables.

    Supports local paths and fsspec URIs (``s3://``, ``ftp://``, etc.).
    Each entry in ``discovered`` contains ``name``, ``path``, ``type``,
    and ``tables`` (list of ``{name, columns}``).
    """
    from provisa.file_source.crawler import crawl_directory

    try:
        discovered = crawl_directory(body.path, body.depth)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    total_tables = sum(len(entry["tables"]) for entry in discovered)

    log.info(
        "Crawled %r → %d files, %d tables",
        body.path,
        len(discovered),
        total_tables,
    )

    return CrawlResponse(
        path=body.path,
        total_files=len(discovered),
        total_tables=total_tables,
        discovered=discovered,
    )

# Copyright (c) 2026 Kenneth Stott
# Canary: 7fa4921d-4e44-4148-abed-f99cc0ddb260
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON:API pagination helpers."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode


DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 1000


def parse_page_params(params: dict[str, str]) -> tuple[int, int]:
    """Parse page[number] and page[size] from query params.

    Returns (page_number, page_size). page_number is 1-based.
    """
    page_number = 1
    page_size = DEFAULT_PAGE_SIZE

    raw_number = params.get("page[number]")
    if raw_number is not None:
        try:
            page_number = max(1, int(raw_number))
        except (ValueError, TypeError):
            page_number = 1

    raw_size = params.get("page[size]")
    if raw_size is not None:
        try:
            page_size = max(1, min(int(raw_size), MAX_PAGE_SIZE))
        except (ValueError, TypeError):
            page_size = DEFAULT_PAGE_SIZE

    return page_number, page_size


def page_to_limit_offset(page_number: int, page_size: int) -> tuple[int, int]:
    """Convert 1-based page number + size to limit/offset."""
    offset = (page_number - 1) * page_size
    return page_size, offset


def build_pagination_links(
    base_path: str,
    page_number: int,
    page_size: int,
    result_count: int,
    extra_params: dict[str, str] | None = None,
) -> dict[str, str | None]:
    """Build JSON:API pagination links (self, first, prev, next).

    next is None when result_count < page_size (last page).
    """
    def _url(pn: int) -> str:
        p = {"page[number]": str(pn), "page[size]": str(page_size)}
        if extra_params:
            p.update(extra_params)
        return f"{base_path}?{urlencode(p)}"

    links: dict[str, str | None] = {
        "self": _url(page_number),
        "first": _url(1),
    }

    if page_number > 1:
        links["prev"] = _url(page_number - 1)
    else:
        links["prev"] = None

    if result_count >= page_size:
        links["next"] = _url(page_number + 1)
    else:
        links["next"] = None

    return links

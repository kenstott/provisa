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

# Requirements: REQ-257

from __future__ import annotations

from urllib.parse import urlencode


DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 1000


def parse_page_params(params: dict[str, str]) -> dict[str, int]:  # REQ-257
    """Parse page[number] and page[size] from query params.

    Returns {"number": page_number, "size": page_size}. page_number is 1-based.
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

    return {"number": page_number, "size": page_size}


def page_to_limit_offset(page: dict[str, int]) -> tuple[int, int]:  # REQ-257
    """Convert page dict {"number": N, "size": S} to (limit, offset)."""
    page_number = page["number"]
    page_size = page["size"]
    offset = (page_number - 1) * page_size
    return page_size, offset


def build_pagination_links(  # REQ-257
    base_url: str,
    page_number: int,
    page_size: int,
    total: int,
    query_params: dict[str, str] | None = None,
) -> dict[str, str | None]:
    """Build JSON:API pagination links (self, first, prev, next, last).

    next is None when on the last page.
    """
    extra = query_params or {}
    last_page = max(1, (total + page_size - 1) // page_size)

    def _url(pn: int) -> str:
        p = {"page[number]": str(pn), "page[size]": str(page_size)}
        p.update(extra)
        return f"{base_url}?{urlencode(p)}"

    links: dict[str, str | None] = {
        "self": _url(page_number),
        "first": _url(1),
        "last": _url(last_page),
    }

    if page_number > 1:
        links["prev"] = _url(page_number - 1)
    else:
        links["prev"] = None

    if page_number < last_page:
        links["next"] = _url(page_number + 1)
    else:
        links["next"] = None

    return links

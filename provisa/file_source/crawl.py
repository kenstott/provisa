# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f41-7c3a-4d9e-a6f1-2e9c8b7d4a10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An HTML crawl declared on a ``files`` source (REQ-1960).

A ``files`` source whose mapping carries ``crawl`` reads web pages before it looks for files:
the file adapter follows links from the start URLs, and each HTML table and each linked data
file (CSV, TSV, Excel, JSON, Parquet) lands in the source's directory as an ordinary file. From
there it is a table like any other file of the source, and nothing it offers is registered until
a steward registers it.

This module is the one description of the crawl's settings: the names a source's mapping
carries, the names the adapter reads, and where a crawl's files land. Every engine's ``files``
connector passes the same operand (REQ-1730).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

# Requirements: REQ-1960

#: A crawl setting as a source's mapping carries it -> as the file adapter reads it.
CRAWL_SETTINGS: dict[str, str] = {
    "start_urls": "startUrls",
    "max_depth": "maxDepth",
    "max_pages": "maxPages",
    "request_delay": "requestDelay",
    "user_agent": "userAgent",
    "content_selector": "contentSelector",
    "remove_selectors": "removeSelectors",
    "link_selector": "linkSelector",
    "link_exclude_patterns": "linkExcludePatterns",
    "follow_external_links": "followExternalLinks",
    "allowed_domains": "allowedDomains",
    "table_selector": "tableSelector",
    "generate_tables_from_html": "generateTablesFromHtml",
    "html_table_min_rows": "htmlTableMinRows",
    "html_table_max_rows": "htmlTableMaxRows",
    "allowed_file_extensions": "allowedFileExtensions",
    "html_cache_ttl": "htmlCacheTTL",
    "max_html_size": "maxHtmlSize",
    "max_data_file_size": "maxDataFileSize",
}

_LISTS = frozenset(
    {
        "start_urls",
        "remove_selectors",
        "link_exclude_patterns",
        "allowed_domains",
        "allowed_file_extensions",
    }
)


class InvalidCrawl(ValueError):
    """A source's crawl settings cannot be passed to the file adapter."""


def crawl_operand(source_id: str, mapping: dict | None) -> dict | None:
    """The file adapter's ``crawl`` operand for a source's mapping, or None when the source
    declares no crawl. A setting the adapter does not read is refused by name: it would
    otherwise be dropped, and the crawl would run without it."""
    settings = (mapping or {}).get("crawl")
    if settings is None:
        return None
    if not isinstance(settings, dict):
        raise InvalidCrawl(f"files source {source_id!r}: mapping.crawl must be an object")
    unknown = sorted(set(settings) - set(CRAWL_SETTINGS))
    if unknown:
        raise InvalidCrawl(
            f"files source {source_id!r}: unknown crawl setting(s) {unknown}; "
            f"known: {sorted(CRAWL_SETTINGS)}"
        )
    operand: dict[str, Any] = {}
    for name, value in settings.items():
        if name in _LISTS and not isinstance(value, list):
            raise InvalidCrawl(f"files source {source_id!r}: crawl.{name} must be a list")
        operand[CRAWL_SETTINGS[name]] = value
    if not operand.get("startUrls"):
        raise InvalidCrawl(f"files source {source_id!r}: crawl.start_urls names no page")
    return operand


def crawl_landing_directory(source_id: str) -> Path:
    """Where a crawling source's files land when the source names no directory of its own: a
    directory of the source's under the data directory, as a staged dataset has one."""
    root = Path(os.environ.get("PROVISA_DATA_DIR") or (Path.home() / ".provisa"))
    return root / "crawl" / source_id

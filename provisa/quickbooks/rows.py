# Copyright (c) 2026 Kenneth Stott
# Canary: c5f02e83-6d1b-47a9-9e30-28b4d7f1a6e2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rows of one page a QuickBooks query answers (REQ-2197), by the rule the entity catalog
names its columns by (``quickbooks.entities``).

A page is ``{"QueryResponse": {"<Entity>": [...], "startPosition": 1, "maxResults": n},
"time": ...}``; a query that matches nothing answers ``{"QueryResponse": {}}``."""

# Requirements: REQ-2197
from __future__ import annotations

from typing import Any

from provisa.quickbooks.entities import METADATA, snake

#: The response normalizer a QuickBooks table's endpoint names (``api_source.normalizers``).
NORMALIZER = "quickbooks_query"

# What a QueryResponse carries beside its records.
_PAGE_KEYS = frozenset({"startPosition", "maxResults", "totalCount"})
_METADATA_COLUMNS = {"CreateTime": "meta_create_time", "LastUpdatedTime": "meta_last_updated_time"}


def records(page: Any) -> list[dict]:
    """The records of one answered page, as QuickBooks wrote them."""
    if not isinstance(page, dict) or not isinstance(page.get("QueryResponse"), dict):
        raise ValueError("a QuickBooks query answer has no QueryResponse")
    lists = [v for k, v in page["QueryResponse"].items() if k not in _PAGE_KEYS]
    if len(lists) > 1:
        raise ValueError("a QuickBooks query answer holds records of more than one entity")
    found = lists[0] if lists else []
    if not isinstance(found, list):
        raise ValueError("a QuickBooks query answer's records are not a list")
    return [r for r in found if isinstance(r, dict)]


def row(record: dict) -> dict:
    """One record as a row: scalars by their snake-case names, a reference as its ``_value`` and
    ``_name``, ``MetaData`` as its two times, any other object or list as itself (a JSON
    column)."""
    out: dict[str, Any] = {}
    for key, value in record.items():
        if key == METADATA and isinstance(value, dict):
            for field, column in _METADATA_COLUMNS.items():
                out[column] = value.get(field)
        elif key.endswith("Ref") and isinstance(value, dict):
            base = snake(key)
            out[f"{base}_value"] = value.get("value")
            out[f"{base}_name"] = value.get("name")
        else:
            out[snake(key)] = value
    return out


def page_rows(page: Any) -> list[dict]:
    """The rows of one answered page: the response normalizer of a QuickBooks table."""
    return [row(r) for r in records(page)]

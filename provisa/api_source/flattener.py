# Copyright (c) 2026 Kenneth Stott
# Canary: 4f17878b-3843-4602-9076-94d664cce22e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Response flattening: API response data -> row dicts (Phase U)."""

from __future__ import annotations

import json

from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.api_source.normalizers import get_normalizer


def _navigate_path(data: object, path: str | None) -> object:
    """Navigate to a nested value via dot-notation path.

    e.g. "data.users" navigates data["data"]["users"].
    """
    if not path:
        return data
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            raise KeyError(f"Cannot navigate path {path!r}: {part!r} not found in {type(current).__name__}")
    return current


def _extract_value(row: dict, col: ApiColumn) -> object:
    """Extract and coerce a single column value from a row dict."""
    value = row.get(col.name)
    if value is None:
        return None
    if col.type == ApiColumnType.jsonb:
        # Objects and arrays stored as JSON strings for JSONB
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    if col.type == ApiColumnType.integer:
        return int(value)
    if col.type == ApiColumnType.number:
        return float(value)
    if col.type == ApiColumnType.boolean:
        return bool(value)
    return str(value)


def flatten_response(
    data: object,
    root_path: str | None,
    columns: list[ApiColumn],
    response_normalizer: str | None = None,
) -> list[dict]:
    """Flatten API response data into row dicts suitable for PG insertion.

    If response_normalizer is set, applies it before root_path navigation.
    Navigates to root_path, then for each item extracts column values.
    Primitives become native values; objects/arrays become JSON strings.
    """
    if response_normalizer:
        # Normalizer returns ready-made row dicts; skip root navigation.
        items = get_normalizer(response_normalizer)(data)
        rows: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            row: dict = {}
            for col in columns:
                row[col.name] = _extract_value(item, col)
            rows.append(row)
        return rows

    root = _navigate_path(data, root_path)

    if isinstance(root, dict):
        items = [root]
    elif isinstance(root, list):
        items = root
    else:
        raise ValueError(f"Expected dict or list at root path {root_path!r}, got {type(root).__name__}")

    rows: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        row: dict = {}
        for col in columns:
            row[col.name] = _extract_value(item, col)
        rows.append(row)

    return rows

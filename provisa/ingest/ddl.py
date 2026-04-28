# Copyright (c) 2026 Kenneth Stott
# Canary: f0fc8dbb-21fb-4b98-a105-9648a1dd5764
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DDL generation for ingest backing tables (Phase AS, REQ-332)."""

from __future__ import annotations

# Allowlisted SQL column types — any other value falls back to TEXT.
_ALLOWED_TYPES: frozenset[str] = frozenset({
    "text", "integer", "bigint", "real", "numeric", "boolean",
    "timestamp", "timestamptz", "jsonb", "date", "uuid", "smallint",
    "float", "double precision", "varchar",
})

_DEFAULT_TYPE = "text"


def _safe_type(data_type: str | None) -> str:
    """Normalise and validate a steward-declared SQL type."""
    if not data_type:
        return _DEFAULT_TYPE
    dt = data_type.lower().strip()
    return dt if dt in _ALLOWED_TYPES else _DEFAULT_TYPE


def generate_create_table(table_name: str, columns: list[dict]) -> str:
    """Return CREATE TABLE IF NOT EXISTS DDL for an ingest backing table.

    Args:
        table_name: Target table name (already sanitised by caller).
        columns: List of ``{column_name, data_type}`` dicts from steward config.

    System columns injected automatically:
        - ``id SERIAL PRIMARY KEY``
        - ``_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()``
        - ``_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()``

    The ``_updated_at`` column is the watermark used by the subscription provider.
    """
    col_defs: list[str] = ["id SERIAL PRIMARY KEY"]
    for col in columns:
        name = col.get("column_name") or col.get("name", "")
        if not name or name.startswith("_"):
            continue
        dtype = _safe_type(col.get("data_type"))
        col_defs.append(f"{name} {dtype.upper()}")
    col_defs.append("_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
    col_defs.append("_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
    joined = ",\n    ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {joined}\n)"


def extract_value(payload: dict, path: str | None) -> object:
    """Extract a value from a nested dict using dot-notation *path*.

    ``path`` of ``"resourceLogs.0.resource.attributes"`` walks into
    ``payload["resourceLogs"][0]["resource"]["attributes"]``.

    Returns ``None`` if any segment is missing or the path is empty.
    """
    if not path:
        return None
    cur: object = payload
    for seg in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(seg)
        elif isinstance(cur, list):
            try:
                cur = cur[int(seg)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur

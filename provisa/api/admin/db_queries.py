# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e4f1a-7c3d-4a8b-9e0f-5d1c6a2b7e3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared asyncpg query helpers for schema rebuild."""

from __future__ import annotations

import re

import asyncpg
import inflect

_inflect = inflect.engine()


def _to_singular(camel: str) -> str:
    """Singularize camelCase word using inflect with round-trip validation."""
    candidate = _inflect.singular_noun(camel)
    if candidate is False:
        return camel  # already singular
    # Validate: if round-trip doesn't reconstruct original, inflect got it wrong
    if _inflect.plural(candidate) == camel:
        return candidate
    return camel


def derive_graphql_alias(target_table_name: str, cardinality: str) -> str | None:
    """Derive GraphQL field name from target table name + cardinality.

    Rules:
    - camelCase of target_table_name (snake_case → camelCase)
    - Normalize to singular, then:
      - one-to-many → pluralize via inflect
      - many-to-one → keep singular
    """
    if not target_table_name:
        return None
    parts = re.split(r'[_\s]+', target_table_name.strip())
    camel = parts[0].lower() + "".join(p.capitalize() for p in parts[1:])
    singular = _to_singular(camel)
    if cardinality == "one-to-many":
        return _inflect.plural(singular)
    return singular


def derive_cypher_alias(source_column: str, cardinality: str) -> str:
    """Derive Cypher relationship type from FK column + cardinality.

    Rules:
    - Strip _id/_fk/_pk suffix from source_column
    - Singularize with inflect (same helper as derive_graphql_alias)
    - Prepend direction verb based on cardinality:
      one-to-many  → HAS_<ENTITY>
      many-to-one  → BELONGS_TO_<ENTITY>
      other        → <ENTITY>
    """
    col = (source_column or "").strip().lower()
    for suffix in ("_id", "_fk", "_pk"):
        if col.endswith(suffix) and len(col) > len(suffix):
            col = col[: -len(suffix)]
            break
    singular = _to_singular(col) if col else col
    entity = (singular or col).upper()
    if cardinality == "one-to-many":
        return f"HAS_{entity}"
    if cardinality == "many-to-one":
        return f"BELONGS_TO_{entity}"
    return entity


def parse_mask_value(raw: str | None) -> object:
    """Parse a stored mask value string back to a Python value."""
    if raw is None:
        return None
    if raw == "None":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


async def fetch_tables(conn: asyncpg.Connection) -> list[dict]:
    """Fetch registered tables with columns."""
    rows = await conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name, governance, "
        "alias, description, column_presets "
        "FROM registered_tables ORDER BY id"
    )
    tables = []
    for row in rows:
        table = dict(row)
        col_rows = await conn.fetch(
            "SELECT column_name, visible_to, writable_by, unmasked_to, "
            "mask_type, alias, description, path, is_primary_key "
            "FROM table_columns WHERE table_id = $1 ORDER BY id",
            row["id"],
        )
        table["column_presets"] = list(row.get("column_presets") or [])
        table["columns"] = [
            {
                "column_name": r["column_name"],
                "visible_to": list(r["visible_to"]),
                "writable_by": list(r.get("writable_by") or []),
                "unmasked_to": list(r.get("unmasked_to") or []),
                "mask_type": r.get("mask_type"),
                "alias": r["alias"],
                "description": r["description"],
                "path": r["path"],
                "is_primary_key": bool(r.get("is_primary_key") or False),
            }
            for r in col_rows
        ]
        tables.append(table)
    return tables


async def fetch_relationships(conn: asyncpg.Connection) -> list[dict]:
    """Fetch relationships, computing graphql_alias when not persisted."""
    rows = await conn.fetch(
        "SELECT r.id, r.source_table_id, r.target_table_id, r.source_column, "
        "r.target_column, r.cardinality, r.materialize, r.refresh_interval, "
        "r.target_function_name, r.function_arg, r.alias, r.graphql_alias, "
        "t.table_name AS target_table_name "
        "FROM relationships r "
        "LEFT JOIN registered_tables t ON t.id = r.target_table_id"
    )
    result = []
    for r in rows:
        d = dict(r)
        if not d.get("graphql_alias"):
            d["graphql_alias"] = derive_graphql_alias(
                d.get("target_table_name") or "", d.get("cardinality") or ""
            )
        if not d.get("alias"):
            d["computed_cypher_alias"] = derive_cypher_alias(
                d.get("source_column") or "", d.get("cardinality") or ""
            )
        else:
            d["computed_cypher_alias"] = None
        result.append(d)
    return result

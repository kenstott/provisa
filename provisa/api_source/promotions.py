# Copyright (c) 2026 Kenneth Stott
# Canary: 35b738c4-2166-44be-a4b0-52e0151ffeed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSONB field promotion: extract nested fields as generated columns (Phase U)."""

# Requirements: REQ-119

from __future__ import annotations

from provisa.api_source.models import PromotionConfig


_PG_CAST_MAP: dict[str, str] = {
    "integer": "::integer",
    "numeric": "::numeric",
    "boolean": "::boolean",
    "timestamptz": "::timestamptz",
    "text": "",
}


def dot_path_to_pg_expression(column: str, path: str, cast_source: bool = False) -> str:  # REQ-119
    """Convert a dot-path to a PG JSONB extraction expression.

    e.g. dot_path_to_pg_expression("data", "a.b.c")
         -> (data->'a'->'b'->>'c')

    ``cast_source`` casts the source column to ``jsonb`` first (``data::jsonb->...``)
    — needed where JSON is stored as text/varchar (e.g. the engine api-cache table).
    """
    if not path:
        raise ValueError(f"Empty dot-path for column {column!r}")
    base = f"{column}::jsonb" if cast_source else column
    parts = path.split(".")
    if len(parts) == 1:
        return f"({base}->>{parts[0]!r})"

    # All but last use -> (returns jsonb), last uses ->> (returns text)
    arrows = "->".join(f"'{p}'" for p in parts[:-1])
    return f"({base}->{arrows}->>'{parts[-1]}')"


def generate_promotion_ddl(  # REQ-119
    table_name: str,
    promotions: list[PromotionConfig],
    cast_source: bool = False,
) -> list[str]:
    """Generate ALTER TABLE statements with GENERATED ALWAYS AS clauses."""
    stmts: list[str] = []
    for p in promotions:
        expr = dot_path_to_pg_expression(p.jsonb_column, p.field, cast_source=cast_source)
        cast = _PG_CAST_MAP.get(p.target_type, "")
        generated_expr = f"{expr}{cast}"
        stmt = (
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
            f"{p.target_column} {p.target_type.upper()} "
            f"GENERATED ALWAYS AS ({generated_expr}) STORED;"
        )
        stmts.append(stmt)
    return stmts


async def apply_promotions(  # REQ-119
    conn,
    table_name: str,
    promotions: list[PromotionConfig],
    cast_source: bool = False,
) -> int:
    """Execute promotion DDL against a Postgres table; return the count applied.

    Each statement is ``ADD COLUMN IF NOT EXISTS ... GENERATED ALWAYS AS ... STORED``,
    so this is idempotent and safe to call repeatedly. ``conn`` is an asyncpg
    connection to the database hosting the table. ``cast_source`` casts the JSON
    source column to ``jsonb`` (for varchar-stored JSON). No-op when empty.
    """
    if not promotions:
        return 0
    stmts = generate_promotion_ddl(table_name, promotions, cast_source=cast_source)
    for stmt in stmts:
        await conn.execute(stmt)
    return len(stmts)

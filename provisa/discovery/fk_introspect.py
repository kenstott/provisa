# Copyright (c) 2026 Kenneth Stott
# Canary: 49ba1bbe-25c1-44e9-af30-ac6b60255c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""FK introspection for automatic relationship discovery (Hasura-style).

Queries information_schema (PostgreSQL/MySQL) or PRAGMA (SQLite) to find
FK constraints for a registered table, then inserts BOTH directions of each
relationship with ON CONFLICT DO NOTHING so existing ones are never overwritten.

Relationship IDs:
  many-to-one  fk__{fk_table}__{fk_column}__to__{ref_table}
  one-to-many  fk__{ref_table}__from__{fk_table}__{fk_column}

Alias naming follows Hasura convention:
  many-to-one  → ref_table name  (e.g. "users")
  one-to-many  → fk_table name   (e.g. "orders")
  Disambiguated with _by_{fk_column} when alias would collide.
"""

from __future__ import annotations

import logging

import asyncpg

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL templates per dialect
# ---------------------------------------------------------------------------

# Outbound: FKs defined ON this table → ref table (many-to-one)
_PG_OUTBOUND = """
SELECT
    kcu.column_name          AS fk_column,
    ccu.table_name           AS ref_table,
    ccu.column_name          AS ref_column
FROM information_schema.table_constraints      AS tc
JOIN information_schema.key_column_usage       AS kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
   AND ccu.table_schema   = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema    = $1
  AND tc.table_name      = $2
"""

# Inbound: OTHER tables have FKs pointing TO this table (one-to-many)
_PG_INBOUND = """
SELECT
    kcu.table_name           AS fk_table,
    kcu.column_name          AS fk_column,
    ccu.column_name          AS ref_column
FROM information_schema.table_constraints      AS tc
JOIN information_schema.key_column_usage       AS kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
   AND ccu.table_schema   = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND ccu.table_schema   = $1
  AND ccu.table_name     = $2
"""


async def _pg_fks(
    driver,
    schema_name: str,
    table_name: str,
) -> list[dict]:
    """Return normalized FK rows from PostgreSQL information_schema.

    Each dict: fk_table, fk_column, ref_table, ref_column.
    Deduplicates across outbound+inbound so registering either table
    produces the same canonical set.
    """
    seen: set[tuple] = set()
    results: list[dict] = []

    out_rows = await driver.execute(_PG_OUTBOUND, [schema_name, table_name])
    for row in out_rows.rows:
        fk_col, ref_tbl, ref_col = row
        key = (table_name, fk_col, ref_tbl, ref_col)
        if key not in seen:
            seen.add(key)
            results.append({"fk_table": table_name, "fk_column": fk_col, "ref_table": ref_tbl, "ref_column": ref_col})

    in_rows = await driver.execute(_PG_INBOUND, [schema_name, table_name])
    for row in in_rows.rows:
        fk_tbl, fk_col, ref_col = row
        key = (fk_tbl, fk_col, table_name, ref_col)
        if key not in seen:
            seen.add(key)
            results.append({"fk_table": fk_tbl, "fk_column": fk_col, "ref_table": table_name, "ref_column": ref_col})

    return results


async def _sqlite_fks(
    driver,
    schema_name: str,
    table_name: str,
) -> list[dict]:
    """Return FK rows from SQLite PRAGMA foreign_key_list."""
    results: list[dict] = []
    rows = await driver.execute(f'PRAGMA foreign_key_list("{table_name}")', [])
    for row in rows.rows:
        # columns: id, seq, table, from, to, on_update, on_delete, match
        _id, _seq, ref_tbl, fk_col, ref_col = row[0], row[1], row[2], row[3], row[4]
        results.append({"fk_table": table_name, "fk_column": fk_col, "ref_table": ref_tbl, "ref_column": ref_col or "id"})
    return results


def _m2o_alias(ref_table: str) -> str:
    """Hasura-style object relationship alias: ref table name."""
    return ref_table


def _o2m_alias(fk_table: str) -> str:
    """Hasura-style array relationship alias: FK table name."""
    return fk_table


async def _insert_rel(
    conn: asyncpg.Connection,
    rel_id: str,
    src_id: int,
    tgt_id: int,
    src_col: str,
    tgt_col: str,
    cardinality: str,
    alias: str,
) -> bool:
    """Insert a relationship; return True if newly inserted."""
    result = await conn.execute(
        """
        INSERT INTO relationships
            (id, source_table_id, target_table_id, source_column, target_column, cardinality, alias)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        """,
        rel_id, src_id, tgt_id, src_col, tgt_col, cardinality, alias,
    )
    return result == "INSERT 0 1"


async def auto_register_fk_relationships(
    source_pools,
    source_type: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    config_conn: asyncpg.Connection,
) -> int:
    """Introspect FK constraints and insert BOTH relationship directions.

    Uses ON CONFLICT DO NOTHING — never overwrites manually configured rels.
    Returns count of newly inserted relationships.
    """
    if not source_pools.has(source_id):
        return 0

    driver = source_pools.get(source_id)
    source_type_lower = source_type.lower()

    try:
        if source_type_lower in ("postgresql", "postgres", "mysql", "mariadb"):
            fk_rows = await _pg_fks(driver, schema_name, table_name)
        elif source_type_lower == "sqlite":
            fk_rows = await _sqlite_fks(driver, schema_name, table_name)
        else:
            return 0
    except Exception:
        _log.debug("FK introspection failed for %s.%s (%s)", schema_name, table_name, source_type, exc_info=True)
        return 0

    if not fk_rows:
        return 0

    # Collect used aliases per source table to detect collisions
    m2o_aliases_used: dict[int, set[str]] = {}
    o2m_aliases_used: dict[int, set[str]] = {}

    inserted = 0
    for fk in fk_rows:
        fk_tbl, fk_col = fk["fk_table"], fk["fk_column"]
        ref_tbl, ref_col = fk["ref_table"], fk["ref_column"]

        fk_row = await config_conn.fetchrow(
            "SELECT id FROM registered_tables WHERE source_id = $1 AND table_name = $2",
            source_id, fk_tbl,
        )
        ref_row = await config_conn.fetchrow(
            "SELECT id FROM registered_tables WHERE source_id = $1 AND table_name = $2",
            source_id, ref_tbl,
        )

        m2o_id = f"fk__{fk_tbl}__{fk_col}__to__{ref_tbl}"
        o2m_id = f"fk__{ref_tbl}__from__{fk_tbl}__{fk_col}"

        # many-to-one: fk_table.fk_col → ref_table.ref_col
        if fk_row and ref_row:
            fk_id, ref_id = fk_row["id"], ref_row["id"]

            m2o_aliases_used.setdefault(fk_id, set())
            base_m2o = _m2o_alias(ref_tbl)
            alias_m2o = base_m2o if base_m2o not in m2o_aliases_used[fk_id] else f"{base_m2o}_by_{fk_col}"
            m2o_aliases_used[fk_id].add(alias_m2o)

            if await _insert_rel(config_conn, m2o_id, fk_id, ref_id, fk_col, ref_col, "many-to-one", alias_m2o):
                inserted += 1
                _log.info("Auto-tracked FK many-to-one: %s.%s → %s.%s (alias: %s)", fk_tbl, fk_col, ref_tbl, ref_col, alias_m2o)

            # one-to-many: ref_table.ref_col ← fk_table.fk_col
            o2m_aliases_used.setdefault(ref_id, set())
            base_o2m = _o2m_alias(fk_tbl)
            alias_o2m = base_o2m if base_o2m not in o2m_aliases_used[ref_id] else f"{base_o2m}_by_{fk_col}"
            o2m_aliases_used[ref_id].add(alias_o2m)

            if await _insert_rel(config_conn, o2m_id, ref_id, fk_id, ref_col, fk_col, "one-to-many", alias_o2m):
                inserted += 1
                _log.info("Auto-tracked FK one-to-many: %s.%s ← %s.%s (alias: %s)", ref_tbl, ref_col, fk_tbl, fk_col, alias_o2m)

        # else: one or both tables not registered — skip; will be created when the missing table is registered

    return inserted

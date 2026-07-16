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

# complexity-gate: allow-ble=3 reason="best-effort constraint introspection over a pluggable set of RDB drivers whose failure taxonomy is unbounded (unreachable source, missing information_schema/PRAGMA, transient driver error): govdata FK fetch, UNIQUE-constraint introspection (REQ-1093), and per-table FK auto-registration each log and return an empty/zero result so one source's metadata read never fails registration or the introspection of other tables"

import asyncio
import logging
from typing import TYPE_CHECKING, cast

import asyncpg

if TYPE_CHECKING:
    from inflect import Word
    from inflect import engine as _Engine

_log = logging.getLogger(__name__)
# inflect's import + engine() build costs ~0.8s — lazy so it never hits cold start; paid once on
# first alias derivation. Same pattern as provisa/compiler/naming.py.
_inflect: "_Engine | None" = None


def _engine() -> "_Engine":
    global _inflect
    if _inflect is None:
        import inflect

        _inflect = inflect.engine()
    return _inflect


# Requirements: REQ-018, REQ-399, REQ-413, REQ-414, REQ-415

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
) -> list[dict]:  # REQ-018, REQ-413
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
            results.append(
                {
                    "fk_table": table_name,
                    "fk_column": fk_col,
                    "ref_table": ref_tbl,
                    "ref_column": ref_col,
                }
            )

    in_rows = await driver.execute(_PG_INBOUND, [schema_name, table_name])
    for row in in_rows.rows:
        fk_tbl, fk_col, ref_col = row
        key = (fk_tbl, fk_col, table_name, ref_col)
        if key not in seen:
            seen.add(key)
            results.append(
                {
                    "fk_table": fk_tbl,
                    "fk_column": fk_col,
                    "ref_table": table_name,
                    "ref_column": ref_col,
                }
            )

    return results


async def _sqlite_fks(
    driver,
    schema_name: str,
    table_name: str,
) -> list[dict]:  # REQ-018, REQ-413
    """Return FK rows from SQLite PRAGMA foreign_key_list."""
    results: list[dict] = []
    rows = await driver.execute(f'PRAGMA foreign_key_list("{table_name}")', [])
    for row in rows.rows:
        # columns: id, seq, table, from, to, on_update, on_delete, match
        _id, _seq, ref_tbl, fk_col, ref_col = row[0], row[1], row[2], row[3], row[4]
        results.append(
            {
                "fk_table": table_name,
                "fk_column": fk_col,
                "ref_table": ref_tbl,
                "ref_column": ref_col or "id",
            }
        )
    return results


# ---------------------------------------------------------------------------
# UNIQUE constraint introspection (REQ-1093)
# ---------------------------------------------------------------------------

# Declared UNIQUE constraints (not PRIMARY KEY) with their ordered columns.
_PG_UNIQUE = """
SELECT
    tc.constraint_name       AS name,
    kcu.column_name          AS column_name,
    kcu.ordinal_position     AS ordinal
FROM information_schema.table_constraints  AS tc
JOIN information_schema.key_column_usage   AS kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
WHERE tc.constraint_type = 'UNIQUE'
  AND tc.table_schema    = $1
  AND tc.table_name      = $2
ORDER BY tc.constraint_name, kcu.ordinal_position
"""


async def _pg_uniques(driver, schema_name: str, table_name: str) -> list[dict]:  # REQ-1093
    """Return declared UNIQUE constraints from PostgreSQL/MySQL information_schema.

    Each dict: {"name": str, "columns": [col, ...]} with columns in ordinal order.
    Composite constraints yield multi-element columns; nothing is inferred from data.
    """
    rows = await driver.execute(_PG_UNIQUE, [schema_name, table_name])
    grouped: dict[str, list[str]] = {}
    for name, column_name, _ordinal in rows.rows:
        grouped.setdefault(name, []).append(column_name)
    return [{"name": name, "columns": cols} for name, cols in grouped.items()]


async def _sqlite_uniques(driver, schema_name: str, table_name: str) -> list[dict]:  # REQ-1093
    """Return declared UNIQUE constraints from SQLite PRAGMA index_list/index_info.

    origin 'u' = a UNIQUE keyword/constraint; 'pk' (primary key) and 'c' (explicit
    CREATE UNIQUE INDEX) are excluded so only table-declared UNIQUE constraints surface.
    """
    idx_rows = await driver.execute(f'PRAGMA index_list("{table_name}")', [])
    results: list[dict] = []
    for row in idx_rows.rows:
        # columns: seq, name, unique, origin, partial
        _seq, idx_name, is_unique, origin = row[0], row[1], row[2], row[3]
        if not is_unique or origin != "u":
            continue
        info = await driver.execute(f'PRAGMA index_info("{idx_name}")', [])
        # index_info columns: seqno, cid, name — already ordered by seqno
        cols = [r[2] for r in info.rows]
        results.append({"name": idx_name, "columns": cols})
    return results


async def introspect_unique_constraints(  # REQ-1093
    source_pools,
    source_type: str,
    source_id: str,
    schema_name: str,
    table_name: str,
) -> list[dict]:
    """Introspect declared UNIQUE constraints for one table.

    Returns [{"name": str, "columns": [col, ...]}]; empty when the source exposes
    none or does not support constraint introspection. Only source-declared
    constraints are returned — uniqueness is never inferred from sampled data.
    """
    source_type_lower = source_type.lower()
    if not source_pools.has(source_id):
        return []
    driver = source_pools.get(source_id)
    try:
        if source_type_lower in ("postgresql", "postgres", "mysql", "mariadb"):
            return await _pg_uniques(driver, schema_name, table_name)
        if source_type_lower == "sqlite":
            return await _sqlite_uniques(driver, schema_name, table_name)
    except Exception:
        _log.debug(
            "UNIQUE introspection failed for %s.%s (%s)",
            schema_name,
            table_name,
            source_type,
            exc_info=True,
        )
    return []


def _m2o_alias(ref_table: str, hasura_v2_style: bool = False) -> str:  # REQ-415
    """Object relationship alias: ref table name.

    REQ-415: under Hasura V2 style the many-to-one (object) alias is singular.
    """
    if hasura_v2_style:
        return _engine().singular_noun(cast("Word", ref_table)) or ref_table
    return ref_table


def _o2m_alias(fk_table: str, hasura_v2_style: bool = False) -> str:  # REQ-415
    """Array relationship alias: FK table name.

    REQ-415: under Hasura V2 style the one-to-many (array) alias is plural.
    """
    if hasura_v2_style:
        # Singularize first so an already-plural table name is not double-pluralized
        # (inflect.plural_noun("orders") → "orderss").
        eng = _engine()
        singular = eng.singular_noun(cast("Word", fk_table)) or fk_table
        return eng.plural_noun(cast("Word", singular)) or singular
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
) -> bool:  # REQ-399, REQ-413
    """Insert a relationship; return True if newly inserted."""
    result = await conn.execute(
        """
        INSERT INTO relationships
            (id, source_table_id, target_table_id, source_column, target_column, cardinality, alias)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        """,
        rel_id,
        src_id,
        tgt_id,
        src_col,
        tgt_col,
        cardinality,
        alias,
    )
    return result == "INSERT 0 1"


async def _govdata_fks(
    source_id: str,
    schema_name: str,
    table_name: str,
    config_conn: asyncpg.Connection,
) -> list[dict]:  # REQ-018, REQ-413
    from provisa.core.models import GovDataSource, GovDataSubject
    from provisa.core.secrets import resolve_secrets as _resolve_secrets
    from provisa.govdata.source import fetch_foreign_keys as _fetch_fks

    try:
        row = await config_conn.fetchrow("SELECT username FROM sources WHERE id = $1", source_id)
        if row is None or row["username"] is None:
            raise ValueError(f"Source {source_id!r} has no username (govdata api_key)")
        api_key = _resolve_secrets(row["username"])
        gds = GovDataSource(
            id=source_id,
            subject=GovDataSubject.all,
            govdata_schemas=[schema_name.lower()],
            domain_id="default",
            api_key=api_key,
        )
        loop = asyncio.get_running_loop()
        raw = await loop.run_in_executor(
            None, _fetch_fks, gds, schema_name.lower(), table_name.lower()
        )
        return [
            {
                "fk_table": table_name,
                "fk_column": fk["fk_col"],
                "ref_table": fk["ref_table"],
                "ref_column": fk["ref_col"],
            }
            for fk in raw
        ]
    except Exception:
        _log.debug(
            "govdata FK introspection failed for %s.%s", schema_name, table_name, exc_info=True
        )
        return []


async def auto_register_fk_relationships(  # REQ-018, REQ-399, REQ-413, REQ-415
    source_pools,
    source_type: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    config_conn: asyncpg.Connection,
    hasura_v2_relationship_style: bool = False,
) -> int:
    """Introspect FK constraints and insert BOTH relationship directions.

    Uses ON CONFLICT DO NOTHING — never overwrites manually configured rels.
    Returns count of newly inserted relationships.
    """
    source_type_lower = source_type.lower()

    if source_type_lower == "govdata":
        fk_rows = await _govdata_fks(source_id, schema_name, table_name, config_conn)
    else:
        if not source_pools.has(source_id):
            return 0
        driver = source_pools.get(source_id)
        try:
            if source_type_lower in ("postgresql", "postgres", "mysql", "mariadb"):
                fk_rows = await _pg_fks(driver, schema_name, table_name)
            elif source_type_lower == "sqlite":
                fk_rows = await _sqlite_fks(driver, schema_name, table_name)
            else:
                return 0
        except Exception:
            _log.debug(
                "FK introspection failed for %s.%s (%s)",
                schema_name,
                table_name,
                source_type,
                exc_info=True,
            )
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
            source_id,
            fk_tbl,
        )
        ref_row = await config_conn.fetchrow(
            "SELECT id FROM registered_tables WHERE source_id = $1 AND table_name = $2",
            source_id,
            ref_tbl,
        )

        m2o_id = f"fk__{fk_tbl}__{fk_col}__to__{ref_tbl}"
        o2m_id = f"fk__{ref_tbl}__from__{fk_tbl}__{fk_col}"

        # many-to-one: fk_table.fk_col → ref_table.ref_col
        if fk_row and ref_row:
            fk_id, ref_id = fk_row["id"], ref_row["id"]

            m2o_aliases_used.setdefault(fk_id, set())
            base_m2o = _m2o_alias(ref_tbl, hasura_v2_relationship_style)
            alias_m2o = (
                base_m2o if base_m2o not in m2o_aliases_used[fk_id] else f"{base_m2o}_by_{fk_col}"
            )
            m2o_aliases_used[fk_id].add(alias_m2o)

            if await _insert_rel(
                config_conn, m2o_id, fk_id, ref_id, fk_col, ref_col, "many-to-one", alias_m2o
            ):
                inserted += 1
                _log.info(
                    "Auto-tracked FK many-to-one: %s.%s → %s.%s (alias: %s)",
                    fk_tbl,
                    fk_col,
                    ref_tbl,
                    ref_col,
                    alias_m2o,
                )

            # one-to-many: ref_table.ref_col ← fk_table.fk_col
            o2m_aliases_used.setdefault(ref_id, set())
            base_o2m = _o2m_alias(fk_tbl, hasura_v2_relationship_style)
            alias_o2m = (
                base_o2m if base_o2m not in o2m_aliases_used[ref_id] else f"{base_o2m}_by_{fk_col}"
            )
            o2m_aliases_used[ref_id].add(alias_o2m)

            if await _insert_rel(
                config_conn, o2m_id, ref_id, fk_id, ref_col, fk_col, "one-to-many", alias_o2m
            ):
                inserted += 1
                _log.info(
                    "Auto-tracked FK one-to-many: %s.%s ← %s.%s (alias: %s)",
                    ref_tbl,
                    ref_col,
                    fk_tbl,
                    fk_col,
                    alias_o2m,
                )

        # else: one or both tables not registered — skip; will be created when the missing table is registered

    return inserted

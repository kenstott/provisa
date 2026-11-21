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
  Disambiguated with _by_{fk_column} when alias would collide, then the table
  (provisa.core.relationship_naming, REQ-415); a derived GraphQL field or Cypher
  type that would collide is pinned under a name told apart the same way.
"""

from __future__ import annotations

# complexity-gate: allow-ble=3 reason="best-effort constraint introspection over a pluggable set of RDB drivers whose failure taxonomy is unbounded (unreachable source, missing information_schema/PRAGMA, transient driver error): govdata FK fetch, UNIQUE-constraint introspection (REQ-1093), and per-table FK auto-registration each log and return an empty/zero result so one source's metadata read never fails registration or the introspection of other tables"

import logging
from typing import TYPE_CHECKING

from provisa.core.inflection import plural, singular

if TYPE_CHECKING:
    from provisa.core.database import Connection

_log = logging.getLogger(__name__)


# Requirements: REQ-018, REQ-399, REQ-413, REQ-414, REQ-415

# ---------------------------------------------------------------------------
# SQL templates per dialect
# ---------------------------------------------------------------------------

# A foreign key's referenced columns are read from the key it references: the constraint's
# row in ``referential_constraints`` names that key (``unique_constraint_schema`` / ``_name``),
# and each foreign-key column's ``position_in_unique_constraint`` is the position of the column
# it is paired with in that key. So a composite key keeps its pairing, column by column, and a
# foreign key listed in another order than the key it references is still paired correctly.
# ``constraint_column_usage`` cannot say this (it lists the referenced columns with no pairing,
# so a two-column key came back as four pairs), and the Calcite pgwire servers do not serve it.
# Standard information_schema only, so one query serves PostgreSQL and every source that speaks
# its wire protocol.
_PG_FK_PAIRS = """
FROM information_schema.table_constraints      AS tc
JOIN information_schema.key_column_usage       AS kcu
    ON kcu.constraint_schema = tc.constraint_schema
   AND kcu.constraint_name   = tc.constraint_name
   AND kcu.table_schema      = tc.table_schema
   AND kcu.table_name        = tc.table_name
JOIN information_schema.referential_constraints AS rc
    ON rc.constraint_schema = tc.constraint_schema
   AND rc.constraint_name   = tc.constraint_name
JOIN information_schema.key_column_usage       AS pk
    ON pk.constraint_schema = rc.unique_constraint_schema
   AND pk.constraint_name   = rc.unique_constraint_name
   AND pk.ordinal_position  = kcu.position_in_unique_constraint
WHERE tc.constraint_type = 'FOREIGN KEY'
"""

# Outbound: FKs defined ON this table → ref table (many-to-one)
_PG_OUTBOUND = (
    """
SELECT
    kcu.column_name          AS fk_column,
    pk.table_name           AS ref_table,
    pk.column_name          AS ref_column
"""
    + _PG_FK_PAIRS
    + """
  AND tc.table_schema    = $1
  AND tc.table_name      = $2
ORDER BY tc.constraint_name, kcu.ordinal_position
"""
)

# Inbound: OTHER tables have FKs pointing TO this table (one-to-many)
_PG_INBOUND = (
    """
SELECT
    kcu.table_name           AS fk_table,
    kcu.column_name          AS fk_column,
    pk.column_name          AS ref_column
"""
    + _PG_FK_PAIRS
    + """
  AND pk.table_schema   = $1
  AND pk.table_name     = $2
ORDER BY kcu.table_name, tc.constraint_name, kcu.ordinal_position
"""
)

#: Source types whose foreign keys are read by the PostgreSQL query above over a pooled driver.
#: MySQL and MariaDB are not among them: they have no ``constraint_column_usage``, so the
#: earlier query never returned a row for them, and their ``referential_constraints`` names
#: every table's primary key ``PRIMARY``, so this query would pair a foreign key with every
#: primary key of the schema. They need a reader of their own (``referenced_table_name``).
_PG_FK_TYPES = ("postgresql", "postgres")


async def _constraint_driver(source_pools, source_type_lower: str, source_id: str):
    """``(driver, close)`` for reading ``source_id``'s declared constraints as PostgreSQL: the
    source's pooled driver, or — for a source read through its bundled Calcite pgwire server
    (``pgwire_replica.PGWIRE_REPLICA_TYPES``) — a connection to that server, the endpoint the
    engines attach. ``(None, None)`` for a source that is neither. Raises
    ``SourceStillStartingError`` while a server has not begun listening."""
    if source_type_lower in _PG_FK_TYPES:
        if not source_pools.has(source_id):
            return None, None
        return source_pools.get(source_id), None
    from provisa.federation.pgwire_replica import PGWIRE_REPLICA_TYPES

    if source_type_lower not in PGWIRE_REPLICA_TYPES:
        return None, None
    import asyncio

    from provisa.api.admin.schema_query import _source_for_introspection
    from provisa.executor.drivers.pgwire_server import PgwireServerDriver
    from provisa.federation.pgwire_endpoint import sign_in_password
    from provisa.federation.pgwire_replica import ensure_endpoint_for_discovery

    source = await _source_for_introspection(source_id)
    if source is None:
        raise LookupError(f"source {source_id!r} is not registered")
    ports = await asyncio.to_thread(ensure_endpoint_for_discovery, source)
    driver = PgwireServerDriver()
    # What the bundled server accepts: one database, signed in with the source's password.
    await driver.connect(
        ports.pgwire_host, ports.pgwire_port, "provisa", "provisa", sign_in_password(source)
    )
    return driver, driver.close


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


async def introspect_fk_constraints(  # REQ-018, REQ-413
    source_pools,
    source_type: str,
    source_id: str,
    schema_name: str,
    table_name: str,
) -> list[dict]:
    """Outbound FK constraints declared directly on one table, read straight from the source's
    own information_schema/PRAGMA over its live driver connection — never the federation engine's
    attached view, which is a plain SELECT and carries no constraint metadata.

    Returns [{"column_name", "referenced_table", "referenced_column"}]; empty when the source
    exposes none or does not support constraint introspection.
    """
    from provisa.federation.pgwire_replica import SourceStillStartingError

    source_type_lower = source_type.lower()
    try:
        pg_driver, close = await _constraint_driver(source_pools, source_type_lower, source_id)
    except SourceStillStartingError:
        # A source whose bundled server has not begun listening offers no candidates yet; the
        # next discovery reads them.
        return []
    if pg_driver is None:
        return []
    try:
        rows = await pg_driver.execute(_PG_OUTBOUND, [schema_name, table_name])
        return [
            {"column_name": r[0], "referenced_table": r[1], "referenced_column": r[2]}
            for r in rows.rows
        ]
    finally:
        if close is not None:
            await close()


def _m2o_alias(ref_table: str, hasura_v2_style: bool = False) -> str:  # REQ-415
    """Object relationship alias: ref table name.

    REQ-415: under Hasura V2 style the many-to-one (object) alias is singular.
    """
    if hasura_v2_style:
        return singular(ref_table)
    return ref_table


def _o2m_alias(fk_table: str, hasura_v2_style: bool = False) -> str:  # REQ-415
    """Array relationship alias: FK table name.

    REQ-415: under Hasura V2 style the one-to-many (array) alias is plural.
    """
    if hasura_v2_style:
        # plural() keeps an already-plural name as written (REQ-194).
        return plural(fk_table)
    return fk_table


async def _insert_rel(
    conn: "Connection",
    rel_id: str,
    src_id: int,
    tgt_id: int,
    src_col: str,
    tgt_col: str,
    cardinality: str,
    alias: str | None,
    graphql_alias: str | None,
) -> bool:  # REQ-399, REQ-413, REQ-415
    """Insert a relationship; return True if newly inserted."""
    result = await conn.execute(
        """
        INSERT INTO relationships
            (id, source_table_id, target_table_id, source_column, target_column, cardinality,
             alias, graphql_alias)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING
        """,
        rel_id,
        src_id,
        tgt_id,
        src_col,
        tgt_col,
        cardinality,
        alias,
        graphql_alias,
    )
    return result == "INSERT 0 1"


async def _registered_table(conn: "Connection", source_id: str, table_name: str):
    return await conn.fetchrow(
        "SELECT id, alias, table_name FROM registered_tables"
        " WHERE source_id = $1 AND table_name = $2",
        source_id,
        table_name,
    )


async def auto_register_fk_relationships(  # REQ-018, REQ-399, REQ-413, REQ-415
    source_pools,
    source_type: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    config_conn: "Connection",
    hasura_v2_relationship_style: bool = False,
) -> int:
    """Introspect FK constraints and insert BOTH relationship directions.

    Uses ON CONFLICT DO NOTHING — never overwrites manually configured rels.
    Returns count of newly inserted relationships.
    """
    from provisa.federation.pgwire_replica import SourceStillStartingError

    source_type_lower = source_type.lower()

    try:
        pg_driver, close = await _constraint_driver(source_pools, source_type_lower, source_id)
    except SourceStillStartingError:
        # A source whose bundled server has not begun listening has no keys to read yet; its
        # relationships are tracked when the table is saved again. Any other failure raises.
        return 0
    if pg_driver is not None:
        try:
            fk_rows = await _pg_fks(pg_driver, schema_name, table_name)
        finally:
            if close is not None:
                await close()
    elif source_type_lower == "sqlite" and source_pools.has(source_id):
        fk_rows = await _sqlite_fks(source_pools.get(source_id), schema_name, table_name)
    else:
        return 0

    if not fk_rows:
        return 0

    from provisa.core.relationship_naming import Generated, unique_names
    from provisa.core.relationship_names import store_effective_names

    held = {r["id"] for r in await config_conn.fetch("SELECT id FROM relationships")}
    generated: list[Generated] = []
    for fk in fk_rows:
        fk_tbl, fk_col = fk["fk_table"], fk["fk_column"]
        ref_tbl, ref_col = fk["ref_table"], fk["ref_column"]
        fk_row = await _registered_table(config_conn, source_id, fk_tbl)
        ref_row = await _registered_table(config_conn, source_id, ref_tbl)
        if fk_row is None or ref_row is None:
            continue  # created when the missing table is registered
        fk_id, ref_id = int(fk_row["id"]), int(ref_row["id"])
        # many-to-one: fk_table.fk_col → ref_table.ref_col; one-to-many: its reverse. An id is
        # unique by construction, so one already registered is this key, tracked before.
        for rel_id, src, tgt, src_col, tgt_col, cardinality, alias, target in (
            (
                f"fk__{fk_tbl}__{fk_col}__to__{ref_tbl}",
                fk_id,
                ref_id,
                fk_col,
                ref_col,
                "many-to-one",
                _m2o_alias(ref_tbl, hasura_v2_relationship_style),
                ref_row,
            ),
            (
                f"fk__{ref_tbl}__from__{fk_tbl}__{fk_col}",
                ref_id,
                fk_id,
                ref_col,
                fk_col,
                "one-to-many",
                _o2m_alias(fk_tbl, hasura_v2_relationship_style),
                fk_row,
            ),
        ):
            if rel_id in held:
                continue
            held.add(rel_id)
            generated.append(
                Generated(
                    rel_id=rel_id,
                    id_typed=True,
                    source_table_id=src,
                    target_table_id=tgt,
                    source_column=src_col,
                    target_column=tgt_col,
                    cardinality=cardinality,
                    target_name=target["alias"] or target["table_name"],
                    target_table=target["table_name"],
                    alias=alias,
                )
            )

    inserted = 0
    for g, named in zip(generated, await unique_names(config_conn, generated), strict=True):
        if await _insert_rel(
            config_conn,
            g.rel_id,
            g.source_table_id,
            g.target_table_id,
            g.source_column,
            g.target_column,
            g.cardinality,
            named.alias,
            named.graphql_alias,
        ):
            inserted += 1
            _log.info("Auto-tracked FK %s: %s (alias: %s)", g.cardinality, g.rel_id, named.alias)
    if inserted:
        await store_effective_names(config_conn, [g.rel_id for g in generated])
    return inserted

# Copyright (c) 2026 Kenneth Stott
# Canary: 9ef9a689-6ea8-4df9-b2ab-11de13bd8ff6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Query Trino INFORMATION_SCHEMA for registered table column metadata."""

import re
from dataclasses import dataclass

import trino

_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_ident(value: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


@dataclass(frozen=True)
class ColumnMetadata:
    column_name: str
    data_type: str
    is_nullable: bool


def introspect_table_columns(
    conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    table: str,
) -> list[ColumnMetadata]:
    """Get column metadata for a single table from Trino INFORMATION_SCHEMA."""
    cat = _validate_ident(catalog)
    _validate_ident(schema)
    _validate_ident(table)
    cur = conn.cursor()
    cur.execute(
        f"SELECT column_name, data_type, is_nullable "
        f"FROM {cat}.information_schema.columns "
        f"WHERE table_schema = '{_escape_literal(schema)}' "
        f"AND table_name = '{_escape_literal(table)}' "
        f"ORDER BY ordinal_position"
    )
    return [
        ColumnMetadata(
            column_name=row[0],
            data_type=row[1].lower(),
            is_nullable=(row[2] == "YES"),
        )
        for row in cur.fetchall()
    ]


def introspect_tables(
    conn: trino.dbapi.Connection,
    registered_tables: list[dict],
    sources: dict[str, dict],
    physical_table_map: dict[str, str] | None = None,
) -> dict[int, list[ColumnMetadata]]:
    """Bulk introspect all registered tables.

    Args:
        conn: Trino connection.
        registered_tables: list of dicts from table_repo.list_all().
        sources: {source_id: source_dict} for catalog name lookup.
        physical_table_map: {virtual_table_name: physical_table_name} for
            Kafka topics where multiple virtual tables map to one physical table.

    Returns:
        {table_id: [ColumnMetadata]}.
    """
    result: dict[int, list[ColumnMetadata]] = {}
    for table in registered_tables:
        source = sources[table["source_id"]]
        catalog_name = source["id"].replace("-", "_")
        # Use physical table name if mapped (e.g., Kafka discriminated tables)
        trino_table_name = table["table_name"]
        if physical_table_map:
            trino_table_name = physical_table_map.get(trino_table_name, trino_table_name)
        try:
            columns = introspect_table_columns(
                conn, catalog_name, table["schema_name"], trino_table_name
            )
            result[table["id"]] = columns
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "Failed to introspect %s.%s.%s: %s. Table will be skipped.",
                catalog_name, table["schema_name"], trino_table_name, e,
            )
    return result


def introspect_pk_columns(
    conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    table: str,
) -> set[str]:
    """Return set of column names that are part of the PRIMARY KEY constraint.

    Queries TABLE_CONSTRAINTS + KEY_COLUMN_USAGE from INFORMATION_SCHEMA.
    Returns empty set if the connector does not expose constraint metadata.
    """
    cat = _validate_ident(catalog)
    sch = _escape_literal(schema)
    tbl = _escape_literal(table)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT kcu.column_name "
            f"FROM {cat}.information_schema.table_constraints tc "
            f"JOIN {cat}.information_schema.key_column_usage kcu "
            f"  ON tc.constraint_name = kcu.constraint_name "
            f"  AND tc.table_schema = kcu.table_schema "
            f"  AND tc.table_name = kcu.table_name "
            f"WHERE tc.table_schema = '{sch}' AND tc.table_name = '{tbl}' "
            f"  AND tc.constraint_type = 'PRIMARY KEY'"
        )
        return {row[0] for row in cur.fetchall()}
    except trino.exceptions.TrinoUserError:
        return set()


def introspect_fk_candidates(
    conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    table: str,
) -> list[dict]:
    """Surface FK candidates from TABLE_CONSTRAINTS + KEY_COLUMN_USAGE (REQ-018).

    Returns list of {constraint_name, column_name, referenced_table, referenced_column}.
    Not all Trino connectors expose this; returns empty list if not supported.
    """
    cat = _validate_ident(catalog)
    sch = _escape_literal(schema)
    tbl = _escape_literal(table)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT tc.constraint_name, kcu.column_name, "
            f"ccu.table_name AS referenced_table, "
            f"ccu.column_name AS referenced_column "
            f"FROM {cat}.information_schema.table_constraints tc "
            f"JOIN {cat}.information_schema.key_column_usage kcu "
            f"  ON tc.constraint_name = kcu.constraint_name "
            f"JOIN {cat}.information_schema.constraint_column_usage ccu "
            f"  ON tc.constraint_name = ccu.constraint_name "
            f"WHERE tc.table_schema = '{sch}' AND tc.table_name = '{tbl}' "
            f"  AND tc.constraint_type = 'FOREIGN KEY'"
        )
        return [
            {
                "constraint_name": row[0],
                "column_name": row[1],
                "referenced_table": row[2],
                "referenced_column": row[3],
            }
            for row in cur.fetchall()
        ]
    except trino.exceptions.TrinoUserError:
        # Some connectors don't expose constraint metadata — return empty
        return []

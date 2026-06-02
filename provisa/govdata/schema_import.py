# Copyright (c) 2026 Kenneth Stott
# Canary: 4d5e6f70-8192-a3b4-d5e6-f708192a3b4c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auto-import Provisa Table/Relationship/view definitions from GovData JDBC metadata.

Usage::

    from provisa.govdata.schema_import import import_govdata_source
    tables, relationships, views = import_govdata_source(source, domain_id, roles)

Returns three lists that can be merged into ProvisaConfig.tables /
.relationships / .tables (view_sql populated) without writing a single YAML line.
"""

from __future__ import annotations

import logging
from typing import TypedDict

from provisa.core.models import (
    Cardinality,
    Column,
    GovDataSource,
    GovernanceLevel,  # noqa: F401 — referenced via source.governance at runtime
    Relationship,
    Table,
)

log = logging.getLogger(__name__)

# All govdata columns default visible to these roles unless overridden.
_DEFAULT_VISIBLE_TO = ["admin", "analyst"]

# JDBC type code → Provisa/GraphQL scalar type
_JDBC_TYPE_MAP: dict[int, str] = {
    -7: "Boolean",  # BIT
    -6: "Int",  # TINYINT
    5: "Int",  # SMALLINT
    4: "Int",  # INTEGER
    -5: "BigInt",  # BIGINT
    6: "Float",  # FLOAT
    7: "Float",  # REAL
    8: "Float",  # DOUBLE
    2: "Decimal",  # NUMERIC
    3: "Decimal",  # DECIMAL
    1: "String",  # CHAR
    12: "String",  # VARCHAR
    -1: "String",  # LONGVARCHAR
    91: "Date",  # DATE
    92: "Time",  # TIME
    93: "Timestamp",  # TIMESTAMP
    16: "Boolean",  # BOOLEAN
}


class _ColumnInfo(TypedDict):
    name: str
    type_code: int
    nullable: bool


def _jdbc_type_name(type_code: int) -> str:
    return _JDBC_TYPE_MAP.get(type_code, "String")


def _read_tables(meta, schema: str) -> list[tuple[str, str]]:
    """Return [(schema, table_name), ...] for views and base tables."""
    rs = meta.getTables(None, schema.upper(), "%", ["TABLE", "VIEW"])
    results = []
    while rs.next():
        tbl_schema = str(rs.getString("TABLE_SCHEM") or schema)
        tbl_name = str(rs.getString("TABLE_NAME"))
        results.append((tbl_schema.lower(), tbl_name.lower()))
    rs.close()
    return results


def _read_columns(meta, schema: str, table: str) -> list[_ColumnInfo]:
    rs = meta.getColumns(None, schema.upper(), table.upper(), "%")
    cols: list[_ColumnInfo] = []
    while rs.next():
        cols.append(
            {
                "name": str(rs.getString("COLUMN_NAME")).lower(),
                "type_code": int(rs.getInt("DATA_TYPE")),
                "nullable": int(rs.getInt("NULLABLE")) != 0,
            }
        )
    rs.close()
    return cols


def _read_fks(meta, schema: str, table: str) -> list[dict[str, str]]:
    """Return imported FK references for *table*."""
    rs = meta.getImportedKeys(None, schema.upper(), table.upper())
    fks = []
    while rs.next():
        fks.append(
            {
                "pk_schema": str(rs.getString("PKTABLE_SCHEM") or schema).lower(),
                "pk_table": str(rs.getString("PKTABLE_NAME")).lower(),
                "pk_col": str(rs.getString("PKCOLUMN_NAME")).lower(),
                "fk_col": str(rs.getString("FKCOLUMN_NAME")).lower(),
            }
        )
    rs.close()
    return fks


def _read_view_sql(conn, schema: str, table: str) -> str | None:
    """Attempt to retrieve VIEW definition via INFORMATION_SCHEMA."""
    try:
        stmt = conn.createStatement()
        rs = stmt.executeQuery(
            f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS "
            f"WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}'"
        )
        result = str(rs.getString(1)) if rs.next() else None
        rs.close()
        stmt.close()
        return result
    except Exception:
        return None


def import_govdata_source(
    source: GovDataSource,
) -> tuple[list[Table], list[Relationship]]:
    """Connect to *source* via JDBC and return (tables, relationships).

    Relationships are derived from FK constraints exported by GovData's
    Calcite metadata layer.  Views have ``view_sql`` populated from
    INFORMATION_SCHEMA where available.
    """
    from provisa.govdata.source import connect

    conn = connect(source)
    meta = conn.getMetaData()

    tables: list[Table] = []
    relationships: list[Relationship] = []
    seen_rel_ids: set[str] = set()

    for schema, table_name in _read_tables(
        meta, source.govdata_schemas[0] if len(source.govdata_schemas) == 1 else ""
    ):
        if schema not in [s.lower() for s in source.govdata_schemas]:
            continue

        raw_cols = _read_columns(meta, schema, table_name)
        if not raw_cols:
            continue

        view_sql = _read_view_sql(conn, schema, table_name)

        columns = [
            Column(
                name=c["name"],
                visible_to=list(_DEFAULT_VISIBLE_TO),
            )
            for c in raw_cols
        ]

        tables.append(
            Table(
                source_id=source.id,
                domain_id=source.domain_id,
                schema=schema,
                table=table_name,
                governance=source.governance,
                columns=columns,
                view_sql=view_sql,
            )
        )

        for fk in _read_fks(meta, schema, table_name):
            rel_id = f"{source.id}-{schema}-{table_name}-{fk['pk_table']}-{fk['fk_col']}"
            if rel_id in seen_rel_ids:
                continue
            seen_rel_ids.add(rel_id)

            relationships.append(
                Relationship(
                    id=rel_id,
                    source_table_id=f"{schema}.{table_name}",
                    target_table_id=f"{fk['pk_schema']}.{fk['pk_table']}",
                    source_column=fk["fk_col"],
                    target_column=fk["pk_col"],
                    cardinality=Cardinality.many_to_one,
                )
            )

    return tables, relationships

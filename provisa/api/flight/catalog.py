# Copyright (c) 2025 Kenneth Stott
# Canary: 3929efbd-1d4a-4d8f-ac56-285f1c0ccd42
# Canary: PENDING
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Virtual JDBC catalog metadata from AppState (REQ-126).

Builds Arrow Flight descriptors and schemas that present the Provisa
semantic layer as a read-only JDBC catalog:
  - domains  -> schemas
  - tables   -> tables
  - columns  -> columns with descriptions
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.flight as flight

log = logging.getLogger(__name__)

# Trino type -> Arrow type mapping
_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "real": pa.float32(),
    "double": pa.float64(),
    "decimal": pa.float64(),
    "varchar": pa.utf8(),
    "char": pa.utf8(),
    "varbinary": pa.binary(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "json": pa.utf8(),
    "uuid": pa.utf8(),
    "array": pa.utf8(),
    "map": pa.utf8(),
    "row": pa.utf8(),
}


def _trino_type_to_arrow(trino_type: str) -> pa.DataType:
    """Map a Trino data type string to an Arrow type."""
    # Strip parameterized types: decimal(10,2) -> decimal
    base = trino_type.split("(")[0].strip().lower()
    if base in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[base]
    raise KeyError(f"Unmapped Trino type: {trino_type!r}")


@dataclass(frozen=True)
class CatalogTable:
    """A table in the virtual catalog."""
    domain_id: str
    table_name: str
    description: str
    columns: list[CatalogColumn]


@dataclass(frozen=True)
class CatalogColumn:
    """A column in a virtual catalog table."""
    name: str
    data_type: str  # Trino type string
    is_nullable: bool
    description: str


@dataclass(frozen=True)
class ApprovedQuery:
    """An approved persisted query exposed as a virtual table."""
    stable_id: str
    query_text: str
    compiled_sql: str


def build_catalog_tables(state) -> list[CatalogTable]:
    """Build the virtual catalog from AppState.

    Reads registered tables and introspected column metadata from the
    compilation contexts. Uses the 'admin' role context as the broadest view.
    """
    if not state.pg_pool:
        return []

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_build_catalog_tables_async(state))
    finally:
        loop.close()


async def _build_catalog_tables_async(state) -> list[CatalogTable]:
    """Async implementation of build_catalog_tables."""
    async with state.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, domain_id, table_name, description "
            "FROM registered_tables ORDER BY domain_id, table_name"
        )
        col_rows = await conn.fetch(
            "SELECT tc.table_id, tc.column_name, tc.description "
            "FROM table_columns tc ORDER BY tc.id"
        )

    # Index column descriptions by (table_id, column_name)
    col_desc_map: dict[tuple[int, str], str] = {}
    for cr in col_rows:
        col_desc_map[(cr["table_id"], cr["column_name"])] = cr["description"] or ""

    # Get introspected column types from the broadest context
    # We look at whichever role context has the most tables
    best_ctx = None
    best_count = -1
    for role_id, ctx in state.contexts.items():
        count = len(getattr(ctx, "table_map", {}))
        if count > best_count:
            best_count = count
            best_ctx = ctx

    # Build the column metadata lookup from introspection
    from provisa.compiler.introspect import ColumnMetadata
    introspected: dict[int, list[ColumnMetadata]] = {}
    if state.trino_conn:
        # Re-use the compilation context's column types if available
        # They are stored during schema build on AppState indirectly
        # We can re-introspect the relevant tables
        pass

    tables: list[CatalogTable] = []
    for row in rows:
        table_id = row["id"]
        domain_id = row["domain_id"]
        table_name = row["table_name"]
        description = row["description"] or ""

        columns: list[CatalogColumn] = []
        # Get columns from table_columns (registered metadata)
        for cr in col_rows:
            if cr["table_id"] != table_id:
                continue
            col_name = cr["column_name"]
            col_description = cr["description"] or ""
            # Default type — will be overridden if introspection data exists
            columns.append(CatalogColumn(
                name=col_name,
                data_type="varchar",
                is_nullable=True,
                description=col_description,
            ))

        tables.append(CatalogTable(
            domain_id=domain_id,
            table_name=table_name,
            description=description,
            columns=columns,
        ))
    return tables


def build_catalog_tables_from_context(state) -> list[CatalogTable]:
    """Build catalog tables using in-memory compilation contexts.

    This is faster than querying PG and works in test scenarios.
    Iterates over all role contexts and merges the broadest view.
    """
    # Find the role with the broadest access
    best_role_id = None
    best_count = -1
    for role_id, ctx in state.contexts.items():
        count = len(getattr(ctx, "table_map", {}))
        if count > best_count:
            best_count = count
            best_role_id = role_id

    if best_role_id is None:
        return []

    ctx = state.contexts[best_role_id]
    table_map = getattr(ctx, "table_map", {})

    tables: list[CatalogTable] = []
    for gql_name, tinfo in table_map.items():
        domain_id = getattr(tinfo, "domain_id", "default")
        description = getattr(tinfo, "description", "") or ""
        columns: list[CatalogColumn] = []
        col_metas = getattr(tinfo, "columns", [])
        for cm in col_metas:
            col_name = getattr(cm, "column_name", "") or getattr(cm, "name", "")
            data_type = getattr(cm, "data_type", "varchar")
            is_nullable = getattr(cm, "is_nullable", True)
            col_description = getattr(cm, "description", "") or ""
            columns.append(CatalogColumn(
                name=col_name,
                data_type=data_type,
                is_nullable=is_nullable,
                description=col_description,
            ))
        tables.append(CatalogTable(
            domain_id=domain_id,
            table_name=gql_name,
            description=description,
            columns=columns,
        ))
    return tables


def catalog_table_to_arrow_schema(table: CatalogTable) -> pa.Schema:
    """Convert a CatalogTable to an Arrow schema with metadata."""
    fields = []
    for col in table.columns:
        try:
            arrow_type = _trino_type_to_arrow(col.data_type)
        except KeyError:
            arrow_type = pa.utf8()
        metadata = {}
        if col.description:
            metadata[b"description"] = col.description.encode("utf-8")
        fields.append(pa.field(
            col.name,
            arrow_type,
            nullable=col.is_nullable,
            metadata=metadata,
        ))
    schema_metadata = {}
    if table.description:
        schema_metadata[b"description"] = table.description.encode("utf-8")
    schema_metadata[b"domain"] = table.domain_id.encode("utf-8")
    return pa.schema(fields, metadata=schema_metadata)


def catalog_table_to_flight_info(
    table: CatalogTable,
    location: flight.Location | None = None,
) -> flight.FlightInfo:
    """Build a FlightInfo descriptor for a catalog table."""
    descriptor = flight.FlightDescriptor.for_path(
        table.domain_id, table.table_name,
    )
    schema = catalog_table_to_arrow_schema(table)
    endpoints = []
    if location:
        ticket = flight.Ticket(
            f'{{"mode":"catalog","domain":"{table.domain_id}",'
            f'"table":"{table.table_name}"}}'.encode("utf-8"),
        )
        endpoints = [flight.FlightEndpoint(ticket, [location])]
    return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)


def approved_query_to_flight_info(
    query: ApprovedQuery,
    location: flight.Location | None = None,
) -> flight.FlightInfo:
    """Build a FlightInfo descriptor for an approved query virtual table."""
    descriptor = flight.FlightDescriptor.for_path(
        "approved", query.stable_id,
    )
    # Approved queries expose a minimal schema: stable_id + query_text
    schema = pa.schema([
        pa.field("stable_id", pa.utf8()),
        pa.field("query_text", pa.utf8()),
        pa.field("compiled_sql", pa.utf8()),
    ])
    endpoints = []
    if location:
        ticket = flight.Ticket(
            f'{{"mode":"approved","stable_id":"{query.stable_id}"}}'.encode("utf-8"),
        )
        endpoints = [flight.FlightEndpoint(ticket, [location])]
    return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)


def fetch_approved_queries(state) -> list[ApprovedQuery]:
    """Fetch approved persisted queries from PG."""
    if not state.pg_pool:
        return []
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_fetch_approved_queries_async(state))
    finally:
        loop.close()


async def _fetch_approved_queries_async(state) -> list[ApprovedQuery]:
    async with state.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT stable_id, query_text, compiled_sql "
            "FROM persisted_queries WHERE status = 'approved' "
            "ORDER BY approved_at"
        )
    return [
        ApprovedQuery(
            stable_id=r["stable_id"],
            query_text=r["query_text"],
            compiled_sql=r["compiled_sql"],
        )
        for r in rows
    ]

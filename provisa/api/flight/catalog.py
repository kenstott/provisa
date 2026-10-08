# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-126, REQ-127, REQ-128, REQ-143

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.flight as flight

from provisa.core.modeling_tags import append_modeling_tag

log = logging.getLogger(__name__)

# the engine type -> Arrow type mapping
_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "real": pa.float32(),
    "double": pa.float64(),
    "double precision": pa.float64(),
    "float": pa.float32(),
    "decimal": pa.float64(),
    "numeric": pa.float64(),
    "varchar": pa.utf8(),
    "char": pa.utf8(),
    "text": pa.utf8(),
    "varbinary": pa.binary(),
    "bytea": pa.binary(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
    "interval": pa.utf8(),
    "json": pa.utf8(),
    "jsonb": pa.utf8(),
    "uuid": pa.utf8(),
    "array": pa.utf8(),
    "map": pa.utf8(),
    "row": pa.utf8(),
}


def _physical_type_to_arrow(column_type: str) -> pa.DataType:
    """Map the engine data type string to an Arrow type."""
    # Strip parameterized types: decimal(10,2) -> decimal
    base = column_type.split("(")[0].strip().lower()
    if base in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[base]
    raise KeyError(f"Unmapped engine type: {column_type!r}")


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
    data_type: str  # the engine type string
    is_nullable: bool
    description: str
    # Key metadata, for clients that draw a model from the catalog (the JDBC driver's
    # getPrimaryKeys / getImportedKeys): the column is part of the table's declared primary key,
    # and the (domain, table, column) it refers to through a declared to-one relationship.
    is_primary_key: bool = False
    references: tuple[str, str, str] | None = None


def role_visibility(state, role_id: str) -> dict[int, set[str]]:
    """table id → the physical columns ``role_id`` is served, from its compiled context: the
    same tables and columns its schema has on every other surface. A role with no data surface
    (a control-plane role, one reaching no domain) has no context and sees nothing."""
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return {}
    visible: dict[int, set[str]] = {meta.table_id: set() for meta in ctx.tables.values()}
    for table_id, column in ctx.physical_to_sql:
        if table_id in visible:
            visible[table_id].add(column)
    return visible


def role_sees_metric(state, role_id: str, metric) -> bool:
    """Whether ``role_id`` may see ``metric`` (its ``visible_to``; ``*`` is every role). A
    meta-role sees what any of its members sees."""
    from provisa.security.meta_role import acting_roles

    granted = metric.visible_to
    return "*" in granted or any(r in granted for r in acting_roles(state, role_id))


def build_catalog_tables(
    state, role_id: str | None = None
) -> list[CatalogTable]:  # REQ-127, REQ-128
    """Build the virtual catalog from AppState.

    With ``role_id`` the catalog is that role's: only the tables and columns it is served
    (:func:`role_visibility`). None is the whole registered catalog — a deployment that
    authenticates nobody has no role to narrow it by.
    """
    import asyncio

    if not state.model_db:
        return []

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_build_catalog_tables_async(state, role_id))
    finally:
        loop.close()


async def _build_catalog_tables_async(state, role_id: str | None = None) -> list[CatalogTable]:
    """Async implementation of build_catalog_tables."""
    visible = None if role_id is None else role_visibility(state, role_id)
    async with state.model_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, domain_id, table_name, description, modeling_role, modeling_history "
            # REQ-1921: a draft table is in no catalog.
            "FROM registered_tables WHERE NOT draft ORDER BY domain_id, table_name"
        )
        col_rows = await conn.fetch(
            "SELECT tc.table_id, tc.column_name, tc.alias, tc.data_type, tc.description, "
            "tc.is_primary_key FROM table_columns tc ORDER BY tc.id"
        )
        rel_rows = await conn.fetch(
            "SELECT source_table_id, source_column, target_table_id, target_column "
            "FROM relationships "
            "WHERE target_table_id IS NOT NULL AND target_column IS NOT NULL "
            "AND cardinality IN ('many-to-one', 'one-to-one') ORDER BY id"
        )

    named = {row["id"]: (row["domain_id"], row["table_name"]) for row in rows}
    # A column is listed under the name SQL reads it by, so a name taken from the catalog can be
    # written into a query: the role's compiled context has it for every column the role is
    # served. The whole catalog (no role) is not one role's, so the name is derived as the
    # context derives it (compiler/context.py): the column's alias, or the SQL naming convention.
    from provisa.compiler.naming import apply_sql_name

    if role_id is None:
        sql_names = {
            (cr["table_id"], cr["column_name"]): cr["alias"] or apply_sql_name(cr["column_name"])
            for cr in col_rows
        }
    else:
        ctx = state.contexts.get(role_id)
        sql_names = {} if ctx is None else dict(ctx.physical_to_sql)
    # (table id, column) → what it refers to. A reference is listed only where its target is
    # listed too: a role is not told of a table or column it is not served by way of a key.
    references: dict[tuple[int, str], tuple[str, str, str]] = {}
    for rel in rel_rows:
        target_id, target_column = rel["target_table_id"], rel["target_column"]
        if target_id not in named or (target_id, target_column) not in sql_names:
            continue
        if visible is not None and target_column not in visible.get(target_id, ()):
            continue
        references.setdefault(
            (rel["source_table_id"], rel["source_column"]),
            (*named[target_id], sql_names[(target_id, target_column)]),
        )

    tables: list[CatalogTable] = []
    for row in rows:
        table_id = row["id"]
        if visible is not None and table_id not in visible:
            continue
        domain_id = row["domain_id"]
        table_name = row["table_name"]
        # REQ-1320: same "[fact]"/"[dimension, scd2]" suffix as GraphQL docs and
        # pg_description, so MCP/Flight callers see the star shape too.
        description = append_modeling_tag(
            row["description"], row["modeling_role"], row["modeling_history"]
        )

        columns: list[CatalogColumn] = []
        # Get columns from table_columns (registered metadata)
        for cr in col_rows:
            if cr["table_id"] != table_id:
                continue
            col_name = cr["column_name"]
            if visible is not None and col_name not in visible[table_id]:
                continue
            col_description = cr["description"] or ""
            columns.append(
                CatalogColumn(
                    name=sql_names[(table_id, col_name)],
                    # The registered type: a column is not stored without one (REQ-1426).
                    data_type=cr["data_type"],
                    is_nullable=True,
                    description=col_description,
                    is_primary_key=bool(cr["is_primary_key"]),
                    references=references.get((table_id, col_name)),
                )
            )

        tables.append(
            CatalogTable(
                domain_id=domain_id,
                table_name=table_name,
                description=description,
                columns=columns,
            )
        )
    return tables


def build_catalog_tables_from_context(state) -> list[CatalogTable]:  # REQ-127, REQ-128
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
        # REQ-1320: same "[fact]"/"[dimension, scd2]" suffix as GraphQL docs and
        # pg_description, so MCP/Flight callers see the star shape too.
        description = append_modeling_tag(
            getattr(tinfo, "description", None),
            getattr(tinfo, "modeling_role", None),
            getattr(tinfo, "modeling_history", None),
        )
        columns: list[CatalogColumn] = []
        col_metas = getattr(tinfo, "columns", [])
        for cm in col_metas:
            col_name = getattr(cm, "column_name", "") or getattr(cm, "name", "")
            data_type = getattr(cm, "data_type", "varchar")
            is_nullable = getattr(cm, "is_nullable", True)
            col_description = getattr(cm, "description", "") or ""
            columns.append(
                CatalogColumn(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    description=col_description,
                )
            )
        tables.append(
            CatalogTable(
                domain_id=domain_id,
                table_name=gql_name,
                description=description,
                columns=columns,
            )
        )
    return tables


def _endpoint(
    ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    location: flight.Location | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> flight.FlightEndpoint:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The one endpoint a FlightInfo carries: its ticket, redeemable where it was issued.

    With no ``location`` the endpoint's location list is EMPTY, which in the Flight protocol
    means "redeem on the service that gave you this" — the advertised port. Every worker of a
    launch accepts on that port (provisa/api/flight/relay.py, REQ-1900) and a ticket names what
    to return and nothing about who issued it, so whichever worker takes the DoGet redeems it."""
    return flight.FlightEndpoint(ticket, [location] if location else [])  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


def command_to_flight_info(  # REQ-1156
    command: dict,
    location: flight.Location | None = None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build a FlightInfo descriptor for a registered command (REQ-1156).

    Descriptor path is ``["commands", <domain>, <name>]`` so a Flight client discovers commands
    alongside tables (path ``[<domain>, <table>]``) without colliding with them. The Arrow schema
    carries one field per declared argument plus command metadata (kind, set_returning, description)
    so the shape is self-describing; invocation stays on the governed SQL-ticket path (``SELECT
    fn(...)``), the single executor every surface shares.
    """
    fields = [pa.field(a["name"], pa.utf8()) for a in command.get("arguments", []) if a.get("name")]
    meta = {
        b"kind": str(command.get("kind", "mutation")).encode("utf-8"),
        b"set_returning": (b"true" if command.get("set_returning") else b"false"),
        b"command": command["name"].encode("utf-8"),
    }
    if command.get("description"):
        meta[b"description"] = command["description"].encode("utf-8")
    schema = pa.schema(fields, metadata=meta)
    descriptor = flight.FlightDescriptor.for_path(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        "commands",
        command.get("domain", ""),
        command["name"],
    )
    ticket = flight.Ticket(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        f'{{"command":"{command["name"]}"}}'.encode("utf-8"),
    )
    return flight.FlightInfo(schema, descriptor, [_endpoint(ticket, location)], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


def catalog_table_to_arrow_schema(table: CatalogTable) -> pa.Schema:  # REQ-143
    """Convert a CatalogTable to an Arrow schema with metadata."""
    fields = []
    for col in table.columns:
        try:
            arrow_type = _physical_type_to_arrow(col.data_type)
        except KeyError:
            arrow_type = pa.utf8()
        metadata = {}
        if col.description:
            metadata[b"description"] = col.description.encode("utf-8")
        if col.is_primary_key:
            metadata[b"primary_key"] = b"true"
        if col.references is not None:
            domain, name, column = col.references
            metadata[b"references"] = json.dumps(
                {"domain": domain, "table": name, "column": column}
            ).encode("utf-8")
        fields.append(
            pa.field(
                col.name,
                arrow_type,
                nullable=col.is_nullable,
                metadata=metadata,
            )
        )
    schema_metadata = {}
    if table.description:
        schema_metadata[b"description"] = table.description.encode("utf-8")
    schema_metadata[b"domain"] = table.domain_id.encode("utf-8")
    return pa.schema(fields, metadata=schema_metadata)


def catalog_table_to_flight_info(  # REQ-143
    table: CatalogTable,
    location: flight.Location | None = None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build a FlightInfo descriptor for a catalog table."""  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    descriptor = flight.FlightDescriptor.for_path(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        table.domain_id,
        table.table_name,
    )
    schema = catalog_table_to_arrow_schema(table)
    ticket = flight.Ticket(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        f'{{"domain":"{table.domain_id}","table":"{table.table_name}"}}'.encode("utf-8"),
    )
    return flight.FlightInfo(schema, descriptor, [_endpoint(ticket, location)], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


def metric_to_flight_info(  # REQ-1319
    metric_name: str,
    dimensions: list[str],
    description: str | None = None,
    location: flight.Location | None = None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Build a FlightInfo for a governed metric (REQ-1319).

    Descriptor path is ``["metrics", <name>, <dim>...]`` — metrics are discoverable
    alongside tables and commands. The schema is the metric shape at the requested
    grain: one utf8 field per dimension plus a float64 ``value``. Execution stays on
    the governed SQL-ticket path (semantic ``metrics.<name>`` SQL, the single
    expansion every surface shares).
    """
    from provisa.compiler.metric_expand import metric_semantic_sql

    fields = [pa.field(d, pa.utf8()) for d in dimensions]
    fields.append(pa.field("value", pa.float64()))
    meta = {b"metric": metric_name.encode("utf-8")}
    if description:
        meta[b"description"] = description.encode("utf-8")
    schema = pa.schema(fields, metadata=meta)
    descriptor = flight.FlightDescriptor.for_path(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        "metrics", metric_name, *dimensions
    )
    ticket = flight.Ticket(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        json.dumps({"query": metric_semantic_sql(metric_name, dimensions)}).encode("utf-8")
    )
    return flight.FlightInfo(schema, descriptor, [_endpoint(ticket, location)], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

# Copyright (c) 2026 Kenneth Stott
# Canary: d9b05dee-a3d6-4d59-8f36-b484974b3796
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL's catalog commands, answered from the role's catalog (REQ-2175).

Catalogs, database schemas, tables, table types and keys are listed from the catalog pgwire
lists to the same role (``pgwire.catalog_populate.role_catalog_index`` and the key constraints
of ``pgwire.catalog_constraints``), so a Flight SQL client and a Postgres client of one role see
one catalog: the tables and columns the role is served, under the names a statement reads them
by. Each answer has the schema FlightSql.proto gives its command, ordered as it says.
"""

# Requirements: REQ-2175

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from typing import Any

import pyarrow as pa

from provisa.proto import FlightSql_pb2 as pb

#: The one table type the catalog lists: every relation a role reads is listed as a table, as
#: pgwire's information_schema.tables lists it (BASE TABLE).
TABLE_TYPE = "TABLE"

CATALOGS_SCHEMA = pa.schema([pa.field("catalog_name", pa.utf8(), nullable=False)])
DB_SCHEMAS_SCHEMA = pa.schema(
    [pa.field("catalog_name", pa.utf8()), pa.field("db_schema_name", pa.utf8(), nullable=False)]
)
_TABLES_FIELDS = [
    pa.field("catalog_name", pa.utf8()),
    pa.field("db_schema_name", pa.utf8()),
    pa.field("table_name", pa.utf8(), nullable=False),
    pa.field("table_type", pa.utf8(), nullable=False),
]
TABLES_SCHEMA = pa.schema(_TABLES_FIELDS)
TABLES_WITH_SCHEMA_SCHEMA = pa.schema(
    [*_TABLES_FIELDS, pa.field("table_schema", pa.binary(), nullable=False)]
)
TABLE_TYPES_SCHEMA = pa.schema([pa.field("table_type", pa.utf8(), nullable=False)])
PRIMARY_KEYS_SCHEMA = pa.schema(
    [
        pa.field("catalog_name", pa.utf8()),
        pa.field("db_schema_name", pa.utf8()),
        pa.field("table_name", pa.utf8(), nullable=False),
        pa.field("column_name", pa.utf8(), nullable=False),
        pa.field("key_name", pa.utf8()),
        pa.field("key_sequence", pa.int32(), nullable=False),
    ]
)
FOREIGN_KEYS_SCHEMA = pa.schema(
    [
        pa.field("pk_catalog_name", pa.utf8()),
        pa.field("pk_db_schema_name", pa.utf8()),
        pa.field("pk_table_name", pa.utf8(), nullable=False),
        pa.field("pk_column_name", pa.utf8(), nullable=False),
        pa.field("fk_catalog_name", pa.utf8()),
        pa.field("fk_db_schema_name", pa.utf8()),
        pa.field("fk_table_name", pa.utf8(), nullable=False),
        pa.field("fk_column_name", pa.utf8(), nullable=False),
        pa.field("key_sequence", pa.int32(), nullable=False),
        pa.field("fk_key_name", pa.utf8()),
        pa.field("pk_key_name", pa.utf8()),
        pa.field("update_rule", pa.uint8(), nullable=False),
        pa.field("delete_rule", pa.uint8(), nullable=False),
    ]
)

#: pg_constraint's referential action letters as Flight SQL's UpdateDeleteRules.
_RULES = {
    "c": pb.CASCADE,
    "r": pb.RESTRICT,
    "n": pb.SET_NULL,
    "a": pb.NO_ACTION,
    "d": pb.SET_DEFAULT,
}


@dataclass(frozen=True)
class _Table:
    catalog: str
    schema: str
    name: str
    toid: int


@dataclass(frozen=True)
class RoleCatalog:
    """A role's catalog: its tables, their columns in order, and its key constraints, under the
    served database, the one catalog every SQL surface lists (REQ-588)."""

    database: str
    tables: list[_Table]
    columns: dict[int, list[tuple[str, str]]]  # table oid → (column, registered type), in order
    constraints: list[tuple]  # pg_constraint rows (pgwire.catalog_constraints)
    names: dict[tuple[int, int], str]  # (table oid, attnum) → column

    def table(self, toid: int) -> _Table:
        return next(t for t in self.tables if t.toid == toid)


def role_catalog(state: Any, role_id: str) -> RoleCatalog:
    """The catalog pgwire lists to ``role_id``."""
    from provisa.pgwire.catalog_constraints import catalog_constraint_rows
    from provisa.pgwire.catalog_populate import role_catalog_index

    ctx, idx, _ = role_catalog_index(state, role_id)
    tables = sorted(
        (_Table(cat, sch, name, toid) for cat, sch, name, _, toid in idx.tables),
        key=lambda t: (t.catalog, t.schema, t.name),
    )
    columns: dict[int, list[tuple[str, str]]] = {}
    for toid, name, column_type, _, ordinal in sorted(idx.all_cols, key=lambda c: (c[0], c[4])):
        columns.setdefault(toid, []).append((name, column_type))
    return RoleCatalog(
        idx.database, tables, columns, catalog_constraint_rows(ctx, idx), dict(idx.attnum_to_col)
    )


#: The schema a session's temporary tables are listed under, as PostgreSQL names it.
TEMPORARY_SCHEMA = "pg_temp"


def with_temporary(catalog: RoleCatalog, session: Any) -> RoleCatalog:
    """``catalog`` with the temporary tables of ``session`` (a TempSession, or None) listed in
    :data:`TEMPORARY_SCHEMA`: listed to that session alone (REQ-615)."""
    if session is None or not session.tables:
        return catalog
    tables = list(catalog.tables)
    columns = dict(catalog.columns)
    for i, table in enumerate(sorted(session.tables.values(), key=lambda t: t.name)):
        toid = -(i + 1)  # no model table's oid: a temporary table is no model object
        tables.append(_Table(catalog.database, TEMPORARY_SCHEMA, table.name, toid))
        columns[toid] = list(table.columns)
    return RoleCatalog(catalog.database, tables, columns, catalog.constraints, catalog.names)


@cache
def _like(pattern: str) -> re.Pattern[str]:
    """A Flight SQL filter pattern (SQL LIKE: ``%`` any run, ``_`` one character) as a regex."""
    parts = []
    for ch in pattern:
        if ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    return re.compile("".join(parts), re.DOTALL)


def _matches(value: str, pattern: str | None) -> bool:
    return pattern is None or _like(pattern).fullmatch(value) is not None


def _optional(command: Any, field: str) -> str | None:
    return getattr(command, field) if command.HasField(field) else None


def catalogs(catalog: RoleCatalog) -> pa.Table:
    names = sorted({t.catalog for t in catalog.tables} | {catalog.database})
    return pa.Table.from_pydict({"catalog_name": names}, schema=CATALOGS_SCHEMA)


def db_schemas(catalog: RoleCatalog, command: pb.CommandGetDbSchemas) -> pa.Table:
    """The schemas of the role's tables, and the current schema, which pgwire lists too."""
    from provisa.pgwire.catalog import CURRENT_SCHEMA

    want_catalog = _optional(command, "catalog")
    pattern = _optional(command, "db_schema_filter_pattern")
    listed = {(t.catalog, t.schema) for t in catalog.tables} | {(catalog.database, CURRENT_SCHEMA)}
    rows = sorted(
        (c, s)
        for c, s in listed
        if (want_catalog is None or c == want_catalog) and _matches(s, pattern)
    )
    return pa.Table.from_pylist(
        [{"catalog_name": c, "db_schema_name": s} for c, s in rows], schema=DB_SCHEMAS_SCHEMA
    )


def table_arrow_schema(columns: list[tuple[str, str]]) -> pa.Schema:
    """A listed table's columns as Arrow, as the native catalog types them: a type neither the
    catalog nor the product's vocabulary names is listed as text and says so in its metadata."""
    from provisa.core.catalog_types import _physical_type_to_arrow

    fields = []
    for name, column_type in columns:
        metadata = {b"ARROW:FLIGHT:SQL:TYPE_NAME": column_type.encode("utf-8")}
        try:
            arrow_type = _physical_type_to_arrow(column_type)
        except KeyError:
            arrow_type = pa.utf8()
            metadata[b"type_unmapped"] = column_type.encode("utf-8")
        fields.append(pa.field(name, arrow_type, metadata=metadata))
    return pa.schema(fields)


def tables(catalog: RoleCatalog, command: pb.CommandGetTables) -> pa.Table:
    want_catalog = _optional(command, "catalog")
    schema_pattern = _optional(command, "db_schema_filter_pattern")
    table_pattern = _optional(command, "table_name_filter_pattern")
    types = set(command.table_types)
    listed = [
        t
        for t in catalog.tables
        if (want_catalog is None or t.catalog == want_catalog)
        and _matches(t.schema, schema_pattern)
        and _matches(t.name, table_pattern)
        and (not types or TABLE_TYPE in types)
    ]
    rows: list[dict[str, Any]] = [
        {
            "catalog_name": t.catalog,
            "db_schema_name": t.schema,
            "table_name": t.name,
            "table_type": TABLE_TYPE,
        }
        for t in listed
    ]
    if not command.include_schema:
        return pa.Table.from_pylist(rows, schema=TABLES_SCHEMA)
    for row, t in zip(rows, listed, strict=True):
        schema = table_arrow_schema(catalog.columns.get(t.toid, []))
        row["table_schema"] = schema.serialize().to_pybytes()
    return pa.Table.from_pylist(rows, schema=TABLES_WITH_SCHEMA_SCHEMA)


def table_types() -> pa.Table:
    return pa.Table.from_pydict({"table_type": [TABLE_TYPE]}, schema=TABLE_TYPES_SCHEMA)


def _named(catalog: RoleCatalog, command: Any) -> list[_Table]:
    """The tables a key command names: its table, in its catalog and schema where it gives them."""
    want_catalog = _optional(command, "catalog")
    want_schema = _optional(command, "db_schema")
    return [
        t
        for t in catalog.tables
        if t.name == command.table
        and (want_catalog is None or t.catalog == want_catalog)
        and (want_schema is None or t.schema == want_schema)
    ]


def primary_keys(catalog: RoleCatalog, command: pb.CommandGetPrimaryKeys) -> pa.Table:
    toids = {t.toid for t in _named(catalog, command)}
    rows = []
    for con in catalog.constraints:
        if con[3] != "p" or con[7] not in toids:
            continue
        table = catalog.table(con[7])
        for sequence, attnum in enumerate(con[18], 1):
            rows.append(
                {
                    "catalog_name": table.catalog,
                    "db_schema_name": table.schema,
                    "table_name": table.name,
                    "column_name": catalog.names[(con[7], attnum)],
                    "key_name": con[1],
                    "key_sequence": sequence,
                }
            )
    rows.sort(key=lambda r: (r["catalog_name"], r["db_schema_name"], r["table_name"]))
    return pa.Table.from_pylist(rows, schema=PRIMARY_KEYS_SCHEMA)


def _key_name(catalog: RoleCatalog, toid: int, attnums: list[int]) -> str | None:
    """The name of the primary or unique key of ``toid`` on exactly ``attnums``."""
    for con in catalog.constraints:
        if con[3] in ("p", "u") and con[7] == toid and sorted(con[18]) == sorted(attnums):
            return con[1]
    return None


def foreign_keys(
    catalog: RoleCatalog,
    *,
    fk_tables: set[int] | None = None,
    pk_tables: set[int] | None = None,
) -> pa.Table:
    """Foreign keys whose referencing table is one of ``fk_tables`` and whose referenced table is
    one of ``pk_tables`` (either unconstrained when None): imported keys, exported keys and the
    cross reference of a pair of tables."""
    rows = []
    for con in catalog.constraints:
        if con[3] != "f":
            continue
        fk_toid, pk_toid = con[7], con[11]
        if fk_tables is not None and fk_toid not in fk_tables:
            continue
        if pk_tables is not None and pk_toid not in pk_tables:
            continue
        fk, pk = catalog.table(fk_toid), catalog.table(pk_toid)
        pk_name = _key_name(catalog, pk_toid, list(con[19]))
        for sequence, (fk_attnum, pk_attnum) in enumerate(zip(con[18], con[19], strict=True), 1):
            rows.append(
                {
                    "pk_catalog_name": pk.catalog,
                    "pk_db_schema_name": pk.schema,
                    "pk_table_name": pk.name,
                    "pk_column_name": catalog.names[(pk_toid, pk_attnum)],
                    "fk_catalog_name": fk.catalog,
                    "fk_db_schema_name": fk.schema,
                    "fk_table_name": fk.name,
                    "fk_column_name": catalog.names[(fk_toid, fk_attnum)],
                    "key_sequence": sequence,
                    "fk_key_name": con[1],
                    "pk_key_name": pk_name,
                    "update_rule": _RULES[con[12]],
                    "delete_rule": _RULES[con[13]],
                }
            )
    rows.sort(
        key=lambda r: (
            r["pk_catalog_name"] or "",
            r["pk_db_schema_name"] or "",
            r["pk_table_name"],
            r["key_sequence"],
        )
    )
    return pa.Table.from_pylist(rows, schema=FOREIGN_KEYS_SCHEMA)


def answer(catalog: RoleCatalog, command: Any) -> pa.Table:
    """The answer to a catalog command."""
    if isinstance(command, pb.CommandGetCatalogs):
        return catalogs(catalog)
    if isinstance(command, pb.CommandGetDbSchemas):
        return db_schemas(catalog, command)
    if isinstance(command, pb.CommandGetTables):
        return tables(catalog, command)
    if isinstance(command, pb.CommandGetTableTypes):
        return table_types()
    if isinstance(command, pb.CommandGetPrimaryKeys):
        return primary_keys(catalog, command)
    if isinstance(command, pb.CommandGetImportedKeys):
        return foreign_keys(catalog, fk_tables={t.toid for t in _named(catalog, command)})
    if isinstance(command, pb.CommandGetExportedKeys):
        return foreign_keys(catalog, pk_tables={t.toid for t in _named(catalog, command)})
    if isinstance(command, pb.CommandGetCrossReference):
        return foreign_keys(
            catalog,
            pk_tables={t.toid for t in _named_pair(catalog, command, "pk")},
            fk_tables={t.toid for t in _named_pair(catalog, command, "fk")},
        )
    raise TypeError(f"{type(command).__name__} is not a catalog command")


def _named_pair(catalog: RoleCatalog, command: Any, side: str) -> list[_Table]:
    """The ``pk`` or ``fk`` table a cross reference names."""
    want_catalog = _optional(command, f"{side}_catalog")
    want_schema = _optional(command, f"{side}_db_schema")
    name = getattr(command, f"{side}_table")
    return [
        t
        for t in catalog.tables
        if t.name == name
        and (want_catalog is None or t.catalog == want_catalog)
        and (want_schema is None or t.schema == want_schema)
    ]


#: Each catalog command's answer schema, for its FlightInfo.
def answer_schema(command: Any) -> pa.Schema:
    if isinstance(command, pb.CommandGetCatalogs):
        return CATALOGS_SCHEMA
    if isinstance(command, pb.CommandGetDbSchemas):
        return DB_SCHEMAS_SCHEMA
    if isinstance(command, pb.CommandGetTables):
        return TABLES_WITH_SCHEMA_SCHEMA if command.include_schema else TABLES_SCHEMA
    if isinstance(command, pb.CommandGetTableTypes):
        return TABLE_TYPES_SCHEMA
    if isinstance(command, pb.CommandGetPrimaryKeys):
        return PRIMARY_KEYS_SCHEMA
    return FOREIGN_KEYS_SCHEMA


CATALOG_COMMANDS = (
    pb.CommandGetCatalogs,
    pb.CommandGetDbSchemas,
    pb.CommandGetTables,
    pb.CommandGetTableTypes,
    pb.CommandGetPrimaryKeys,
    pb.CommandGetImportedKeys,
    pb.CommandGetExportedKeys,
    pb.CommandGetCrossReference,
)

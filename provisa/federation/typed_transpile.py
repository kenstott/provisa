# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e1f3a-6d2c-49a7-8e5b-1c9f7a3d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registered column types an engine statement is transpiled with (REQ-2236).

An engine whose dialect writes a construct by its operands' types (Db2 and SAP HANA,
``typed_columns.TYPED_DIALECTS``) is handed the types of every registered table's columns, by
each name a statement bound for the engine gives the table: its catalog-physical name, the name
with the catalog folded into the schema on an engine whose SQL has none, and its replica's
address where it is served from its replica (REQ-1912). Built from what the registry published
for the org (``state.tables`` and ``state.schema_build_cache["column_types"]``)."""

# Requirements: REQ-2236

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.compiler.schema_types import physical_table_name
from provisa.federation.replica_address import TableKey, engine_table_keys
from provisa.transpiler.typed_columns import TYPED_DIALECTS

#: Where a key with no catalog (a replica's address on an engine whose SQL has none) is held in
#: the schema, which has one depth for every table: a statement names such a table by schema and
#: table, which sqlglot finds as the one table of the schema under any catalog.
_NO_CATALOG = ""


def registered_column_types(state: Any, engine: Any) -> dict[str, Any]:
    """Each registered table's column types, as a sqlglot schema (catalog -> schema -> table ->
    column -> type) keyed by each name a statement bound for ``engine`` gives it. A name two
    tables of different columns share is left out, so neither is typed by the other's columns."""
    physical = state.kafka_table_physical
    routes = state.replica_routes.routes
    by_key: dict[TableKey, dict[str, exp.DataType]] = {}
    shared: set[TableKey] = set()
    for table in state.tables:
        columns = _registered_columns(state, table)
        if not columns:
            continue  # no column of it is registered: nothing to type it by
        name = physical_table_name(physical, table["source_id"], table["table_name"])
        keys = engine_table_keys(
            engine, state.catalog_for(table["source_id"]), table["schema_name"], name
        )
        for key in keys:
            route = routes.get(key)
            for named in (key,) if route is None else (key, route.target):
                if named in by_key and by_key[named] != columns:
                    shared.add(named)
                by_key[named] = columns
    nested: dict[str, Any] = {}
    for (catalog, schema, table), columns in by_key.items():
        if (catalog, schema, table) in shared:
            continue
        nested.setdefault(catalog or _NO_CATALOG, {}).setdefault(schema, {})[table] = columns
    return nested


def source_column_types(state: Any, source_id: str) -> dict[str, Any]:
    """The column types of each table registered on ``source_id``, as a sqlglot schema keyed by
    the name a statement sent straight to that source (the DIRECT route) gives it: schema and
    table, with no catalog (``sql_rewrite.rewrite_semantic_to_physical``). A name two tables of
    different columns share is left out, as in :func:`registered_column_types`."""
    physical = state.kafka_table_physical
    by_key: dict[tuple[str, str], dict[str, exp.DataType]] = {}
    shared: set[tuple[str, str]] = set()
    for table in state.tables:
        if table["source_id"] != source_id:
            continue
        columns = _registered_columns(state, table)
        if not columns:
            continue  # no column of it is registered: nothing to type it by
        key = (table["schema_name"], physical_table_name(physical, source_id, table["table_name"]))
        if key in by_key and by_key[key] != columns:
            shared.add(key)
        by_key[key] = columns
    schemas: dict[str, Any] = {}
    for (schema, table), columns in by_key.items():
        if (schema, table) not in shared:
            schemas.setdefault(schema, {})[table] = columns
    return {_NO_CATALOG: schemas} if schemas else {}


def direct_column_types(state: Any, source_id: str, dialect: str) -> dict[str, Any] | None:
    """The column types a statement sent straight to ``source_id`` in ``dialect`` is transpiled
    with: the source's registered types for a dialect that writes by its operands' types
    (``typed_columns.TYPED_DIALECTS``: Db2, SAP HANA), None for every other dialect, which reads
    none."""
    if dialect not in TYPED_DIALECTS:
        return None
    return source_column_types(state, source_id)


def _registered_columns(state: Any, table: dict[str, Any]) -> dict[str, exp.DataType]:
    """The registered columns of ``table`` by name, each with its PostgreSQL type."""
    return {
        column.column_name: exp.DataType.build(column.data_type, dialect="postgres", udt=True)
        for column in state.schema_build_cache["column_types"].get(table["id"], ())
    }

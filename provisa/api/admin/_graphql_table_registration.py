# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a71-6f85-4d3b-a1c9-3b7e5d8f0a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a table of a remote GraphQL source (REQ-308, REQ-1923).

Adding a remote GraphQL source registers no tables. Every table its schema offers is listed by
the Register Table picker, and the steward registers the ones wanted through the same mutation
every other source uses; that registration is the curation step. This module is what the
mutation needs from the source -- the tables on offer, a table's columns, the columns as they
are stored once the table has been fitted to what the source's credential may read, and how the
registered table is read.

The schema a source's tables are offered from is the brand's, shipped with Provisa, for a
branded source (GitHub, GitLab), and the source's own, read from its endpoint, for any other.
"""

# Requirements: REQ-308, REQ-1923
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.compiler.naming import apply_sql_name
from provisa.core.schema_org import registered_tables, sources, table_columns
from provisa.graphql_remote.brands import (
    BRANDS,
    LiveSchema,
    SchemaOffer,
    available_tables,
    map_table,
    table_spec,
)

if TYPE_CHECKING:
    from provisa.core.models import Column

log = logging.getLogger(__name__)

# Mapper column type -> the engine type stored as table_columns.data_type.
_PHYSICAL_TYPE = {
    "text": "varchar",
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "jsonb": "json",
}

# Where a plain source's row records how each of its registered tables is read
# (``sources.mapping``, the per-type table mapping every non-relational source keeps there).
TABLE_SPECS_KEY = "tables"
# What of a mapped table says how it is read.
SPEC_FIELDS = (
    "name",  # the table as the schema names it; the registered name is the row's own
    "field_name",
    "gql_type_name",
    "required_args",
    "pagination",
    "rows_path",
)


async def source_offer(state: Any, source_id: str) -> tuple[SchemaOffer, dict] | None:
    """What ``source_id`` offers its tables from, and its live registration. None when it is not
    a remote GraphQL source this process holds.

    A plain source's schema is read from its endpoint the first time it is needed and kept with
    the registration until the source is refreshed."""
    reg = getattr(state, "graphql_remote_sources", {}).get(source_id)
    if reg is None:
        return None
    if reg.get("brand"):
        return BRANDS[reg["brand"]], reg
    if reg.get("schema") is None:
        from provisa.graphql_remote.introspect import introspect_schema

        reg["schema"] = await introspect_schema(reg["url"], reg.get("auth"))
    limits = state.config.graphql_remote
    return (
        LiveSchema(
            label=source_id,
            introspected=reg["schema"],
            max_object_depth=limits.max_object_depth,
            max_list_depth=limits.max_list_depth,
            max_list_items=limits.max_list_items,
            field_overrides=reg.get("field_overrides") or None,
        ),
        reg,
    )


def offered_tables(offer: SchemaOffer, reg: dict) -> list[dict]:
    """Every table the source offers, each as ``{name, description}`` under the name it
    registers with."""
    return [
        {"name": t["sql_name"], "description": t.get("description")}
        for t in available_tables(offer, reg["namespace"])
    ]


def offered_name(offer: SchemaOffer, reg: dict, table_name: str) -> str:
    """The name the schema offers ``table_name`` under. A table is registered under its sql
    name; a model written by hand may name the remote field as the remote spells it."""
    for name in (table_name, apply_sql_name(table_name)):
        if table_spec(offer, reg["namespace"], name) is not None:
            return name
    raise KeyError(f"{offer.label} offers no table {table_name!r}")


async def registered_column_names(
    state: Any, source_id: str, schema_name: str, table_name: str
) -> set[str]:
    """The columns ``table_name`` is registered with; empty when it is not registered."""
    async with state.tenant_db.acquire() as conn:
        rows = await conn.execute_core(
            select(table_columns.c.column_name)
            .select_from(
                table_columns.join(
                    registered_tables, registered_tables.c.id == table_columns.c.table_id
                )
            )
            .where(
                registered_tables.c.source_id == source_id,
                registered_tables.c.schema_name == schema_name,
                registered_tables.c.table_name == table_name,
            )
        )
        return {r.column_name for r in rows.fetchall()}


def _column_models(table: dict) -> "list[Column]":
    """A mapped table's columns as they are stored: each selected field, then one native-filter
    column per argument the table's root field requires."""
    from provisa.api.admin.graphql_remote_router import _build_object_fields
    from provisa.core.models import Column

    columns = [
        Column(
            name=apply_sql_name(c["name"]),
            visible_to=[],
            description=c.get("description"),
            data_type=_PHYSICAL_TYPE[c["type"]],
            object_fields=_build_object_fields(c.get("gql_object_fields") or []),
            gql_selection=c.get("gql_selection"),
        )
        for c in table["columns"]
    ]
    columns += [
        Column(
            name=f"_nf_{apply_sql_name(a['name'])}",
            visible_to=[],
            native_filter_type="query_param",
            data_type=_PHYSICAL_TYPE[a["provisa_type"]],
        )
        for a in table.get("required_args") or []
    ]
    return columns


def offered_columns(
    offer: SchemaOffer, reg: dict, table_name: str
) -> list[tuple[str, str, str | None]]:
    """The columns a not-yet-registered table offers, for the picker's column list: each as
    (name, stored type, description), native-filter columns included."""
    name = offered_name(offer, reg, table_name)
    table = map_table(offer, reg["namespace"], reg["source_id"], "", name)
    return [
        (apply_sql_name(c["name"]), _PHYSICAL_TYPE[c["type"]], c.get("description"))
        for c in table["columns"]
    ] + [
        (f"_nf_{apply_sql_name(a['name'])}", _PHYSICAL_TYPE[a["provisa_type"]], None)
        for a in table.get("required_args") or []
    ]


async def columns_to_register(
    offer: SchemaOffer,
    reg: dict,
    table_name: str,
    domain_id: str,
    chosen: "list[Column]",
    page_size: int,
) -> "tuple[list[Column], list[dict], dict]":
    """The columns to store for a table being registered, the fields left out of it, and the
    table as it is read.

    ``chosen`` is what the steward picked -- its governance (visibility, masking, alias) is
    kept, and when it names columns only those are registered. A native-filter column is always
    registered: without it the table's required arguments cannot be supplied.

    The table, with the columns picked, is offered to the remote once with the source's
    credential at ``page_size``, the size a read asks for, where the source declares something
    to check for. A field the remote refuses that credential is left out and reported; a query
    the remote prices above what it allows raises ``QueryTooComplex``
    (provisa.graphql_remote.probe).
    """
    from provisa.graphql_remote.probe import fit_table_to_credential

    name = offered_name(offer, reg, table_name)
    mapped = map_table(offer, reg["namespace"], reg["source_id"], domain_id, name)
    picked = {c.name for c in chosen if c.native_filter_type is None}
    if picked:
        kept = [c for c in mapped["columns"] if apply_sql_name(c["name"]) in picked]
        mapped = {**mapped, "columns": kept}
    fitted, omitted = await fit_table_to_credential(
        reg["url"],
        reg.get("auth"),
        mapped,
        offer.refused_error_types,
        page_size,
        offer.too_complex_messages,
    )
    offered = _column_models(fitted)
    by_choice = {c.name: c for c in chosen}
    columns = []
    for col in offered:
        pick = by_choice.get(col.name)
        if pick is None:
            columns.append(col)
            continue
        columns.append(
            pick.model_copy(
                update={
                    "data_type": col.data_type,
                    "object_fields": col.object_fields,
                    "gql_selection": col.gql_selection,
                    "native_filter_type": col.native_filter_type,
                    "description": pick.description or col.description,
                }
            )
        )
    return columns, omitted, fitted


async def refreshed_registered_tables(state: Any, offer: SchemaOffer, reg: dict) -> list[dict]:
    """The source's registered tables as the schema now describes them: each mapped again, with
    the columns it is registered with (a column the schema has lost is gone; one it has gained
    is on offer and is not added), in the domain it was registered into. A registered table the
    schema no longer offers is not among them."""
    async with state.tenant_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    registered_tables.c.table_name,
                    registered_tables.c.domain_id,
                    table_columns.c.column_name,
                )
                .select_from(
                    registered_tables.join(
                        table_columns, table_columns.c.table_id == registered_tables.c.id
                    )
                )
                .where(
                    registered_tables.c.source_id == reg["source_id"],
                    registered_tables.c.schema_name == "graphql",
                )
            )
        ).fetchall()
    registered: dict[str, tuple[str, set[str]]] = {}
    for row in rows:
        registered.setdefault(row.table_name, (row.domain_id or "", set()))[1].add(row.column_name)
    tables = []
    for table_name, (domain_id, column_names) in registered.items():
        try:
            name = offered_name(offer, reg, table_name)
        except KeyError:
            continue  # no longer offered; retired by the caller unless something refers to it
        mapped = map_table(offer, reg["namespace"], reg["source_id"], domain_id, name)
        columns = [c for c in mapped["columns"] if apply_sql_name(c["name"]) in column_names]
        tables.append(
            {**mapped, "columns": columns, "sql_name": table_name, "registered_name": table_name}
        )
    return tables


async def remember_table(state: Any, reg: dict, table_name: str, fitted: dict) -> None:
    """Record how a table being registered is read: in this process's registration, which the
    readers consult, and -- for a plain source -- on the source's row, so a process that starts
    later reads the table without asking the remote for its schema. A branded source's tables
    are read by the brand's shipped schema and need nothing stored."""
    entry = {**fitted, "sql_name": table_name}
    reg["tables"] = [t for t in reg.get("tables", []) if t.get("sql_name") != table_name]
    reg["tables"].append(entry)
    if reg.get("brand"):
        return
    spec = {k: fitted[k] for k in SPEC_FIELDS if k in fitted}
    async with state.tenant_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(sources.c.mapping).where(sources.c.id == reg["source_id"])
            )
        ).fetchone()
        mapping = dict(row.mapping or {}) if row is not None else {}
        specs = dict(mapping.get(TABLE_SPECS_KEY) or {})
        specs[table_name] = spec
        mapping[TABLE_SPECS_KEY] = specs
        await conn.execute_core(
            sources.update().where(sources.c.id == reg["source_id"]).values(mapping=mapping)
        )


async def sync_detected_relationships(state: Any, source_id: str) -> int:
    """Store the relationships the schema shows between this source's REGISTERED tables
    (REQ-313), and return how many. A relationship is one object-typed column of a registered
    table whose type another registered table returns; it is stored only when that column is
    among the columns registered. Nothing is stored for a source that is not a remote GraphQL
    source, or has fewer than two tables registered in this process."""
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as rel_repo
    from provisa.graphql_remote.mapper import map_schema

    if source_id not in getattr(state, "graphql_remote_sources", {}):
        return 0
    reg = state.graphql_remote_sources[source_id]
    registered = {t["name"]: t for t in reg.get("tables", []) if t.get("gql_type_name")}
    if len(registered) < 2:
        return 0
    offered = await source_offer(state, source_id)
    assert offered is not None
    offer, _ = offered
    _, _, detected = map_schema(
        offer.schema(),
        reg["namespace"],
        source_id,
        "",
        max_object_depth=offer.max_object_depth,
        max_list_depth=offer.max_list_depth,
        max_list_items=offer.max_list_items,
        field_overrides=offer.field_overrides,
        only=set(registered),
    )
    stored = 0
    async with state.tenant_db.acquire() as conn:
        for rel in detected:
            source = registered.get(rel["source_table_id"])
            target = registered.get(rel["target_table_id"])
            via = rel["id"].rsplit("__", 1)[-1]  # the object column the relationship rides on
            if source is None or target is None:
                continue
            if via not in {c["name"] for c in source["columns"]}:
                continue
            await rel_repo.upsert(
                conn,
                Relationship(
                    id=rel["id"],
                    source_table_id=source["sql_name"],
                    target_table_id=target["sql_name"],
                    source_column=apply_sql_name(rel["source_column"])
                    if rel["source_column"]
                    else "",
                    target_column=apply_sql_name(rel["target_column"])
                    if rel["target_column"]
                    else "",
                    cardinality=Cardinality(rel["cardinality"]),
                ),
                origin="admin",  # REQ-1919: completed by an admin's table registration
            )
            stored += 1
    return stored

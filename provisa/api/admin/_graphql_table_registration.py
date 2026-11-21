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
branded source (GitHub, GitLab, Shopify), and the source's own, read from its endpoint, for any other.
"""

# Requirements: REQ-308, REQ-1923
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.compiler.naming import apply_sql_name
from provisa.core.schema_org import registered_tables, sources, table_columns
from provisa.graphql_remote.by_keys import BY_KEYS, TABLE_SPECS_KEY, key_fetches
from provisa.graphql_remote.brands import (
    BRANDS,
    LiveSchema,
    SchemaOffer,
    available_tables,
    map_table,
    map_tables,
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
# What of a mapped table says how it is read.
SPEC_FIELDS = (
    "name",  # the table as the schema names it; the registered name is the row's own
    "field_name",
    "gql_type_name",
    "required_args",
    "optional_args",
    "pagination",
    "rows_path",
    BY_KEYS,  # REQ-1865: the columns its rows are fetched by, a list at a time
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
    """Every table the source offers, each as ``{name, description, connection,
    required_parameters}`` under the name it registers with; ``connection``: its rows sit under a
    Relay connection; ``required_parameters``: the arguments a read of it must give (REQ-1986)."""
    from provisa.core.parameters import required_parameters

    return [
        {
            "name": t["sql_name"],
            "description": t.get("description"),
            "connection": bool(t.get("rows_path")),
            "required_parameters": required_parameters(parameter_columns(t)),
        }
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
    async with state.model_db.acquire() as conn:
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
            gql_object_type=c.get("gql_object_type"),
            gql_is_list=c.get("gql_is_list", False),
        )
        for c in table["columns"]
    ]
    return columns + parameter_columns(table)


def parameter_columns(table: dict) -> "list[Column]":
    """A mapped table's native-filter columns, one per argument of its root field; a table spec
    without its columns has them too."""
    from provisa.core.models import Column

    return [
        Column(
            name=f"_nf_{apply_sql_name(a['name'])}",
            visible_to=[],
            native_filter_type="graphql_arg",
            # REQ-1986: required where the schema requires it (non-null, with no default).
            native_filter_required=required,
            data_type=_PHYSICAL_TYPE[a["provisa_type"]],
        )
        for a, required in arguments(table)
    ]


def arguments(table: dict) -> list[tuple[dict, bool]]:
    """A mapped table's arguments, each with whether the remote schema requires it (REQ-1986)."""
    return [(a, True) for a in table.get("required_args") or []] + [
        (a, False) for a in table.get("optional_args") or []
    ]


def offered_columns(
    offer: SchemaOffer, reg: dict, table_name: str
) -> list[tuple[str, str, str | None]]:
    """The columns a not-yet-registered table offers, for the picker's column list: each as
    (name, stored type, description), native-filter columns included."""
    name = offered_name(offer, reg, table_name)
    return _picker_columns(map_table(offer, reg["namespace"], reg["source_id"], "", name))


def offered_columns_of_every_table(
    offer: SchemaOffer, reg: dict
) -> dict[str, list[tuple[str, str, str | None]]]:
    """``offered_columns`` of every table the source offers, by its registered name, read in
    one walk of the schema (REQ-464): a table at a time, a shipped schema of 500 tables took
    minutes."""
    names = [t["name"] for t in offered_tables(offer, reg)]
    mapped = map_tables(offer, reg["namespace"], reg["source_id"], "", names)
    return {name: _picker_columns(table) for name, table in mapped.items()}


def _picker_columns(table: dict) -> list[tuple[str, str, str | None]]:
    return [
        (apply_sql_name(c["name"]), _PHYSICAL_TYPE[c["type"]], c.get("description"))
        for c in table["columns"]
    ] + [
        (f"_nf_{apply_sql_name(a['name'])}", _PHYSICAL_TYPE[a["provisa_type"]], None)
        for a, _ in arguments(table)
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
        offer.error_policy,
    )
    fitted = {**fitted, BY_KEYS: key_fetches(offer.schema(), fitted)}
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
                    "gql_object_type": col.gql_object_type,
                    "gql_is_list": col.gql_is_list,
                    "native_filter_type": col.native_filter_type,
                    "native_filter_required": col.native_filter_required,
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
    async with state.model_db.acquire() as conn:
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
            {
                **mapped,
                "columns": columns,
                "sql_name": table_name,
                "registered_name": table_name,
                BY_KEYS: key_fetches(offer.schema(), mapped),
            }
        )
    return tables


async def remember_table(state: Any, reg: dict, table_name: str, fitted: dict) -> None:
    """Record how a table is read: in this process's registration, which the readers consult,
    and -- for a plain source -- on the source's row, so a process that starts later reads the
    table without asking the remote for its schema. A branded source's tables are read by the
    brand's shipped schema and need nothing stored."""
    entry = {**fitted, "sql_name": table_name}
    async with state.model_db.acquire() as conn:
        await _store_spec(conn, reg, table_name, entry)
    _hold(reg, entry)


def _hold(reg: dict, entry: dict) -> None:
    """``entry`` is how this process reads its table, in place of what it held for it."""
    reg["tables"] = [t for t in reg.get("tables", []) if t.get("sql_name") != entry["sql_name"]]
    reg["tables"].append(entry)


async def _store_spec(conn: Any, reg: dict, table_name: str, entry: dict) -> None:
    """How ``table_name`` is read, kept on a plain source's row; nothing for a branded one."""
    if reg.get("brand"):
        return
    spec = {k: entry[k] for k in SPEC_FIELDS if k in entry}
    row = (
        await conn.execute_core(select(sources.c.mapping).where(sources.c.id == reg["source_id"]))
    ).fetchone()
    mapping = dict(row.mapping or {}) if row is not None else {}
    specs = dict(mapping.get(TABLE_SPECS_KEY) or {})
    specs[table_name] = spec
    mapping[TABLE_SPECS_KEY] = specs
    await conn.execute_core(
        sources.update().where(sources.c.id == reg["source_id"]).values(mapping=mapping)
    )


# A table being registered, from the step that resolves its columns to the transaction that
# stores it (REQ-1929, A COLUMN IN USE REFUSES THE REGISTRATION): this process holds it only
# once that transaction commits, so a refused registration leaves nothing behind.
_PENDING: dict[tuple[str, str], dict] = {}


def hold_pending(reg: dict, table_name: str, fitted: dict) -> None:
    """``fitted`` is how ``table_name`` is read once the registration in progress commits."""
    _PENDING[(reg["source_id"], table_name)] = {**fitted, "sql_name": table_name}


def drop_pending(source_id: str, table_name: str) -> None:
    """The registration of ``table_name`` in progress resolves no new columns."""
    _PENDING.pop((source_id, table_name), None)


def held_table(state: Any, source_id: str, table_name: str) -> dict | None:
    """How ``table_name`` is read: as the registration in progress has it, else as this process
    holds it; None when neither has it."""
    pending = _PENDING.get((source_id, table_name))
    if pending is not None:
        return pending
    reg = (getattr(state, "graphql_remote_sources", None) or {}).get(source_id)
    tables = reg["tables"] if reg is not None else []
    return next((t for t in tables if t["sql_name"] == table_name), None)


@dataclass
class StagedRegistration:
    """What a registration's transaction stored that this process takes once it commits: the
    table as it is read, and each nested column a stored relationship took (table, column)."""

    reg: dict
    entry: dict | None
    taken: list[tuple[str, str]]

    def apply(self) -> int:
        """This process takes what was stored; the number of relationships stored."""
        if self.entry is not None:
            _hold(self.reg, self.entry)
        for table_name, via in self.taken:
            table = next(t for t in self.reg["tables"] if t["sql_name"] == table_name)
            table["columns"] = [c for c in table["columns"] if c["name"] != via]
        return len(self.taken)


async def stage_registration(
    conn: Any, state: Any, source_id: str, table_name: str
) -> StagedRegistration | None:
    """On a registration's own transaction, after its table is stored: how the table is read,
    kept with the source, and the relationships it completes, each taking its nested column
    (REQ-313, REQ-1929). ``ColumnDropRefused`` when a column so taken is in use, and the
    transaction stores nothing. None for a source that is not a remote GraphQL source."""
    reg = (getattr(state, "graphql_remote_sources", None) or {}).get(source_id)
    if reg is None:
        return None
    entry = _PENDING.pop((source_id, table_name), None)
    tables = list(reg.get("tables", []))
    if entry is not None:
        await _store_spec(conn, reg, table_name, entry)
        tables = [t for t in tables if t.get("sql_name") != table_name] + [entry]
    taken = await _store_relationships(conn, state, reg, tables)
    return StagedRegistration(reg, entry, taken)


async def stored_key_fetches(state: Any, source_id: str, table_name: str) -> dict[str, dict]:
    """The columns registered ``table_name`` fetches its rows by, a list at a time (REQ-1865,
    graphql_remote.by_keys), as its registration holds them. A table registered before they were
    kept has none held: they are read off the source's schema now and kept, in this process and
    with the source's row."""
    reg = state.graphql_remote_sources[source_id]
    entry = next(t for t in reg["tables"] if t.get("sql_name") == table_name)
    if BY_KEYS in entry:
        return entry[BY_KEYS]
    offered = await source_offer(state, source_id)
    assert offered is not None  # the source is in graphql_remote_sources
    offer, _ = offered
    name = offered_name(offer, reg, table_name)
    entry[BY_KEYS] = key_fetches(
        offer.schema(), map_table(offer, reg["namespace"], source_id, "", name)
    )
    if not reg.get("brand"):
        async with state.model_db.acquire() as conn:
            row = (
                await conn.execute_core(select(sources.c.mapping).where(sources.c.id == source_id))
            ).fetchone()
            mapping = dict(row.mapping or {})
            specs = dict(mapping.get(TABLE_SPECS_KEY) or {})
            specs[table_name] = {**(specs.get(table_name) or {}), BY_KEYS: entry[BY_KEYS]}
            mapping[TABLE_SPECS_KEY] = specs
            await conn.execute_core(
                sources.update().where(sources.c.id == source_id).values(mapping=mapping)
            )
    return entry[BY_KEYS]


async def sync_detected_relationships(state: Any, source_id: str) -> int:
    """Store the relationships the schema shows between this source's REGISTERED tables
    (REQ-313), in one transaction, and return how many; this process takes the columns they
    took once it commits. 0 for a source that is not a remote GraphQL source."""
    if source_id not in getattr(state, "graphql_remote_sources", {}):
        return 0
    reg = state.graphql_remote_sources[source_id]
    async with state.model_db.acquire() as conn:
        async with conn.transaction():
            taken = await _store_relationships(conn, state, reg, list(reg.get("tables", [])))
    return StagedRegistration(reg, None, taken).apply()


async def _store_relationships(
    conn: Any, state: Any, reg: dict, tables: list[dict]
) -> list[tuple[str, str]]:
    """Store the relationships the schema shows between ``tables``, the source's registered
    tables, and return the nested column each took (table, column). A relationship is one
    object-typed column of a registered table whose type another registered table returns; it is
    stored only when that column is among the columns registered, and storing it takes that
    column out of the table (REQ-1929). Nothing is stored when fewer than two tables are
    registered. ``tables`` is not changed."""
    from provisa.graphql_remote.mapper import map_schema

    registered = {t["name"]: t for t in tables if t.get("gql_type_name")}
    if len(registered) < 2:
        return []
    offered = await source_offer(state, reg["source_id"])
    assert offered is not None
    offer, _ = offered
    _, _, detected = map_schema(
        offer.schema(),
        reg["namespace"],
        reg["source_id"],
        "",
        max_object_depth=offer.max_object_depth,
        max_list_depth=offer.max_list_depth,
        max_list_items=offer.max_list_items,
        field_overrides=offer.field_overrides,
        only=set(registered),
    )
    taken: list[tuple[str, str]] = []
    for rel in detected:
        source = registered.get(rel["source_table_id"])
        target = registered.get(rel["target_table_id"])
        via = rel["id"].rsplit("__", 1)[-1]  # the object column the relationship rides on
        if source is None or target is None:
            continue
        if via not in {c["name"] for c in source["columns"]}:
            continue
        if (source["sql_name"], via) in taken:
            continue
        await _store_taking_its_column(conn, reg, source, target, rel, via)
        taken.append((source["sql_name"], via))
    return taken


async def _store_taking_its_column(
    conn: Any, reg: dict, source: dict, target: dict, rel: dict, via: str
) -> None:
    """Store ``rel`` and take the nested column ``via`` it rides on out of its source table's
    registration (REQ-1929, A STORED RELATIONSHIP TAKES ITS NESTED COLUMN): a source mapped in
    full never has that column, so a relationship stored once both tables are registered leaves
    the model as that mapping does, with no column of the relationship's name. A column other
    objects refer to is not taken: ``ColumnDropRefused`` names them, and nothing is stored."""
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories.integrity import column_dependents
    from provisa.core.repositories.table import ColumnDropRefused

    column = apply_sql_name(via)
    table_id = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.source_id == reg["source_id"],
                registered_tables.c.schema_name == "graphql",
                registered_tables.c.table_name == source["sql_name"],
            )
        )
    ).scalar_one()
    dependents = await column_dependents(conn, table_id, column)
    if dependents:
        raise ColumnDropRefused(source["sql_name"], {column: dependents})
    await conn.execute_core(
        table_columns.delete().where(
            table_columns.c.table_id == table_id, table_columns.c.column_name == column
        )
    )
    await rel_repo.upsert(
        conn,
        Relationship(
            id=rel["id"],
            source_table_id=source["sql_name"],
            target_table_id=target["sql_name"],
            source_column=apply_sql_name(rel["source_column"]) if rel["source_column"] else "",
            target_column=apply_sql_name(rel["target_column"]) if rel["target_column"] else "",
            cardinality=Cardinality(rel["cardinality"]),
        ),
    )

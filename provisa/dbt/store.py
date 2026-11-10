# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6fb3d9-52e4-4a17-9d83-b74e12a8f630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What is held of a dbt source, and what follows from it when a new manifest arrives (REQ-2228).

Held per source in the control plane, and read on each use, so every process sees the newest
manifest. A new manifest replaces what is held: the commands on offer and the lineage follow from
it, and the descriptions of the models already registered are updated in place.
"""

# Requirements: REQ-2228

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, update

from provisa.api.errors import ApiError
from provisa.compiler.naming import domain_to_sql_name
from provisa.core.schema_org import dbt_manifests, registered_tables, sources, table_columns
from provisa.dbt.commands import TargetRefused, cloud_binding, command_named, script_binding
from provisa.dbt.lineage import compiled_views
from provisa.dbt.manifest import DbtManifest, DbtModel, from_definitions, to_definitions
from provisa.dbt.warehouse import warehouse_for

#: Where a dbt source's manifest comes from, in its row's ``mapping``.
SPEC_CLOUD = "cloud"
SPEC_LOCATION = "location"
SPEC_UPLOAD = "upload"
#: The dbt source type, as a source row names it.
DBT = "dbt"


@dataclass
class _Warehouse:
    id: str
    type: str
    database: str
    path: str | None


async def held_manifest(conn: Any, source_id: str) -> DbtManifest | None:
    row = (
        await conn.execute_core(
            select(dbt_manifests.c.definitions).where(dbt_manifests.c.source_id == source_id)
        )
    ).fetchone()
    return None if row is None else from_definitions(row.definitions)


async def warehouse_rows(conn: Any) -> list[_Warehouse]:
    """Every registered source a manifest's database could be read through."""
    rows = (
        await conn.execute_core(
            select(sources.c.id, sources.c.type, sources.c.database, sources.c.path).where(
                sources.c.type != DBT
            )
        )
    ).fetchall()
    return [_Warehouse(r.id, r.type, r.database or "", r.path) for r in rows]


async def model_warehouse(conn: Any, manifest: DbtManifest, model: DbtModel) -> str | None:
    """The id of the source that reads ``model``; None when none matches its adapter and database."""
    return warehouse_for(manifest.adapter, model.database, await warehouse_rows(conn))


async def _registered_of(conn: Any, source_id: str) -> list[Any]:
    return (
        await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
            ).where(registered_tables.c.source_id == source_id)
        )
    ).fetchall()


async def model_tables(conn: Any, manifest: DbtManifest) -> list[tuple[DbtModel, Any]]:
    """Each registered table that is a model of ``manifest``: its model and its registered row."""
    warehouses = await warehouse_rows(conn)
    out: list[tuple[DbtModel, Any]] = []
    rows_of: dict[str, list[Any]] = {}
    for model in manifest.models:
        warehouse = warehouse_for(manifest.adapter, model.database, warehouses)
        if warehouse is None:
            continue
        if warehouse not in rows_of:
            rows_of[warehouse] = await _registered_of(conn, warehouse)
        out.extend(
            (model, row)
            for row in rows_of[warehouse]
            if row.schema_name == model.schema and row.table_name == model.relation
        )
    return out


async def _update_descriptions(conn: Any, manifest: DbtManifest) -> None:
    """What the manifest says of each model already registered, over what the table carries."""
    for model, row in await model_tables(conn, manifest):
        if model.description is not None:
            await conn.execute_core(
                update(registered_tables)
                .where(registered_tables.c.id == row.id)
                .values(description=model.description)
            )
        for column in model.columns:
            if column.description is None:
                continue
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == row.id,
                    table_columns.c.column_name == column.name,
                )
                .values(description=column.description)
            )


async def store_manifest(conn: Any, source_id: str, manifest: DbtManifest) -> None:
    """Hold ``manifest`` as ``source_id``'s, in place of any before it, updating what follows."""
    await conn.upsert(
        dbt_manifests,
        {"source_id": source_id, "definitions": to_definitions(manifest)},
        index_elements=["source_id"],
    )
    await _update_descriptions(conn, manifest)


async def lineage_views(conn: Any) -> list[dict]:
    """Each compiled model of every dbt source, as view rows of the federation graph: a model the
    steward registered under its registered relation, any other under its own."""
    rows = (await conn.execute_core(select(dbt_manifests.c.definitions))).fetchall()
    out: list[dict] = []
    for manifest in (from_definitions(r.definitions) for r in rows):
        registered = {
            (row.schema_name, row.table_name): (
                f"{domain_to_sql_name(row.domain_id)}.{row.table_name}"
            )
            for _, row in await model_tables(conn, manifest)
        }
        out.extend(
            {"relation": relation, "table_name": relation.rsplit(".", 1)[-1], "view_sql": sql}
            for relation, sql in compiled_views(manifest, registered)
        )
    return out


async def command_target(pool: Any, source_id: str, name: str) -> tuple[str, dict[str, Any]]:
    """Where ``name`` runs: the kind of command it is, and its binding."""
    async with pool.acquire() as conn:
        manifest = await held_manifest(conn, source_id)
        row = (
            await conn.execute_core(select(sources.c.mapping).where(sources.c.id == source_id))
        ).fetchone()
    command = None if manifest is None else command_named(manifest, name)
    if command is None or row is None:
        raise ApiError(
            422,
            "functions.operation_not_offered",
            f"Source {source_id!r} offers no operation {name!r}",
            source_id=source_id,
            operation=name,
        )
    mapping = row.mapping or {}
    try:
        if mapping.get("spec") == SPEC_CLOUD:
            cloud = mapping.get("cloud") or {}
            return "http", cloud_binding(
                command,
                source_id=source_id,
                host=cloud.get("host", ""),
                account_id=cloud.get("account_id", ""),
                job_id=cloud.get("job_id", ""),
            )
        return "script", script_binding(command, mapping.get("command_line", ""))
    except TargetRefused as refused:
        raise ApiError(422, "dbt.target_not_stated", str(refused), source_id=source_id) from refused

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
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update

from provisa.api.errors import ApiError
from provisa.compiler.naming import domain_to_sql_name
from provisa.core.schema_org import (
    dbt_manifests,
    dbt_node_runs,
    registered_tables,
    sources,
    table_columns,
)
from provisa.dbt.commands import (
    TargetRefused,
    cloud_binding,
    command_named,
    connector_binding,
    script_binding,
)
from provisa.dbt.lineage import compiled_views
from provisa.dbt.manifest import DbtManifest, DbtModel, from_definitions, to_definitions
from provisa.dbt.run_results import SUCCESS, NodeResult, RunResults
from provisa.dbt.warehouse import warehouse_for

#: Where a dbt source's manifest comes from, in its row's ``mapping``.
SPEC_CLOUD = "cloud"
SPEC_LOCATION = "location"
SPEC_UPLOAD = "upload"
#: The dbt source type, as a source row names it.
DBT = "dbt"
#: The ``@command`` parameter a connector takes the dbt invocation in, unless its source says.
INVOCATION_ARGUMENT = "invocation"


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


async def _model_tables(conn: Any, manifest: DbtManifest) -> list[tuple[DbtModel, Any]]:
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


def _unchanged(current: str | None, manifest_said: str | None) -> bool:
    """Whether a field still holds what the last manifest said of it: no steward has changed it.
    A manifest that said nothing and a field left empty are the same."""
    return (current or None) == manifest_said


async def _update_table_description(
    conn: Any, row: Any, model: DbtModel, previous: DbtModel | None
) -> None:
    if model.description is None or previous is None:
        return
    current = (
        await conn.execute_core(
            select(registered_tables.c.description).where(registered_tables.c.id == row.id)
        )
    ).scalar()
    if _unchanged(current, previous.description):
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == row.id)
            .values(description=model.description)
        )


async def _update_column_descriptions(
    conn: Any, row: Any, model: DbtModel, previous: DbtModel | None
) -> None:
    if previous is None:
        return
    said = {c.name: c.description for c in previous.columns}
    held = {
        r.column_name: r.description
        for r in (
            await conn.execute_core(
                select(table_columns.c.column_name, table_columns.c.description).where(
                    table_columns.c.table_id == row.id
                )
            )
        ).fetchall()
    }
    for column in model.columns:
        if column.description is None or column.name not in held:
            continue
        if _unchanged(held[column.name], said.get(column.name)):
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == row.id,
                    table_columns.c.column_name == column.name,
                )
                .values(description=column.description)
            )


async def _update_descriptions(
    conn: Any, manifest: DbtManifest, previous: DbtManifest | None
) -> None:
    """What the manifest says of each model already registered, over each description that still
    holds what the manifest before it said: one a steward has changed stands, field by field."""
    if previous is None:
        return
    earlier = {m.unique_id: m for m in previous.models}
    for model, row in await _model_tables(conn, manifest):
        await _update_table_description(conn, row, model, earlier.get(model.unique_id))
        await _update_column_descriptions(conn, row, model, earlier.get(model.unique_id))


async def store_manifest(conn: Any, source_id: str, manifest: DbtManifest) -> None:
    """Hold ``manifest`` as ``source_id``'s, in place of any before it, updating what follows."""
    previous = await held_manifest(conn, source_id)
    await conn.upsert(
        dbt_manifests,
        {"source_id": source_id, "definitions": to_definitions(manifest)},
        index_elements=["source_id"],
    )
    await _update_descriptions(conn, manifest, previous)


def _utc(value: datetime) -> datetime:
    """``value`` comparable with another: a database may hand a stored instant back naive."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


async def _store_node_run(
    conn: Any, source_id: str, generated_at: datetime, result: NodeResult
) -> None:
    held = (
        await conn.execute_core(
            select(dbt_node_runs.c.generated_at, dbt_node_runs.c.built_at).where(
                dbt_node_runs.c.source_id == source_id,
                dbt_node_runs.c.unique_id == result.unique_id,
            )
        )
    ).fetchone()
    if held is not None and _utc(held.generated_at) >= _utc(generated_at):
        return  # a document no newer than what is known of the node says nothing new of it
    built_now = result.status == SUCCESS and result.completed_at is not None
    await conn.upsert(
        dbt_node_runs,
        {
            "source_id": source_id,
            "unique_id": result.unique_id,
            "status": result.status,
            "generated_at": generated_at,
            "built_at": result.completed_at if built_now else (held.built_at if held else None),
        },
        index_elements=["source_id", "unique_id"],
    )


async def store_run_results(conn: Any, source_id: str, results: RunResults) -> None:
    """Add what ``results`` says of the nodes dbt builds to what is known of ``source_id``'s."""
    for result in results.results:
        if result.builds:
            await _store_node_run(conn, source_id, results.generated_at, result)


async def model_freshness_token(conn: Any, source_id: str, unique_id: str) -> str | None:
    """The version of the node's rows: when dbt last built it. None while the node has never been
    built or its latest run did not build it, so its freshness is unknown (REQ-858)."""
    row = (
        await conn.execute_core(
            select(dbt_node_runs.c.status, dbt_node_runs.c.built_at).where(
                dbt_node_runs.c.source_id == source_id, dbt_node_runs.c.unique_id == unique_id
            )
        )
    ).fetchone()
    if row is None or row.status != SUCCESS or row.built_at is None:
        return None
    return _utc(row.built_at).isoformat()


async def dbt_model_tables(conn: Any) -> dict[tuple[str, str, str], tuple[str, str]]:
    """Each model of every dbt source, as ``(warehouse, schema, relation): (source, unique id)``."""
    rows = (
        await conn.execute_core(
            select(dbt_manifests.c.source_id, dbt_manifests.c.definitions).order_by(
                dbt_manifests.c.source_id
            )
        )
    ).fetchall()
    warehouses = await warehouse_rows(conn)
    out: dict[tuple[str, str, str], tuple[str, str]] = {}
    for row in rows:
        manifest = from_definitions(row.definitions)
        for model in manifest.models:
            warehouse = warehouse_for(manifest.adapter, model.database, warehouses)
            if warehouse is not None:
                out[(warehouse, model.schema, model.relation)] = (row.source_id, model.unique_id)
    return out


def model_probe(state: Any, source: Any, table: Any) -> Any:
    """The token transport of a table dbt builds, from its run results; None for any other."""
    held = (getattr(state, "dbt_model_tables", None) or {}).get(
        (source.id, table.schema_name, table.table_name)
    )
    if held is None:
        return None
    source_id, unique_id = held

    async def probe() -> str | None:
        async with state.model_db.acquire() as conn:
            return await model_freshness_token(conn, source_id, unique_id)

    return probe


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
            for _, row in await _model_tables(conn, manifest)
        }
        out.extend(
            {"relation": relation, "table_name": relation.rsplit(".", 1)[-1], "view_sql": sql}
            for relation, sql in compiled_views(manifest, registered)
        )
    return out


async def _require_code_source(pool: Any, source_id: str) -> None:
    from provisa.codesource.languages import CODE_SOURCE_TYPES

    async with pool.acquire() as conn:
        row = (
            await conn.execute_core(select(sources.c.type).where(sources.c.id == source_id))
        ).fetchone()
    if row is None or row.type not in CODE_SOURCE_TYPES:
        raise TargetRefused(f"the connector {source_id!r} is not a registered code source")


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
        connector = mapping.get("connector") or {}
        if connector.get("source_id"):
            await _require_code_source(pool, connector["source_id"])
            return "code_command", connector_binding(
                command,
                source_id=connector["source_id"],
                code_command=connector.get("command", ""),
                invocation_argument=connector.get("invocation_argument") or INVOCATION_ARGUMENT,
            )
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

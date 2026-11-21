# Copyright (c) 2026 Kenneth Stott
# Canary: a37d9c02-e15b-48f6-b08c-6f42d1e7ca95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offering a dbt source's models and registering one (REQ-2228).

Register Table lists the manifest's models and registers nothing by listing them. A model the
steward registers becomes a table of the warehouse source the manifest names, read at the schema
and relation the manifest gives, so it is read as every table of that source is: there is no second
way to reach a dbt model's rows.
"""

# Requirements: REQ-2228

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.dbt.store import DBT, held_manifest, model_warehouse

if TYPE_CHECKING:
    from provisa.api.admin.types import (
        AvailableColumnType,
        AvailableTableType,
        MutationResult,
        TableInput,
    )
    from provisa.dbt.manifest import DbtManifest


async def offered_schemas(source_id: str, conn: Any) -> list[str]:
    manifest = await held_manifest(conn, source_id)
    return [] if manifest is None else sorted({m.schema for m in manifest.models})


async def offered_tables(source_id: str, schema_name: str, conn: Any) -> list[AvailableTableType]:
    from provisa.api.admin.types import AvailableTableType

    manifest = await held_manifest(conn, source_id)
    if manifest is None:
        return []
    return [
        AvailableTableType(name=m.relation, comment=m.description)
        for m in manifest.models
        if m.schema == schema_name
    ]


def _refused(code: str, message: str, **params: Any) -> MutationResult:
    from provisa.api.admin.types import MutationResult

    return MutationResult(success=False, message=message, code=code, params=params)


def _no_warehouse(manifest: DbtManifest, database: str) -> str:
    return (
        f"no registered source reads the {manifest.adapter} database {database!r}; "
        "edit the dbt source to create one"
    )


async def model_columns(
    source_id: str, schema_name: str, table_name: str
) -> list[AvailableColumnType]:
    """The warehouse's columns of a model, each with the description the manifest holds of it."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.admin.schema_query import _introspect_available_columns
    from provisa.api.errors import ApiError

    pool = await _get_pool()
    async with pool.acquire() as conn:
        manifest = await held_manifest(conn, source_id)
        model = None if manifest is None else manifest.model_of(schema_name, table_name)
        if manifest is None or model is None:
            return []
        warehouse = await model_warehouse(conn, manifest, model)
    if warehouse is None:
        raise ApiError(
            422,
            "dbt.no_warehouse",
            _no_warehouse(manifest, model.database),
            adapter=manifest.adapter,
            database=model.database,
        )
    described = {c.name: c.description for c in model.columns}
    columns = await _introspect_available_columns(warehouse, schema_name, table_name)
    for column in columns:
        column.comment = described.get(column.name) or column.comment
    return columns


async def read_through_warehouse(input: TableInput) -> MutationResult | None:
    """``input`` as the registration of the model it names on the warehouse source that reads it,
    with the manifest's descriptions where the steward gave none. None for any other source, and
    when ``input`` has been rewritten; the refusal when the model cannot be read."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.app import state

    if (getattr(state, "source_types", None) or {}).get(input.source_id) != DBT:
        return None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        manifest = await held_manifest(conn, input.source_id)
        model = None if manifest is None else manifest.model_of(input.schema_name, input.table_name)
        if manifest is None or model is None:
            return _refused(
                "dbt.model_not_offered",
                f"Source {input.source_id!r} offers no model "
                f"{input.schema_name}.{input.table_name}",
                source_id=input.source_id,
                schema=input.schema_name,
                table=input.table_name,
            )
        warehouse = await model_warehouse(conn, manifest, model)
    if warehouse is None:
        return _refused(
            "dbt.no_warehouse",
            _no_warehouse(manifest, model.database),
            adapter=manifest.adapter,
            database=model.database,
        )
    input.source_id = warehouse
    input.description = input.description or model.description
    described = {c.name: c.description for c in model.columns}
    for column in input.columns:
        column.description = column.description or described.get(column.name)
    return None

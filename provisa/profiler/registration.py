# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e1d63-a2f5-47c9-8d31-e6c0f9a7b254
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a profile result table, and a table's profiler membership (REQ-1934).

A table registered on a profiler source exposes one result relation of one member table. Its name
says which (``<member>_profile_<kind>``), so its column types, its watermark and its physical
location are derived: the declared columns must be fields of that kind and take its shipped types,
``run_time`` is the watermark, and the relation lives in the org's control-plane schema where the
run writes it. Grants and masks are the operator's, column by column, as on any table. Both registration paths -- the
YAML loader and the admin mutations -- call :func:`derive_result_table`.
"""

from __future__ import annotations

from typing import Any

from provisa.profiler.schema import (
    PROFILE_WATERMARK_COLUMN,
    kind_fields,
    parse_result_table_name,
)
from provisa.profiler.source import PROFILER_SOURCE_TYPE, profiler_settings


def is_profiler_source_type(source_type: Any) -> bool:
    return str(getattr(source_type, "value", source_type)) == PROFILER_SOURCE_TYPE


def derive_result_table(table: Any) -> None:
    """Fix ``table``'s columns to its result kind's shipped schema, IN PLACE, or ValueError.

    A result table is registered like any table: it exposes the columns it declares, each with its
    own grants and masks. What is not the operator's to choose is the shape -- every declared column
    must be a field of the kind, its data type is the shipped one, and the run key (``run_id``,
    ``run_time``) must be declared, ``run_time`` being the watermark.
    """
    _, _, kind = parse_result_table_name(table.table_name)
    if table.profiler_source_id is not None:
        raise ValueError(
            f"Table {table.table_name!r}: a profile result table cannot itself join a profiler"
        )
    shipped = {name: (data_type, description) for name, data_type, description in kind_fields(kind)}
    declared = {c.name for c in table.columns}
    unknown = sorted(declared - set(shipped))
    if unknown:
        raise ValueError(
            f"Table {table.table_name!r}: {unknown} are not fields of the profile {kind} table; "
            f"its fields are {list(shipped)}"
        )
    missing = [k for k in ("run_id", PROFILE_WATERMARK_COLUMN) if k not in declared]
    if missing:
        raise ValueError(
            f"Table {table.table_name!r}: a profile result table must expose {missing}, the run "
            f"key every row carries"
        )
    for column in table.columns:
        data_type, description = shipped[column.name]
        column.data_type = data_type
        if not column.description:
            column.description = description
    table.watermark_column = PROFILE_WATERMARK_COLUMN


def validate_config(config: Any) -> None:
    """The YAML half: profiler sources' settings, result tables' derivation, and membership.

    A config carries no table ids, so a result table's member is matched by name and profiler
    here; the id in its name is checked against the stored member when it is registered through
    the admin surface.
    """
    sources_by_id = {s.id: s for s in config.sources}
    for source in config.sources:
        if is_profiler_source_type(source.type):
            profiler_settings(source.id, dict(source.mapping))
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        if source is not None and is_profiler_source_type(source.type):
            derive_result_table(table)
            member, _, _ = parse_result_table_name(table.table_name)
            if not any(
                t.table_name == member and t.profiler_source_id == source.id for t in config.tables
            ):
                raise ValueError(
                    f"Table {table.table_name!r}: {member!r} is not a member of profiler "
                    f"{source.id!r}, so it has no such profile to expose"
                )
        sid = table.profiler_source_id
        if sid is None:
            continue
        profiler = sources_by_id.get(sid)
        if profiler is None or not is_profiler_source_type(profiler.type):
            raise ValueError(f"Table {table.table_name!r}: {sid!r} is not a Data Profiler source")


async def apply_registration(conn: Any, model: Any) -> None:
    """The admin half: derive a result table registered on a profiler, and check membership."""
    from sqlalchemy import select
    from sqlalchemy.schema import CreateTable

    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources
    from provisa.profiler.schema import result_sa_table
    from provisa.profiler.source import check_membership

    row = (
        await conn.execute_core(select(sources.c.type).where(sources.c.id == model.source_id))
    ).fetchone()
    if row is not None and is_profiler_source_type(row[0]):
        from provisa.api.app import state
        from provisa.core.environments import active_org_schema

        derive_result_table(model)
        member, member_id, kind = parse_result_table_name(model.table_name)
        found = (
            await conn.execute_core(
                select(rt.c.id).where(
                    rt.c.id == member_id,
                    rt.c.table_name == member,
                    rt.c.profiler_source_id == model.source_id,
                )
            )
        ).fetchone()
        if found is None:
            raise ValueError(
                f"Table {model.table_name!r}: {member!r} (id {member_id}) is not a member of "
                f"profiler {model.source_id!r}, so it has no such profile to expose"
            )
        # The relation exists, empty, before the member's first run, so the registered table
        # compiles and reads as an empty history.
        await conn.execute_core(
            CreateTable(result_sa_table(member, member_id, kind), if_not_exists=True)
        )
        # The run writes the relation into the org's control-plane schema (provisa.profiler.run),
        # the location the compiler reaches a profiler's tables through, as for ingest (REQ-1771).
        model.schema_name = active_org_schema(state.org_id, "")
    await check_membership(conn, model)
